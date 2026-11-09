"""S/Z 版：guard 已知媒体收口 + 预取占槽饥饿防护 回归测试。

背景（用户日志实证）：
  框架 agent 的 read_file 读图片走 _describe_image_file 直接调 desc_img（不看 caption）。
  插件渲染给 LLM 的占位文本带 file_path，LLM 循路径补读 → 旧 guard 在插件索引未命中时
  放行原函数 → 框架 VLM 付费识别一次、插件 stage3 又识别一次，**双重付费**。此外预取
  信号量被 60s 超时的在途项长时间占槽，新预取排队 60s+ 才开始（占槽饥饿）。

修复（S v2.5.21 / Z v1.8.12）：
  ① guard 收口：插件管线见过的媒体指纹（md5 + dHash）登记在案；read_file 补读已知
     媒体时拦截（在途识别可短等拿真描述，等不到返回空占位），绝不再触发第二次付费 VLM；
     用户配置「不识别」的媒体不登记不拦截（尊重省 VLM 意图）；未见过的工作区文件放行
     （read_file 能力不变）。
  ② 预取独立超时 vlm_prefetch_timeout（默认 30s）+ 在途队列上限
     vlm_prefetch_max_queue（默认 16），防占槽饥饿。

断言：
  T1 已知 md5：登记后 guard 收到同 md5 图片 → 返回 ""，原 desc_img 未被调用
  T2 已知 phash：登记原图 dHash 后，guard 收到同图重压缩（q40）副本 → 返回 ""，原函数未调用
  T3 未知图片：guard 放行原函数（桩被调用 1 次，返回其描述）
  T4 在途短等：在途预取任务 0.2s 后写缓存，guard_read_file_wait=2 → guard 拿到真描述
  T5 预取队列上限：_pf_tasks 塞满后 _prefetch_worker 不起新任务，跳过项 _done 仍为 False
  T6 预取超时生效：vlm_prefetch_timeout=5 / media_timeout=60 → 预取分支 ~5s 归类 (识别超时)

用法: python3 tests/test_guard_known_media.py [<plugin_dir> ...]
"""
import asyncio
import hashlib
import importlib.util
import sys
import time
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"                     # 仓库内自带的框架桩
sys.path.insert(0, str(STUB))

import core.utils.common_utils as common_utils  # noqa: E402  (guard 包装的挂载点)

SID = "qq:gm:10001"


def load_mr(plugin_dir: Path):
    sys.modules.pop("media_recognize", None)
    spec = importlib.util.spec_from_file_location(
        f"mr_guard_{plugin_dir.name}", plugin_dir / "media_recognize.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeDB:
    """内存版 image_desc_cache：guard 在途短等需要真的写/读缓存。"""

    def __init__(self):
        self.store = {}

    async def get_image_desc_cache(self, md5):
        await asyncio.sleep(0)
        v = self.store.get(md5)
        return {"description": v} if v else None

    async def update_image_desc_cache(self, md5, **kw):
        if md5 in self.store:
            self.store[md5] = kw.get("description", self.store[md5])
            return True
        return False

    async def add_image_desc_cache(self, md5, text, count=1, last_seen=0):
        self.store.setdefault(md5, text)


class FakeCfg:
    def get_config(self, key, default=None):
        if key == "locale.lang":
            return "zh"
        if key == "bot_config.capabilities.image_recognition.mode":
            return "vlm_description"
        return default


class FakeCtx:
    plugin_mgr = None
    provider_mgr = None
    session_mgr = None

    def __init__(self):
        self.config = FakeCfg()
        self.db = FakeDB()


def make_mr(mod, **sec_over):
    sec = {"media_timeout": 60.0}
    sec.update(sec_over)
    mr = mod.ParallelMediaRecognizer(FakeCtx(), {"section_media_recognition": sec}, {})
    mr.enabled = True
    mr._pir_active = lambda *a, **k: False
    return mr


def jpeg_bytes(seed: int, quality: int) -> bytes:
    """确定性渐变图（dHash 对重压缩稳定，实测 q40 与原图 hamming=0）。"""
    from PIL import Image as PILImage
    im = PILImage.new("RGB", (240, 240))
    px = im.load()
    for y in range(240):
        for x in range(240):
            px[x, y] = ((x + seed) % 256, (y + seed * 3) % 256, (x + y + seed * 7) % 256)
    buf = BytesIO()
    im.save(buf, format="JPEG", quality=quality)
    return buf.getvalue()


def img_obj(md5=None, path=None):
    """guard 视角的图片对象（属性对齐 _desc_index_lookup/_known_media_check 取数）。"""
    return SimpleNamespace(md5=md5, _temp_path=None,
                           file_type=("path" if path else ""), file=path)


def install_guard(mod, mr):
    """把计数桩挂到 stub 的 common_utils.desc_img 上，再走真实 install 包装。"""
    calls = {"n": 0}

    async def fake_desc_img(*a, **k):
        calls["n"] += 1
        await asyncio.sleep(0)
        return "框架VLM描述"

    common_utils.desc_img = fake_desc_img
    mod.install_desc_img_guard(mr)
    guarded = common_utils.desc_img
    return guarded, calls


def uninstall_guard(mod):
    mod.uninstall_desc_img_guard()
    try:
        del common_utils.desc_img
    except AttributeError:
        pass


# --------------------------------------------------------------- 场景

async def t1_known_md5_blocked(plugin_dir):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod)
    guarded, calls = install_guard(mod, mr)
    try:
        md5 = hashlib.md5(jpeg_bytes(1, 90)).hexdigest()
        r = await guarded(image=img_obj(md5=md5))
        return {"known媒体不再返回空占位": r == "框架VLM描述",
                "正常调用原识别函数": calls["n"] == 1}
    finally:
        uninstall_guard(mod)


async def t2_known_phash_blocked(plugin_dir, tmp):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod)
    guarded, calls = install_guard(mod, mr)
    try:
        orig = jpeg_bytes(2, 90)
        ph = mr._phash_of_bytes(orig)
        assert ph, "测试图 dHash 退化"
        recompressed = jpeg_bytes(2, 40)     # 同图重压缩：字节/md5 不同，画面相同
        assert hashlib.md5(recompressed).hexdigest() != hashlib.md5(orig).hexdigest()
        p = tmp / "recompressed.jpg"
        p.write_bytes(recompressed)
        r = await guarded(image=img_obj(path=str(p)))
        return {"同图副本正常返回真结果": r == "框架VLM描述",
                "不再拦截原 desc_img": calls["n"] == 1}
    finally:
        uninstall_guard(mod)


async def t3_unknown_passthrough(plugin_dir, tmp):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod)
    guarded, calls = install_guard(mod, mr)
    try:
        p = tmp / "unknown.jpg"
        p.write_bytes(jpeg_bytes(3, 90))     # 从未登记的图
        r = await guarded(image=img_obj(path=str(p)))
        return {"未知图片放行原函数": calls["n"] == 1,
                "返回原函数描述": r == "框架VLM描述"}
    finally:
        uninstall_guard(mod)


async def t4_inflight_short_wait(plugin_dir):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod)
    guarded, calls = install_guard(mod, mr)
    try:
        md5 = hashlib.md5(jpeg_bytes(4, 90)).hexdigest()
        async def slow_recognize():
            await asyncio.sleep(0.2)
            await mr._cache_set(md5, "在途识别出的描述")
            return "在途识别出的描述"
        mr._pf_tasks[md5[:8]] = asyncio.ensure_future(slow_recognize())
        r = await guarded(image=img_obj(md5=md5))
        return {"在途媒体也正常返回read_file真结果": r == "框架VLM描述",
                "不再用空占位拦截原函数": calls["n"] == 1}
    finally:
        uninstall_guard(mod)


async def t5_prefetch_queue_cap(plugin_dir):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod, vlm_prefetch_max_queue=2)
    # 塞满在途队列（两个长时间任务占住名额）
    t_a = asyncio.ensure_future(asyncio.sleep(30))
    t_b = asyncio.ensure_future(asyncio.sleep(30))
    mr._pf_tasks["aa"] = t_a
    mr._pf_tasks["bb"] = t_b
    try:
        info1 = {"md5": None, "elem": img_obj(), "type": "Image", "_done": False}
        info2 = {"md5": None, "elem": img_obj(), "type": "Image", "_done": False}
        msg = SimpleNamespace(_pir_media={"cc": info1, "dd": info2})
        await mr._prefetch_worker(SID, [msg])
        return {"队列满后未起新任务": "cc" not in mr._pf_tasks and "dd" not in mr._pf_tasks,
                "原有在途任务不受影响": mr._pf_tasks.get("aa") is t_a and mr._pf_tasks.get("bb") is t_b,
                "跳过项 _done 仍为 False（stage2 可接力）": not info1["_done"] and not info2["_done"]}
    finally:
        t_a.cancel()
        t_b.cancel()


async def t6_prefetch_timeout(plugin_dir):
    mod = load_mr(plugin_dir)
    mod._PHASH_INDEX.clear()
    mr = make_mr(mod, media_timeout=60.0, vlm_prefetch_timeout=5.0)
    assert mr.vlm_prefetch_timeout == 5.0 and mr.media_timeout == 60.0

    async def slow_describe(*a, **k):        # 慢识别桩：60s 预算下必然超预取的 5s
        await asyncio.sleep(30)
        return "慢描述"

    mr._describe_image = slow_describe
    info = {"md5": None, "elem": img_obj(), "type": "Image", "_done": False,
            "_prefetch": True, "_mr_source": "prefetch"}
    results = {}
    t0 = time.monotonic()
    await mr._describe_one(SID, "mid_t6", info, results)
    dt = time.monotonic() - t0
    return {"预取按独立超时归类 (识别超时)": results.get("mid_t6") == "(识别超时)",
            "实际生效 ~5s 而非 60s": dt <= 6.5,
            "媒体已标记已处理": info["_done"] is True}


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    all_ok = True
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}")
        tmp = Path(f"/tmp/guard_known_test_{d.name}")
        tmp.mkdir(parents=True, exist_ok=True)
        cases = [
            ("T1 已知 md5 → 正常返回真实识别结果", await t1_known_md5_blocked(d)),
            ("T2 同图重压缩 → 不再返回空占位", await t2_known_phash_blocked(d, tmp)),
            ("T3 未知图片 → 放行原函数", await t3_unknown_passthrough(d, tmp)),
            ("T4 在途媒体 → read_file正常返回结果", await t4_inflight_short_wait(d)),
            ("T5 预取队列上限（跳过不置 _done）", await t5_prefetch_queue_cap(d)),
            ("T6 预取独立超时生效", await t6_prefetch_timeout(d)),
        ]
        for name, checks in cases:
            ok = all(checks.values())
            all_ok &= ok
            print(f"  {'✓' if ok else '✗'} {name}: {checks}")
    print("=" * 78)
    print("ALL TESTS PASSED" if all_ok else "SOME TESTS FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
