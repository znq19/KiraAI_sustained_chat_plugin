"""S/Z 版：媒体预取的**调度时机**（"进批次即识别"，而不是等推批次）。

背景（2026-09-16 用户反馈 + 复现）：
  v2.5.13/v1.8.4 起的"真·预处理（预取池）"在 handle_msg（**先注册**的 im_message
  钩子）里 `schedule_prefetch()` —— 而 `_pir_media` 要等 stage1 `on_media_rec_im`
  （**后注册**）跑完才写入。`create_task` 的预取 worker 会在 stage1 的第一次 await
  （`_cache_get` → 数据库查询 / URL 图片下载，必然让出事件循环）时抢跑 → 读到空媒体表
  → 直接返回（一次性任务，不重试）→ **预取形同虚设**：识别只能等批次被推送时的
  stage2（用户感觉"等推批次才识别"；被其它插件拦截、stage2 不跑的批次更是永远空占位）。

  修法（v2.5.18/v1.8.9）：宿主在 `event.buffer()` 之后只打 `_batch_entered` 标记，
  真正调度挪到 **stage1 末尾**（媒体登记进 `_pir_media` 之后）。

本测试用真实 `media_recognize.py` 驱动：
  A) 修复后的顺序（标记 + stage1）→ 预取真的启动 VLM
  B) 旧顺序（先 schedule 再 stage1）→ 0 次 VLM（反向验证：证明旧代码确实失效）
  C) 「仅唤醒识别」判定不变：非唤醒图（_media_skip）既不识别也不预取
  D) 概率未中 不预取
  E) 没有 _batch_entered 标记的消息（未进批次）不预取
  F) 原生多模态模式：图片不预取（只做音频 STT）

用法: python3 tests/test_prefetch_timing.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Image, Sticker, Text  # noqa: E402
from core.chat.message_utils import KiraMessageEvent  # noqa: E402

SID = "qq:gm:10001"


def load_mr(plugin_dir: Path):
    for m in ("media_recognize",):
        sys.modules.pop(m, None)
    spec = importlib.util.spec_from_file_location(
        f"mr_timing_{plugin_dir.name}", plugin_dir / "media_recognize.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeDB:
    """真实框架的 db 调用是异步 I/O，会让出事件循环（这正是旧顺序失效的原因）。"""
    async def get_image_desc_cache(self, md5):
        await asyncio.sleep(0)          # ← 一次真实的 await 让出
        return None

    async def update_image_desc_cache(self, *a, **k):
        return None


class FakeCfg:
    def __init__(self, mode="vlm_description", lang="zh"):
        self._mode = mode
        self._lang = lang

    def get_config(self, key, default=None):
        if key == "locale.lang":
            return self._lang
        if key == "bot_config.capabilities.image_recognition.mode":
            return self._mode
        return default


class FakeVLM:
    model = SimpleNamespace(model_id="fake-vlm", provider_name="test")

    async def chat(self, req):
        await asyncio.sleep(0)
        return SimpleNamespace(text_response="一只猫")


class FakeProvider:
    def get_default_vlm(self):
        return FakeVLM()


class FakeCtx:
    plugin_mgr = None
    provider_mgr = FakeProvider()
    session_mgr = None

    def __init__(self, mode="vlm_description"):
        self.config = FakeCfg(mode=mode)
        self.db = FakeDB()


def make_msg(with_image=True, mentioned=True, batch=True, skip=False, sticker=False):
    chain = [Text("看这个")]
    if sticker:
        elem = Sticker(sticker_id="s1", sticker="base64://aGVsbG8=", caption=None)
    else:
        elem = Image(image="base64://aGVsbG8=", caption=None)
    if skip:
        elem._media_skip = True
        elem._media_skip_reason = "mention"
    chain.append(elem)
    m = KiraIMMessage(timestamp=0, sender=User("20001", "小明"),
                      group=Group("10001", "测试群"), message_id="m1",
                      self_id="10000", chain=MessageChain(chain), is_mentioned=mentioned)
    if batch:
        m._batch_entered = True
    return m, elem


def make_event(m):
    ev = KiraMessageEvent(message=m, session=Session())
    ev.is_mentioned = getattr(m, "is_mentioned", True)
    return ev


async def settle(n=50):
    for _ in range(n):
        await asyncio.sleep(0)


def spy(mr):
    calls = []
    orig = mr._describe_one

    async def wrapper(sess_sid, media_id, info, results, batch_sem=None):
        calls.append(media_id)
        return await orig(sess_sid, media_id, info, results, batch_sem=batch_sem)

    mr._describe_one = wrapper
    return calls


def make_mr(mod, mode="vlm_description"):
    mr = mod.ParallelMediaRecognizer(FakeCtx(mode=mode), {}, {})
    mr.enabled = True
    mr._pir_active = lambda *a, **k: False
    return mr


# --------------------------------------------------------------- 场景

async def a_fixed_order_starts_prefetch(plugin_dir):
    """修复后的顺序：宿主打标记 → stage1（登记媒体）→ 预取启动。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, _ = make_msg(batch=True)
    await mr.on_im_message(make_event(m))      # stage1：登记 + 末尾调度（标记已由宿主打好）
    await settle()
    return {"stage1 后预取已启动": len(calls) >= 1,
            "媒体已登记": bool(getattr(m, "_pir_media", None))}


async def b_old_order_prefetch_dies(plugin_dir):
    """新保证（v2.6.0 / Fix 3）：**提前**调度的预取也会等后台登记收尾 ——
    「worker 抢跑读到空媒体表」从构造上消除。

    历史（v2.5.13~v2.5.17）：handle_msg 里 schedule 的 worker 在 stage1 第一次
    await 时抢跑，读到空表直接退出（本测试曾据此反向验证竞态，断言 0 次 VLM）。
    v2.5.18 把调度点挪到 stage1 末尾；v2.6.0 登记本身也后台化，worker 统一先等
    _stage1_pending 收尾再读表 —— 任何调用点、任何时序都不可能再读到空表。

    做法同旧反向验证：显式调 `schedule_prefetch`（模拟「登记完成前」的提前调度），
    不给消息打 `_batch_entered`（自动调度不介入，纯测这一条通道）。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, _ = make_msg(batch=False)               # 无标记：只有"旧调用点"这一条调度
    mr.schedule_prefetch(SID, [m])             # ← 登记完成前的提前调度
    await mr.on_im_message(make_event(m))      # stage1 随后才启动后台登记
    await settle()
    return {"提前调度不再读空表（≥1 次 VLM）": len(calls) >= 1,
            "（对照）自动调度未介入": bool(getattr(m, "_batch_entered", False)) is False}


async def c_media_skip_not_prefetched(plugin_dir):
    """「仅唤醒识别」判定不变：非唤醒图（_media_skip）既不识别也不预取。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, elem = make_msg(mentioned=False, skip=True, batch=True)
    await mr.on_im_message(make_event(m))
    await settle()
    ok_stage1 = elem.caption == ""             # 空占位（官方空占位，省 VLM）
    return {"非唤醒图未预取": len(calls) == 0,
            "非唤醒图未暂存": not bool(getattr(m, "_pir_media", None)),
            "非唤醒图空占位": ok_stage1}


async def d_not_in_batch_not_prefetched(plugin_dir):
    """没进批次的消息（无 _batch_entered）不预取（不给不触发 LLM 的消息白烧 VLM）。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, _ = make_msg(batch=False)
    await mr.on_im_message(make_event(m))
    await settle()
    return {"未进批次不预取": len(calls) == 0}


async def e_sticker_and_image_both(plugin_dir):
    """表情包与图片同规则：进了批次就预取。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, _ = make_msg(sticker=True, batch=True)
    await mr.on_im_message(make_event(m))
    await settle()
    return {"表情包进批次即预取": len(calls) >= 1}


async def f_native_mode_no_image_prefetch(plugin_dir):
    """原生多模态模式：图片直传模型，本模块不预取图片（只做音频 STT）。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod, mode="native")
    calls = spy(mr)
    m, _ = make_msg(batch=True)
    await mr.on_im_message(make_event(m))
    await settle()
    return {"native 下图片不预取": len(calls) == 0}


async def g_second_schedule_is_deduped(plugin_dir):
    """重复调度（宿主/兜底双通道）不得重复烧 VLM —— 已有描述或在飞时去重。"""
    mod = load_mr(plugin_dir)
    mr = make_mr(mod)
    calls = spy(mr)
    m, _ = make_msg(batch=True)
    await mr.on_im_message(make_event(m))
    mr.schedule_prefetch(SID, [m])          # 再调度一次（幂等）
    await settle()
    return {"重复调度不重复预取": len(calls) == 1}


SCENARIOS = [
    ("A 修复后顺序：进批次即启动预取", a_fixed_order_starts_prefetch),
    ("B 新保证：提前调度的预取等登记收尾（竞态构造性消除）", b_old_order_prefetch_dies),
    ("C 仅唤醒识别判定不变：_media_skip 不预取", c_media_skip_not_prefetched),
    ("D 未进批次的消息不预取", d_not_in_batch_not_prefetched),
    ("E 表情包与图片同规则", e_sticker_and_image_both),
    ("F native 模式图片不预取", f_native_mode_no_image_prefetch),
    ("G 重复调度去重（不重复烧 VLM）", g_second_schedule_is_deduped),
]


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    failed = 0
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}/media_recognize.py")
        for name, fn in SCENARIOS:
            try:
                r = await fn(d)
                bad = [k for k, v in r.items() if v is False]
                if bad:
                    failed += 1
                    print(f"  ✗ FAIL   {name}")
                    print(f"            {r}")
                else:
                    print(f"  ✓ PASS   {name}")
                    print(f"            {r}")
            except Exception as e:
                failed += 1
                print(f"  ERR       {name}: {type(e).__name__}: {e}")
    print("=" * 78)
    print("ALL TESTS PASSED" if failed == 0 else f"{failed} CHECK(S) FAILED")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
