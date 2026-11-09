"""S/Z 版：desc_img 接管守卫 —— 表情包库绕行 + 3.0 渲染模块接管。

背景（用户审查问题 3）：
  内置「默认表情包」插件（core/plugin/builtin_plugins/sticker，两代框架同构）在用户
  webui 手动添加表情包 / 定时扫描未注册文件时，用**专用 prompt** 调 desc_img 给表情包
  生成描述（运行时 from-import core.utils.common_utils.desc_img ⇒ 必然拿到本插件
  包装后的版本）。若按聊天口径拦截：
    · 缓存命中 ⇒ 写入聊天口径描述（prompt 语义错配）；
    · 已知媒体收口 ⇒ 返回空串 ⇒ 空描述**永久写入**（scan 不重试已注册文件）；
    · wait_for(media_timeout) ⇒ 慢 VLM 静默吞空（原框架无超时）。
  ⇒ 表情包库文件（data/sticker/ 下）必须**完全绕行**守卫（= 原生行为）。

  另：3.0 的 F1 渲染迁至 core/workflow/src/im/message_formatter（顶层 from-import
  desc_img），v2.6.0 及以前的包装清单没有它 ⇒ 3.0 下 F1 无缓存短路/无超时兜底。
  ⇒ 清单必须包含该模块（2.x import 失败自动跳过）。

断言：
  G1 _is_sticker_library_image：sticker 库内 True；库外 path False；非 path False
  G2 表情包调用 ⇒ 直调原始 desc_img（不查索引/不收口/不包超时）
  G3 聊天图片调用 ⇒ 走守卫路径（索引未命中 + 未知 → 原函数被 wait_for 包装调用）
  G4 3.0 渲染模块（core.workflow.src.im.message_formatter）也被包装
  G5 uninstall 还原全部包装（幂等）

Run: python3 tests/test_guard_sticker_bypass.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import os
import sys
import types
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))

results = []

def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label
          + ((" — " + str(detail)) if (detail and not cond) else ""))

def load_mr(plugin_dir: Path):
    """只加载 media_recognize 模块（带出最小同目录依赖）。"""
    for m in ("queue_merge", "media_recognize", "chat_enhance", "jev"):
        sys.modules.pop(m, None)
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location("media_recognize", plugin_dir / "media_recognize.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["media_recognize"] = mod
    spec.loader.exec_module(mod)
    return mod

# 桩 core.utils.path_utils.get_data_path() = /tmp（见 _stub）
STICKER_PATH = "/tmp/sticker/neko.png"
CHAT_PATH = "/tmp/data/chatimg/abc.jpg"

def _img(path, ftype="path"):
    return SimpleNamespace(file=path, file_type=ftype, md5=None, _temp_path=None)

# ---------------------------------------------------------------- G1
def g1_predicate(mr):
    return {
        "G1 sticker 库内判真": mr._is_sticker_library_image(_img(STICKER_PATH)) is True,
        "G1 sticker 库内子目录判真": mr._is_sticker_library_image(_img("/tmp/sticker/sub/a.png")) is True,
        "G1 库外 path 判假": mr._is_sticker_library_image(_img(CHAT_PATH)) is False,
        "G1 相似前缀不误判": mr._is_sticker_library_image(_img("/tmp/sticker_evil/a.png")) is False,
        "G1 非 path 判假": mr._is_sticker_library_image(_img("http://x/sticker/a.png", "url")) is False,
        "G1 base64 判假": mr._is_sticker_library_image(_img("aGVsbG8=", "base64")) is False,
    }

# ---------------------------------------------------------------- G2-G5
class _Rec:
    """最小 recognizer：记录守卫各分支是否被调用。"""
    def __init__(self, index_desc=None, known=False):
        self.media_timeout = 60.0
        self.guard_read_file_wait = 8.0
        self.calls = {"index": 0, "known": 0, "await": 0}
        self._index_desc = index_desc
        self._known = known

    async def _desc_index_lookup(self, image):
        self.calls["index"] += 1
        return self._index_desc

    async def _known_media_check(self, image):
        self.calls["known"] += 1
        return (self._known, "md5x", "phx")

    async def _await_inflight_desc(self, md5, ph, wait_s):
        self.calls["await"] += 1
        return None

def _make_fake_modules():
    """构造三代共存的 desc_img 宿主模块（2.x message_manager / 3.0 message_formatter /
    agent.main / common_utils）。"""
    orig_calls = []

    async def orig_desc_img(*args, **kwargs):
        orig_calls.append((args, kwargs))
        return "原始VLM描述"

    mods = {}
    for name in ("core.message_manager",
                 "core.workflow.src.im.message_formatter",
                 "core.plugin.builtin_plugins.agent.main",
                 "core.utils.common_utils"):
        m = types.ModuleType(name)
        m.desc_img = orig_desc_img
        sys.modules[name] = m
        mods[name] = m
    return mods, orig_calls

def _cleanup_fake_modules(names):
    for n in names:
        sys.modules.pop(n, None)

async def g2_g5(mr):
    mods, orig_calls = _make_fake_modules()
    out = {}
    try:
        rec = _Rec()
        patched = mr.install_desc_img_guard(rec)
        out["G4 包装点数 ≥ 3（含 3.0 message_formatter）"] = patched >= 3
        wrapped_fmt = getattr(mods["core.workflow.src.im.message_formatter"], "desc_img", None)
        out["G4 3.0 message_formatter 已被包装"] = getattr(wrapped_fmt, "_kira_media_guard", False) is True

        cu = mods["core.utils.common_utils"]
        wrapped = cu.desc_img
        # G2 表情包调用：应直调原始（守卫各分支零调用）
        rec.calls = {"index": 0, "known": 0, "await": 0}
        n_before = len(orig_calls)
        r = await wrapped(client="vlm", image=_img(STICKER_PATH), prompt="表情包专用prompt")
        out["G2 表情包调用返回原描述"] = (r == "原始VLM描述")
        out["G2 表情包直调原始函数"] = (len(orig_calls) == n_before + 1)
        out["G2 表情包零守卫分支"] = (rec.calls == {"index": 0, "known": 0, "await": 0})

        # G3 聊天图片/read_file：共享缓存索引可用，但不再检查known或拦截原函数。
        rec.calls = {"index": 0, "known": 0, "await": 0}
        r2 = await wrapped(client="vlm", image=_img(CHAT_PATH), prompt="chat")
        out["G3 聊天图走守卫（索引被查）"] = rec.calls["index"] == 1
        out["G3 read_file不再检查known或返回空占位"] = rec.calls["known"] == 0
        out["G3 聊天图最终调原函数"] = (r2 == "原始VLM描述")

        # G2b 表情包即使「已知媒体」也放行（不收口吞空）
        rec2 = _Rec(known=True)
        mr.uninstall_desc_img_guard()
        mr.install_desc_img_guard(rec2)
        wrapped2 = cu.desc_img
        rec2.calls = {"index": 0, "known": 0, "await": 0}
        r3 = await wrapped2(client="vlm", image=_img(STICKER_PATH), prompt="x")
        out["G2b 已知表情包也不收口（返回原描述）"] = (r3 == "原始VLM描述" and rec2.calls["known"] == 0)
    finally:
        restored = mr.uninstall_desc_img_guard()
        out["G5 uninstall 还原包装"] = restored >= 1 and not getattr(
            mods["core.utils.common_utils"].desc_img, "_kira_media_guard", False)
        _cleanup_fake_modules(list(mods.keys()))
    return out

async def run(plugin_dir: Path):
    mr = load_mr(plugin_dir)
    for label, cond in g1_predicate(mr).items():
        check(label, cond)
    out = await g2_g5(mr)
    for label, cond in out.items():
        check(label, cond)

def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}")
        asyncio.run(run(d))
    failed = [l for l, c in results if not c]
    print("=" * 78)
    if failed:
        print(f"{len(failed)} CHECK(S) FAILED")
        for l in failed:
            print("  FAIL", l)
        sys.exit(1)
    print("ALL TESTS PASSED")

if __name__ == "__main__":
    main()
