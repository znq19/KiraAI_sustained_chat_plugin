"""S/Z：预取作用域（"哪些消息才该预取"）—— v2.5.19 / v1.8.10。

用户观察（真实日志）：**没进批次的围观图也在预取**（白烧 VLM）。
根因：v2.5.13 的调用点把"已进缓冲"当成"会被送进 LLM"，对**前文阶段**也打了标记；
竞态修复后调用点活了 → 前文图开始预取。

本套件用**真实 handle_msg + stage1** 驱动（假 ctx 只提供 buffer/config/VLM 桩），断言：
  P1 前文阶段（无批次）的非唤醒图 → **不打标记、不预取**（用户报的场景）
  P2 唤醒起批 → 本条预取 ✓ **且前文图被"起批预热"一起识别**（会随本批送，不浪费）
  P3 批次进行中到达的非唤醒图 → 打标记 → 预取 ✓
  P4 主动回复命中（本条自己会被 flush 推出）→ 打标记 → 预取 ✓
  P5 对照：「仅唤醒识别」开启时，非唤醒图无论何时都不预取（跳过标记语义不变）

Run: python3 tests/test_prefetch_scope.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Image, Text  # noqa: E402

SID = "qq:gm:427674145"
results = []


def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label + ((" — " + str(detail)) if (detail and not cond) else ""))


# ------------------------------------------------------------------ 假环境

class FakeDB:
    async def get_image_desc_cache(self, md5):
        await asyncio.sleep(0)
        return None

    async def set_image_desc_cache(self, *a, **k):
        return None

    async def update_image_desc_cache(self, *a, **k):
        return None


class FakeVLM:
    model = type("M", (), {"model_id": "fake-vlm", "provider_name": "test"})()

    async def chat(self, req):
        await asyncio.sleep(0)
        return type("R", (), {"text_response": "一只猫"})()


class Cfg:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get_config(self, key, default=None):
        return self.values.get(key, default)

    def __getitem__(self, key):
        if key == "bot_config":
            return {"agent": {"max_tool_loop": 2, "tool_call_timeout": 60},
                    "bot": {"max_buffer_messages": 5, "max_message_interval": 30}}
        return {}


class Buf:
    def __init__(self):
        self.buffer = []
        self.lock = asyncio.Lock()

    def get_length(self):
        return len(self.buffer)

    def pop(self, count=1):
        for _ in range(count):
            if self.buffer:
                self.buffer.pop(0)

    def flush(self):
        out = list(self.buffer)
        self.buffer.clear()
        return out


class Processor:
    def __init__(self, ctx):
        self.ctx = ctx

    def get_session_buffer_length(self, sid):
        return self.ctx.get_buffer(sid).get_length()


class Ctx:
    plugin_mgr = None
    session_mgr = None

    def __init__(self, cfg_values=None):
        self.config = Cfg(cfg_values)
        self.db = FakeDB()
        self.provider_mgr = type("P", (), {"get_default_vlm": lambda s: FakeVLM()})()
        self.message_processor = Processor(self)
        self.buffers = {}

    def get_buffer(self, sid):
        return self.buffers.setdefault(sid, Buf())

    def get_default_llm_client(self):
        return None

    def get_timezone(self):
        return None

    def get_plugin_inst(self, pid):
        return None


class SEvent:
    def __init__(self, message, session, mentioned=True):
        self.message = message
        self.session = session
        self.adapter = type("A", (), {"name": "qq", "platform": "qq"})()
        self.message_types = []
        self.is_mentioned = mentioned
        self.is_notice = False
        self._strategy = "discard"

    @property
    def sid(self):
        return self.session.sid

    @property
    def process_strategy(self):
        return self._strategy

    def buffer(self, force=False):
        self._strategy = "buffer"

    def flush(self, force=False):
        self._strategy = "flush"

    def discard(self, force=False):
        self._strategy = "discard"

    def is_group_message(self):
        return True


class RealImage(Image):
    """桩 Image 的 hash_image 固定；这里让每张图有自己的 md5，便于断言"识别了哪张"。"""

    def __init__(self, md5, caption=None):
        super().__init__(image=f"base64://{md5}", caption=caption)
        self._md5 = md5

    async def hash_image(self):
        return self._md5

    async def to_data_url(self):
        return "data:image/jpeg;base64,aGVsbG8="


class Shim:
    def __init__(self, m):
        self.message = m
        self.message_types = []
        self.adapter = None
        self.session = None

    def is_group_message(self):
        return True


def make_msg(mid, text="", md5=None, mentioned=False):
    chain = [Text(text)] if text else []
    img = None
    if md5:
        img = RealImage(md5, caption=None)
        chain.append(img)
    m = KiraIMMessage(timestamp=time.time(), sender=User("1", "小明"),
                      group=Group("427674145", "测试群"), message_id=str(mid),
                      self_id="10000", chain=MessageChain(chain), is_mentioned=mentioned)
    m.message_str = None
    return m, img


def load_plugin(plugin_dir: Path):
    for m in ("queue_merge", "media_recognize", "chat_enhance"):
        sys.modules.pop(m, None)
    spec = importlib.util.spec_from_file_location(f"plug_scope_{plugin_dir.name}",
                                                  plugin_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def spy_describe(plug):
    seen = []
    orig = plug.media_recognizer._describe_one

    async def wrapper(sess_sid, media_id, info, res, batch_sem=None):
        seen.append(media_id)
        return await orig(sess_sid, media_id, info, res, batch_sem=batch_sem)

    plug.media_recognizer._describe_one = wrapper
    return seen


async def drive(plug, ctx, m, mentioned):
    """按框架真实顺序驱动（v2.6.0 修正，对齐 core/message_manager.py）：
    handle_msg(HIGH,先) → stage1(HIGH,后) → 框架 buffer.add → ON_MESSAGE_BUFFERED。

    ⚠ 旧版 drive 把 buffer.add 插在 handle_msg 与 stage1 之间，那不是框架真实顺序
    （框架先跑完全部 ON_IM_MESSAGE 钩子才 buffer.add）；且 v2.6.0（Fix 1）起批次
    确立/顺延武装在 on_buffered，必须调用它才算完整驱动一轮。"""
    session = Session(adapter_name="qq", session_type="gm", session_id="427674145")
    ev = SEvent(m, session, mentioned=mentioned)
    await plug.handle_msg(ev)
    await plug.media_recognizer.on_im_message(ev)
    if ev.process_strategy == "buffer":
        ctx.get_buffer(SID).buffer.append(Shim(m))
        await plug.on_buffered(SID)
    for _ in range(30):
        await asyncio.sleep(0.02)
    return ev


async def shutdown(plug):
    for name in ("shutdown", "terminate"):
        fn = getattr(plug, name, None)
        if callable(fn):
            try:
                await fn()
            except Exception:
                pass
            return


# ------------------------------------------------------------------ 场景

async def p1_context_no_prefetch(plugin_dir):
    """前文阶段（无批次）的非唤醒图 → 不打标记、不预取。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = mod.DebouncePlugin(ctx, {
        "section_basic": {"receive_unmentioned": True},
        "section_media": {"image_recognition_only_on_mention": False},
    })
    seen = spy_describe(plug)

    m, img = make_msg(1, "看这张", md5="p1img")
    await drive(plug, ctx, m, mentioned=False)
    r = {"P1 前文图未打预取标记": getattr(m, "_batch_entered", False) is False,
         "P1 前文图未被预取（0 次 VLM）": len(seen) == 0,
         "P1 消息仍在缓冲里（作前文）": ctx.get_buffer(SID).get_length() >= 1}
    await shutdown(plug)
    return r


async def p2_wake_warms_context_and_self(plugin_dir):
    """唤醒起批 → 本条预取 + 前文图被"起批预热"一起识别。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = mod.DebouncePlugin(ctx, {
        "section_basic": {"receive_unmentioned": True},
        "section_media": {"image_recognition_only_on_mention": False},
    })
    seen = spy_describe(plug)

    m1, _ = make_msg(1, "前文的图", md5="ctximg")
    await drive(plug, ctx, m1, mentioned=False)          # 前文（不预取）
    m2, _ = make_msg(2, "在吗", md5="wakeimg")
    await drive(plug, ctx, m2, mentioned=True)           # 唤醒 → 起批
    r = {"P2 唤醒消息打了标记": getattr(m2, "_batch_entered", False) is True,
         "P2 唤醒消息被预取": "wakeimg" in seen,
         "P2 前文图被起批预热一起识别": "ctximg" in seen,
         "P2 批次已开启": plug.batch_started.get(SID) is True,
         "细节": f"seen={seen}"}
    await shutdown(plug)
    return r


async def p3_in_batch_unmentioned_prefetched(plugin_dir):
    """批次进行中到达的非唤醒图 → 打标记 → 预取（本来就要进 LLM）。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = mod.DebouncePlugin(ctx, {
        "section_basic": {"receive_unmentioned": True},
        "section_media": {"image_recognition_only_on_mention": False},
    })
    seen = spy_describe(plug)

    m1, _ = make_msg(1, "在吗", md5="wake2img")
    await drive(plug, ctx, m1, mentioned=True)           # 起批
    seen.clear()
    m2, _ = make_msg(2, "又一张", md5="batinimg")
    await drive(plug, ctx, m2, mentioned=False)          # 批次内的非唤醒图
    r = {"P3 批次内非唤醒图打了标记": getattr(m2, "_batch_entered", False) is True,
         "P3 该图被预取": "batinimg" in seen,
         "细节": f"seen={seen}"}
    await shutdown(plug)
    return r


async def p4_proactive_flush_prefetched(plugin_dir):
    """主动回复命中（本条会被 flush 推出）→ 补标记 → 预取。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx({"proactive_score_gate_deny": False, "proactive_score_gate_boost": False})
    plug = mod.DebouncePlugin(ctx, {"section_basic": {
        "receive_unmentioned": True,
        "group_proactive_chat": True,
        "group_proactive_chat_probability": 1.0,
        "proactive_score_gate_deny": False,
        "proactive_score_gate_boost": False,
    }})
    plug.image_recognition_only_on_mention = False
    plug.group_proactive_chat = True
    plug.group_proactive_chat_probability = 1.0
    plug.proactive_score_gate_deny = False
    plug.proactive_score_gate_boost = False
    seen = spy_describe(plug)

    m, _ = make_msg(1, "围观图", md5="proaimg")
    ev = await drive(plug, ctx, m, mentioned=False)
    r = {"P4 主动命中被推出": ev.process_strategy == "flush",
         "P4 打出预取标记": getattr(m, "_batch_entered", False) is True,
         "P4 被预取": "proaimg" in seen,
         "细节": f"strategy={ev.process_strategy} seen={seen}"}
    await shutdown(plug)
    return r


async def p5_only_on_mention_respected(plugin_dir):
    """对照：「仅唤醒识别」开启时，非唤醒图任何时候都不预取（跳过语义不变）。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = mod.DebouncePlugin(ctx, {"section_basic": {"receive_unmentioned": True},
                                    "section_media": {
        "image_recognition_only_on_mention": True,
        "image_recognition_probability": 1.0,
    }})
    plug.image_recognition_only_on_mention = True
    seen = spy_describe(plug)

    m1, img1 = make_msg(1, "在吗", md5="w3img")
    await drive(plug, ctx, m1, mentioned=True)           # 起批
    seen.clear()
    m2, img2 = make_msg(2, "围观图", md5="skipimg")
    await drive(plug, ctx, m2, mentioned=False)
    r = {"P5 非唤醒图被打跳过标记": getattr(img2, "_media_skip", False) is True,
         "P5 非唤醒图未预取": "skipimg" not in seen,
         "细节": f"seen={seen} skip={getattr(img2, '_media_skip', None)}"}
    await shutdown(plug)
    return r


SCENARIOS = [
    ("P1 前文图不预取（用户报的场景）", p1_context_no_prefetch),
    ("P2 起批预热：本条预取 + 前文图一起识别", p2_wake_warms_context_and_self),
    ("P3 批次内非唤醒图仍预取", p3_in_batch_unmentioned_prefetched),
    ("P4 主动命中被推出时补标记并预取", p4_proactive_flush_prefetched),
    ("P5 对照：仅唤醒识别语义不变", p5_only_on_mention_respected),
]


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}")
        for name, fn in SCENARIOS:
            try:
                r = await fn(d)
                bad = [k for k in r if k != "细节" and r[k] is False]
                results.append((name, not bad))
                if bad:
                    print(f"  FAIL  {name}")
                    print(f"        {r}")
                else:
                    print(f"  PASS  {name}")
                    if "细节" in r:
                        print(f"        {r['细节']}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"  ERR   {name}: {type(e).__name__}: {e}")
                results.append((name, False))
    print()
    passed = sum(1 for _, ok in results if ok)
    print("TOTAL %d/%d passed" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
