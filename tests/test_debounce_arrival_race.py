"""S/Z：「唤醒消息慢于顺延窗口落地 → 回复卡住等下一批」根因修复回归 —— v2.6.0 / v1.9.0。

事故（2026-10-02 用户日志）：用户引用 bot 自己发的图片，bot 5 分 16 秒不回，
该消息被压到下一次 @ 才一起进 LLM。

根因链（详见 FIX_PLAN.md）：
  框架 handle_im_message **先跑完全部 ON_IM_MESSAGE 钩子**（含 stage1 媒体预处理，
  引用图为 URL 型需两次下载，实测 ~8s）**才把消息 buffer.add**；旧版在 handle_msg
  （钩子链中途）就确立批次并启动顺延 → 顺延到点时缓冲是空的 →
  `buffer_len == 0` 分支把还没落地的批次误判为「已被外部消费」清掉 →
  唤醒消息沦为孤儿前文，永远没人 flush。

修复（两版）：
  Fix 1  批次确立/顺延武装移到 @on.message_buffered（消息真正进缓冲的时刻）；
  Fix 2  URL 型媒体全程只下载一次（先落盘再从字节算 md5）；
  Fix 3  stage1 重活全后台化（ON_IM_MESSAGE 零 I/O），stage2/stage3/预取等登记收尾。

用例：
  R1  核心复现：stage1 慢于窗口，唤醒消息**仍被 flush**（不需要下一次唤醒救场）
  R1b 反向验证：按**旧版**时点武装顺延（消息未落地就启动）→ 复现「永远没人 flush」
  R2  S 版：静默轮 _stop_sustain_round 交叠时，真实唤醒仍 flush；
      R2b 停窗后迟到的持续命中消息**不**确立批次（留作前文，不回一轮）
  R3  正常路径回归：快速唤醒按窗口 flush / 批次内消息落地重置窗口 / 纯围观永不 flush
  R4  异步 stage1 契约：语音**同步**占位替换（防框架自动 STT）+ ON_IM_MESSAGE 零阻塞
  R5  Fix 2：URL 型图片全程只下载一次（persist），hash_image 不再被调

Run: python3 tests/test_debounce_arrival_race.py [<plugin_dir> ...]
"""
import asyncio
import base64
import hashlib
import importlib.util
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Image, Record, Text  # noqa: E402

SID = "qq:gm:427674145"
WINDOW = 0.3          # 测试用顺延窗口（远小于真实 8s，加速测试）
results = []


def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label + ((" — " + str(detail)) if (detail and not cond) else ""))


# ------------------------------------------------------------------ 假环境

class FakeDB:
    def __init__(self):
        self.store = {}

    async def get_image_desc_cache(self, md5):
        await asyncio.sleep(0)          # 真实 await 让出（模拟框架 DB 是异步 I/O）
        row = self.store.get(md5)
        return {"description": row} if row else None

    async def update_image_desc_cache(self, md5, **kw):
        self.store[md5] = kw.get("description", self.store.get(md5, ""))
        return True

    async def add_image_desc_cache(self, md5, text, **kw):
        self.store[md5] = text
        return True


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
    """记录每次 flush（时刻 + 消息列表），供断言。"""

    def __init__(self, ctx):
        self.ctx = ctx
        self.flushes = []          # [(ts, [KiraIMMessage, ...])]

    def get_session_buffer_length(self, sid):
        return self.ctx.get_buffer(sid).get_length()

    async def flush_session_messages(self, sid, extra_event=None):
        buf = self.ctx.get_buffer(sid)
        items = list(buf.buffer)
        buf.buffer.clear()
        msgs = [getattr(i, "message", i) for i in items]
        if extra_event is not None:
            msgs.append(extra_event.message)
        self.flushes.append((time.monotonic(), msgs))
        return bool(items)


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
    def process_strategy(self):
        return self._strategy

    def buffer(self, force=False):
        self._strategy = "buffer"

    def flush(self, force=False):
        self._strategy = "flush"

    def discard(self, force=False):
        self._strategy = "discard"

    def trigger(self, force=False):
        self._strategy = "trigger"

    def is_group_message(self):
        return True


class Shim:
    """与框架 buffer 里的元素同形（.message），对齐 queue_merge.BufferedMsgShim。"""

    def __init__(self, m):
        self.message = m
        self.message_types = []
        self.adapter = None
        self.session = None

    def is_group_message(self):
        return True


class UrlImage(Image):
    """URL 型图片：记录 to_base64/hash_image 调用次数（Fix 2/R5 用）。"""

    def __init__(self, payload=b"imgbytes", slow=0.0):
        super().__init__(image="https://example.com/pic.jpg", caption=None)
        self.file_type = "url"
        self._payload = payload
        self._slow = slow
        self.n_b64 = 0
        self.n_hash = 0

    async def to_base64(self):
        self.n_b64 += 1
        if self._slow:
            await asyncio.sleep(self._slow)
        return base64.b64encode(self._payload).decode()

    async def hash_image(self):
        self.n_hash += 1
        return hashlib.md5(self._payload).hexdigest()


class FakeRecord(Record):
    """base64 型语音（QQ 适配器形态）：to_base64 立即可得。"""

    def __init__(self, payload=b"fakeaudio"):
        super().__init__(record="base64://x", caption=None)
        self.file_type = "base64"
        self._payload = payload

    async def to_base64(self):
        return base64.b64encode(self._payload).decode()


def make_msg(mid, text="", mentioned=False, chain=None):
    m = KiraIMMessage(timestamp=time.time(), sender=User("2361", "小怪兽"),
                      group=Group("427674145", "测试群"), message_id=str(mid),
                      self_id="3436698519", chain=MessageChain(chain or [Text(text)]),
                      is_mentioned=mentioned)
    m.message_str = None
    return m


def load_plugin(plugin_dir: Path):
    for m in ("queue_merge", "media_recognize", "chat_enhance"):
        sys.modules.pop(m, None)
    spec = importlib.util.spec_from_file_location(f"plug_race_{plugin_dir.name}",
                                                  plugin_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def make_plugin(mod, ctx):
    return mod.DebouncePlugin(ctx, {
        "section_basic": {"receive_unmentioned": True, "merge_window_seconds": WINDOW},
    })


async def settle(sec):
    await asyncio.sleep(sec)


async def shutdown(plug):
    for name in ("shutdown", "terminate"):
        fn = getattr(plug, name, None)
        if callable(fn):
            try:
                await fn()
            except Exception:
                pass
            return


def flushed_msgs(ctx):
    return [m for _, msgs in ctx.message_processor.flushes for m in msgs]


def session():
    return Session(adapter_name="qq", session_type="gm", session_id="427674145")


# ------------------------------------------------------------------ 场景

async def r1_slow_stage1_wake_still_flushed(plugin_dir):
    """R1 核心复现：stage1 慢于顺延窗口，唤醒消息仍被 flush（Fix 1）。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    m = make_msg(1, "@bot 亲亲", mentioned=True)
    ev = SEvent(m, session(), mentioned=True)
    await plug.handle_msg(ev)
    # 模拟 stage1 慢处理（0.5s > 窗口 0.3s）——这期间框架还没 buffer.add
    await settle(WINDOW + 0.2)
    ctx.get_buffer(SID).buffer.append(Shim(m))
    await plug.on_buffered(SID)                  # 框架派发 ON_MESSAGE_BUFFERED
    await settle(WINDOW * 3)
    ok_flush = m in flushed_msgs(ctx)
    check("R1 唤醒消息最终被 flush（不被压到下一批）", ok_flush,
          f"flushes={len(ctx.message_processor.flushes)}")
    check("R1 只 flush 了一次", len(ctx.message_processor.flushes) == 1,
          f"flushes={len(ctx.message_processor.flushes)}")
    check("R1 flush 后批次状态已清", plug.batch_started.get(SID, False) is False)
    await shutdown(plug)


async def r1b_old_arming_orphans_wake(plugin_dir):
    """R1b 反向验证：按**旧版**时点武装顺延（handle_msg 时点、消息未落地）→
    窗口到点看到空缓冲 → 批次被清 → 唤醒消息永远没人 flush（事故复现）。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    m = make_msg(1, "@bot 亲亲", mentioned=True)
    ev = SEvent(m, session(), mentioned=True)
    await plug.handle_msg(ev)
    # —— 手动复刻旧版 handle_msg 的收尾（v2.5.22/v1.8.12 的真实行为）——
    plug.batch_started[SID] = True
    plug.batch_count[SID] = 1
    plug.session_events[SID] = asyncio.Event()
    plug.session_tasks[SID] = asyncio.create_task(plug._debounce_loop(SID))
    plug.session_events[SID].set()
    # stage1 慢处理：窗口到点时缓冲仍为空 → buffer_len==0 分支清批次
    await settle(WINDOW + 0.2)
    check("R1b 旧时点：窗口到点后批次状态已被误清",
          plug.batch_started.get(SID, False) is False)
    # 消息此刻才落地（旧版没有 on_buffered 确立）
    ctx.get_buffer(SID).buffer.append(Shim(m))
    await settle(WINDOW * 3)
    check("R1b 旧时点：唤醒消息永远没人 flush（事故复现）",
          len(ctx.message_processor.flushes) == 0,
          f"flushes={len(ctx.message_processor.flushes)}")
    check("R1b 旧时点：消息仍滞留缓冲（沦为孤儿前文）",
          ctx.get_buffer(SID).get_length() == 1)
    await shutdown(plug)


async def r2_sustain_stop_real_wake_survives(plugin_dir):
    """R2（仅 S 版）：静默轮停窗与慢 stage1 交叠时，真实唤醒仍 flush。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    if not hasattr(plug, "sustain_stopped"):
        check("R2 非 S 版（无 sustain），跳过", True)
        return
    m = make_msg(1, "@bot 在吗", mentioned=True)
    ev = SEvent(m, session(), mentioned=True)
    await plug.handle_msg(ev)
    # 上一轮静默收尾：停窗（pop 批次状态 + sustain_stopped=True）
    await plug._stop_sustain_round(SID)
    await settle(WINDOW + 0.2)                   # stage1 慢
    ctx.get_buffer(SID).buffer.append(Shim(m))
    await plug.on_buffered(SID)
    await settle(WINDOW * 3)
    check("R2 停窗交叠：真实唤醒仍被 flush", m in flushed_msgs(ctx),
          f"flushes={len(ctx.message_processor.flushes)}")
    await shutdown(plug)


async def r2b_sustain_hit_late_no_batch(plugin_dir):
    """R2b（仅 S 版）：停窗后迟到的持续命中消息不确立批次（留作前文，不回一轮）。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    if not hasattr(plug, "sustain_stopped"):
        check("R2b 非 S 版（无 sustain），跳过", True)
        return
    m = make_msg(77, "持续命中的消息", mentioned=True)
    ev = SEvent(m, session(), mentioned=True)
    await plug.handle_msg(ev)
    # 该消息是持续命中；随后整轮被终止
    plug.sustain_hit_ids.setdefault(SID, set()).add(m.message_id)
    plug.sustain_stopped[SID] = True
    ctx.get_buffer(SID).buffer.append(Shim(m))
    await plug.on_buffered(SID)
    await settle(WINDOW * 3)
    check("R2b 迟到的持续命中不确立批次", plug.batch_started.get(SID, False) is False)
    check("R2b 迟到的持续命中不被 flush", len(ctx.message_processor.flushes) == 0)
    check("R2b 消息留作前文（内容不丢）", ctx.get_buffer(SID).get_length() == 1)
    await shutdown(plug)


async def r3_normal_flow_regression(plugin_dir):
    """R3 正常路径回归：快速唤醒按窗口 flush；批次内消息落地重置窗口；纯围观不 flush。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)

    # R3a：快速唤醒 → 一个窗口后 flush
    m1 = make_msg(1, "@bot 早", mentioned=True)
    ev = SEvent(m1, session(), mentioned=True)
    t0 = time.monotonic()
    await plug.handle_msg(ev)
    ctx.get_buffer(SID).buffer.append(Shim(m1))
    await plug.on_buffered(SID)
    await settle(WINDOW + 0.25)
    check("R3a 快速唤醒按窗口 flush", m1 in flushed_msgs(ctx),
          f"flushes={len(ctx.message_processor.flushes)}")
    if ctx.message_processor.flushes:
        dt = ctx.message_processor.flushes[0][0] - t0
        check("R3a flush 时刻≈一个窗口后", dt >= WINDOW - 0.05, f"dt={dt:.2f}")

    # R3b：批次内消息落地重置窗口（flush 以**它落地**起算）
    ctx.message_processor.flushes.clear()
    m2 = make_msg(2, "@bot 又在吗", mentioned=True)
    ev = SEvent(m2, session(), mentioned=True)
    await plug.handle_msg(ev)
    ctx.get_buffer(SID).buffer.append(Shim(m2))
    await plug.on_buffered(SID)
    await settle(WINDOW / 2)
    m3 = make_msg(3, "跟一句", mentioned=False)      # 批次内非唤醒消息
    ev3 = SEvent(m3, session(), mentioned=False)
    await plug.handle_msg(ev3)
    t_member = time.monotonic()
    ctx.get_buffer(SID).buffer.append(Shim(m3))
    await plug.on_buffered(SID)                       # 落地 → 重置窗口
    await settle(WINDOW + 0.25)
    ok = (m2 in flushed_msgs(ctx) and m3 in flushed_msgs(ctx)
          and len(ctx.message_processor.flushes) == 1)
    check("R3b 两条消息合并为一次 flush", ok,
          f"flushes={[(len(ms)) for _, ms in ctx.message_processor.flushes]}")
    if ctx.message_processor.flushes:
        dt = ctx.message_processor.flushes[0][0] - t_member
        check("R3b flush 以批次内消息落地起算窗口", dt >= WINDOW - 0.05, f"dt={dt:.2f}")

    # R3c：纯围观（无唤醒）→ 永不 flush
    ctx.message_processor.flushes.clear()
    m4 = make_msg(4, "纯围观", mentioned=False)
    ev4 = SEvent(m4, session(), mentioned=False)
    await plug.handle_msg(ev4)
    ctx.get_buffer(SID).buffer.append(Shim(m4))
    await plug.on_buffered(SID)
    await settle(WINDOW * 3)
    check("R3c 纯围观消息不被 flush（保险丝）", len(ctx.message_processor.flushes) == 0)
    check("R3c 围观消息留作前文", ctx.get_buffer(SID).get_length() >= 1)
    await shutdown(plug)


async def r4_async_stage1_contract(plugin_dir):
    """R4 异步 stage1 契约：语音同步占位 + ON_IM_MESSAGE 零阻塞 + 后台登记完整。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    mr = plug.media_recognizer

    # R4a：语音在 stage1 **返回时**已被替换为占位 Text（防框架渲染时自动 STT）
    rec = FakeRecord()
    m = make_msg(10, chain=[Text("听"), rec])
    m._batch_entered = False
    ev = SEvent(m, session(), mentioned=False)
    await mr.on_im_message(ev)          # 不 settle：同步部分必须已完成
    elem1 = m.chain[1]
    check("R4a 语音被同步替换为占位 Text", isinstance(elem1, Text),
          f"type={type(elem1).__name__}")
    check("R4a 占位为空描述标识符", isinstance(elem1, Text)
          and elem1.text.startswith("[Record #") and elem1.text.endswith(": ]"),
          f"text={getattr(elem1, 'text', None)!r}")
    check("R4a 登记表已挂载（含语音条目）", bool(getattr(m, "_pir_media", None)))
    await settle(0.3)
    info = (getattr(m, "_pir_media", None) or {}).get(getattr(rec, "_pir_short_id", ""), {})
    check("R4a 后台登记补齐 md5", info.get("md5") == hashlib.md5(b"fakeaudio").hexdigest(),
          f"md5={info.get('md5')}")

    # R4b：URL 图片慢下载（0.4s）下，ON_IM_MESSAGE 也必须近乎零阻塞返回
    img = UrlImage(payload=b"slowimg", slow=0.4)
    m2 = make_msg(11, chain=[Text("看"), img])
    m2._batch_entered = False
    ev2 = SEvent(m2, session(), mentioned=False)
    t0 = time.monotonic()
    await mr.on_im_message(ev2)
    dt = time.monotonic() - t0
    check("R4b ON_IM_MESSAGE 零阻塞（慢下载不入钩子链）", dt < 0.15, f"dt={dt:.2f}s")
    await settle(0.6)                   # 后台登记完成
    md5v = hashlib.md5(b"slowimg").hexdigest()
    bucket = getattr(m2, "_pir_media", None) or {}
    check("R4b 后台登记完成（md5 键）", md5v[:8] in bucket, f"keys={list(bucket)}")
    check("R4b _pir_short_id 已钉", getattr(img, "_pir_short_id", None) == md5v[:8])
    check("R4b 未命中保持空占位 caption", (img.caption or "") == "")
    await shutdown(plug)


async def r5_single_download(plugin_dir):
    """R5（Fix 2）：URL 型图片全程只下载一次（persist），hash_image 不再被调。"""
    mod = load_plugin(plugin_dir)
    ctx = Ctx()
    plug = make_plugin(mod, ctx)
    mr = plug.media_recognizer
    img = UrlImage(payload=b"one-download")
    m = make_msg(20, chain=[Text("看"), img])
    m._batch_entered = False
    ev = SEvent(m, session(), mentioned=False)
    await mr.on_im_message(ev)
    await settle(0.5)
    check("R5 to_base64 只调一次（persist 唯一下载）", img.n_b64 == 1, f"n_b64={img.n_b64}")
    check("R5 hash_image 未被调（md5 取自落盘字节）", img.n_hash == 0, f"n_hash={img.n_hash}")
    check("R5 已落盘（_temp_path）", bool(getattr(img, "_temp_path", None)),
          f"_temp_path={getattr(img, '_temp_path', None)}")
    md5v = hashlib.md5(b"one-download").hexdigest()
    check("R5 登记键==内容 md5", md5v[:8] in (getattr(m, "_pir_media", None) or {}))
    await shutdown(plug)


SCENARIOS = [
    ("R1 stage1 慢于窗口：唤醒仍被 flush", r1_slow_stage1_wake_still_flushed),
    ("R1b 反向验证：旧时点武装 → 唤醒变孤儿", r1b_old_arming_orphans_wake),
    ("R2 S 版：停窗交叠，真实唤醒仍 flush", r2_sustain_stop_real_wake_survives),
    ("R2b S 版：迟到的持续命中不确立批次", r2b_sustain_hit_late_no_batch),
    ("R3 正常路径回归（窗口/重置/保险丝）", r3_normal_flow_regression),
    ("R4 异步 stage1 契约（同步占位+零阻塞）", r4_async_stage1_contract),
    ("R5 URL 型媒体单次下载（Fix 2）", r5_single_download),
]


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}")
        for name, fn in SCENARIOS:
            try:
                await fn(d)
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
