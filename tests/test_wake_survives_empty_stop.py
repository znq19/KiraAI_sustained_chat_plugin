"""S 版：真实唤醒不被空 msg 停窗连坐（2026-10-12 用户日志事故回归）。

事故时间线（用户日志实证）：
  08:29:21  QueueMerge 放行持续命中批 ffa80a01 → LLM 开跑
  08:29:28  C7 引用回复 bot（真唤醒）→ buffer + batch_started=True + 顺延武装
  08:29:30  LLM 返回 <msg/>（空消息）→ _stop_sustain_round：
            旧实现**无条件** pop batch_started/batch_count + 落 sustain_stopped 闩
  08:29:30+ 顺延到点 → _debounce_loop 保险丝见 batch_started 已清 → 永不 flush
            ⇒ C7 的消息永远滞留缓冲（既不回复也不进任何一轮）

修复（v2.7.0）：
  · _stop_sustain_round / _end_sustain_window 清批次前调 _pending_batch_has_real_wake：
    在途批次含真实唤醒（mentioned 且 id ∉ sustain_hit_ids）⇒ 保留批次照常顺延，
    且本轮不落 sustain_stopped 闩（新轮次已在排队）；
  · on_queue_merge_batch：停窗拦截遇含真实唤醒的批次 ⇒ 解闩放行（覆盖唤醒在 stop
    前已进 QueueMerge pending / 顺延批 stop 后才 flush 的竞态）。

断言：
  T1 真唤醒在途 + 空 msg 停窗 ⇒ 批次保留 + 不落闩（本案直接回归）
  T2 纯持续命中在途 + 停窗 ⇒ 批次撤销 + 落闩（旧行为保持）
  T3 缓冲无唤醒消息 + 停窗 ⇒ 批次撤销 + 落闩（旧行为保持）
  T4 停窗拦截遇真唤醒批 ⇒ 解闩 + 放行进 QueueMerge（不 stop、不还原）
  T5 停窗拦截遇纯持续命中批 ⇒ 还原前文 + stop（旧行为保持）
  T6 判定异常（get_buffer 抛）⇒ 保守保留（不误杀）
  T7 _end_sustain_window 同款守卫：真唤醒在途时批次不被超时清理连坐
  T8 停窗拦截遇纯非唤醒批（主动概率）⇒ **不拦截**（主动发言是独立通路/别的唤醒）

Run: python3 tests/test_wake_survives_empty_stop.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import sys
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

# ---------------------------------------------------------------- 加载插件
def load_main(plugin_dir: Path):
    for m in ("main", "queue_merge", "media_recognize", "chat_enhance", "jev"):
        sys.modules.pop(m, None)
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location(f"main_ws_{plugin_dir.name}",
                                                  plugin_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod

class _Cfg:
    def get_config(self, key, default=None):
        return {"bot_config.agent.max_tool_loop": 2,
                "bot_config.agent.tool_call_timeout": 60,
                "bot_config.bot.max_buffer_messages": 5,
                "bot_config.bot.max_message_interval": 30}.get(key, default)

    def __getitem__(self, key):
        return {"agent": {"max_tool_loop": 2, "tool_call_timeout": 60},
                "bot": {"max_buffer_messages": 5, "max_message_interval": 30}} \
            if key == "bot_config" else {}

class _Buf:
    """会话缓冲桩：buffer 列表里是带 .message 的 shim（与框架缓冲元素同形）。"""
    def __init__(self):
        self.buffer = []

    def get_length(self):
        return len(self.buffer)

    def pop(self, count=1):
        popped = self.buffer[:count]
        del self.buffer[:count]
        return popped

class _Processor:
    def __init__(self, buf):
        self._buf = buf
        self.flushed = []

    async def flush_session_messages(self, sid, **_):
        self.flushed.append(sid)
        return True

    def get_session_buffer_length(self, sid):
        return self._buf.get_length()

class _Ctx:
    def __init__(self, buf, proc):
        self.config = _Cfg()
        self._buf = buf
        self.message_processor = proc

    def get_plugin_data_dir(self):
        return Path("/tmp")

    def get_buffer(self, sid):
        return self._buf

def _msg(mid, mentioned, text="x"):
    return SimpleNamespace(message_id=mid, is_mentioned=mentioned,
                           sender=SimpleNamespace(nickname="n", user_id="u"),
                           chain=[])

def _shim(m):
    return SimpleNamespace(message=m)

def make_plugin(mod):
    buf, proc = _Buf(), None
    proc = _Processor(buf)
    ctx = _Ctx(buf, proc)
    cfg = {
        "section_basic": {"merge_window_seconds": 0.3},
        "section_group_sustain": {"sustain_enabled": True, "stop_on_ai_empty": True},
        "section_dm_sustain": {},
    }
    plug = mod.DebouncePlugin(ctx, cfg)
    return plug, buf, proc

SID = "qq:gm:188395693"

# ---------------------------------------------------------------- T1 本案回归
async def t1_real_wake_survives(mod):
    plug, buf, proc = make_plugin(mod)
    # C7 引用回复 bot：真唤醒已 buffer + 批次已确立（on_buffered 做完）
    c7 = _msg("255952681", True, "别聊了，去补觉")
    buf.buffer.append(_shim(c7))
    plug.batch_started[SID] = True
    plug.batch_count[SID] = 1
    # 空 msg 停窗（sustain_hit_ids 已被真唤醒清空）
    plug.sustain_hit_ids.pop(SID, None)
    await plug._stop_sustain_round(SID)
    ok1 = plug.batch_started.get(SID) is True
    ok2 = plug.batch_count.get(SID) == 1
    ok3 = not plug.sustain_stopped.get(SID, False)
    # 顺延到点 flush：批次正常送出
    await proc.flush_session_messages(SID)
    ok4 = proc.flushed == [SID]
    return {"T1 批次保留(batch_started)": ok1, "T1 批次计数保留": ok2,
            "T1 不落 sustain_stopped 闩": ok3, "T1 批次可正常 flush": ok4}

# ---------------------------------------------------------------- T2 纯持续命中照旧撤销
async def t2_pure_hit_still_dropped(mod):
    plug, buf, proc = make_plugin(mod)
    hit = _msg("1620253499", True, "[Sticker]")
    buf.buffer.append(_shim(hit))
    plug.batch_started[SID] = True
    plug.batch_count[SID] = 1
    plug.sustain_hit_ids[SID] = {"1620253499"}
    await plug._stop_sustain_round(SID)
    return {
        "T2 纯持续命中批次被撤销": not plug.batch_started.get(SID, False),
        "T2 落 sustain_stopped 闩": plug.sustain_stopped.get(SID, False) is True,
    }

# ---------------------------------------------------------------- T3 无唤醒消息照旧撤销
async def t3_no_wake_still_dropped(mod):
    plug, buf, proc = make_plugin(mod)
    plain = _msg("111", False, "围观闲聊")
    buf.buffer.append(_shim(plain))
    plug.batch_started[SID] = True     # 残留状态（异常情况）
    await plug._stop_sustain_round(SID)
    return {
        "T3 无唤醒消息批次被撤销": not plug.batch_started.get(SID, False),
        "T3 落 sustain_stopped 闩": plug.sustain_stopped.get(SID, False) is True,
    }

# ---------------------------------------------------------------- T4/T5 批次入口
def _batch_event(mod, plug, messages, eid="b1"):
    ev = SimpleNamespace(
        event_id=eid,
        messages=messages,
        extra=None,
        session=SimpleNamespace(sid=SID),
        is_stopped=False,
    )
    ev.is_group_message = lambda: True
    ev.sid = SID
    def _stop():
        ev.is_stopped = True
    ev.stop = _stop
    return ev

async def t4_unlatch_on_real_wake_batch(mod):
    plug, buf, proc = make_plugin(mod)
    plug.sustain_stopped[SID] = True
    plug.sustain_hit_ids.pop(SID, None)
    wake = _msg("255952681", True, "引用回复 bot 的消息")
    ev = _batch_event(mod, plug, [wake])
    await plug.on_queue_merge_batch(ev)
    return {
        "T4 真唤醒批解闩": not plug.sustain_stopped.get(SID, False),
        "T4 批次未被 stop": not ev.is_stopped,
        "T4 批次进入 QueueMerge(放行登记)": plug.merge_scheduler._inflight.get(SID) == "b1",
    }

async def t5_pure_hit_batch_restored(mod):
    plug, buf, proc = make_plugin(mod)
    plug.sustain_stopped[SID] = True
    plug.sustain_hit_ids[SID] = {"1620253499"}
    hit = _msg("1620253499", True, "[Sticker]")
    plain = _msg("1620253500", False, "附和")
    ev = _batch_event(mod, plug, [hit, plain], eid="b2")
    await plug.on_queue_merge_batch(ev)
    restored_ids = [getattr(getattr(x, "message", x), "message_id", None) for x in buf.buffer]
    return {
        "T5 纯持续命中批被 stop": ev.is_stopped,
        "T5 消息还原回缓冲（不丢内容）": restored_ids == ["1620253499", "1620253500"],
        "T5 闩保持": plug.sustain_stopped.get(SID, False) is True,
    }

# ---------------------------------------------------------------- T6 异常保守
async def t6_error_conservative(mod):
    plug, buf, proc = make_plugin(mod)
    class _BadCtx(_Ctx):
        def get_buffer(self, sid):
            raise RuntimeError("boom")
    plug.ctx = _BadCtx(buf, proc)
    plug.batch_started[SID] = True
    await plug._stop_sustain_round(SID)
    return {
        "T6 判定异常时批次保守保留": plug.batch_started.get(SID) is True,
        "T6 判定异常时不落闩": not plug.sustain_stopped.get(SID, False),
    }

# ---------------------------------------------------------------- T7 窗口超时守卫
async def t7_window_timeout_guard(mod):
    plug, buf, proc = make_plugin(mod)
    import time as _t
    c7 = _msg("255952681", True, "引用回复")
    buf.buffer.append(_shim(c7))
    plug.batch_started[SID] = True
    plug.batch_count[SID] = 1
    plug.sustain_until[SID] = _t.time() - 1   # 已过期
    # 直接调用守卫判定而不是 sleep：模拟 _end_sustain_window 的清理段
    if not plug._pending_batch_has_real_wake(SID):
        plug.batch_started.pop(SID, None)
        plug.batch_count.pop(SID, None)
    return {
        "T7 超时清理遇真唤醒批次保留": plug.batch_started.get(SID) is True,
        "T7 _pending_batch_has_real_wake 判真": plug._pending_batch_has_real_wake(SID),
    }

async def t8_pure_nonwake_batch_not_blocked(mod):
    """停窗后的无 mentioned 批次（主动概率）不拦截：主动发言是独立通路，
    也是用户口中的“别的唤醒”，不能被持续停窗误杀。"""
    plug, buf, proc = make_plugin(mod)
    plug.sustain_stopped[SID] = True
    p = _msg("p1", False, "主动概率命中")
    ev = _batch_event(mod, plug, [p], eid="b3")
    await plug.on_queue_merge_batch(ev)
    return {
        "T8 纯非唤醒批不被 stop": not ev.is_stopped,
        "T8 消息不被还原（继续走主动通路）": buf.buffer == [],
        "T8 闩保持（只影响持续命中批）": plug.sustain_stopped.get(SID, False) is True,
    }

async def run(plugin_dir: Path):
    mod = load_main(plugin_dir)
    for fn in (t1_real_wake_survives, t2_pure_hit_still_dropped, t3_no_wake_still_dropped,
               t4_unlatch_on_real_wake_batch, t5_pure_hit_batch_restored,
               t6_error_conservative, t7_window_timeout_guard,
               t8_pure_nonwake_batch_not_blocked):
        out = await fn(mod)
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
