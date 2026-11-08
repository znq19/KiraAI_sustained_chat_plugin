"""S 版：端到端场景 —— 真实钩子链 + 真实计时（用户日志时间线的完整重演）。

与单测的差异：不是孤立调用被测函数，而是让消息真的走
  on_buffered（批次确立+顺延武装）→ _debounce_loop（真实 asyncio 计时）
  → flush_session_messages（记录）完整链路。

场景：
  S1【本案时间线】唤醒消息落缓冲 → 顺延进行中 _stop_sustain_round（空 msg 停窗）
     → 顺延到点 ⇒ 批次必须照常 flush（修复前：永远滞留）
  S2【对照】纯持续命中批 + 停窗 ⇒ 顺延到点不 flush（旧语义保持）
  S3【JEV 守望全链路】空停武装 → 新消息 track → 顺延到点 → JEV reply
     ⇒ 解闩 + flush + 解除守望
  S4【JEV 守望 wait】→ 不 flush + 冷却 + 闩保持
  S5【JEV confirm 延迟链路】概率命中 + JEV wait ⇒ 否决不 flush

Run: python3 tests/test_scenario_e2e.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import sys
import time
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

def load_main(plugin_dir: Path):
    for m in ("main", "queue_merge", "media_recognize", "chat_enhance", "jev"):
        sys.modules.pop(m, None)
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location(f"main_e2e_{plugin_dir.name}",
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
    def __init__(self):
        self.buffer = []

    def get_length(self):
        return len(self.buffer)

    def pop(self, count=1):
        popped = self.buffer[:count]
        del self.buffer[:count]
        return popped

class _Processor:
    """flush 记录 + 真实把缓冲弹出（模拟框架 flush_session_messages）。"""
    def __init__(self, buf):
        self._buf = buf
        self.flushed = []

    async def flush_session_messages(self, sid, **_):
        # 真实框架的 flush 有挂起点（缓冲锁/事件发布）——必须 sleep(0) 制造挂起点，
        # 才能暴露「当前任务被误 cancel 打断 flush」这类竞态（参见 _watch_disarm 注释）
        await asyncio.sleep(0)
        msgs = [getattr(it, "message", it) for it in list(self._buf.buffer)]
        self._buf.buffer.clear()
        if not msgs:
            return False
        self.flushed.append((sid, [getattr(m, "message_id", None) for m in msgs]))
        return True

    def get_session_buffer_length(self, sid):
        return self._buf.get_length()

class _Ctx:
    def __init__(self, buf, proc):
        self.config = _Cfg()
        self._buf = buf
        self.message_processor = proc

    async def flush_session_messages(self, sid, **kw):
        # 镜像真 PluginContext：ctx 级方法就是委托给 message_processor
        return await self.message_processor.flush_session_messages(sid, **kw)

    def get_plugin_data_dir(self):
        return Path("/tmp")

    def get_buffer(self, sid):
        return self._buf

SID = "qq:gm:188395693"

def _msg(mid, text, mentioned=False):
    from core.chat.message_elements import Text
    return SimpleNamespace(message_id=mid, is_mentioned=mentioned,
                           sender=SimpleNamespace(nickname="n" + str(mid)[-2:], user_id="u"),
                           chain=[Text(text)])

def make_plugin(mod, watch=False, jev_answer=None):
    buf = _Buf()
    proc = _Processor(buf)
    ctx = _Ctx(buf, proc)
    cfg = {
        "section_basic": {"merge_window_seconds": 0.2},
        "section_group_sustain": {"sustain_enabled": True, "stop_on_ai_empty": True},
        "section_dm_sustain": {},
    }
    plug = mod.DebouncePlugin(ctx, cfg)
    if watch:
        plug.jev_enabled = True
        plug.jev_watch_enabled = True
        plug.jev_watch_cooldown = 60.0
        plug._jev_client = SimpleNamespace(
            ready=True,
            calls=0,
            decide_trigger=None,
        )
        async def _decide(state, instructions, timeout=None):
            plug._jev_client.calls += 1
            return jev_answer
        plug._jev_client.decide_trigger = _decide
    return plug, buf, proc

# ---------------------------------------------------------------- S1 本案时间线
async def s1_wake_survives_full_chain(mod):
    plug, buf, proc = make_plugin(mod)
    # 08:29:28 C7 引用回复（真唤醒）落缓冲
    c7 = _msg("255952681", "别聊了，去补觉", mentioned=True)
    c7._wake_pending = True
    buf.buffer.append(SimpleNamespace(message=c7))
    await plug.on_buffered(SID)          # 批次确立 + 顺延武装（merge_window=0.2s）
    ok0 = plug.batch_started.get(SID) is True
    # 08:29:30 空 msg 停窗（顺延进行中）
    await asyncio.sleep(0.05)
    await plug._stop_sustain_round(SID)
    ok1 = plug.batch_started.get(SID) is True           # 守卫：批次保留
    ok2 = not plug.sustain_stopped.get(SID, False)      # 不落闩
    # 顺延到点 ⇒ 必须照常 flush（修复前：batch_started 被清 ⇒ 永不 flush）
    await asyncio.sleep(0.45)
    ok3 = len(proc.flushed) == 1 and proc.flushed[0][0] == SID
    ok4 = proc.flushed and "255952681" in proc.flushed[0][1]
    await plug.terminate()
    return {"S1 批次已确立": ok0, "S1 停窗后批次保留": ok1, "S1 不落闩": ok2,
            "S1 顺延到点批次照常 flush": ok3, "S1 C7 消息在批次里": ok4}

# ---------------------------------------------------------------- S2 对照：纯持续命中照旧不 flush
async def s2_pure_hit_still_held(mod):
    plug, buf, proc = make_plugin(mod)
    hit = _msg("1620253499", "[Sticker]", mentioned=True)
    hit._wake_pending = True
    buf.buffer.append(SimpleNamespace(message=hit))
    plug.sustain_hit_ids[SID] = {"1620253499"}
    await plug.on_buffered(SID)
    ok0 = plug.batch_started.get(SID) is True
    await asyncio.sleep(0.05)
    await plug._stop_sustain_round(SID)
    ok1 = not plug.batch_started.get(SID, False)        # 旧语义：纯命中批撤销
    ok2 = plug.sustain_stopped.get(SID, False) is True  # 落闩
    await asyncio.sleep(0.45)
    ok3 = proc.flushed == []                            # 顺延到点不 flush
    ok4 = len(buf.buffer) == 1                          # 消息留在缓冲（前文，不丢）
    await plug.terminate()
    return {"S2 批次已确立": ok0, "S2 纯命中批被撤销": ok1, "S2 落闩": ok2,
            "S2 到点不 flush": ok3, "S2 消息滞留前文不丢": ok4}

# ---------------------------------------------------------------- S3/S4 JEV 守望全链路
async def s3_watch_full_chain_reply(mod):
    plug, buf, proc = make_plugin(mod, watch=True,
                                  jev_answer={"choice": "reply", "confidence": 0.9,
                                              "probabilities": {"reply": 0.9}, "model": "m"})
    # 空 msg 停窗已落闩 + 守望武装（缓冲此刻为空 = 旧上下文为空）
    plug.sustain_stopped[SID] = True
    plug._arm_watch_after_empty_stop(SID)
    ok0 = plug._watch_active(SID)
    # 停窗后新到两条消息（走非唤醒路径 → buffer + watch_track）
    for mid, txt in (("n1", "紫小贱 这个怎么看"), ("n2", "对啊说说")):
        m = _msg(mid, txt)
        buf.buffer.append(SimpleNamespace(message=m))
        plug._watch_track(SID, SimpleNamespace(message=m))
    # 顺延 0.2s 到点 → JEV reply ⇒ 解闩 + flush + 解除守望
    await asyncio.sleep(0.6)
    ok1 = plug._jev_client.calls >= 1
    ok2 = len(proc.flushed) == 1 and set(proc.flushed[0][1]) == {"n1", "n2"}
    ok3 = not plug.sustain_stopped.get(SID, False)
    ok4 = not plug._watch_active(SID)
    await plug.terminate()
    return {"S3 守望武装": ok0, "S3 JEV 被调用": ok1,
            "S3 reply ⇒ flush 出新消息": ok2, "S3 解闩": ok3, "S3 解除守望": ok4}

async def s4_watch_wait_once(mod):
    """wait ⇒ 维持停窗并结束本次守望；后续新消息不再触发 JEV。"""
    plug, buf, proc = make_plugin(mod, watch=True,
                                  jev_answer={"choice": "wait", "confidence": 0.95,
                                              "probabilities": {"wait": 0.95}, "model": "m"})
    plug.sustain_stopped[SID] = True
    plug._arm_watch_after_empty_stop(SID)
    m = _msg("n1", "哈哈哈哈")
    buf.buffer.append(SimpleNamespace(message=m))
    plug._watch_track(SID, SimpleNamespace(message=m))
    await asyncio.sleep(0.6)
    ok1 = plug._jev_client.calls >= 1
    ok2 = proc.flushed == []
    ok3 = plug.sustain_stopped.get(SID, False) is True
    ok4 = not plug._watch_active(SID)
    # 守望已解除：新消息只进缓冲，不再判
    m2 = _msg("n2", "又一条")
    buf.buffer.append(SimpleNamespace(message=m2))
    plug._watch_track(SID, SimpleNamespace(message=m2))
    await asyncio.sleep(0.5)
    ok5 = plug._jev_client.calls == 1
    await plug.terminate()
    return {"S4 JEV 被调用": ok1, "S4 wait ⇒ 不 flush": ok2, "S4 闩保持": ok3,
            "S4 wait ⇒ 立即解除守望": ok4, "S4 后续批次不再判断": ok5}

# ---------------------------------------------------------------- S5 confirm 延迟否决全链路
async def s5_confirm_veto_chain(mod):
    plug, buf, proc = make_plugin(mod, watch=True,
                                  jev_answer={"choice": "wait", "confidence": 0.9,
                                              "probabilities": {"wait": 0.9}, "model": "m"})
    # 概率命中（机械=True）+ JEV wait ⇒ 否决不 flush（延迟链路：先落地后判定）
    m = _msg("m1", "随手一发")
    async def _late_buffer():
        await asyncio.sleep(0.05)
        buf.buffer.append(SimpleNamespace(message=m))
    asyncio.create_task(_late_buffer())
    await plug._jev_deferred_trigger(SID, "m1", "proactive", True)
    ok1 = proc.flushed == []
    ok2 = plug._jev_client.calls >= 1
    # 对照：JEV reply ⇒ flush
    plug._jev_client.calls = 0
    async def _decide2(state, instructions, timeout=None):
        return {"choice": "reply", "confidence": 0.9, "probabilities": {"reply": 0.9}, "model": "m"}
    plug._jev_client.decide_trigger = _decide2
    await plug._jev_deferred_trigger(SID, "m1", "proactive", True)
    ok3 = len(proc.flushed) == 1
    await plug.terminate()
    return {"S5 confirm 否决不 flush": ok1, "S5 JEV 被调用": ok2,
            "S5 confirm 确认 flush": ok3}

async def run(plugin_dir: Path):
    mod = load_main(plugin_dir)
    for label, cond in (await s1_wake_survives_full_chain(mod)).items():
        check(label, cond)
    for label, cond in (await s2_pure_hit_still_held(mod)).items():
        check(label, cond)
    for label, cond in (await s3_watch_full_chain_reply(mod)).items():
        check(label, cond)
    for label, cond in (await s4_watch_wait_once(mod)).items():
        check(label, cond)
    for label, cond in (await s5_confirm_veto_chain(mod)).items():
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
