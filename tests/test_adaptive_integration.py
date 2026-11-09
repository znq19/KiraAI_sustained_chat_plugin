"""Host-level adaptive debounce, live config, notice and watch integration."""
import asyncio
import time
from pathlib import Path
import sys

from test_debounce_arrival_race import (
    Ctx, SEvent, Shim, SID, flushed_msgs, load_plugin, make_msg, session, shutdown,
)


def config(merge=0.2, **overrides):
    basic = {
        "receive_unmentioned": True,
        "merge_window_seconds": merge,
        "adaptive_merge_enabled": True,
        "adaptive_merge_mode": "balanced",
        "adaptive_merge_max_seconds": 0,
        "adaptive_merge_max_batch_wait": 0,
        "adaptive_merge_allow_below_base": False,
        "adaptive_merge_min_seconds": 0,
    }
    basic.update(overrides)
    return {"section_basic": basic}


def ctx(base=0.2):
    return Ctx({
        "bot_config.bot.max_message_interval": base,
        "bot_config.bot.max_buffer_messages": 50,
    })


async def add(plug, context, mid, text, mentioned):
    message = make_msg(mid, text, mentioned=mentioned)
    event = SEvent(message, session(), mentioned=mentioned)
    await plug.handle_msg(event)
    context.get_buffer(SID).buffer.append(Shim(message))
    await plug.on_buffered(SID)
    return message


async def run(plugin_dir: Path):
    results = []
    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if not condition else ""))

    # Live WebUI base, custom override, and NoticeMerger share the same provider.
    mod = load_plugin(plugin_dir)
    context = ctx(0.2)
    plug = mod.DebouncePlugin(context, config(-1))
    check("WebUI基准即时读取", plug.debounce_interval == 0.2)
    check("默认不允许低于基准", plug.adaptive_merge.min_interval() == 0.2)
    check("通知合并读取当前基准", abs(plug.enhance.merger._window(SID) - 0.2) < 1e-6)
    context.config.values["bot_config.bot.max_message_interval"] = 0.4
    check("保存后无需重载即可见新值", plug.debounce_interval == 0.4)
    check("通知与动态上限同步新基准",
          abs(plug.enhance.merger._window(SID) - 0.4) < 1e-6
          and abs(plug.adaptive_merge.max_interval() - 1.2) < 1e-6
          and abs(plug.adaptive_merge.max_batch_wait() - 1.6) < 1e-6)
    await shutdown(plug)

    plug = mod.DebouncePlugin(context, config(0.7))
    check("自定义合并基准优先于WebUI", plug.adaptive_merge.base_interval() == 0.7)
    await shutdown(plug)

    # An in-flight timer must not flush at the old deadline after base increases.
    context = ctx(0.1)
    plug = mod.DebouncePlugin(context, config(-1))
    start = time.monotonic()
    await add(plug, context, 1, "wake", True)
    await asyncio.sleep(0.04)
    context.config.values["bot_config.bot.max_message_interval"] = 0.25
    await asyncio.sleep(0.3)
    flushes = context.message_processor.flushes
    check("在飞基准调大不会按旧截止误flush", len(flushes) == 1
          and flushes[0][0] - start >= 0.20,
          None if not flushes else flushes[0][0] - start)
    await shutdown(plug)

    # Continuous arrivals still hit the hard per-batch cap.
    context = ctx(0.2)
    plug = mod.DebouncePlugin(context, config(
        0.2, adaptive_merge_max_seconds=0.2, adaptive_merge_max_batch_wait=0.3))
    start = time.monotonic()
    messages = [await add(plug, context, 10, "a", True)]
    await asyncio.sleep(0.12)
    messages.append(await add(plug, context, 11, "b", False))
    await asyncio.sleep(0.12)
    messages.append(await add(plug, context, 12, "c", False))
    await asyncio.sleep(0.25)
    flushes = context.message_processor.flushes
    check("持续发言在单批最长等待到点flush", len(flushes) == 1
          and all(m in flushed_msgs(context) for m in messages),
          [(round(ts - start, 3), len(ms)) for ts, ms in flushes])
    if flushes:
        check("硬上限早于最后一次后的完整窗口", flushes[0][0] - start < 0.42,
              flushes[0][0] - start)
    await shutdown(plug)

    # max_buffer_messages must update the host and every QueueMerge derived limit.
    context = ctx(5.0)
    context.config.values["bot_config.bot.max_buffer_messages"] = 5
    plug = mod.DebouncePlugin(context, config(10.0, max_unmentioned_messages=5))
    scheduler = plug.merge_scheduler
    check("主buffer初始条数上限", plug.max_buffer_messages == 5)
    check("QueueMerge自动条数读取section_basic",
          scheduler._short_merge_max_allowed() == 5
          and scheduler._max_merge_messages_allowed() == 30)
    context.config.values["bot_config.bot.max_buffer_messages"] = 2
    check("主buffer与QueueMerge同步调小",
          plug.max_buffer_messages == 2
          and scheduler._short_merge_max_allowed() == 2
          and scheduler._max_merge_messages_allowed() == 21)
    manual = mod.BatchMergeScheduler(context, {"section_queue_merge": {
        "max_merge_messages": 9, "short_merge_max_messages": 7}}, {})
    context.config.values["bot_config.bot.max_buffer_messages"] = 1
    check("手动正数上限不随全局值变化",
          manual._max_merge_messages_allowed() == 9
          and manual._short_merge_max_allowed() == 7)
    context.config.values["bot_config.bot.max_buffer_messages"] = 5
    first = await add(plug, context, 15, "one", True)
    second = await add(plug, context, 16, "two", True)
    context.config.values["bot_config.bot.max_buffer_messages"] = 2
    third = await add(plug, context, 17, "three", True)
    flushed = flushed_msgs(context)
    check("调小后下一条立即满即推且不裁剪已有消息",
          len(context.message_processor.flushes) == 1
          and all(m in flushed for m in (first, second, third)))
    await shutdown(plug)

    # End-to-end near-miss: a real host timeout flush must enlarge the next batch.
    context = ctx(0.2)
    plug = mod.DebouncePlugin(context, config(0.2))
    await add(plug, context, 20, "first", True)
    for _ in range(100):
        if context.message_processor.flushes:
            break
        await asyncio.sleep(0.005)
    check("主链路先完成一次timeout批次", bool(context.message_processor.flushes))
    await add(plug, context, 21, "late continuation", True)
    enlarged = plug.adaptive_merge.current_window(SID, "2361")
    check("实际主批次near-miss增大下一窗口", enlarged > 0.2, enlarged)
    await shutdown(plug)

    # S's independent watch loop must call the shared adaptive timing policy.
    if hasattr(mod.DebouncePlugin, "_watch_loop"):
        context = ctx(0.1)
        plug = mod.DebouncePlugin(context, config(0.1))
        calls = []
        original = plug.adaptive_merge.timing
        def spy(*args):
            calls.append(args)
            return original(*args)
        plug.adaptive_merge.timing = spy
        fired = []
        async def fake_fire(sid, gen):
            fired.append((sid, gen))
        plug._watch_fire = fake_fire
        plug.sustain_stopped[SID] = True
        plug._watch_since[SID] = set()
        plug._watch_gen[SID] = 0
        plug._watch_events[SID] = asyncio.Event()
        now = time.monotonic()
        plug._watch_first_at[SID] = now
        plug._watch_last_at[SID] = now
        plug._watch_anchor_uid[SID] = "u"
        plug.adaptive_merge.start_batch(SID, "u", now)
        task = asyncio.create_task(plug._watch_loop(SID, 0))
        plug._watch_events[SID].set()
        await asyncio.sleep(0.25)
        check("S空停守望使用共享动态计时", bool(calls) and fired == [(SID, 0)],
              {"timing_calls": len(calls), "fired": fired})
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await shutdown(plug)

    return all(results)


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    ok = True
    for directory in dirs:
        print(f"=== {directory.name} ===")
        ok = await run(directory) and ok
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
