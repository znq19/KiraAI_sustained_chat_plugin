"""QueueMerge dynamic-auto window and serialization regression tests."""
import asyncio
import sys
import time
from pathlib import Path

from test_queue_timing import load_qm, make_sched, batch, msg, SID


class Policy:
    def __init__(self, window=0.08, enabled=True):
        self.window = window
        self.enabled = enabled

    def current_window(self, sid):
        return self.window


def scheduler(mod, window=0.08, manual=0):
    sched = make_sched(mod, {"section_queue_merge": {
        "enabled": True,
        "max_merge_seconds": manual,
        # Two one-message batches must not take the immediate soft-merge branch.
        "short_merge_max_messages": 1,
    }})
    sched.adaptive_policy = Policy(window)
    return sched


async def run(plugin_dir):
    mod = load_qm(plugin_dir)
    results = []
    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if not condition else ""))

    # Dynamic auto: hold the whole pending group until the window expires.
    s = scheduler(mod)
    first = batch(msg(1, "running"))
    await s.on_batch_message(first)
    await s.on_batch_message(batch(msg(2, "b")))
    await s.on_batch_message(batch(msg(3, "c")))
    await s.on_llm_response(first, type("R", (), {"tool_calls": None})())
    await s.on_step_result(first)
    check("动态窗口未到期不推、不拆第一批", not s.ctx.event_bus.published
          and len(s._pending.get(SID, [])) == 2)
    await asyncio.sleep(0.01)  # 让既有 pending 唤醒先被事件循环消费
    check("动态等待不产生自唤醒忙循环", not s._tick_wake.is_set())
    pending = next(iter(s._pending.values()))
    for item in pending:
        item.arrival_ts -= 10
    await s._tick()
    check("到期后两个批次合并推送", len(s.ctx.event_bus.published) == 1
          and len(s.ctx.event_bus.published[0].messages) == 2)
    await s.shutdown()

    # Dynamic timeout is merge-ready only: never interrupt an active batch.
    s = scheduler(mod)
    first = batch(msg(10, "running"))
    await s.on_batch_message(first)
    await s.on_batch_message(batch(msg(11, "pending")))
    pending = next(iter(s._pending.values()))
    pending[0].arrival_ts -= 10
    await s._tick()
    check("动态到点不打断在飞批次", not s.ctx.event_bus.published
          and next(iter(s._inflight.values())) == first.event_id)
    await s.on_llm_response(first, type("R", (), {"tool_calls": None})())
    await s.on_step_result(first)
    check("前批正常收尾后立即合并", len(s.ctx.event_bus.published) == 1)
    await s.shutdown()

    # Explicit positive values keep the legacy in-flight timeout behavior.
    s = scheduler(mod, manual=0.05)
    first = batch(msg(20, "running"))
    await s.on_batch_message(first)
    await s.on_batch_message(batch(msg(21, "pending")))
    next(iter(s._pending.values()))[0].arrival_ts -= 1
    await s._tick()
    check("手动正数保留在飞超时强制推送", len(s.ctx.event_bus.published) == 1)
    await s.shutdown()

    # Disabling the policy restores the old zero=immediate behavior.
    s = scheduler(mod)
    s.adaptive_policy.enabled = False
    first = batch(msg(30, "running"))
    await s.on_batch_message(first)
    await s.on_batch_message(batch(msg(31, "a")))
    await s.on_batch_message(batch(msg(32, "b")))
    await s.on_llm_response(first, type("R", (), {"tool_calls": None})())
    await s.on_step_result(first)
    check("动态关闭时0恢复旧的立即合并", len(s.ctx.event_bus.published) == 1
          and len(s.ctx.event_bus.published[0].messages) == 2)
    await s.shutdown()
    return all(results)


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    ok = True
    for directory in dirs:
        print(f"=== {directory.name}/queue_merge.py ===")
        ok = await run(directory) and ok
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
