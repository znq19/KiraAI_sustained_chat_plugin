"""AdaptiveMergePolicy unit tests; run against one or more plugin directories."""
import importlib.util
import math
import sys
import time
from pathlib import Path

SID = "qq:gm:1"


def load(path):
    name = f"adaptive_merge_test_{path.resolve().parent.name}"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def policy(mod, cfg=None, base=5.0):
    return mod.AdaptiveMergePolicy(lambda: base, dict(cfg or {}))


def quantile_sample(mod, mode, allow_below=True):
    p = policy(mod, {"adaptive_merge_mode": mode,
                     "adaptive_merge_allow_below_base": allow_below,
                     "adaptive_merge_min_seconds": 0})
    profile = mod._Profile()
    for gap in (0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0):
        profile.samples.append((1000.0, gap))
    profile.touched = 1000.0
    p._profiles[(SID, "u")] = profile
    return p.start_batch(SID, "u", 1000.0)


def run(path):
    mod = load(path)
    results = []
    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if not condition else ""))

    p = policy(mod)
    check("默认开启且基准5秒冷启动", p.enabled and p.start_batch(SID, "u", 100) == 5.0)
    check("默认不低于基准", p.min_interval() == 5.0)
    check("自动上限15/总等待20", p.max_interval() == 15.0 and p.max_batch_wait() == 20.0)
    check("基准可变且无固定15/30硬顶", policy(mod, base=8).max_interval() == 24.0
          and policy(mod, base=8).max_batch_wait() == 32.0)

    p = policy(mod, {"adaptive_merge_allow_below_base": True})
    check("允许低于基准时自动下限2.5", p.min_interval() == 2.5)
    p = policy(mod, {"adaptive_merge_allow_below_base": True,
                     "adaptive_merge_min_seconds": 3})
    check("自定义最低窗口3秒", p.min_interval() == 3.0)
    p = policy(mod, {"adaptive_merge_allow_below_base": True,
                     "adaptive_merge_min_seconds": 8})
    check("最低窗口不高于基准", p.min_interval() == 5.0)

    gaps = [0.5, 0.8, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0]
    samples = [(1000.0, gap) for gap in gaps]
    quantiles = [mod.AdaptiveMergePolicy._weighted_quantile(samples, 1000.0, q)[0]
                 for q in (0.60, 0.75, 0.90)]
    check("P60/P75/P90=3/6/8", quantiles == [3.0, 6.0, 8.0], quantiles)
    responsive = quantile_sample(mod, "responsive")
    balanced = quantile_sample(mod, "balanced")
    merge_first = quantile_sample(mod, "merge_first")
    check("三档实际窗口严格递增", responsive < balanced < merge_first,
          (responsive, balanced, merge_first))
    check("禁止低于基准时响应档由5秒托底",
          quantile_sample(mod, "responsive", allow_below=False) == 5.0)

    p = policy(mod)
    p.note_flush(SID, "u", last_message_at=4.0, reason="timeout", now=9.0)
    p.observe(SID, "u", 10.2)
    window = p.start_batch(SID, "u", 10.2)
    check("near-miss 5秒→6.45秒", math.isclose(window, 6.45), window)

    p = policy(mod)
    p.note_flush(SID, "u", last_message_at=4.0, reason="count", now=9.0)
    p.observe(SID, "u", 10.2)
    check("条数满不触发near-miss", p.start_batch(SID, "u", 10.2) == 5.0)
    p = policy(mod)
    p.note_flush(SID, "u", last_message_at=4.0, reason="timeout", now=9.0)
    p.observe(SID, "other", 10.2)
    check("其他用户不触发该用户补偿", p.start_batch(SID, "u", 10.2) == 5.0)

    p = policy(mod)
    p._windows[SID] = [5.0, 15.0, "u"]
    deadline, reason, window = p.timing(SID, 0.0, 19.0, "u", 19.0)
    check("整批最长等待优先于无限顺延", deadline == 20.0 and reason == "max_wait",
          (deadline, reason, window))

    base = [5.0]
    p = mod.AdaptiveMergePolicy(lambda: base[0], {})
    check("WebUI基准按需读取", p.start_batch(SID, "u", 100) == 5.0)
    base[0] = 8.0
    check("基准变更后窗口和上限同步", p.current_window(SID, "u", 101) == 8.0
          and p.max_interval() == 24.0)

    p = policy(mod, {"adaptive_merge_enabled": False})
    p.observe(SID, "u", 1.0)
    check("总开关关闭保持固定基准且不学习", p.start_batch(SID, "u", 2) == 5.0
          and not p._profiles)

    p = policy(mod)
    start = time.perf_counter()
    for _ in range(5000):
        p.current_window(SID)
    elapsed = time.perf_counter() - start
    check("当前窗口读取无I/O且微秒级", elapsed < 0.5, f"5000次={elapsed:.4f}s")
    return all(results)


def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    ok = True
    for directory in dirs:
        print(f"=== {directory.name}/adaptive_merge.py ===")
        ok = run(directory / "adaptive_merge.py") and ok
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
