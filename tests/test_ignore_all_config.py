"""完整拉黑（all）独立时长 + 热更新回归。"""
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))
results = []


def check(label, condition, detail=""):
    results.append(bool(condition))
    print(("PASS " if condition else "FAIL ") + label
          + (f" — {detail}" if detail and not condition else ""))


def run(plugin_dir: Path):
    if str(plugin_dir) not in sys.path:
        sys.path.insert(0, str(plugin_dir))
    import chat_enhance

    cfg = {
        "section_poke": {"default_duration": 999, "allow_bot_duration": True,
                         "max_duration": 1000},
        "section_ignore": {"default_duration": 10, "allow_bot_duration": True,
                           "max_duration": 30},
    }
    d = chat_enhance.HarassDetector(cfg, None)
    now = time.time()
    d.apply_ignore("s", "u", "all", 0)
    until = d._ignored[("s", "u", "poke")]
    check("all 默认时长独立于 poke", 9 <= until - now <= 11, until - now)

    d.apply_ignore("s", "u", "all", 100)
    until = d._ignored[("s", "u", "poke")]
    check("all 最大时长独立钳制", 29 <= until - time.time() <= 31,
          until - time.time())

    # 热更新：不重建 detector，只 reload，新配置立即生效
    d.reload({
        "section_poke": {"default_duration": 999, "allow_bot_duration": True,
                         "max_duration": 1000},
        "section_ignore": {"default_duration": 5, "allow_bot_duration": False,
                           "max_duration": 30},
    })
    d.apply_ignore("s", "u", "all", 100)
    until = d._ignored[("s", "u", "poke")]
    check("all 热更新默认时长", 4 <= until - time.time() <= 6, until - time.time())
    check("all 热更新后 allow_bot_duration=false 生效",
          until - time.time() < 10, until - time.time())

    # Z 版 schema 前缀键归一化（poke_enabled/poke_default_duration/…）
    dz = chat_enhance.HarassDetector({
        "section_poke": {"poke_enabled": True, "poke_window_seconds": 45,
                         "poke_threshold": 4, "poke_default_duration": 12,
                         "poke_allow_bot_duration": False, "poke_max_duration": 20,
                         "poke_scope": "all"},
    }, None)
    check("Z 前缀键归一化：poke 配置生效",
          dz._conf["poke"]["default_duration"] == 12
          and dz._conf["poke"]["threshold"] == 4
          and dz._conf["poke"]["scope"] == "all")

    # 单项 poke 仍使用自己的配置
    d.apply_ignore("s", "u", "poke", 0)
    until = d._ignored[("s", "u", "poke")]
    check("poke 单项仍用 poke 配置", until - time.time() > 900, until - time.time())
    return all(results)


def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    ok = True
    for directory in dirs:
        print(f"=== {directory.name} ===")
        ok = run(directory) and ok
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
