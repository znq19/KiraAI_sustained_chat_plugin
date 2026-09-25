"""S/Z 插件测试入口（无需框架）：python3 tests/run_tests.py

用法:
    python3 tests/run_tests.py                 # 测当前这份代码
    python3 tests/run_tests.py ./s ./z         # 可传多个插件目录（前后对比）
"""
import subprocess
import sys
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
PLUGIN = HERE.parent

SUITES = ["test_guard_await.py", "test_guard_known_media.py", "test_prefetch_timing.py", "test_prefetch_scope.py",
          "test_foreign_event.py", "test_queue_timing.py", "test_media_fixes.py", "test_empty_stop.py"]

rc = 0
for suite in SUITES:
    print(f"\n########## {suite} ##########")
    r = subprocess.call([sys.executable, str(HERE / suite), str(PLUGIN)])
    rc = rc or r
print("\n" + ("ALL TEST SUITES PASSED" if rc == 0 else "SOME TEST SUITES FAILED"))
sys.exit(rc)
