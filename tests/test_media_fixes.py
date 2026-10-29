"""S/Z：VLM 泄露收口与媒体缓存健壮性回归 —— v2.5.20 / v1.8.11。

覆盖本次修复的三个核心行为（详见 KiraAI插件全量排查与修复方案.md §1.2/§1.4/§2.4）：

  T1 _is_valid_desc 拒绝四种失败占位文案 "(未识别)/(已过期)/(识别超时)/(下载失败)"
     （占位污染免疫：不进持久缓存、不当有效描述），接受正常描述；
  T2 _cache_set 走 upsert：先 update_image_desc_cache(md5, description=…, last_seen>0)，
     update 未命中/失败才 add_image_desc_cache(md5, text, count=1, last_seen>0)
     —— 旧实现固定 add(last_seen=0)，次日必被框架清理规则删掉且主键冲突永远写不进；
  T3 guard_captions 在 _pir_active() 为真（PIR"已加载但 handler 未摘除"的竞态窗口）
     时仍把 caption=None 占成 "" —— 堵住框架渲染 F1（caption is None → 付费识图）窗口。

Run: python3 tests/test_media_fixes.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"                     # 仓库内自带的框架桩
sys.path.insert(0, str(STUB))

from core.chat.message_elements import Image, Text  # noqa: E402

SID = "qq:gm:427674145"
results = []


def check(label, cond, detail=""):
    results.append((label, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + label + ((" — " + str(detail)) if (detail and not cond) else ""))


# ------------------------------------------------------------------ 假环境

class RecDB:
    """记录 image_desc_cache 写入调用序的 DB 桩。"""

    def __init__(self, update_ok=False):
        self.calls = []          # [("update", md5, kwargs) / ("add", md5, text, kwargs)]
        self.update_ok = update_ok

    async def get_image_desc_cache(self, md5):
        await asyncio.sleep(0)
        return None

    async def update_image_desc_cache(self, md5, **kw):
        self.calls.append(("update", md5, kw))
        return self.update_ok

    async def add_image_desc_cache(self, md5, text, **kw):
        self.calls.append(("add", md5, text, kw))
        return True


class Cfg:
    def get_config(self, key, default=None):
        return default


class Ctx:
    plugin_mgr = None
    session_mgr = None

    def __init__(self, update_ok=False):
        self.config = Cfg()
        self.db = RecDB(update_ok=update_ok)


def load_media(plugin_dir: Path):
    """独立加载 media_recognize.py（不需要 main.py / 完整插件环境）。"""
    sys.modules.pop("media_recognize", None)
    spec = importlib.util.spec_from_file_location(
        f"media_fixes_{plugin_dir.name}", plugin_dir / "media_recognize.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def make_recognizer(mod, update_ok=False):
    ctx = Ctx(update_ok=update_ok)
    recog = mod.ParallelMediaRecognizer(ctx, {"section_media_recognition": {}}, {})
    return recog, ctx


# ------------------------------------------------------------------ 用例

def t1_is_valid_desc(plugin_dir):
    mod = load_media(plugin_dir)
    recog, _ = make_recognizer(mod)
    for ph in ("(未识别)", "(已过期)", "(识别超时)", "(下载失败)"):
        check(f"T1 拒绝占位 {ph}", recog._is_valid_desc(ph) is False)
        check(f"T1 拒绝带空白占位 {ph!r}", recog._is_valid_desc(f"  {ph}  ") is False)
    check("T1 接受正常描述", recog._is_valid_desc("一只橘猫趴在键盘上") is True)
    check("T1 拒绝空串", recog._is_valid_desc("") is False)
    check("T1 拒绝 None", recog._is_valid_desc(None) is False)


async def t2_cache_set_upsert(plugin_dir):
    mod = load_media(plugin_dir)
    now0 = int(time.time())

    # (a) update 未命中 → 退回 add；两者都必须带活的 last_seen
    recog, ctx = make_recognizer(mod, update_ok=False)
    await recog._cache_set("md5aaa", "一只猫")
    kinds = [c[0] for c in ctx.db.calls]
    check("T2a 先 update 后 add（upsert 调用序）", kinds == ["update", "add"], f"calls={kinds}")
    if kinds == ["update", "add"]:
        _, _, ukw = ctx.db.calls[0]
        _, _, atext, akw = ctx.db.calls[1]
        check("T2a update 带 description 与活 last_seen",
              ukw.get("description") == "一只猫" and int(ukw.get("last_seen", 0)) >= now0,
              f"kw={ukw}")
        check("T2a add 带 text 与活 last_seen",
              atext == "一只猫" and int(akw.get("last_seen", 0)) >= now0,
              f"text={atext} kw={akw}")

    # (b) update 命中 → 不再 add（upsert 短路）
    recog, ctx = make_recognizer(mod, update_ok=True)
    await recog._cache_set("md5bbb", "一只狗")
    kinds = [c[0] for c in ctx.db.calls]
    check("T2b update 命中后不 add", kinds == ["update"], f"calls={kinds}")

    # (c) update 抛异常 → 仍兜底 add（不丢缓存写入）
    class BoomDB(RecDB):
        async def update_image_desc_cache(self, md5, **kw):
            self.calls.append(("update", md5, kw))
            raise RuntimeError("db locked")

    recog, ctx = make_recognizer(mod)
    ctx.db = BoomDB()
    recog.ctx = ctx
    await recog._cache_set("md5ccc", "一只鸟")
    kinds = [c[0] for c in ctx.db.calls]
    check("T2c update 异常后兜底 add", kinds == ["update", "add"], f"calls={kinds}")


async def t3_guard_occupies_caption_when_pir_active(plugin_dir):
    mod = load_media(plugin_dir)
    recog, _ = make_recognizer(mod)
    # 模拟 PIR"已加载但 handler 未摘除"的竞态窗口：_pir_active() 恒真
    recog._pir_active = lambda: True
    img = Image(image="base64://t3img", caption=None)
    event = SimpleNamespace(
        session=SimpleNamespace(sid=SID),
        message=SimpleNamespace(chain=[Text("看图"), img]),
    )
    n = await recog.guard_captions(event)
    check("T3 PIR 竞态窗口 guard 仍占位 caption=\"\"", img.caption == "", f"caption={img.caption!r}")
    check("T3 guard 返回占位数 1", n == 1, f"n={n}")

    # 对照：已有描述绝不覆盖
    img2 = Image(image="base64://t3img2", caption="已有描述")
    event2 = SimpleNamespace(
        session=SimpleNamespace(sid=SID),
        message=SimpleNamespace(chain=[img2]),
    )
    await recog.guard_captions(event2)
    check("T3 对照：已有 caption 不被覆盖", img2.caption == "已有描述", f"caption={img2.caption!r}")


def t4_model_label(plugin_dir):
    """识图日志必须显示人类可读模型名：3.x 的 model_id 是 uuid（用户认不出）。"""
    mod = load_media(plugin_dir)
    label = getattr(mod, "_model_label", None)
    check("T4 _model_label 存在", callable(label))
    if not callable(label):
        return
    # 3.x 形状：uuid + model_name ⇒ 取 model_name
    check("T4 3.x 取 model_name",
          label(SimpleNamespace(model_name="视觉模型A", model_id="f451aabc23ea5724")) == "视觉模型A")
    # 2.x 形状：没有 model_name ⇒ 回落 model_id（旧输出不变）
    check("T4 2.x 回落 model_id",
          label(SimpleNamespace(model_id="gpt-4o")) == "gpt-4o")
    # 空值/异常不得影响识别
    check("T4 None 安全", label(None) == "?")


SCENARIOS = [
    ("T1 占位污染免疫（_is_valid_desc）", t1_is_valid_desc),
    ("T2 缓存 upsert 带活 last_seen", t2_cache_set_upsert),
    ("T3 PIR 竞态窗口 guard 占位", t3_guard_occupies_caption_when_pir_active),
    ("T4 识图日志显示可读模型名（3.x 非 uuid）", t4_model_label),
]


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    for d in dirs:
        print("=" * 78)
        print(f"### {d.name}")
        for name, fn in SCENARIOS:
            try:
                if asyncio.iscoroutinefunction(fn):
                    await fn(d)
                else:
                    fn(d)
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
