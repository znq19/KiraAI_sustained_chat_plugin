"""S 版：AI 空消息停止 —— 静默判定（结构化）+ 空 msg 多写法 + 停止词口径 + 接线回归。

背景（用户日志实证，2026-09-25）：
  bot 输出 `<reasoning>…继续空msg。</reasoning>\\n<msg />`，`stop_on_ai_empty=True`
  却不停窗。根因是判据 `_is_empty_msg` 把**整串原始输出**用
  `^\\s*<msg\\s*/>\\s*$|^\\s*<msg>\\s*</msg>\\s*$` 框死 —— 该正则本身已覆盖
  两种写法（自闭合 / 空对，写法覆盖是刻意设计），但前置的 `<reasoning>` 一挂，
  两条分支同时失配 ⇒ 默认配置下该通路**永不触发**，bot 反复重开窗口。
  （真只有空 msg 时本来就能停。）

修复（S v2.5.22）：
  * `_is_empty_msg` → `is_silent_output`：先用 `visible_output()` 剥掉思考过程，
    再 `ET.fromstring` **结构化**判"有没有非空 <msg>"，不再堆正则；
  * 写法覆盖在原设计两种之外补齐：空白子元素、多段全空、空 msg + root 标签、
    空响应/纯空白；
  * 口径对齐框架真实产出：`<text>` 空白**不产出元素**（算空），其它标签
    （at/reply/poke/img/…）空值**也产出元素**（算非空，保守）；
  * 畸形/截断一律 False（保守，绝不误停窗口；旧行为也是 False ⇒ 零回归）；
  * 停止词判定同样改用 `visible_output()`：默认停止词（不说了/不想理/晚安…）
    恰是最常出现在 reasoning 里的词 ⇒ 旧实现会误停。

断言：
  T1 日志原文（reasoning + `<msg />`）⇒ 判定静默           ← 本次 bug 直接回归
  T2 空 msg 多写法全集 ⇒ 全部判静默（原设计两种 + 补齐的）
  T3 非静默用例 ⇒ 一律不判静默（绝不误停）
  T4 `visible_output()` 剥离口径（成对 / 未闭合两种形态）
  T5 停止词不读 reasoning（误停场景 + 真停场景）
  T6 接线：`@on.message_sent` 已注册且转发（bot_speech 按「条」检测）
  T7 静默轮不计 bot 发言（`on_llm_response(silent=True)` 直接返回）
  T8 schema ↔ 代码 `dm_proactive_prompt` 默认值逐字符一致且无孤立 `</msg>`

Run: python3 tests/test_empty_stop.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import json
import re
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
    """加载 main.py（带桩 core 包），返回插件类与实例。"""
    for m in ("main", "queue_merge", "media_recognize", "chat_enhance"):
        sys.modules.pop(m, None)
    # 让 `from queue_merge import ...` 这类同目录导入可用
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location(f"main_es_{plugin_dir.name}",
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


class _Ctx:
    def __init__(self):
        self.config = _Cfg()

    def get_plugin_data_dir(self):
        return Path("/tmp")


def make_plugin(mod, cfg=None):
    """构造插件实例（只需判定函数可用；不跑 initialize）。"""
    full_cfg = {"section_basic": {}, "section_group_sustain": {}, "section_dm_sustain": {}}
    if cfg:
        full_cfg.update(cfg)
    return mod.DebouncePlugin(_Ctx(), full_cfg)


# ---------------------------------------------------------------- T1/T2/T3
LOG_REPLY = ("<reasoning>\n紫小贱接着General New的“1500年陈酿”梗调侃“开盖即晕”，"
             "还是他们几个之间的老人味斗嘴，跟我没有任何关系，没被@也没被提到。"
             "按群规和周武定的规矩，无关消息保持沉默不刷屏，继续空msg。\n</reasoning>\n<msg />")

SILENT_CASES = [
    ("原设计①自闭合 <msg/>",          "<msg/>"),
    ("原设计①自闭合带空格 <msg />",    "<msg />"),
    ("自闭合多空格 <msg   />",         "<msg   />"),
    ("原设计②空对 <msg></msg>",        "<msg></msg>"),
    ("空对带空白内部",                  "<msg>   </msg>"),
    ("空对带换行内部",                  "<msg>\n</msg>"),
    ("空对带空白+换行",                 "<msg>\n  \n</msg>"),
    ("标签外前导/尾随空白",             "   \n <msg />  \t"),
    ("空白子元素 <msg><text/></msg>",   "<msg>\n<text>\n</text>\n</msg>"),
    ("多段全空",                        "<msg/>\n<msg />"),
    ("空 msg + root 动作标签",          "<msg/>\n<wake_extend>yes</wake_extend>"),
    ("空响应 ''",                       ""),
    ("纯空白响应",                      "  \n  "),
    ("【日志原文】reasoning + <msg />",  LOG_REPLY),
    ("reasoning + <msg/>",              "<reasoning>x</reasoning><msg/>"),
    ("只有 reasoning（无 msg）",         "<reasoning>保持沉默</reasoning>"),
    ("reasoning 未闭合且无 msg",         "<reasoning>我决定不说话"),
    ("reasoning 多行 + 空对",            "<reasoning>\n想了很久\n</reasoning>\n<msg></msg>"),
]

NON_SILENT_CASES = [
    ("正常消息",                        "<msg><text>你好</text></msg>"),
    ("reasoning + 正常消息",             "<reasoning>x</reasoning>\n<msg><text>你好</text></msg>"),
    ("reasoning 未闭合 + 有消息",         "<reasoning>思考中<msg><text>你好</text></msg>"),
    ("多段里有一段非空",                  "<msg/>\n<msg><text>在的</text></msg>"),
    ("poke 有内容",                      "<msg><poke>123</poke></msg>"),
    ("at 有内容",                        "<msg><at>123</at><text>hi</text></msg>"),
    ("img 有属性无文本（框架仍会发图）",   '<msg><img path="a.png"></img></msg>'),
    ("msg 外裸文本",                     "我就不回了吧\n<msg/>"),
    ("纯 <ignore> root 标签",            "<ignore>user|duration:60</ignore>"),
    ("截断的 <msg><text>你好",           "<msg><text>你好"),
    ("畸形 XML",                         "<msg>你好"),
]


def t1_log_case(plugin):
    return {"T1 日志原文（reasoning + <msg />）⇒ 静默": plugin.is_silent_output(LOG_REPLY)}


def t2_silent_forms(plugin):
    out = {}
    for name, xml in SILENT_CASES:
        out[f"T2 {name} ⇒ 静默"] = plugin.is_silent_output(xml)
    return out


def t3_non_silent(plugin):
    out = {}
    for name, xml in NON_SILENT_CASES:
        out[f"T3 {name} ⇒ 非静默"] = not plugin.is_silent_output(xml)
    return out


def t4_visible_output(plugin):
    vo = plugin.visible_output
    return {
        "T4 成对 reasoning 被剥（只剩 <msg />）": vo(LOG_REPLY).strip() == "<msg />",
        "T4 未闭合 reasoning 且无 msg ⇒ 剥空": vo("<reasoning>只有思考").strip() == "",
        "T4 未闭合 reasoning 但后面有 <msg> ⇒ 不剥":
            "<msg>" in vo("<reasoning>思考中<msg><text>hi</text></msg>"),
        "T4 无 reasoning 时原样（去首尾空白）": vo("  <msg/>  ").strip() == "<msg/>",
    }


def t5_stop_keywords(plugin):
    """停止词必须只看「说了什么」，不看「想了什么」。"""
    kws = ['晚安', '再见', '拜拜', '下次再聊', '下次聊', '不聊了', '不想理', '不理你', '不说了']
    f = lambda x: plugin._check_stop_keywords(plugin.visible_output(x), kws)
    return {
        "T5 reasoning 含「不说了」+ 正常消息 ⇒ 不命中":
            not f("<reasoning>他们不说了，我插一句</reasoning>\n<msg><text>这梗我懂</text></msg>"),
        "T5 reasoning 含「不想理」+ 正常消息 ⇒ 不命中":
            not f("<reasoning>这人我不想理，但话题有意思</reasoning>\n<msg><text>哈哈哈</text></msg>"),
        "T5 reasoning 含「晚安」+ 正常消息 ⇒ 不命中":
            not f("<reasoning>他们道晚安了，我补一句</reasoning>\n<msg><text>那我也来一句</text></msg>"),
        "T5 真的说了停止词 ⇒ 命中":
            f("<msg><text>不聊了，晚安</text></msg>"),
    }


def t6_hook_registered(plugin_dir: Path):
    """@on.message_sent 必须存在并转调 enhance.on_message_sent（bot_speech 的唯一入口）。"""
    src = (plugin_dir / "main.py").read_text(encoding="utf-8")
    enh = (plugin_dir / "chat_enhance.py").read_text(encoding="utf-8")
    has_hook = bool(re.search(r'@on\.message_sent', src))
    calls = bool(re.search(r'enhance\.on_message_sent\(', src))
    # bot_speech 仍按「条」写入（设计如此，不改）
    appends = len(re.findall(r'"bot_speech"\]\.append', enh))
    return {
        "T6 已注册 @on.message_sent": has_hook,
        "T6 钩子转调 enhance.on_message_sent": calls,
        "T6 bot_speech 按「条」写入（设计如此）": appends >= 1,
        "T6 on_message_sent 不再重复 note_bot_reply（避免与按轮计数冲突）":
            not re.search(r'def on_message_sent[\s\S]{0,1600}?self\._get_presence', enh),
    }


def t7_silent_turn_not_counted(plugin):
    """静默轮不该被当成一次 bot 发言（不计分、不推进休眠维持期）。"""
    calls = []
    orig = plugin.enhance._get_presence

    class _Spy:
        score_threshold = 0.0

        def note_bot_reply(self, sid, ts):
            calls.append(("presence", sid))

    plugin.enhance._get_presence = lambda is_dm=False: _Spy()
    dorm_calls = []
    orig_nr = plugin.enhance.dormant.note_reply
    plugin.enhance.dormant.note_reply = lambda sid, ts: dorm_calls.append(sid)
    try:
        ev = SimpleNamespace(sid="qq:gm:1", is_group_message=lambda: True)
        plugin.enhance.on_llm_response(ev, SimpleNamespace(text_response="<msg />"), silent=True)
        silent_ok = not calls and not dorm_calls
        plugin.enhance.on_llm_response(ev, SimpleNamespace(text_response="<msg><text>hi</text></msg>"),
                                       silent=False)
        normal_ok = bool(calls) and bool(dorm_calls)
    finally:
        plugin.enhance._get_presence = orig
        plugin.enhance.dormant.note_reply = orig_nr
    return {
        "T7 静默轮（silent=True）不计 bot 发言/不推进维持期": silent_ok,
        "T7 正常轮（silent=False）照常计数": normal_ok,
    }


def t8_schema_parity(plugin_dir: Path):
    sch = json.loads((plugin_dir / "schema.json").read_text(encoding="utf-8"))
    schema_prompt = sch["section_dm_sustain"]["fields"]["dm_proactive_prompt"]["default"]
    src = (plugin_dir / "main.py").read_text(encoding="utf-8")
    m = re.search(r'self\.dm_proactive_prompt = dm_sustain\.get\(\s*\n\s*"dm_proactive_prompt",\s*\n\s*"(.*?)"\s*\n\s*\)',
                  src, re.S)
    code_prompt = m.group(1) if m else None
    return {
        "T8 dm_proactive_prompt 两处默认值逐字符一致": code_prompt == schema_prompt,
        "T8 不含孤立 </msg>（畸形标签样板）":
            "</msg>" not in schema_prompt and (code_prompt is None or "</msg>" not in code_prompt),
        "T8 使用合法 <msg/> 表达「可以不说话」": "<msg/>" in schema_prompt,
    }


def t9_old_regex_actually_broken(plugin):
    """反向验证（行为级）：把旧判据拿来对照，证明它**真的**在这些用例上错。

    只断言"新实现通过"是不够的 —— 还要证明旧实现**确实**错，
    否则判据可能是个恒真式（永远通过，什么也没检查）。
    """
    OLD = r'^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$'
    old_is_empty = lambda x: bool(re.match(OLD, x))

    must_be_silent = [
        LOG_REPLY,                                   # 本次 bug 的原始场景
        "<reasoning>保持沉默</reasoning>",             # 只有 reasoning
        "<reasoning>我决定不说话",                     # 未闭合 reasoning
        "<msg>\n<text>\n</text>\n</msg>",             # 空白子元素
        "<msg/>\n<msg />",                            # 多段全空
        "<msg/>\n<wake_extend>yes</wake_extend>",     # 空 msg + root 标签
        "",                                           # 空响应
        "  \n  ",                                     # 纯空白
    ]
    # 旧实现在这些用例上的判定（全应为 False = 漏判）
    old_misses = sum(1 for x in must_be_silent if not old_is_empty(x))
    new_hits = sum(1 for x in must_be_silent if plugin.is_silent_output(x))
    return {
        f"T9 旧判据在这些用例上漏判 {old_misses}/{len(must_be_silent)} 条":
            old_misses == len(must_be_silent),
        f"T9 新判据全部命中 {new_hits}/{len(must_be_silent)} 条":
            new_hits == len(must_be_silent),
    }


SCENARIOS = [
    ("T1 日志原文回归", t1_log_case),
    ("T2 空 msg 多写法全集", t2_silent_forms),
    ("T3 非静默（绝不误停）", t3_non_silent),
    ("T4 visible_output 剥离口径", t4_visible_output),
    ("T5 停止词不读 reasoning", t5_stop_keywords),
    ("T6 message_sent 接线", t6_hook_registered),
    ("T7 静默轮不计 bot 发言", t7_silent_turn_not_counted),
    ("T8 schema 默认值一致", t8_schema_parity),
    ("T9 反向验证（旧判据确实错）", t9_old_regex_actually_broken),
]


async def main():
    plugin_dirs = [Path(p) for p in sys.argv[1:]] or [HERE.parent]
    for plugin_dir in plugin_dirs:
        print(f"\n===== 插件目录: {plugin_dir} =====")
        try:
            mod = load_main(plugin_dir)
            plugin = make_plugin(mod)
        except Exception as e:  # noqa: BLE001
            check(f"加载插件失败: {e}", False)
            continue
        for title, fn in SCENARIOS:
            print(f"\n---------- {title} ----------")
            try:
                if title.startswith(("T6", "T8")):
                    res = fn(plugin_dir)
                else:
                    res = fn(plugin)
                for k, v in res.items():
                    check(k, v)
            except Exception as e:  # noqa: BLE001
                import traceback
                traceback.print_exc()
                check(f"{title} 抛异常: {e}", False)
    n_fail = sum(1 for _, ok in results if not ok)
    print(f"\n===== {len(results) - n_fail}/{len(results)} 通过 =====")
    sys.exit(1 if n_fail else 0)

if __name__ == "__main__":
    asyncio.run(main())
