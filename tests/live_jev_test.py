# -*- coding: utf-8 -*-
"""S/Z 版：JEV 实联测试（可选，需环境变量提供中转站凭据）。

  export KIRA_JEV_BASE=https://api.typesafe.ai   # 或官方 https://api.typesafe.ai
  export KIRA_JEV_KEY=sk-...                        # 不写入仓库
  python3 tests/live_jev_test.py [plugin_dir]

未设置 KIRA_JEV_KEY 时只跑离线检查（端点规整/熔断/配置解析）并优雅跳过实联。

实联断言：
  L1 守望场景：沉默后被点名提问 ⇒ reply（且置信度合理）
  L2 守望场景：沉默后用户间闲聊/表情 ⇒ wait
  L3 触发场景：值得回应的分享/提问 ⇒ reply
  L4 触发场景：灌水/纯表情 ⇒ wait
  L5 插件 _jev_judge 全链路（手动兜底链路 → 真 JEV）
  L6 熔断在实联错误端点下生效（错误地址 3 连败 → ready=False）
并打印每次判定的 置信度/概率/耗时/input token（成本观测）。
"""
import asyncio
import importlib.util
import json
import os
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
          + ((" — " + str(detail)) if (detail and not cond) else ""), flush=True)

def note(msg):
    print(f"  · {msg}", flush=True)

def load_plugin(plugin_dir: Path):
    for m in ("main", "queue_merge", "media_recognize", "chat_enhance", "jev"):
        sys.modules.pop(m, None)
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location(f"main_live_{plugin_dir.name}",
                                                  plugin_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod, sys.modules["jev"]

BASE = os.environ.get("KIRA_JEV_BASE", "https://api.typesafe.ai").strip()
KEY = os.environ.get("KIRA_JEV_KEY", "").strip()
MODEL = os.environ.get("KIRA_JEV_MODEL", "jev-latest").strip()

WATCH_REPLY_STATE = (
    "[渔丈人] 紫小贱 你说这个表情包是不是在撒娇\n"
    "[C7] 对啊  bot 你怎么看\n"
    "[渔丈人] 她刚不理我了哈哈"
)
WATCH_WAIT_STATE = (
    "[渔丈人] [表情包]\n"
    "[C7] 笑死 这个图绝了\n"
    "[渔丈人] 我再去睡个回笼觉\n"
    "[C7] 去吧去吧"
)
TRIGGER_REPLY_STATE = (
    "[小明] 有人知道这个报错怎么解决吗：ImportError: cannot import name 'foo'\n"
    "[小红] 同问，卡了一下午了"
)
TRIGGER_WAIT_STATE = (
    "[小明] [表情包]\n"
    "[小红] 哈哈哈哈\n"
    "[小明] 笑死我了"
)

async def live(jev):
    log = SimpleNamespace(info=lambda *a, **k: print("   ", *a),
                          warning=lambda *a, **k: print("   [warn]", *a),
                          debug=lambda *a, **k: None)
    cli = jev.JevClient([(BASE, KEY, MODEL)], 15.0, 0.0, log)
    watch_prompt = (
        "你是群聊机器人的沉默守望裁判。机器人刚刚主动选择沉默（输出空消息）结束了上一轮对话，"
        "之后群里又来了一些新消息（见对话内容）。判断机器人现在是否应该重新开口回复。"
        "只有在新消息明确期待/适合它回应时才选 reply；用户间闲聊、表情互动、与它无关的寒暄都选 wait。"
    )
    trig_prompt = (
        "你是群聊机器人的插话裁判。机器人在旁观群消息，请判断它是否应该主动回复对话内容里的新消息。"
        "只有内容明显值得回应（提问、分享、延续它的话题、场面需要它）才选 reply；"
        "日常寒暄、表情、琐碎闲聊选 wait。"
    )

    async def judge(state, prompt, tag):
        t0 = time.time()
        r = await cli.decide_trigger(state, prompt, timeout=15.0)
        dt = time.time() - t0
        if r:
            note(f"{tag}: choice={r['choice']} conf={r['confidence']:.2f} "
                 f"P={ {k: round(v, 2) for k, v in r.get('probabilities', {}).items()} } "
                 f"{dt:.1f}s model={r.get('model')}")
        else:
            note(f"{tag}: 判定失败（None）{dt:.1f}s")
        return r

    r1 = await judge(WATCH_REPLY_STATE, watch_prompt, "守望·被点名")
    check("L1 守望：沉默后被点名 ⇒ reply", r1 and r1["choice"] == "reply",
          str(r1))
    r2 = await judge(WATCH_WAIT_STATE, watch_prompt, "守望·闲聊")
    check("L2 守望：沉默后用户闲聊 ⇒ wait", r2 and r2["choice"] == "wait",
          str(r2))
    r3 = await judge(TRIGGER_REPLY_STATE, trig_prompt, "触发·提问")
    check("L3 触发：提问求助 ⇒ reply", r3 and r3["choice"] == "reply",
          str(r3))
    r4 = await judge(TRIGGER_WAIT_STATE, trig_prompt, "触发·灌水")
    check("L4 触发：表情灌水 ⇒ wait", r4 and r4["choice"] == "wait",
          str(r4))
    note(f"累计 input token ≈ {cli.tokens}（{cli.calls} 次判定）")

def offline(jev):
    check("离线 端点规整", jev.endpoint_of(BASE) == BASE.rstrip("/") + "/v1/systemone")
    cfg = jev.JevConfig(enabled=True, links=[(BASE, "k", MODEL)])
    check("离线 配置 ready", cfg.ready)

class _Cfg(dict):
    def get_config(self, key, default=None):
        cur = self
        for part in str(key).split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

def _make_cfg():
    return _Cfg({"bot_config": {"bot": {"max_message_interval": 1.5,
                                        "max_buffer_messages": 3},
                                "agent": {"max_tool_loop": 2, "tool_call_timeout": 60}}})

async def plugin_e2e(mod):
    """插件 _jev_judge 全链路：提供商链路（base/key/model）→ 真 JEV。"""
    buf = SimpleNamespace(buffer=[], get_length=lambda: 0,
                          pop=lambda count=1: [])
    proc = SimpleNamespace(flushed=[],
                           flush_session_messages=lambda sid, **k: asyncio.sleep(0),
                           get_session_buffer_length=lambda sid: 0)
    class _Info:
        provider_name = "实联"
        model_name = MODEL
        provider_config = {"base_url": BASE, "api_key": KEY}

    class _Mgr:
        def get_model_info(self, pid, mid):
            return _Info() if (pid, mid) == ("live", "jev") else None

    ctx = SimpleNamespace(config=_make_cfg(), message_processor=proc,
                          get_buffer=lambda sid: buf,
                          get_plugin_data_dir=lambda: Path("/tmp"),
                          provider_mgr=_Mgr())
    cfg = {"section_basic": {}, "section_group_sustain": {}, "section_dm_sustain": {},
           "section_jev": {"jev_enabled": True, "jev_models": ["live:jev"],
                           "jev_timeout_ms": 15000}}
    plug = mod.DebouncePlugin(ctx, cfg)
    # 链路为懒解析（首次取客户端时构建），先取客户端再断言
    cli = plug._get_jev_client()
    check("L5 提供商链路解析成链", cli is not None and len(plug._jev_links) == 1,
          str(plug._jev_link_displays))
    r = await plug._jev_judge("qq:gm:1", WATCH_REPLY_STATE, "watch")
    check("L5 插件 _jev_judge 全链路 ⇒ reply", r == "reply", str(r))

async def breaker_live(jev):
    log = SimpleNamespace(info=lambda *a, **k: None,
                          warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    cli = jev.JevClient([("https://invalid.example.com", "sk-x", MODEL)], 3.0, 0.0, log)
    for _ in range(3):
        r = await cli.decide_trigger("x", "y", timeout=3.0)
        assert r is None
    check("L6 坏端点 3 连败熔断", not cli.ready and cli.cooldown_left > 0)

# ---- L7：aux_llm 后端实联（真实框架 ProviderManager → LLMModelClient.chat） ----
# 用真框架的提供商注册（OpenAI 兼容格式）拿到 LLMModelClient，再交给 LlmJudgeClient
# 判定——与生产环境 webui 下拉借道的路径完全一致。模型名走 KIRA_AUX_MODEL 环境变量
# （默认 deepseek/deepseek-v4.1-flash；中转站无该模型时报错即跳过，不算失败）。
AUX_MODEL = os.environ.get("KIRA_AUX_MODEL", "deepseek/deepseek-v4.1-flash").strip()

async def _aux_live_one_gen(jev, gen, core_root):
    sys.path.insert(0, core_root)
    os.chdir(core_root)
    os.makedirs("data", exist_ok=True)
    for m in list(sys.modules):
        if m == "core" or m.startswith("core."):
            del sys.modules[m]
    from core.provider.provider_manager import ProviderManager
    providers = {"provAux": {
        # 注意：format 键 = 提供商 manifest 的 name（大小写敏感，OpenAI 首字母大写）
        "format": "OpenAI", "status": "active", "model_config_version": 2,
        "name": "aux实联",
        "provider_config": {"base_url": BASE, "api_key": KEY},
        "model_config": {"llm": {"m1": {"model_name": AUX_MODEL, "config": {}}}},
    }}
    class _KC(dict):
        """KiraConfig 最小面：dict + save_config 空操作（_load_providers 回填补齐时会调）。"""
        def save_config(self):
            return None

        def get_config(self, key, default=None):
            cur = self
            for part in str(key).split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return default
            return cur
    pm = ProviderManager(None, _KC({"providers": providers}))
    try:
        pm.set_provider("provAux", providers["provAux"])   # 幂等补登记（容错 init 失败路径）
    except Exception:
        pass
    try:
        client = pm.get_model_client("provAux", "m1", "llm")   # 3.0
    except TypeError:
        client = pm.get_model_client("provAux", "m1")          # 2.x
    check(f"L7[{gen}] 真 ProviderManager 出小模型客户端", client is not None)
    if client is None:
        return
    log = SimpleNamespace(info=lambda *a, **k: None,
                          warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    cli = jev.LlmJudgeClient([("provAux:m1", client)], 30.0, 0.0, log)
    try:
        t0 = time.time()
        r = await cli.decide_trigger(WATCH_REPLY_STATE,
                                     "判断机器人是否应该回复这些消息。期待/点名则 reply，闲聊则 wait。")
        dt = time.time() - t0
    except Exception as e:
        note(f"L7[{gen}] 中转站小模型不可用（{type(e).__name__}）⇒ 跳过判定断言")
        return
    if r is None:
        note(f"L7[{gen}] 中转站小模型不可用（判定 None）⇒ 跳过判定断言")
        return
    note(f"L7[{gen}] aux_llm 判定: {r['choice']} conf={r['confidence']:.2f} {dt:.1f}s")
    check(f"L7[{gen}] aux_llm 后端实联判定（reply/wait 合法）",
          r["choice"] in ("reply", "wait"))

# ---- L8：aux_llm 本机自实测（minis-model-use 直调真实小模型当判官） ----
# 不经过框架提供商注册：用 minis-model-use CLI 包装成 LLMModelClient 形态的 shim，
# 走 LlmJudgeClient 完整链路（prompt 组装 → 真实生成 → JSON 解析 → 置信度）。
# 仅在本机有 minis-model-use 时运行；模型 id 走 KIRA_SELF_MODEL（默认 deepseek-v4.1-flash）。
SELF_MODEL = os.environ.get("KIRA_SELF_MODEL", "deepseek-v4.1-flash").strip()

class _CliLLMShim:
    """把 minis-model-use CLI 包装成 LLMModelClient 的 chat() 形状。"""

    def __init__(self, model: str):
        self.model = model

    async def chat(self, req, **kw):
        prompt = req.messages[0]["content"]
        proc = await asyncio.create_subprocess_exec(
            "minis-model-use", "run", "--model", self.model,
            "--input", json.dumps({"messages": [{"role": "user", "content": prompt}]},
                                  ensure_ascii=False),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        data = json.loads(out.decode("utf-8", "replace"))
        if proc.returncode != 0:
            raise RuntimeError(f"cli_exit_{proc.returncode}")
        return SimpleNamespace(text_response=data.get("text", ""))

async def self_model_live(jev):
    import shutil
    if not shutil.which("minis-model-use"):
        note("L8 本机无 minis-model-use ⇒ 跳过")
        return
    log = SimpleNamespace(info=lambda *a, **k: None,
                          warning=lambda *a, **k: print("   [warn]", *a),
                          debug=lambda *a, **k: None)
    cli = jev.LlmJudgeClient([("self:" + SELF_MODEL, _CliLLMShim(SELF_MODEL))],
                             45.0, 0.0, log)
    instr = ("你是群聊机器人的插话裁判。机器人在旁观群消息，请判断它是否应该主动回复"
             "对话内容里的新消息。只有内容明显值得回应（提问、求助、延续它的话题）"
             "才选 reply；日常寒暄、表情、琐碎闲聊选 wait。")
    t0 = time.time()
    r1 = await cli.decide_trigger(TRIGGER_REPLY_STATE, instr)
    t1 = time.time() - t0
    note(f"L8 自实测·提问: {r1 and r1['choice']} conf={r1 and r1['confidence']} {t1:.1f}s")
    check("L8 自实测：提问 ⇒ reply", r1 and r1["choice"] == "reply", str(r1))
    t0 = time.time()
    r2 = await cli.decide_trigger(TRIGGER_WAIT_STATE, instr)
    t2 = time.time() - t0
    note(f"L8 自实测·灌水: {r2 and r2['choice']} conf={r2 and r2['confidence']} {t2:.1f}s")
    check("L8 自实测：灌水 ⇒ wait", r2 and r2["choice"] == "wait", str(r2))

async def aux_llm_live(jev):
    for gen, root in (("2.x", os.environ.get("KIRA_CORE_2X", "")),
                      ("3.0", os.environ.get("KIRA_CORE_30", ""))):
        root = (root or "").strip()
        if root and os.path.isdir(os.path.join(root, "core", "plugin")):
            try:
                await _aux_live_one_gen(jev, gen, root)
            except Exception as e:
                note(f"L7[{gen}] 套件异常（{type(e).__name__}: {e}）⇒ 跳过")
        else:
            note(f"L7[{gen}] 未提供核心路径 ⇒ 跳过")

async def run(plugin_dir: Path):
    mod, jev = load_plugin(plugin_dir)
    offline(jev)
    if not KEY:
        print("  (未设置 KIRA_JEV_KEY ⇒ 实联部分跳过)")
        return
    await live(jev)
    await plugin_e2e(mod)
    await breaker_live(jev)
    await aux_llm_live(jev)
    await self_model_live(jev)

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
