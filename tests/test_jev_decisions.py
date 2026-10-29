"""S/Z 版：JEV 触发决策 —— 协议层 + 判定逻辑 + confirm/boost/守望状态机。

覆盖：
  P1 endpoint_of 端点规整（裸域/带 v1/带完整路径/中转站）
  P2 JevConfig 链过滤（缺地址/密钥剔除、$$ENV 解析）
  P3 decide_trigger 协议解析：reply/wait + 置信度 + 概率；坏载荷；低置信弃权
  P4 熔断：连续 3 失败 → ready=False（冷却中直接 None），成功复位
  P5 有序回退链：第 1 档失败自动换第 2 档
  M1 _jev_mode_has 真值表（off/confirm/boost/confirm_boost）
  M2 _render_msgs_for_jev 渲染与截断（昵称/媒体占位/总量截断保最新）
  D1 deferred confirm 否决：机械命中 + JEV wait ⇒ 不 flush
  D2 deferred confirm 确认：机械命中 + JEV reply ⇒ flush
  D3 deferred 弃权：JEV None ⇒ 机械命中照旧 flush
  D4 deferred boost 补触发：机械未中 + JEV reply ⇒ flush
  D5 deferred boost 维持：机械未中 + JEV wait/None ⇒ 不 flush
  D6 sustain 场景窗口已关 ⇒ 即使 reply 也不触发（S 版）
  W1 守望武装/触发 reply ⇒ 解闩 + flush + 解除守望（S 版）
  W2 守望触发 wait ⇒ 冷却 + 维持停窗（S 版）
  W3 守望期间真唤醒 ⇒ _watch_disarm ⇒ 不再 active（S 版）

Run: python3 tests/test_jev_decisions.py [<plugin_dir> ...]
"""
import asyncio
import importlib.util
import json
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

def load_plugin(plugin_dir: Path):
    for m in ("main", "queue_merge", "media_recognize", "chat_enhance", "jev"):
        sys.modules.pop(m, None)
    sys.path.insert(0, str(plugin_dir))
    spec = importlib.util.spec_from_file_location(f"main_jev_{plugin_dir.name}",
                                                  plugin_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    jev = sys.modules["jev"]
    return mod, jev

# ---------------------------------------------------------------- 桩
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

    async def flush_session_messages(self, sid, **kw):
        # 镜像真 PluginContext：ctx 级方法就是委托给 message_processor
        return await self.message_processor.flush_session_messages(sid, **kw)

    def get_plugin_data_dir(self):
        return Path("/tmp")

    def get_buffer(self, sid):
        return self._buf

class _FakeJevClient:
    """canned JEV 客户端：按队列返回决策；None 表示失败。"""
    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = 0

    @property
    def ready(self):
        return True

    async def decide_trigger(self, state, instructions, timeout=None):
        self.calls += 1
        a = self.answers.pop(0) if self.answers else None
        return a

def _text_elem(t):
    from core.chat.message_elements import Text
    return Text(t)

def _msg(mid, text, nick="n"):
    return SimpleNamespace(message_id=mid, is_mentioned=False,
                           sender=SimpleNamespace(nickname=nick, user_id="u"),
                           chain=[_text_elem(text)])

SID = "qq:gm:10001"

def make_plugin(mod, extra_cfg=None):
    buf = _Buf()
    proc = _Processor(buf)
    ctx = _Ctx(buf, proc)
    cfg = {"section_basic": {}, "section_group_sustain": {}, "section_dm_sustain": {}}
    if extra_cfg:
        cfg.update(extra_cfg)
    plug = mod.DebouncePlugin(ctx, cfg)
    plug.jev_enabled = True
    return plug, buf, proc

# ---------------------------------------------------------------- P1/P2 协议静态
def p1_endpoint(jev):
    e = jev.endpoint_of
    return {
        "P1 裸域": e("https://api.typesafe.ai") == "https://api.typesafe.ai/v1/systemone",
        "P1 带 /v1": e("https://x.com/v1") == "https://x.com/v1/systemone",
        "P1 完整路径": e("https://x.com/v1/systemone") == "https://x.com/v1/systemone",
        "P1 chat/completions 端点": e("https://x.com/v1/chat/completions") == "https://x.com/v1/systemone",
        "P1 中转站": e("https://jev-relay.example.com") == "https://jev-relay.example.com/v1/systemone",
        "P1 尾斜杠": e("https://x.com/") == "https://x.com/v1/systemone",
    }

def p2_config(jev):
    c = jev.JevConfig(enabled=True, links=[
        ("https://a.com", "k1", "m1"),
        ("", "k2", "m2"),            # 缺地址剔除
        ("https://b.com", "", "m3"),  # 缺密钥剔除
        ("https://c.com", "$$__JEV_TEST_NONE__", "m4"),  # 环境变量缺省剔除
    ])
    return {
        "P2 链过滤只留合法档": c.links == (("https://a.com", "k1", "m1"),),
        "P2 ready": c.ready,
        "P2 空链不 ready": not jev.JevConfig(enabled=True, links=[]).ready,
        "P2 未启用不 ready": not jev.JevConfig(enabled=False, links=[("https://a", "k", "m")]).ready,
    }

# ------------------------------------------------ P2b 提供商分组（3.1.1 决策模型）
class _ProvInfo:
    def __init__(self, mid, name, base, key, pname="测试商"):
        self.model_id = mid
        self.model_name = name
        self.provider_id = "p"
        self.provider_name = pname
        self.provider_config = {"base_url": base, "api_key": key}

class _ProvMgr:
    def __init__(self, mapping):
        self.mapping = mapping
    def get_model_info(self, pid, mid):
        return self.mapping.get(f"{pid}:{mid}")

def p2b_provider_groups(mod):
    """3.1.1 的 TypeSafe 决策模型（decision 分组）与旧 llm 组合同链；空值零变化。"""
    llm = _ProvInfo("uuid-llm", "llm-readable", "https://llm.example/v1", "k1")
    dec = _ProvInfo("uuid-dec", "jev-latest", "https://dec.example/v1", "k2")
    dup = _ProvInfo("uuid-dup", "llm-readable", "https://llm.example/v1", "k1")  # 与 llm 档完全相同
    mgr = _ProvMgr({"p:uuid-llm": llm, "p:uuid-dec": dec, "p:uuid-dup": dup})
    out = {}

    # (a) 旧配置（只有 llm 组）⇒ 与 2.7.4 完全一致
    plug, _, _ = make_plugin(mod, {"section_jev": {"jev_models": ["p:uuid-llm"]}})
    plug.ctx.provider_mgr = mgr
    plug._jev_resolve_links()
    out["P2b 仅 llm 组：链路与旧版一致"] = plug._jev_links == [
        ("https://llm.example/v1", "k1", "llm-readable")]
    out["P2b 仅 llm 组：显示名用 model_name"] = (
        plug._jev_link_displays == ["llm-readable (测试商) ⇒ llm-readable@llm.example"])

    # (b) 只选决策组也能用（新功能）
    plug, _, _ = make_plugin(mod, {"section_jev": {"jev_decision_models": ["p:uuid-dec"]}})
    plug.ctx.provider_mgr = mgr
    plug._jev_resolve_links()
    out["P2b 仅决策组：链路成立"] = plug._jev_links == [
        ("https://dec.example/v1", "k2", "jev-latest")]

    # (c) 两组都有 ⇒ 顺序 llm → decision
    plug, _, _ = make_plugin(mod, {"section_jev": {
        "jev_models": ["p:uuid-llm"], "jev_decision_models": ["p:uuid-dec"]}})
    plug.ctx.provider_mgr = mgr
    plug._jev_resolve_links()
    out["P2b 两组顺序 llm→decision"] = plug._jev_links == [
        ("https://llm.example/v1", "k1", "llm-readable"),
        ("https://dec.example/v1", "k2", "jev-latest")]

    # (d) 跨组重复档去重
    plug, _, _ = make_plugin(mod, {"section_jev": {
        "jev_models": ["p:uuid-llm"], "jev_decision_models": ["p:uuid-dup"]}})
    plug.ctx.provider_mgr = mgr
    plug._jev_resolve_links()
    out["P2b 跨组重复去重"] = plug._jev_links == [
        ("https://llm.example/v1", "k1", "llm-readable")]

    # (e) aux_llm 只用 llm 组（决策模型不是 LLM，不能混进来）
    plug, _, _ = make_plugin(mod, {"section_jev": {
        "jev_models": ["p:uuid-llm"], "jev_decision_models": ["p:uuid-dec"]}})
    plug.ctx.provider_mgr = mgr
    seen = []
    plug.ctx.get_llm_client = lambda model_uuid=None, **k: seen.append(model_uuid) or None
    plug._jev_resolve_llm_clients()
    out["P2b aux_llm 只取 llm 组"] = seen == ["p:uuid-llm"]

    # (f) 旧版手动连接字段已从设置页移除，但旧配置仍必须生效
    plug, _, _ = make_plugin(mod, {"section_jev": {
        "jev_api_key": "sk-legacy", "jev_base_url": "https://legacy",
        "jev_model_name": "jev-latest"}})
    plug.ctx.provider_mgr = mgr
    plug._jev_resolve_links()
    out["P2b 旧手动配置已忽略（不隐形生效）"] = plug._jev_links == []

    # (g) 两组各自 10 档：llm 满 10 也不会挤掉 decision
    many = {("p", "uuid-dec"): dec}
    refs = []
    for i in range(10):
        many[("p", "m%d" % i)] = _ProvInfo("m%d" % i, "m%d" % i, "https://llm%d.example/v1" % i, "k")
        refs.append("p:m%d" % i)
    plug, _, _ = make_plugin(mod, {"section_jev": {
        "jev_models": refs, "jev_decision_models": ["p:uuid-dec"]}})
    plug.ctx.provider_mgr = _ProvMgr({"%s:%s" % k: v for k, v in many.items()})
    plug._jev_resolve_links()
    out["P2b llm 满 10 不挤掉 decision"] = (
        len(plug._jev_links) == 11 and plug._jev_links[-1][2] == "jev-latest")
    return out

# ---------------------------------------------------------------- P3/P4/P5 客户端
async def p3_decide(jev):
    log = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    cli = jev.JevClient([("https://x", "k", "m")], 2.0, 0.0, log)
    payload_seen = {}
    async def fake_post(payload, timeout):
        payload_seen.update(payload)
        return {"model": "jev-1.13.0",
                "answers": {"trigger": {"type": "choice", "choice": "reply",
                                        "confidence": 0.87,
                                        "probabilities": {"reply": 0.87, "wait": 0.13}}},
                "usage": {"input_tokens": 321}}
    cli._post_sync = lambda p, t: json.loads(json.dumps(fake_post_sync(p, t)))
    def fake_post_sync(p, t):
        payload_seen.update(p)
        return {"model": "jev-1.13.0",
                "answers": {"trigger": {"type": "choice", "choice": "reply",
                                        "confidence": 0.87,
                                        "probabilities": {"reply": 0.87, "wait": 0.13}}},
                "usage": {"input_tokens": 321}}
    cli._post_sync = fake_post_sync
    r = await cli.decide_trigger("用户: 在吗", "判断")
    ok1 = r and r["choice"] == "reply" and abs(r["confidence"] - 0.87) < 1e-6
    ok2 = payload_seen.get("state") == "用户: 在吗"
    ok3 = payload_seen.get("questions", {}).get("trigger", {}).get("type") == "choice"
    ok4 = set(payload_seen.get("questions", {}).get("trigger", {}).get("criteria", {})) == {"reply", "wait"}
    ok5 = cli.tokens == 321 and cli.calls == 1
    # 低置信弃权
    cli2 = jev.JevClient([("https://x", "k", "m")], 2.0, 0.5, log)
    cli2._post_sync = lambda p, t: {"answers": {"trigger": {"type": "choice", "choice": "wait",
                                    "confidence": 0.3, "probabilities": {}}}, "usage": {}}
    r2 = await cli2.decide_trigger("x", "y")
    ok6 = r2 is not None and r2.get("_abstain") is True and cli2.abstains == 1
    return {"P3 reply 解析": ok1, "P3 state 透传": ok2, "P3 问题类型 choice": ok3,
            "P3 criteria 双档": ok4, "P3 用量记账": ok5, "P3 低置信弃权标记": ok6}

async def p4_breaker(jev):
    log = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    cli = jev.JevClient([("https://x", "k", "m")], 0.5, 0.0, log)
    def bad(p, t):
        raise ConnectionError("down")
    cli._post_sync = bad
    for _ in range(3):
        r = await cli.decide_trigger("x", "y")
        assert r is None
    ok1 = cli.fails >= 3 and not cli.ready
    ok2 = cli.cooldown_left > 0
    # 冷却中直接 None
    ok3 = (await cli.decide_trigger("x", "y")) is None
    # 冷却过期后 ready 恢复
    cli.opened_at = time.time() - 400
    ok4 = cli.ready
    # 成功复位
    cli._post_sync = lambda p, t: {"answers": {"trigger": {"type": "choice", "choice": "wait",
                                   "confidence": 0.9, "probabilities": {}}}, "usage": {}}
    r2 = await cli.decide_trigger("x", "y")
    ok5 = r2 and r2["choice"] == "wait" and cli.fails == 0
    return {"P4 三连败熔断": ok1, "P4 冷却读数": ok2, "P4 冷却中短路 None": ok3,
            "P4 冷却过期恢复": ok4, "P4 成功复位": ok5}

async def p5_fallback_chain(jev):
    log = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    # 本测试在 urlopen 层模拟链路故障/恢复 ⇒ 明确钉在 urllib 投递路径
    #（httpx 原生路径的回退链由 test_jev_ga_upgrade.py 覆盖）
    _orig_httpx = jev.httpx
    jev.httpx = None
    cli = jev.JevClient([("https://bad1", "k1", "m1"), ("https://good", "k2", "m2")], 0.5, 0.0, log)
    # 在 urlopen 层模拟链路故障/恢复（保留真实 _post_sync 的逐档回退逻辑）
    calls = []
    class _Resp:
        def __init__(self, body):
            self._b = body

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return self._b.encode()

    def fake_urlopen(req, timeout=None):
        body = json.loads(req.data.decode())
        calls.append((req.full_url, body.get("model")))
        if "bad1" in req.full_url:
            raise ConnectionError("first link down")
        return _Resp(json.dumps(
            {"answers": {"trigger": {"type": "choice", "choice": "reply",
                         "confidence": 0.9, "probabilities": {}}}, "usage": {}},
            ensure_ascii=False))
    orig = jev.urllib.request.urlopen
    jev.urllib.request.urlopen = fake_urlopen
    try:
        r = await cli.decide_trigger("x", "y")
    finally:
        jev.urllib.request.urlopen = orig
        jev.httpx = _orig_httpx   # 恢复传输探测
    ok1 = len(calls) == 2 and "bad1" in calls[0][0] and "good" in calls[1][0]
    ok2 = calls[0][1] == "m1" and calls[1][1] == "m2"
    return {"P5 第 1 档失败换第 2 档": ok1 and ok2,
            "P5 换档后判定成功": r is not None and r["choice"] == "reply"}

# ---------------------------------------------------------------- M1/M2 宿主静态
def m1_mode_table(mod):
    f = mod.DebouncePlugin._jev_mode_has
    return {
        "M1 off": (not f("off", "confirm")) and (not f("off", "boost")),
        "M1 confirm": f("confirm", "confirm") and not f("confirm", "boost"),
        "M1 boost": f("boost", "boost") and not f("boost", "confirm"),
        "M1 confirm_boost": f("confirm_boost", "confirm") and f("confirm_boost", "boost"),
    }

def m2_render(mod):
    plug, buf, proc = make_plugin(mod)
    msgs = [_msg(str(i), f"内容{i}", nick=f"用户{i}") for i in range(15)]
    out = plug._render_msgs_for_jev(msgs, max_msgs=10)
    ok1 = "用户14" in out and "用户0" not in out      # 只留最新 10 条
    ok2 = "[用户14] 内容14" in out
    long_out = plug._render_msgs_for_jev([_msg("1", "长" * 5000)], total=2000)
    ok3 = len(long_out) <= 2000
    return {"M2 max_msgs 截断保最新": ok1, "M2 昵称内容格式": ok2, "M2 总量截断": ok3}

# ---------------------------------------------------------------- D1-D6 deferred
async def _deferred_case(mod, decision, mechanical_hit, scene="proactive"):
    plug, buf, proc = make_plugin(mod)
    fake = _FakeJevClient([decision])
    plug._jev_client = fake
    m = _msg("m1", "最新消息")
    buf.buffer.append(SimpleNamespace(message=m))
    await plug._jev_deferred_trigger(SID, "m1", scene, mechanical_hit)
    return proc.flushed, fake.calls

async def d1_confirm_veto(mod):
    flushed, calls = await _deferred_case(mod, {"choice": "wait", "confidence": 0.9,
                                                "probabilities": {}, "model": "m"}, True)
    return {"D1 confirm 被否决不 flush": flushed == [], "D1 JEV 被调一次": calls == 1}

async def d2_confirm_ok(mod):
    flushed, _ = await _deferred_case(mod, {"choice": "reply", "confidence": 0.9,
                                            "probabilities": {}, "model": "m"}, True)
    return {"D2 confirm 确认 flush": flushed == [SID]}

async def d3_abstain_fallback(mod):
    flushed, _ = await _deferred_case(mod, None, True)
    return {"D3 弃权机械照旧 flush": flushed == [SID]}

async def d4_boost_hit(mod):
    flushed, _ = await _deferred_case(mod, {"choice": "reply", "confidence": 0.8,
                                            "probabilities": {}, "model": "m"}, False)
    return {"D4 boost 补触发 flush": flushed == [SID]}

async def d5_boost_hold(mod):
    f1, _ = await _deferred_case(mod, {"choice": "wait", "confidence": 0.8,
                                       "probabilities": {}, "model": "m"}, False)
    f2, _ = await _deferred_case(mod, None, False)
    return {"D5 boost wait 不 flush": f1 == [], "D5 boost 弃权不 flush": f2 == []}

async def d6_sustain_window_closed(mod):
    plug, buf, proc = make_plugin(mod, {"section_group_sustain": {"sustain_enabled": True}})
    plug._jev_client = _FakeJevClient([{"choice": "reply", "confidence": 0.9,
                                        "probabilities": {}, "model": "m"}])
    m = _msg("m1", "x")
    buf.buffer.append(SimpleNamespace(message=m))
    # 窗口不在（sustain_until 空）⇒ sustain 场景不触发
    await plug._jev_deferred_trigger(SID, "m1", "sustain", True)
    ok1 = proc.flushed == []
    # 窗口在 ⇒ 触发 + 命中簿记
    plug.sustain_until[SID] = time.time() + 100
    await plug._jev_deferred_trigger(SID, "m1", "sustain", True)
    ok2 = proc.flushed == [SID]
    ok3 = plug.sustain_count.get(SID) == 1
    ok4 = "m1" in (plug.sustain_hit_ids.get(SID) or set())
    return {"D6 窗口关不触发": ok1, "D6 窗口开触发 flush": ok2,
            "D6 sustain 计数+1": ok3, "D6 hit_ids 登记": ok4}

# ---------------------------------------------------------------- W1-W3 守望（S 版）
async def w1_watch_reply(mod):
    plug, buf, proc = make_plugin(mod, {"section_group_sustain": {"sustain_enabled": True}})
    plug._jev_client = _FakeJevClient([{"choice": "reply", "confidence": 0.92,
                                        "probabilities": {}, "model": "m"}])
    plug.jev_watch_enabled = True
    # 空 msg 停窗已落闩 + 守望武装
    plug.sustain_stopped[SID] = True
    plug._arm_watch_after_empty_stop(SID)
    ok0 = plug._watch_active(SID)
    # 停窗后新到两条消息
    buf.buffer.append(SimpleNamespace(message=_msg("n1", "紫小贱 这个问题你怎么看")))
    buf.buffer.append(SimpleNamespace(message=_msg("n2", "对啊想听听")))
    gen = plug._watch_gen.get(SID, 0)
    await plug._watch_fire(SID, gen)
    return {
        "W1 守望武装 active": ok0,
        "W1 reply ⇒ flush": proc.flushed == [SID],
        "W1 reply ⇒ 解闩": not plug.sustain_stopped.get(SID, False),
        "W1 reply ⇒ 解除守望": not plug._watch_active(SID),
    }

async def w2_watch_wait(mod):
    plug, buf, proc = make_plugin(mod, {"section_group_sustain": {"sustain_enabled": True}})
    plug._jev_client = _FakeJevClient([{"choice": "wait", "confidence": 0.95,
                                        "probabilities": {}, "model": "m"}])
    plug.jev_watch_enabled = True
    plug.sustain_stopped[SID] = True
    plug._arm_watch_after_empty_stop(SID)
    buf.buffer.append(SimpleNamespace(message=_msg("n1", "哈哈哈哈")))
    gen = plug._watch_gen.get(SID, 0)
    await plug._watch_fire(SID, gen)
    return {
        "W2 wait ⇒ 不 flush": proc.flushed == [],
        "W2 wait ⇒ 闩保持": plug.sustain_stopped.get(SID, False) is True,
        "W2 wait ⇒ 冷却登记": plug._watch_cooldown_until.get(SID, 0) > time.time(),
    }

async def w3_watch_disarm_on_wake(mod):
    plug, buf, proc = make_plugin(mod, {"section_group_sustain": {"sustain_enabled": True}})
    plug.jev_watch_enabled = True
    plug.sustain_stopped[SID] = True
    plug._arm_watch_after_empty_stop(SID)
    ok0 = plug._watch_active(SID)
    # 真唤醒路径（handle_msg 唤醒分支的等效操作）
    plug.sustain_stopped.pop(SID, None)
    plug.sustain_hit_ids.pop(SID, None)
    plug._watch_disarm(SID)
    return {"W3 武装时 active": ok0,
            "W3 真唤醒解闩后不再 active": not plug._watch_active(SID)}

# ---------------------------------------------------------------- L 系：小模型判官（aux_llm 后端）
class _FakeLLM:
    """canned 小模型客户端：chat() 返回 SimpleNamespace(text_response=...)。"""
    def __init__(self, texts):
        self.texts = list(texts)
        self.calls = 0

    async def chat(self, req, **kw):
        self.calls += 1
        t = self.texts.pop(0) if self.texts else '{"choice":"wait","confidence":0.9}'
        if isinstance(t, Exception):
            raise t
        return SimpleNamespace(text_response=t)

async def l1_llm_parse_and_chain(jev):
    log = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                          debug=lambda *a, **k: None)
    # 纯解析函数
    p = jev.parse_llm_answer
    oks = [
        p('{"choice":"reply","confidence":0.9}')[0] == "reply",
        p('……\n{"choice":"wait","confidence":0.2}\n……')[0] == "wait",
        p('I choose to reply now') == ("reply", 0.5),
        p('wait 一下')[0] == "wait",
        p('胡说八道没有答案')[0] is None,
        p('{"choice":"reply","confidence":"bad"}')[1] == 0.5,
        p('{"choice":"reply","confidence":9}')[1] == 1.0,   # clamp
    ]
    # 链式：第 1 档解析失败 → 换第 2 档
    c1, c2 = _FakeLLM(["不是JSON"]), _FakeLLM(['{"choice":"reply","confidence":0.8}'])
    cli = jev.LlmJudgeClient([("r1", c1), ("r2", c2)], 3.0, 0.0, log)
    r = await cli.decide_trigger("state", "判断")
    chain_ok = c1.calls == 1 and c2.calls == 1 and r and r["choice"] == "reply" \
        and r.get("_backend") == "aux_llm"
    # 低置信弃权
    c3 = _FakeLLM(['{"choice":"reply","confidence":0.2}'])
    cli2 = jev.LlmJudgeClient([("r3", c3)], 3.0, 0.6, log)
    r2 = await cli2.decide_trigger("s", "i")
    abstain_ok = r2 is not None and r2.get("_abstain") is True
    # 全档异常 → None + 熔断计数
    c4 = _FakeLLM([ConnectionError("x")])
    c5 = _FakeLLM([TimeoutError("y")])
    cli3 = jev.LlmJudgeClient([("r4", c4), ("r5", c5)], 3.0, 0.0, log)
    r3 = await cli3.decide_trigger("s", "i")
    fail_ok = r3 is None and cli3.fails == 1
    return {
        "L1 解析：JSON块": oks[0], "L1 解析：噪声包裹": oks[1],
        "L1 解析：关键词兜底 reply": oks[2], "L1 解析：关键词兜底 wait": oks[3],
        "L1 解析：无答案 None": oks[4], "L1 解析：坏置信度回落0.5": oks[5],
        "L1 解析：置信度钳制": oks[6],
        "L1 解析失败自动换档": chain_ok, "L1 低置信弃权": abstain_ok,
        "L1 全档失败 None 且计熔断": fail_ok,
    }

async def l2_backend_dispatch(mod, jev):
    """_get_jev_client 后端分派：jev/aux_llm 各回各家。"""
    plug, buf, proc = make_plugin(mod)
    plug.jev_backend = "aux_llm"
    plug.jev_models = ["prov:m1"]
    fake_client = _FakeLLM(['{"choice":"reply","confidence":0.9}'])
    plug.ctx.get_llm_client = lambda model_uuid=None, **k: fake_client if model_uuid == "prov:m1" else None
    plug.ctx.provider_mgr = SimpleNamespace(
        get_model_info=lambda pid, mid, **k: SimpleNamespace(
            model_name="小模型", provider_name="测试商", model_id=mid,
            provider_config={}, model_config={}))
    cli = plug._get_jev_client()
    ok1 = isinstance(cli, jev.LlmJudgeClient) if hasattr(jev, "LlmJudgeClient") else False
    r = await cli.decide_trigger("state", "判断") if cli else None
    ok2 = r and r["choice"] == "reply"
    # jev 后端无链路 → None（不炸）
    plug2, _, _ = make_plugin(mod)
    plug2.jev_backend = "jev"
    ok3 = plug2._get_jev_client() is None
    return {"L2 aux_llm 分派出 LlmJudgeClient": ok1, "L2 小模型判定可用": bool(ok2),
            "L2 jev 后端无链路透 None": ok3}

# ---------------------------------------------------------------- R 系：评分快照/回补
async def r1_refund_on_veto(mod):
    plug, buf, proc = make_plugin(mod)
    plug._jev_client = _FakeJevClient([{"choice": "wait", "confidence": 0.9,
                                        "probabilities": {}, "model": "m"}])
    pres = plug.enhance._get_presence(False)
    pres._scores[SID] = 66.0
    snap = plug.enhance.score_snapshot(SID)
    pres._scores.pop(SID, None)          # 模拟 boost 清零
    pres._scores[SID] = 2.0              # 否决期间新消息攒了 2 分
    m = _msg("m1", "x")
    buf.buffer.append(SimpleNamespace(message=m))
    await plug._jev_deferred_trigger(SID, "m1", "proactive", True, score_snapshot=snap)
    cur = pres.score(SID, None)
    return {"R1 否决后评分回补至快照": cur == 66.0,
            "R1 否决不 flush": proc.flushed == []}

async def r2_no_refund_on_confirm(mod):
    plug, buf, proc = make_plugin(mod)
    plug._jev_client = _FakeJevClient([{"choice": "reply", "confidence": 0.9,
                                        "probabilities": {}, "model": "m"}])
    pres = plug.enhance._get_presence(False)
    pres._scores[SID] = 50.0
    snap = plug.enhance.score_snapshot(SID)
    pres._scores.pop(SID, None)          # boost 清零（触发即清分，正常语义）
    m = _msg("m1", "x")
    buf.buffer.append(SimpleNamespace(message=m))
    await plug._jev_deferred_trigger(SID, "m1", "proactive", True, score_snapshot=snap)
    cur = pres.score(SID, None)
    return {"R2 确认触发不回补（清分语义保持）": cur == 0.0,
            "R2 确认 flush": proc.flushed == [SID]}

async def r3_no_refund_on_abstain(mod):
    plug, buf, proc = make_plugin(mod)
    plug._jev_client = _FakeJevClient([None])   # 弃权 ⇒ 机械命中照旧触发
    pres = plug.enhance._get_presence(False)
    pres._scores[SID] = 50.0
    snap = plug.enhance.score_snapshot(SID)
    pres._scores.pop(SID, None)
    m = _msg("m1", "x")
    buf.buffer.append(SimpleNamespace(message=m))
    await plug._jev_deferred_trigger(SID, "m1", "proactive", True, score_snapshot=snap)
    return {"R3 弃权不回补（照旧触发清分）": pres.score(SID, None) == 0.0,
            "R3 弃权照旧 flush": proc.flushed == [SID]}

# ---------------------------------------------------------------- SC 系：守望范围
def sc_scope_table(mod):
    plug, _, _ = make_plugin(mod)
    out = {}
    for scope, expects in (
        ("ai_empty", {"ai_empty": True, "ai_keyword": False, "user_keyword": False}),
        ("ai_stop", {"ai_empty": True, "ai_keyword": True, "user_keyword": False}),
        ("all", {"ai_empty": True, "ai_keyword": True, "user_keyword": True}),
    ):
        plug.jev_watch_scope = scope
        for reason, want in expects.items():
            out[f"SC {scope}/{reason}={'守望' if want else '不守望'}"] = \
                plug._watch_scope_allows(reason) == want
    # 非法值回落默认 ai_stop
    plug.jev_watch_scope = "garbage"
    out["SC 非法值按 ai_stop 行为"] = plug._watch_scope_allows("ai_keyword") is False
    return out

# ---------------------------------------------------------------- 汇总
async def run(plugin_dir: Path):
    mod, jev = load_plugin(plugin_dir)
    for label, cond in p1_endpoint(jev).items():
        check(label, cond)
    for label, cond in p2_config(jev).items():
        check(label, cond)
    for label, cond in p2b_provider_groups(mod).items():
        check(label, cond)
    for fn in (p3_decide, p4_breaker, p5_fallback_chain):
        for label, cond in (await fn(jev)).items():
            check(label, cond)
    for label, cond in m1_mode_table(mod).items():
        check(label, cond)
    for label, cond in m2_render(mod).items():
        check(label, cond)
    for fn in (d1_confirm_veto, d2_confirm_ok, d3_abstain_fallback, d4_boost_hit, d5_boost_hold):
        for label, cond in (await fn(mod)).items():
            check(label, cond)
    # 小模型判官 + 评分回补（S/Z 共有）
    for label, cond in (await l1_llm_parse_and_chain(jev)).items():
        check(label, cond)
    for label, cond in (await l2_backend_dispatch(mod, jev)).items():
        check(label, cond)
    for fn in (r1_refund_on_veto, r2_no_refund_on_confirm, r3_no_refund_on_abstain):
        for label, cond in (await fn(mod)).items():
            check(label, cond)
    # sustain / 守望仅 S 版有（z 版无持续对话与空停守望）
    if hasattr(mod.DebouncePlugin, "_watch_fire") or hasattr(mod.DebouncePlugin, "_arm_watch_after_empty_stop"):
        for fn in (d6_sustain_window_closed, w1_watch_reply, w2_watch_wait, w3_watch_disarm_on_wake):
            for label, cond in (await fn(mod)).items():
                check(label, cond)
        for label, cond in sc_scope_table(mod).items():
            check(label, cond)
    else:
        print("  (z 版：sustain/守望范围用例按设计跳过)")

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
