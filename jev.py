"""JEV（TypeSafe System-One）触发决策引擎 —— 可选的低成本判断后端。

与主 LLM 的区别：JEV 不生成文本，输入 state + 类型化问题，直接返回
「选项 + 概率 + 置信度」。聊天触发型判断天然是一道 choice 题：

  state     = 裁剪后的消息文本（昵称: 内容，只计一次费）
  questions = {"trigger": {"type": "choice", "instructions": <场景指令>,
                           "criteria": {"reply": ..., "wait": ...}}}

协议实测（2026-10-12，经中转站，与人设进阶插件同一套实现）：
  POST {base}/v1/systemone  Authorization: Bearer <key>
  → {"model": "jev-1.13.0",
     "answers": {"trigger": {"type": "choice", "choice": "wait",
                 "confidence": 0.9, "probabilities": {"reply": 0.1, "wait": 0.9}}},
     "usage": {"input_tokens": 501, "output_tokens": 41}}

设计要点：
  · 零依赖：urllib 同步实现跑在 asyncio.to_thread（可硬超时）；
  · 熔断：连续失败 3 次 → 冷却 300s（每次失败重新计时）；冷却中直接返回 None；
  · 端点规整：容忍用户粘贴 .../v1、.../v1/systemone、.../v1/chat/completions 等；
  · 有序回退链：webui 已注册提供商模型多选（自动继承地址/密钥/模型名）；
  · 任何失败都返回 None（上层回退机械逻辑），绝不外泄异常、绝不阻塞消息管线。
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from typing import Optional

# httpx：两代框架都自带（requirements.txt 有 httpx>=0.28.1）；纯桩环境没有时回退 urllib+线程池
try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None

#: HTTP 状态码 → 可操作提示（对齐官方 jev 插件的日志可读性）
_HTTP_HINTS = {
    400: "请求格式错误（检查模型名/协议）",
    401: "API Key 无效或过期",
    403: "无权访问该模型（检查账号权限）",
    404: "接口地址错误（检查 base_url）",
    408: "上游超时",
    409: "并发冲突",
    429: "触发速率限制（降频或换档）",
    500: "上游内部错误",
    502: "网关错误",
    503: "服务不可用",
    504: "网关超时",
    529: "服务过载",
}

DEFAULT_BASE_URL = "https://api.typesafe.ai"
DEFAULT_MODEL = "jev-latest"

# 常见"末端路径"——用户可能直接粘贴完整端点，规整时先剥掉
_ENDPOINT_SUFFIXES = ("/v1/systemone", "/systemone", "/chat/completions",
                      "/completions", "/embeddings", "/models", "/v1/messages")


def endpoint_of(base_url: str) -> str:
    """把 base_url 规整成 systemone 端点。

      https://api.typesafe.ai            → .../v1/systemone
      https://api.typesafe.ai/v1         → .../v1/systemone
      https://x/v1/chat/completions      → .../v1/systemone
      https://x/v1/systemone             → .../v1/systemone（不重复拼）
    """
    b = (base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    for suffix in _ENDPOINT_SUFFIXES:
        if b.endswith(suffix):
            b = b[: -len(suffix)].rstrip("/")
            break
    if b.endswith("/v1"):
        b = b[:-3].rstrip("/")
    return b + "/v1/systemone"


def _env(value: str) -> str:
    """支持 `$$ENV_NAME` 形式（与框架敏感配置一致）。"""
    v = (value or "").strip()
    if v.startswith("$$"):
        import os
        return os.environ.get(v[2:], "") or ""
    return v


# 不同提供商/第三方格式对这两个字段的叫法可能不同 ⇒ 多键名兜底
_BASE_KEYS = ("base_url", "api_base", "openai_api_base", "base", "endpoint",
              "url", "host", "server", "api_host")
_KEY_KEYS = ("api_key", "apikey", "api_token", "token", "key",
             "access_key", "secret", "auth_token")


def _pick(cfg: dict, names) -> str:
    """从 provider_config 里挑第一个非空的候选键；支持嵌套一层（如 {"openai": {...}}）。"""
    if not isinstance(cfg, dict):
        return ""
    for name in names:
        value = cfg.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for value in cfg.values():
        if isinstance(value, dict):
            found = _pick(value, names)
            if found:
                return found
    return ""


def split_ref(ref: str):
    """`provider_id:model_id` → 两段（model_id 允许含冒号）。"""
    pid, _, mid = (ref or "").partition(":")
    return pid.strip(), mid.strip()


#: 触发题的固定两档（键是协议，不可改）；描述给 JEV 判读用。
TRIGGER_CRITERIA = {
    "reply": "新消息明显期待或适合 bot 回应：向 bot 提问/求助/点名相关、延续 bot 参与的话题、"
             "需要安抚或解释、或场面冷场且 bot 插话自然不突兀",
    "wait": "用户间闲聊、表情/灌水/寒暄、与 bot 无关、或 bot 插话会显得突兀甚至刷屏",
}


def build_trigger_questions(instructions: str) -> dict:
    """构造 reply/wait 触发判断题。instructions 来自对应场景的可配置指令。"""
    return {
        "trigger": {
            "type": "choice",
            "instructions": instructions,
            "criteria": dict(TRIGGER_CRITERIA),
        }
    }


def parse_choice(answers: dict, key: str = "trigger"):
    """→ (choice|None, confidence, probabilities)。"""
    item = (answers or {}).get(key) or {}
    if not isinstance(item, dict):
        return None, 0.0, {}
    choice = item.get("choice")
    try:
        conf = float(item.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    probs = item.get("probabilities") if isinstance(item.get("probabilities"), dict) else {}
    return (choice if isinstance(choice, str) else None), conf, probs


class JevConfig:
    """不可变快照：从插件配置解析一次。

    links = [(base_url, api_key, model), ...] **有序回退链**：
      · 来自 webui 已注册提供商的模型多选（jev_models），自动继承接口地址/密钥/模型名；
      · 旧版手动连接字段已移除（3.1.1 起改用提供商注册）；
      · 缺地址或密钥的档在解析侧就被跳过（记日志），不会进链。
    """

    __slots__ = ("enabled", "links", "timeout", "min_confidence")

    def __init__(self, enabled=False, links=(), timeout=4.0, min_confidence=0.0):
        self.enabled = bool(enabled)
        norm = []
        for link in links or ():
            try:
                b, k, m = link
            except (TypeError, ValueError):
                continue
            b = (b or "").strip()
            k = _env(k or "")
            m = (m or DEFAULT_MODEL).strip() or DEFAULT_MODEL
            if b and k:
                norm.append((b, k, m))
        self.links = tuple(norm)
        try:
            self.timeout = max(0.5, float(timeout))
        except (TypeError, ValueError):
            self.timeout = 4.0
        try:
            self.min_confidence = min(1.0, max(0.0, float(min_confidence)))
        except (TypeError, ValueError):
            self.min_confidence = 0.0

    @property
    def ready(self) -> bool:
        return bool(self.enabled and self.links)

    def client(self, logger) -> Optional["JevClient"]:
        if not self.ready:
            return None
        return JevClient(self.links, self.timeout, self.min_confidence, logger)


class JevClient:
    """最小原生 JEV 客户端。任何失败 → None；连续失败自动熔断。

    有序回退链：每次调用从第 1 档开始逐档尝试（超时/连接失败/HTTP 错/坏 JSON
    ⇒ 换下一档），全档失败才计入熔断。
    """

    COOLDOWN = 300.0

    def __init__(self, links, timeout: float, min_confidence: float, logger):
        self.links = tuple(links)
        self.timeout = float(timeout)
        self.min_confidence = float(min_confidence)
        self.logger = logger
        self.fails = 0
        self.opened_at = 0.0
        self.last_error = ""
        self.tokens = 0          # 累计输入 token（状态观测）
        self.calls = 0           # 成功调用次数
        self.abstains = 0        # 低置信弃权次数
        self._http = None        # httpx.AsyncClient 惰性实例（无 httpx 时保持 None）

    @property
    def model(self) -> str:
        return self.links[0][2] if self.links else DEFAULT_MODEL

    @property
    def endpoint(self) -> str:
        return endpoint_of(self.links[0][0]) if self.links else ""

    # ---------- 状态 ----------
    @property
    def ready(self) -> bool:
        if not self.links:
            return False
        if self.fails >= 3 and (time.time() - self.opened_at) < self.COOLDOWN:
            return False
        return True

    @property
    def cooldown_left(self) -> int:
        if self.fails >= 3:
            left = self.COOLDOWN - (time.time() - self.opened_at)
            return int(left) if left > 0 else 0
        return 0

    def _ok(self):
        self.fails = 0

    def _bad(self, err: str = ""):
        self.fails += 1
        if err:
            self.last_error = str(err)[:200]
        if self.fails >= 3:
            # 每次失败都让冷却从当前时刻重新计时（否则冷却一次后熔断失效）
            self.opened_at = time.time()
            if self.fails == 3:
                self.logger.warning("[JEV] 连续失败 3 次，冷却 300 秒（期间自动回退机械逻辑）")

    # ---------- 投递（httpx 原生异步优先；无 httpx 回退线程池同步） ----------
    async def _post_async(self, payload: dict, timeout: float) -> dict:
        """与 _post_sync 同语义的有序回退链；httpx 在则零线程零堵塞。

        定制契约：实例级替换 `_post_sync` 的（测试/定制注入）优先尊重，
        仍走线程池调用它——显式替换永不被 httpx 路径吞掉。
        """
        override = self.__dict__.get("_post_sync")
        if override is not None:
            return await asyncio.to_thread(override, payload, timeout)
        if httpx is None:
            return await asyncio.to_thread(self._post_sync, payload, timeout)
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=timeout)
        last_exc = None
        for idx, (base, key, model_name) in enumerate(self.links, 1):
            body = dict(payload)
            if model_name:
                body["model"] = model_name
            try:
                resp = await self._http.post(
                    endpoint_of(base),
                    json=body,
                    headers={"Authorization": "Bearer " + (key or "")},
                )
                if resp.status_code != 200:
                    hint = _HTTP_HINTS.get(resp.status_code, "上游错误")
                    raise RuntimeError(f"HTTP {resp.status_code}（{hint}）")
                return resp.json()
            except Exception as exc:      # 逐档兜住，最后一档才抛
                last_exc = exc
                if len(self.links) > 1:
                    self.logger.info("[JEV] 第 %d 档失败（%s）⇒ 换下一档", idx, type(exc).__name__)
        raise last_exc if last_exc else RuntimeError("jev_no_endpoint")

    # ---------- 同步实现 ----------
    def _post_sync(self, payload: dict, timeout: float) -> dict:
        """投递 —— 有序回退链逐档尝试，全档失败抛最后一个异常（由 decide 收敛）。"""
        last_exc = None
        for idx, (base, key, model_name) in enumerate(self.links, 1):
            body = dict(payload)
            if model_name:
                body["model"] = model_name
            req = urllib.request.Request(
                endpoint_of(base),
                data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Authorization": "Bearer " + (key or ""),
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", "replace")
                return json.loads(raw)
            except Exception as exc:      # 逐档兜住，最后一档才抛
                last_exc = exc
                if len(self.links) > 1:
                    self.logger.info("[JEV] 第 %d 档失败（%s）⇒ 换下一档", idx, type(exc).__name__)
        raise last_exc if last_exc else RuntimeError("jev_no_endpoint")

    # ---------- 对外 ----------
    async def decide_trigger(self, state: str, instructions: str,
                             timeout: Optional[float] = None) -> Optional[dict]:
        """→ {"choice": "reply"|"wait", "confidence": float, ...}；
        失败/未就绪/熔断 → None（调用方回退机械逻辑）；
        置信度低于 min_confidence → 返回带 "_abstain" 标记（调用方按弃权处理，
        机械结果照旧，防止低置信抖动改变行为）。"""
        if not self.ready or not instructions:
            return None
        payload = {
            "model": self.model,
            "state": state or "",
            "questions": build_trigger_questions(instructions),
        }
        limit = float(timeout or self.timeout)
        try:
            data = await asyncio.wait_for(
                self._post_async(payload, limit), timeout=limit + 0.5
            )
        except Exception as exc:  # 契约：任何失败都不得外泄
            self._bad(str(exc))
            self.logger.info("[JEV] 判断未成功（%s: %s），本轮走机械回退", type(exc).__name__, str(exc)[:150])
            return None
        if not isinstance(data, dict) or not isinstance(data.get("answers"), dict):
            self._bad("bad_response")
            return None
        choice, conf, probs = parse_choice(data["answers"])
        if choice not in ("reply", "wait"):
            self._bad("bad_choice")
            return None
        self._ok()
        self.calls += 1
        try:
            self.tokens += int((data.get("usage") or {}).get("input_tokens") or 0)
        except (TypeError, ValueError):
            pass
        result = {
            "choice": choice,
            "confidence": conf,
            "probabilities": probs,
            "model": str(data.get("model") or self.model),
        }
        if conf < self.min_confidence:
            self.abstains += 1
            result["_abstain"] = True
        return result


# ================= 小模型判官（aux_llm 后端：用 webui 已注册 LLM 做同类判断） =================
#
# 与 JEV 的差异（schema hint 已明示）：生成式路径 —— 延迟更高（秒级，受模型速度影响）、
# 成本更高（输入+输出都计费，JEV 输出免费）、无概率分布（置信度由模型自报，仅供卡阈）。
# 适用：找不到/用不了 JEV 官方或中转的用户，拿自己的便宜小模型做同样的触发判断。

_LLM_JUDGE_SUFFIX = (
    "\n\n只输出一行 JSON：{\"choice\":\"reply\" 或 \"wait\",\"confidence\":0到1的数字}。"
    "不要输出任何其他内容。"
)


def parse_llm_answer(text: str):
    """从小模型输出解析判定 → (choice|None, confidence)。

    顺序：第一个 {...} JSON 块 → 关键词兜底（reply/wait 单词）→ None（解析失败）。
    解析失败不猜——调用方换下一档或回退机械逻辑。
    """
    if not text:
        return None, 0.0
    import re as _re
    m = _re.search(r'\{[^{}]*\}', text, _re.S)
    if m:
        try:
            obj = json.loads(m.group(0))
            choice = str(obj.get("choice") or "").strip().lower()
            if choice in ("reply", "wait"):
                try:
                    conf = float(obj.get("confidence", 0.5))
                except (TypeError, ValueError):
                    conf = 0.5
                return choice, min(1.0, max(0.0, conf))
        except Exception:
            pass
    low = text.strip().lower()
    if "reply" in low:
        return "reply", 0.5
    if "wait" in low:
        return "wait", 0.5
    return None, 0.0


class LlmJudgeClient:
    """小模型判官客户端：接口与 JevClient 对齐（decide_trigger / ready / 熔断 / 记账）。

    clients = [(ref, LLMModelClient)] 有序回退链：仅**异常或解析失败**才换下一档
    （能解析出结论的回复是有效答案，不触发回退——与 KPP trigger 同语义）。
    """

    COOLDOWN = 300.0

    def __init__(self, clients, timeout: float, min_confidence: float, logger):
        self.clients = [(r, c) for r, c in (clients or []) if c is not None]
        self.timeout = max(1.0, float(timeout))
        self.min_confidence = float(min_confidence)
        self.logger = logger
        self.fails = 0
        self.opened_at = 0.0
        self.last_error = ""
        self.tokens = 0
        self.calls = 0
        self.abstains = 0

    @property
    def model(self) -> str:
        return self.clients[0][0] if self.clients else ""

    @property
    def ready(self) -> bool:
        if not self.clients:
            return False
        if self.fails >= 3 and (time.time() - self.opened_at) < self.COOLDOWN:
            return False
        return True

    @property
    def cooldown_left(self) -> int:
        if self.fails >= 3:
            left = self.COOLDOWN - (time.time() - self.opened_at)
            return int(left) if left > 0 else 0
        return 0

    def _ok(self):
        self.fails = 0

    def _bad(self, err: str = ""):
        self.fails += 1
        if err:
            self.last_error = str(err)[:200]
        if self.fails >= 3:
            self.opened_at = time.time()
            if self.fails == 3:
                self.logger.warning("[JEV·LLM] 连续失败 3 次，冷却 300 秒（期间自动回退机械逻辑）")

    async def _chat_one(self, client, prompt: str, timeout: float) -> str:
        """调一档小模型（import 延迟到调用时，保持本模块零框架依赖）。"""
        from core.provider import LLMRequest
        req = LLMRequest(messages=[{"role": "user", "content": prompt}])
        resp = await asyncio.wait_for(client.chat(req), timeout=timeout)
        return (getattr(resp, "text_response", "") or "").strip()

    async def decide_trigger(self, state: str, instructions: str,
                             timeout: Optional[float] = None) -> Optional[dict]:
        """→ {"choice","confidence","model","_backend":"aux_llm"}；失败/熔断 → None。"""
        if not self.ready or not instructions:
            return None
        prompt = (instructions or "") + _LLM_JUDGE_SUFFIX + "\n\n对话内容：\n" + (state or "")
        limit = float(timeout or self.timeout)
        last_exc = None
        for idx, (ref, client) in enumerate(self.clients, 1):
            try:
                text = await self._chat_one(client, prompt, limit)
                choice, conf = parse_llm_answer(text)
                if choice is None:
                    raise ValueError(f"unparseable_answer:{text[:60]!r}")
                self._ok()
                self.calls += 1
                result = {"choice": choice, "confidence": conf,
                          "probabilities": {}, "model": f"llm:{ref}", "_backend": "aux_llm"}
                if conf < self.min_confidence:
                    self.abstains += 1
                    result["_abstain"] = True
                return result
            except Exception as exc:
                last_exc = exc
                if len(self.clients) > 1:
                    self.logger.info("[JEV·LLM] 第 %d 档失败（%s）⇒ 换下一档", idx, type(exc).__name__)
        self._bad(str(last_exc) if last_exc else "no_clients")
        self.logger.info("[JEV·LLM] 判断未成功（%s），本轮走机械回退",
                         type(last_exc).__name__ if last_exc else "?")
        return None
