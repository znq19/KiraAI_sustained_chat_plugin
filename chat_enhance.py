"""聊天增强引擎（s/z 新版本共享）：存在感节流 / 骚扰感知化 / 休眠状态机 / 通知合并

设计规格：融合版设计规格v3.md
- 存在感节流：双窗口 bot 发言占比 → k_prob 调节系数 + 存在感评分（评分补正概率触发）
- 骚扰感知化：戳/at/关键词/引用频率检测 → System 通知 → bot 用 XML tag 决策屏蔽
- 休眠状态机：dormant_ranges 休眠时段 + 起夜概率 + 维持期（续窗/一次性/次数/主动续窗限制）
- 通知合并：per-session 挂起队列，on_llm_request 时统一注入，短窗口兜底

宿主插件（s/z main.py）实例化本引擎，并在钩子里调用对应方法。
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from collections import defaultdict, deque
from typing import Optional

from core.plugin import logger

try:
    from core.chat import MessageChain
    from core.chat.message_elements import Text as _Text
    from core.chat.message_elements import Reply as _Reply
except Exception:
    MessageChain = None
    _Text = None
    _Reply = None

try:
    from core.prompt_manager import Prompt as _Prompt
except Exception:
    _Prompt = None


def _safe_int(v, default: int) -> int:
    """安全整数转换：None/非法类型/非法值回退默认。"""
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _safe_float(v, default: float) -> float:
    """安全浮点转换：None/非法类型/非法值回退默认。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# 存在感节流
# ---------------------------------------------------------------------------

class PresenceThrottle:
    """双窗口 bot 发言占比统计 + k_prob 调节系数 + 存在感评分 + 闲时判定。

    统计口径（n 版双窗口保留）：条数窗口（最近 N 条）与时间窗口（最近 M 分钟）
    任一满足即统计。时间线按会话维护，7 天无活动自动回收。
    """

    def __init__(self, cfg: dict):
        self.window_size = max(2, _safe_int(cfg.get("presence_window_size"), 20))
        self.decay_minutes = max(0.0, _safe_float(cfg.get("presence_decay_minutes"), 10))
        self.target_ratio = max(0.01, min(0.99, _safe_float(cfg.get("presence_target_ratio"), 0.3)))
        self.k_min = max(0.01, _safe_float(cfg.get("presence_k_min"), 0.2))
        self.k_max = max(self.k_min, _safe_float(cfg.get("presence_k_max"), 2.0))
        self.score_threshold = max(0.0, _safe_float(cfg.get("score_threshold"), 60.0))
        self.idle_bonus = max(0.0, _safe_float(cfg.get("idle_bonus_score"), 15.0))
        # 闲时相对判定倍数：静默时长 > 该会话历史平均 × 倍数 才算闲时（默认 1.5）
        self.idle_bonus_ratio = max(0.0, _safe_float(cfg.get("idle_bonus_ratio"), 1.5))
        self.force_suppress = bool(cfg.get("force_suppress", False))
        self.score_gate_enabled = bool(cfg.get("score_gate_enabled", False))
        # 累计加分（评分补正用）：用户消息 +1，bot 回复 -5，攒到阈值补触发一次后清零
        self.score_increment = max(0.0, _safe_float(cfg.get("score_increment"), 1.0))
        self.score_penalty = max(0.0, _safe_float(cfg.get("score_penalty"), 5.0))
        self.score_cap = max(1.0, _safe_float(cfg.get("score_cap"), 100.0))
        # sid -> deque[(ts, is_bot)]，容量 512
        self._timeline: dict[str, deque] = defaultdict(lambda: deque(maxlen=512))
        # sid -> 平均静默间隔（秒），用于闲时相对判定
        self._idle_avg: dict[str, float] = defaultdict(float)
        self._last_ts: dict[str, float] = {}
        # sid -> 累计分（评分补正用）
        self._scores: dict[str, float] = defaultdict(float)

    # ---- 记录 ----

    def note_incoming(self, sid: str, ts: float, is_bot: bool = False) -> None:
        tl = self._timeline[sid]
        prev = self._last_ts.get(sid)
        if prev is not None and ts > prev:
            gap = ts - prev
            avg = self._idle_avg[sid]
            # 指数滑动平均（α=0.2），对突发不敏感
            self._idle_avg[sid] = avg * 0.8 + gap * 0.2 if avg else gap
        self._last_ts[sid] = ts
        tl.append((ts, is_bot))
        # 累计加分：用户消息 +score_increment，bot 回复 -score_penalty（下限 0）
        if is_bot:
            self._scores[sid] = max(0.0, self._scores[sid] - self.score_penalty)
        else:
            self._scores[sid] = min(self.score_cap, self._scores[sid] + self.score_increment)

    def note_bot_reply(self, sid: str, ts: float) -> None:
        self.note_incoming(sid, ts, is_bot=True)

    # ---- 统计 ----

    def _recent(self, sid: str, now: float) -> list:
        tl = self._timeline[sid]
        if self.decay_minutes > 0:
            cutoff = now - self.decay_minutes * 60
            return [e for e in tl if e[0] >= cutoff]
        return list(tl)

    def ratio(self, sid: str, now: float) -> float:
        """bot 发言占比（双窗口任一满足即统计）。"""
        recent = self._recent(sid, now)
        if not recent:
            return 0.0
        bot = sum(1 for _, b in recent if b)
        return bot / len(recent)

    def k_prob(self, sid: str, now: float) -> float:
        """调节系数：回得少（占比低）→ >1 提高概率；回得多 → <1 降低。"""
        r = self.ratio(sid, now)
        k = 1.0 - (r - self.target_ratio) / self.target_ratio
        return max(self.k_min, min(self.k_max, k))

    def score(self, sid: str, now: float) -> float:
        """累计分：用户消息 +1、bot 回复 -5 累积，用于评分补正判定。"""
        return self._scores.get(sid, 0.0)

    def consume_score(self, sid: str) -> None:
        """触发后清零累计分（触发即清分，防同一批消息重复触发）。"""
        self._scores.pop(sid, None)

    def idle_bonus_ok(self, sid: str, now: float) -> bool:
        """静默时长高于该会话历史平均 → 闲时加分（相对判定，活跃群/死群标准不同）。"""
        prev = self._last_ts.get(sid)
        if prev is None:
            return False
        gap = now - prev
        avg = self._idle_avg.get(sid, 0.0)
        return avg > 0 and gap > avg * self.idle_bonus_ratio

    # ---- 回收 ----

    def prune(self, keep_sids: set, max_idle_seconds: float = 7 * 24 * 3600.0) -> int:
        now = time.time()
        dropped = 0
        for sid in list(self._timeline.keys()):
            if sid in keep_sids:
                continue
            last = self._last_ts.get(sid, 0.0)
            if now - last > max_idle_seconds:
                self._timeline.pop(sid, None)
                self._idle_avg.pop(sid, None)
                self._last_ts.pop(sid, None)
                self._scores.pop(sid, None)
                dropped += 1
        return dropped


# ---------------------------------------------------------------------------
# 骚扰感知化（戳/at/关键词/引用）
# ---------------------------------------------------------------------------

class HarassDetector:
    """频率检测 + XML 决策 + 屏蔽。

    每类骚扰独立配置（窗口/阈值/默认屏蔽时长/允许 bot 自设/最大时长/累计范围/开关）。
    屏蔽键：(sid, user_id, kind) 或 (sid, '*', kind)（all 累计/全局）。
    """

    KINDS = ("poke", "at", "keyword", "reply")

    def __init__(self, cfg: dict, plugin):
        self._plugin = plugin
        self._load(cfg)
        # 作用域/白名单（全局，所有类型共用）：
        # scope_sessions 非空时仅对这些会话检测（空=全部）；白名单命中不检测
        self._scope_sessions = set(str(x) for x in (cfg.get("harass_scope_sessions") or []))
        self._whitelist_users = set(str(x) for x in (cfg.get("harass_whitelist_users") or []))
        self._whitelist_sessions = set(str(x) for x in (cfg.get("harass_whitelist_sessions") or []))
        # sid -> kind -> deque[(ts, user_id)]
        self._counts: dict[str, dict[str, deque]] = defaultdict(
            lambda: {k: deque(maxlen=256) for k in self.KINDS}
        )
        # (sid, user_id, kind) -> until_ts
        self._ignored: dict[tuple, float] = {}
        # (sid, kind) -> 最近触发者（达阈值时记录，供 user|duration:N 无 ID 时反查）
        self._last_trigger_user_map: dict[tuple, str] = {}
        self._prune_task: Optional[asyncio.Task] = None

    def _load(self, cfg: dict) -> None:
        self._conf = {}
        for kind in self.KINDS:
            sec = cfg.get(f"section_{kind}", {}) or {}
            self._conf[kind] = {
                "enabled": bool(sec.get("enabled", kind in ("poke", "at"))),
                "window": max(1.0, _safe_float(sec.get("window_seconds"), 60)),
                "threshold": max(1, _safe_int(sec.get("threshold"), 3)),
                "default_duration": max(1, _safe_int(sec.get("default_duration"), 180)),
                "allow_bot_duration": bool(sec.get("allow_bot_duration", True)),
                "max_duration": max(0, _safe_int(sec.get("max_duration"), 300)),
                "scope": sec.get("scope", "per_user"),
            }

    # ---- 检测 ----

    def check(self, sid: str, kind: str, user_id: str, now: float) -> Optional[str]:
        """记录一次事件；达阈值返回通知文本，未达返回 None。"""
        conf = self._conf.get(kind)
        if not conf or not conf["enabled"]:
            return None
        # 作用域/白名单：会话不在作用域内、或用户/会话在白名单 → 不检测
        if self._scope_sessions and sid not in self._scope_sessions:
            return None
        if user_id in self._whitelist_users or sid in self._whitelist_sessions:
            return None
        if self.is_ignored(sid, user_id, kind, now):
            return None
        counts = self._counts[sid][kind]
        counts.append((now, user_id))
        # 窗口内计数（scope=all 时按会话累计，per_user 时按用户累计）
        cutoff = now - conf["window"]
        if conf["scope"] == "all":
            n = sum(1 for ts, _ in counts if ts >= cutoff)
        else:
            n = sum(1 for ts, u in counts if ts >= cutoff and u == user_id)
        if n < conf["threshold"]:
            return None
        # 达阈值：记录最近触发者（供 user|duration:N 无 ID 时反查），清空窗口计数避免重复触发
        self._last_trigger_user_map[(sid, kind)] = user_id
        self._counts[sid][kind] = deque(maxlen=256)
        return self._build_notice(kind, user_id, n, conf)

    def _build_notice(self, kind: str, user_id: str, n: int, conf: dict) -> str:
        tag = {"poke": "poke_ignore", "at": "at_ignore",
               "keyword": "kw_ignore", "reply": "reply_ignore"}[kind]
        dur = conf["default_duration"]
        max_dur = conf["max_duration"] if conf["allow_bot_duration"] else dur
        dur_txt = f" (max {max_dur}s)" if conf["allow_bot_duration"] and max_dur > dur else ""
        return (
            f"[System: User {user_id} {kind} you {n} times in {int(conf['window'])}s. "
            f"Reply with <{tag}>user|duration:{dur}</{tag}> to ignore this user, "
            f"<{tag}>all|duration:{dur}</{tag}> to ignore everyone, "
            f"or <{tag}>none</{tag}> to do nothing. "
            f"Ignore lasts {dur}s by default{dur_txt}.]"
        )

    # ---- 屏蔽 ----

    def is_ignored(self, sid: str, user_id: str, kind: str, now: float) -> bool:
        # 全局屏蔽（所有会话所有用户）
        if self._ignored.get(("*", "*", kind), 0.0) > now:
            return True
        # 会话级屏蔽（该会话所有用户）
        if self._ignored.get((sid, "*", kind), 0.0) > now:
            return True
        # 用户级屏蔽
        if self._ignored.get((sid, user_id, kind), 0.0) > now:
            return True
        return False

    def is_blocked(self, sid: str, user_id: str, now: float) -> bool:
        """拉黑语义：该用户/会话是否有「非 poke」的未过期屏蔽（含全局/会话级）。

        与 is_ignored 的区别：is_ignored 按 kind 精确匹配（检测跳过用）；
        is_blocked 不看 kind——只要该用户/会话被屏蔽过（无论 poke/at/keyword/
        reply 还是额外信号），其消息就完全不进 LLM（宿主 handle_msg 入口调用）。
        """
        for (s, uid, kind), until in self._ignored.items():
            if until <= now:
                continue
            if kind == "poke":
                # poke 屏蔽只挡戳一戳（通知事件），不拉黑普通消息
                continue
            if s == "*" or s == sid:
                if uid == "*" or uid == user_id:
                    return True
        return False

    def apply_ignore(self, sid: str, user_id: str, kind: str, duration: int) -> str:
        """执行屏蔽。user_id='*' 表示该会话内所有用户；sid='*' 表示全局（所有会话）。
        kind='all' 时展开为全部 4 类（poke/at/keyword/reply）。返回结果文本。"""
        # kind='all' 时用任一具体 kind 的配置（默认时长/钳制）
        conf = self._conf.get(kind) or self._conf.get("poke", {})
        if duration < 0:
            # -1 = 永久屏蔽（工具描述约定）
            until = float("inf")
        else:
            # allow_bot_duration=False：bot 不允许自设时长，强制用默认时长
            # （配置语义：仅允许使用默认屏蔽时长，忽略 bot 建议值）
            if not conf.get("allow_bot_duration", True):
                duration = conf.get("default_duration", 180)
            elif duration <= 0:
                duration = conf.get("default_duration", 180)
            if conf.get("allow_bot_duration", True) and conf.get("max_duration", 0) > 0:
                duration = min(duration, conf["max_duration"])
            until = time.time() + duration
        if kind == "all":
            # 拉黑语义：all = 全部形式（含 poke）——该用户/会话消息完全不进 LLM
            kinds = self.KINDS
        else:
            kinds = (kind,)
        for k in kinds:
            self._ignored[(sid, user_id, k)] = until
        if sid == "*":
            scope_txt = "all sessions, all users"
        elif user_id == "*":
            scope_txt = f"session {sid}, all users"
        else:
            scope_txt = f"user {user_id} in {sid}"
        # -1 = 永久（until=inf），返回文本显示"永久"避免误导
        if duration < 0:
            dur_txt = "永久"
        else:
            dur_txt = f"{duration} 秒"
        return f"已屏蔽 {scope_txt} 的 {kind} 唤醒 {dur_txt}"

    def apply_ignore_from_tag(self, sid: str, kind: str, value: str) -> str:
        """解析 XML tag 值：user|duration:N / all|duration:N / none。

        额外信号通知格式 <ignore>user:{uid}|type:{kind}|duration:N</ignore>：
        - user:{uid}：内嵌具体用户 ID（无需反查最近触发者）
        - type:{kind}：指定信号类型（user_msgs/session_msgs…），缺省沿用 tag 的 kind
        """
        value = (value or "").strip()
        if not value or value.lower() == "none":
            return ""
        parts = [p.strip() for p in value.split("|")]
        target = parts[0].lower()
        duration = 0
        uid_inline = None
        kind_inline = kind
        # parts[0] 可为 user:{uid}（额外信号通知格式）或 user/all（旧格式）
        if target == "all":
            uid_inline = "*"
        elif target.startswith("user:"):
            uid_inline = target[5:] or None
        for p in parts[1:]:
            if p.startswith("duration:"):
                try:
                    duration = int(p.split(":", 1)[1])
                except (ValueError, IndexError):
                    duration = 0
            elif p.startswith("type:"):
                kind_inline = p.split(":", 1)[1].strip() or kind
            elif p.startswith(("user_id:", "uid:")):
                uid_inline = p.split(":", 1)[1].strip() or None
        if uid_inline is not None:
            return self.apply_ignore(sid, uid_inline, kind_inline, duration)
        if target == "user":
            # user 需要具体用户 ID：tag 里没带时用最近触发者
            uid = self._last_trigger_user(sid, kind_inline)
            if uid is None:
                return "（无法确定目标用户，未屏蔽）"
            return self.apply_ignore(sid, uid, kind_inline, duration)
        return ""

    def _last_trigger_user(self, sid: str, kind: str) -> Optional[str]:
        # 优先用达阈值时记录的最近触发者（窗口已清空，deque 里可能没有）
        uid = self._last_trigger_user_map.get((sid, kind))
        if uid:
            return uid
        counts = self._counts.get(sid, {}).get(kind)
        if counts:
            for ts, u in reversed(list(counts)):
                return u
        return None

    def unblock(self, sid: str, user_id: str, kind: str) -> str:
        kinds = self.KINDS if kind == "all" else (kind,)
        removed = 0
        for k in kinds:
            key = (sid, user_id, k)
            if key in self._ignored:
                self._ignored.pop(key, None)
                removed += 1
        if removed:
            return f"已解除 {user_id} 的 {kind} 屏蔽"
        return f"未找到 {user_id} 的 {kind} 屏蔽"

    def list_ignored(self, sid: str) -> str:
        now = time.time()
        rows = []
        for (s, uid, kind), until in self._ignored.items():
            # 全局屏蔽（s="*"）对所有会话可见
            if (s == sid or s == "*") and until > now:
                scope = "全局" if s == "*" else (f"会话{s}" if uid == "*" else f"用户{uid}")
                if until == float("inf"):
                    remain = "永久"
                else:
                    remain = f"{int(until - now)}s"
                rows.append(f"{scope} {kind} 剩余 {remain}")
        return "当前屏蔽: " + ("; ".join(rows) if rows else "无")

    def prune(self) -> None:
        now = time.time()
        for key in [k for k, v in self._ignored.items() if v <= now]:
            self._ignored.pop(key, None)
        # 回收 7 天无活动的检测计数（_counts / _last_trigger_user_map），防长期运行内存增长
        for sid in list(self._counts.keys()):
            last = 0.0
            for dq in self._counts[sid].values():
                if dq:
                    last = max(last, dq[-1][0])
            if now - last > 7 * 24 * 3600:
                self._counts.pop(sid, None)
                for key in [k for k in self._last_trigger_user_map if k[0] == sid]:
                    self._last_trigger_user_map.pop(key, None)


# ---------------------------------------------------------------------------
# 休眠状态机
# ---------------------------------------------------------------------------

class DormantState:
    """dormant_ranges 休眠时段 + 起夜概率 + 维持期（续窗/一次性/次数/主动续窗限制）。"""

    def __init__(self, cfg: dict):
        self.ranges = self._parse_ranges(cfg.get("dormant_ranges", []))
        self.wake_prob = max(0.0, min(1.0, _safe_float(cfg.get("dormant_wake_probability"), 0.3)))
        self.keep_mode = cfg.get("wake_keep_mode", "renew")
        self.keep_seconds = max(1.0, _safe_float(cfg.get("wake_keep_seconds"), 300))
        self.max_rounds = _safe_int(cfg.get("wake_max_rounds"), -1)
        self.max_extensions = _safe_int(cfg.get("wake_max_extensions"), -1)
        # 作用域/白名单：scope_sessions 非空时仅这些会话休眠（空=全部）；白名单命中不休眠
        self._scope_sessions = set(str(x) for x in (cfg.get("dormant_scope_sessions") or []))
        self._whitelist_users = set(str(x) for x in (cfg.get("dormant_whitelist_users") or []))
        self._whitelist_sessions = set(str(x) for x in (cfg.get("dormant_whitelist_sessions") or []))
        # sid -> 唤醒到期时间戳
        self._awake_until: dict[str, float] = {}
        # sid -> 已互动次数 / 已续窗次数
        self._rounds: dict[str, int] = defaultdict(int)
        self._extensions: dict[str, int] = defaultdict(int)

    @staticmethod
    def _parse_ranges(raw) -> list:
        """解析休眠时段：HH:MM-HH:MM，支持跨午夜，可多条。"""
        out = []
        if not raw:
            return out
        for line in raw:
            if isinstance(line, str):
                line = line.strip()
            else:
                continue
            if "-" not in line:
                continue
            a, b = line.split("-", 1)
            a, b = a.strip(), b.strip()
            try:
                ah, am = (int(x) for x in a.split(":"))
                bh, bm = (int(x) for x in b.split(":"))
            except (ValueError, IndexError):
                continue
            # 范围校验：小时 0-23，分钟 0-59
            if not (0 <= ah <= 23 and 0 <= am <= 59 and 0 <= bh <= 23 and 0 <= bm <= 59):
                continue
            out.append((ah * 60 + am, bh * 60 + bm))
        return out

    def in_dormant(self, now_hhmm: str, sid: str = None) -> bool:
        """当前是否在休眠时段内（sid 用于作用域/白名单判断，None 时仅按时段）。"""
        if not self.ranges:
            return False
        if sid is not None:
            if self._scope_sessions and sid not in self._scope_sessions:
                return False
            if sid in self._whitelist_sessions:
                return False
        now_min = int(now_hhmm[:2]) * 60 + int(now_hhmm[3:5])
        for start, end in self.ranges:
            if start == end:
                continue
            if start < end:
                if start <= now_min < end:
                    return True
            else:  # 跨午夜
                if now_min >= start or now_min < end:
                    return True
        return False

    def try_wake(self, sid: str, now: float, user_id: str = None) -> bool:
        """休眠期内被提及：起夜概率判定。命中则唤醒并返回 True。

        白名单用户/会话不受休眠限制：直接唤醒（填了不生效）。
        """
        if self.is_awake(sid, now):
            return True
        if user_id is not None and user_id in self._whitelist_users:
            self._awake_until[sid] = now + self.keep_seconds
            self._rounds[sid] = 0
            self._extensions[sid] = 0
            return True
        if random.random() < self.wake_prob:
            self._awake_until[sid] = now + self.keep_seconds
            self._rounds[sid] = 0
            self._extensions[sid] = 0
            return True
        return False

    def is_awake(self, sid: str, now: float) -> bool:
        return self._awake_until.get(sid, 0.0) > now

    def note_reply(self, sid: str, now: float) -> None:
        """bot 回复：续窗型续期；次数 +1。"""
        if sid not in self._awake_until:
            return
        self._rounds[sid] += 1
        if self.keep_mode == "renew":
            self._awake_until[sid] = now + self.keep_seconds

    def can_reply(self, sid: str) -> bool:
        """次数限制：-1 不限；0 或已达上限则不可再互动。"""
        if self.max_rounds < 0:
            return True
        return self._rounds.get(sid, 0) < self.max_rounds

    def extend(self, sid: str, now: float) -> str:
        """主动续窗：受次数限制（-1 无限，0 不能续）。"""
        if self.max_extensions == 0:
            return "（续窗已禁用）"
        if self.max_extensions > 0 and self._extensions.get(sid, 0) >= self.max_extensions:
            return f"（已达续窗上限 {self.max_extensions} 次）"
        self._extensions[sid] += 1
        self._awake_until[sid] = now + self.keep_seconds
        return f"已续窗（第 {self._extensions[sid]} 次，剩余 {int(self.keep_seconds)}s）"

    def wake_notice(self, sid: str) -> str:
        """唤醒通知：告知 bot 可回复状态。"""
        mode_txt = "renew" if self.keep_mode == "renew" else "once"
        rounds_txt = f"up to {self.max_rounds} more rounds" if self.max_rounds >= 0 else "unlimited rounds"
        ext_txt = ""
        if self.max_extensions != 0:
            ext_txt = f" Reply <wake_extend>yes</wake_extend> to extend (max {self.max_extensions})." \
                if self.max_extensions > 0 else \
                " Reply <wake_extend>yes</wake_extend> to extend."
        return (
            f"[System: You were woken up (dormant probability hit). You may reply. "
            f"You will stay awake for {int(self.keep_seconds)}s (mode: {mode_txt}), "
            f"{rounds_txt}.{ext_txt} Output <msg/> to stay silent.]"
        )

    def prune(self, keep_sids: set) -> None:
        now = time.time()
        for sid in list(self._awake_until.keys()):
            if sid in keep_sids:
                continue
            if now - self._awake_until[sid] > 7 * 24 * 3600:
                self._awake_until.pop(sid, None)
                self._rounds.pop(sid, None)
                self._extensions.pop(sid, None)


# ---------------------------------------------------------------------------
# 通知合并
# ---------------------------------------------------------------------------

class NoticeMerger:
    """per-session 挂起队列 + 触发点合并。

    通知进挂起队列，on_llm_request 时统一注入（一次 LLM 调用处理所有通知）；
    bot 休眠中长时间无请求时，短窗口（默认跟随 bot 配置 max_message_interval）
    兜底统一 publish。
    """

    def __init__(self, plugin, merge_seconds: float = 1.5):
        self._plugin = plugin
        self._merge_seconds = max(0.5, merge_seconds)
        self._pending: dict[str, list] = defaultdict(list)
        self._flush_tasks: dict[str, asyncio.Task] = {}
        self._publish_tasks: set = set()

    def queue(self, sid: str, text: str) -> None:
        self._pending[sid].append(text)
        if sid not in self._flush_tasks or self._flush_tasks[sid].done():
            try:
                self._flush_tasks[sid] = asyncio.create_task(self._flush_later(sid))
            except RuntimeError:
                # 事件循环外调用（测试/错误时序）：直接同步 flush，避免通知丢失
                self._flush_tasks.pop(sid, None)
                self.flush(sid)

    async def _flush_later(self, sid: str) -> None:
        try:
            await asyncio.sleep(self._merge_seconds)
        except asyncio.CancelledError:
            return
        self.flush(sid)

    def drain(self, sid: str) -> list:
        """取走并清空挂起通知（on_llm_request 注入用）。"""
        items = self._pending.pop(sid, [])
        task = self._flush_tasks.pop(sid, None)
        if task and not task.done():
            task.cancel()
        return items

    def flush(self, sid: str) -> None:
        """短窗口兜底：统一 publish 一次。"""
        items = self._pending.pop(sid, [])
        task = self._flush_tasks.pop(sid, None)
        if task and not task.done():
            task.cancel()
        if not items:
            return
        text = "\n".join(items)
        try:
            t = asyncio.create_task(self._publish(sid, text))
            self._publish_tasks.add(t)
            t.add_done_callback(self._publish_tasks.discard)
        except RuntimeError:
            # 事件循环外调用：同步 publish（尽力而为，失败记录）
            logger.warning(f"[Enhance] 通知合并 publish 在事件循环外调用，同步执行: {sid}")
            try:
                asyncio.get_event_loop().run_until_complete(self._publish(sid, text))
            except Exception as e:
                logger.warning(f"[Enhance] 通知合并 publish 失败: {e}")
        except Exception as e:
            logger.warning(f"[Enhance] 通知合并 publish 失败: {e}")

    async def _publish(self, sid: str, text: str) -> None:
        try:
            if MessageChain is None or _Text is None:
                logger.warning(f"[Enhance] 通知合并 publish 跳过（框架模块不可用）: {sid}")
                return
            chain = MessageChain([_Text(text)])
            await self._plugin.ctx.publish_notice(
                session=sid, chain=chain, is_mentioned=True
            )
        except Exception as e:
            logger.warning(f"[Enhance] 通知合并 publish 异常: {e}")

    def shutdown(self) -> None:
        for task in self._flush_tasks.values():
            if not task.done():
                task.cancel()
        self._flush_tasks.clear()
        # 等待正在 publish 的任务完成（避免对已销毁 ctx 调用）
        if self._publish_tasks:
            try:
                asyncio.get_event_loop().run_until_complete(
                    asyncio.gather(*self._publish_tasks, return_exceptions=True)
                )
            except Exception:
                pass
        self._publish_tasks.clear()
        self._pending.clear()


# ---------------------------------------------------------------------------
# 引擎总装
# ---------------------------------------------------------------------------

class ChatEnhanceEngine:
    """宿主插件实例化本引擎，并在钩子里调用对应方法。"""

    def __init__(self, ctx, cfg: dict, plugin, merge_seconds: float = 1.5):
        self.ctx = ctx
        self.plugin = plugin
        self.presence = PresenceThrottle(cfg)
        self.dm_presence_enabled = bool(cfg.get("dm_presence_enabled", True))
        _dm_cfg = {
            "presence_window_size": _safe_int(cfg.get("dm_presence_window_size"), 10),
            "presence_decay_minutes": _safe_float(cfg.get("presence_decay_minutes"), 10),
            "presence_target_ratio": _safe_float(cfg.get("dm_presence_target_ratio"), 0.7),
            "presence_k_min": _safe_float(cfg.get("dm_presence_k_min"), 0.5),
            "presence_k_max": max(_safe_float(cfg.get("dm_presence_k_min"), 0.5),
                                  _safe_float(cfg.get("dm_presence_k_max"), 2.0)),
            "score_threshold": _safe_float(cfg.get("dm_score_threshold"), 30),
            "score_increment": _safe_float(cfg.get("dm_score_increment"), 2),
            "score_penalty": _safe_float(cfg.get("dm_score_penalty"), 3),
            "score_cap": _safe_float(cfg.get("dm_score_cap"), 50),
            "idle_bonus_score": _safe_float(cfg.get("dm_idle_bonus_score"), 15),
            "idle_bonus_ratio": _safe_float(cfg.get("dm_idle_bonus_ratio"), 1.5),
        }
        self.dm_presence = PresenceThrottle(_dm_cfg) if self.dm_presence_enabled else self.presence
        self.mentioned_dm_score_gate_deny = bool(cfg.get("mentioned_dm_score_gate_deny", False))
        self.mentioned_dm_score_gate_boost = bool(cfg.get("mentioned_dm_score_gate_boost", False))
        self.harass = HarassDetector(cfg, plugin)
        self.dormant = DormantState(cfg)
        self.merger = NoticeMerger(plugin, merge_seconds)
        self.score_threshold = _safe_float(cfg.get("score_threshold"), 60.0)
        # 评分补正：门槛过滤 + 补偿触发 独立控制（三个通路各自独立）
        self.score_gate_deny = bool(cfg.get("score_gate_deny", False))
        self.score_gate_boost = bool(cfg.get("score_gate_boost", False))
        self.sustain_score_gate_deny = bool(cfg.get("sustain_score_gate_deny", False))
        self.sustain_score_gate_boost = bool(cfg.get("sustain_score_gate_boost", False))
        self.dm_sustain_score_gate_deny = bool(cfg.get("dm_sustain_score_gate_deny", False))
        self.dm_sustain_score_gate_boost = bool(cfg.get("dm_sustain_score_gate_boost", False))
        self.force_suppress = bool(cfg.get("force_suppress", False))
        # 评分补正：提及消息（@/关键词/引用）独立控制
        self.mentioned_score_gate_deny = bool(cfg.get("mentioned_score_gate_deny", False))
        self.mentioned_score_gate_boost = bool(cfg.get("mentioned_score_gate_boost", False))
        # 额外信号（user_msgs/session_msgs 群聊与私聊都可检测，各自独立开关/参数：
        # 群聊走 detect_user_msgs/session_msgs + user/session_msgs_* 参数；
        # 私聊走 dm_detect_* + dm_* 参数，默认关。bot_speech 仅群聊。）
        self.detect_user_msgs = bool(cfg.get("detect_user_msgs", False))
        self.detect_session_msgs = bool(cfg.get("detect_session_msgs", False))
        self.dm_detect_user_msgs = bool(cfg.get("dm_detect_user_msgs", False))
        self.dm_detect_session_msgs = bool(cfg.get("dm_detect_session_msgs", False))
        self.detect_bot_speech = bool(cfg.get("detect_bot_speech", False))
        self.user_msgs_window = _safe_float(cfg.get("user_msgs_window_seconds"), 60)
        self.user_msgs_threshold = _safe_int(cfg.get("user_msgs_threshold"), 10)
        self.session_msgs_window = _safe_float(cfg.get("session_msgs_window_seconds"), 60)
        self.session_msgs_threshold = _safe_int(cfg.get("session_msgs_threshold"), 20)
        self.dm_user_msgs_window = _safe_float(cfg.get("dm_user_msgs_window_seconds"), 60)
        self.dm_user_msgs_threshold = _safe_int(cfg.get("dm_user_msgs_threshold"), 10)
        self.dm_session_msgs_window = _safe_float(cfg.get("dm_session_msgs_window_seconds"), 60)
        self.dm_session_msgs_threshold = _safe_int(cfg.get("dm_session_msgs_threshold"), 20)
        self.bot_speech_window = _safe_float(cfg.get("bot_speech_window_seconds"), 300)
        self.bot_speech_threshold = _safe_int(cfg.get("bot_speech_threshold"), 10)
        # 额外信号默认屏蔽时长（通知里建议 bot 使用的 duration；独立于各骚扰类别的 default_duration）
        self.extra_default_duration = _safe_int(cfg.get("extra_default_duration"), 180)
        self._extra_counts: dict[str, dict[str, deque]] = defaultdict(
            lambda: {k: deque(maxlen=512) for k in ("bot_speech", "user_msgs", "session_msgs")}
        )
        self._prune_task: Optional[asyncio.Task] = None

    def _get_presence(self, is_dm: bool = False) -> "PresenceThrottle":
        """返回群聊（self.presence）或私聊（self.dm_presence）的 PresenceThrottle。"""
        if is_dm and self.dm_presence_enabled:
            return self.dm_presence
        return self.presence

    # ---- 生命周期 ----

    def start(self) -> None:
        if self._prune_task is None or self._prune_task.done():
            self._prune_task = asyncio.create_task(self._prune_loop())

    async def _prune_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(3600)
                # 各组件内部按 7 天闲置判断回收，活跃会话（最近有活动）天然保留
                self.presence.prune(set())
                if self.dm_presence_enabled:
                    self.dm_presence.prune(set())
                self.dormant.prune(set())
                self.harass.prune()
                # 额外信号计数回收：按 deque 最新时间戳 7 天淘汰
                for sid2 in list(self._extra_counts.keys()):
                    for k2, dq in self._extra_counts[sid2].items():
                        if dq and time.time() - dq[-1][0] > 7 * 86400:
                            self._extra_counts.pop(sid2, None)
                            break
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("[Enhance] prune loop error")

    async def shutdown(self) -> None:
        if self._prune_task and not self._prune_task.done():
            self._prune_task.cancel()
            try:
                await asyncio.wait_for(self._prune_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        self.merger.shutdown()

    # ---- 消息入口（宿主 handle_msg 调用） ----

    def _build_extra_notice(self, kind: str, user_id: str, n: int, window: float, threshold: int) -> str:
        """额外信号通知：user_msgs（单用户消息条数）/ session_msgs（会话消息条数）。

        带两种拉黑选项：定向屏蔽该信号（<ignore>user:{uid}|type:{kind}|...</ignore>）
        或全拉黑该用户（<ignore>user:{uid}|duration:...</ignore>）——bot 可主动拉黑。
        """
        label = {"user_msgs": f"user {user_id} sent", "session_msgs": "this session received",
                 "bot_speech": "you spoke"}[kind]
        dur = self.extra_default_duration
        return (
            f"[System: {label} {n} messages in {int(window)}s (threshold {threshold}). "
            f"Reply with <ignore>user:{user_id}|type:{kind}|duration:{dur}</ignore> to block "
            f"this user\'s {kind}, or <ignore>user:{user_id}|duration:{dur}</ignore> to fully "
            f"block, or <ignore>none</ignore> to do nothing. Ignore lasts {dur}s by default.]"
        )

    def on_im_message(self, event) -> None:
        """在宿主 handle_msg 中调用：存在感记录 + 骚扰检测 + 休眠判定。"""
        sid = event.session.sid
        now = time.time()
        is_dm = not getattr(event, "is_group_message", lambda: True)()
        self._get_presence(is_dm).note_incoming(sid, now, is_bot=False)

        # 骚扰检测（戳/at/关键词/引用）
        kind = self._detect_kind(event)
        if kind:
            user_id = self._sender_id(event)
            notice = self.harass.check(sid, kind, user_id, now)
            if notice:
                self.merger.queue(sid, notice)

        # 额外信号（user_msgs/session_msgs 群聊与私聊都可检测，各自独立开关/参数：
        # 群聊走 detect_user_msgs/session_msgs + user/session_msgs_* 参数；
        # 私聊走 dm_detect_* + dm_* 参数，默认关。bot_speech 仅群聊。）
        if is_dm:
            if self.dm_detect_user_msgs and not self.harass.is_ignored(sid, self._sender_id(event), "user_msgs", now):
                self._extra_counts[sid]["user_msgs"].append((now, self._sender_id(event)))
                n = sum(1 for ts, u in self._extra_counts[sid]["user_msgs"] if ts >= now - self.dm_user_msgs_window and u == self._sender_id(event))
                if n >= self.dm_user_msgs_threshold:
                    self._extra_counts[sid]["user_msgs"].clear()
                    self.merger.queue(sid, self._build_extra_notice("user_msgs", self._sender_id(event), n, self.dm_user_msgs_window, self.dm_user_msgs_threshold))
            if self.dm_detect_session_msgs and not self.harass.is_ignored(sid, self._sender_id(event), "session_msgs", now):
                self._extra_counts[sid]["session_msgs"].append((now, self._sender_id(event)))
                n = sum(1 for ts, _ in self._extra_counts[sid]["session_msgs"] if ts >= now - self.dm_session_msgs_window)
                if n >= self.dm_session_msgs_threshold:
                    self._extra_counts[sid]["session_msgs"].clear()
                    self.merger.queue(sid, self._build_extra_notice("session_msgs", self._sender_id(event), n, self.dm_session_msgs_window, self.dm_session_msgs_threshold))
        else:
            if self.detect_user_msgs and not self.harass.is_ignored(sid, self._sender_id(event), "user_msgs", now):
                self._extra_counts[sid]["user_msgs"].append((now, self._sender_id(event)))
                n = sum(1 for ts, u in self._extra_counts[sid]["user_msgs"] if ts >= now - self.user_msgs_window and u == self._sender_id(event))
                if n >= self.user_msgs_threshold:
                    self._extra_counts[sid]["user_msgs"].clear()
                    self.merger.queue(sid, self._build_extra_notice("user_msgs", self._sender_id(event), n, self.user_msgs_window, self.user_msgs_threshold))
            if self.detect_session_msgs and not self.harass.is_ignored(sid, self._sender_id(event), "session_msgs", now):
                self._extra_counts[sid]["session_msgs"].append((now, self._sender_id(event)))
                n = sum(1 for ts, _ in self._extra_counts[sid]["session_msgs"] if ts >= now - self.session_msgs_window)
                if n >= self.session_msgs_threshold:
                    self._extra_counts[sid]["session_msgs"].clear()
                    self.merger.queue(sid, self._build_extra_notice("session_msgs", self._sender_id(event), n, self.session_msgs_window, self.session_msgs_threshold))

        # 休眠判定：休眠期内被提及 → 起夜概率
        _just_woken = False
        if self.dormant.in_dormant(self._now_hhmm(), sid):
            mentioned = getattr(event.message, "is_mentioned", False) or event.is_mentioned
            if mentioned and not self.dormant.is_awake(sid, now):
                if self.dormant.try_wake(sid, now, user_id=self._sender_id(event)):
                    self.merger.queue(sid, self.dormant.wake_notice(sid))
                    _just_woken = True
                else:
                    # 起夜未命中：休眠期内不触发（宿主据此决定是否抑制）
                    event._enhance_dormant_blocked = True

        # 强制通路超额抑制：占比超标时被唤醒降级为评分门槛（分值到了才回）。
        # 刚被休眠唤醒的消息（起夜命中）是明确用户意图，不受存在感抑制
        if not _just_woken and self.force_suppress and not is_dm and self.presence.ratio(sid, now) > self.presence.target_ratio:
            mentioned = getattr(event.message, "is_mentioned", False) or event.is_mentioned
            if mentioned and self.presence.score(sid, now) < self.presence.score_threshold:
                event._enhance_force_suppressed = True

    def _detect_kind(self, event) -> Optional[str]:
        """识别事件类型：戳/at/关键词/引用。"""
        # 戳一戳：notice 事件
        if getattr(event, "is_notice", False):
            raw = getattr(event, "raw_message", None)
            if isinstance(raw, dict) and raw.get("notice_type") == "notify" \
                    and raw.get("sub_type") == "poke":
                return "poke"
            return None
        # 私聊消息：框架（qq.py 私聊路径）is_mentioned 写死 True（私聊=天然提及，
        # 无 @ 概念），at/关键词/引用检测对私聊无意义——会把正常私聊误判成骚扰。
        # 只保留上述 poke 检测，其余 kind 对私聊一律不检测。
        if not getattr(event, "is_group_message", lambda: True)():
            return None
        # 引用回复
        for m in getattr(event.message, "chain", []):
            if _Reply is not None and isinstance(m, _Reply):
                return "reply"
        # at / 关键词（宿主 handle_msg 已标记 _wake_source 区分来源）
        if getattr(event.message, "is_mentioned", False) or getattr(event, "is_mentioned", False):
            src = getattr(event, "_wake_source", None)
            if src == "keyword":
                return "keyword"
            return "at"
        return None

    @staticmethod
    def _sender_id(event) -> str:
        try:
            return str(event.message.sender.user_id)
        except Exception:
            return "unknown"

    @staticmethod
    def _now_hhmm() -> str:
        return time.strftime("%H:%M")

    # ---- LLM 请求（宿主 on_llm_request 调用）：注入合并通知 ----

    def on_llm_request(self, event, req) -> None:
        sid = getattr(event, "sid", None) or getattr(getattr(event, "session", None), "sid", None)
        if sid is None:
            return
        sid = str(sid)
        items = self.merger.drain(sid)
        if not items:
            return
        text = "\n".join(items)
        # 注入到 system prompt 的 chat_env 段（先注入，失败则把通知放回队列）
        try:
            for p in getattr(req, "system_prompt", []) or []:
                if getattr(p, "name", "") == "chat_env":
                    p.content = (p.content or "") + "\n" + text
                    return
            # 无 chat_env 段时追加一个框架 Prompt 实例（框架序列化按 isinstance(p, Prompt) 过滤，
            # 裸对象/裸 dict 会被静默丢弃——见 core/provider/llm_model.py）
            if _Prompt is not None:
                req.system_prompt.append(
                    _Prompt(text, name="chat_env", source="system", render_template=False)
                )
            else:
                # 框架模块不可用（测试环境）：尽力追加，失败放回队列
                req.system_prompt.append(
                    type("_SP", (), {"name": "chat_env", "content": text})()
                )
        except Exception:
            # 注入失败：通知放回队列，等待下次请求或短窗口兜底
            logger.exception("[Enhance] 通知注入 system prompt 失败，放回队列")
            for item in items:
                self.merger.queue(sid, item)

    # ---- LLM 响应（宿主 on_llm_response 调用）：存在感记录 + 维持期 ----

    def on_llm_response(self, event, resp) -> None:
        sid = getattr(event, "sid", None)
        if sid is None:
            return
        sid = str(sid)
        now = time.time()
        is_dm = not getattr(event, "is_group_message", lambda: True)()
        self._get_presence(is_dm).note_bot_reply(sid, now)
        self.dormant.note_reply(sid, now)

    # ---- 消息发送（宿主 on.message_sent 调用）：存在感统计 ----

    def on_message_sent(self, event) -> None:
        try:
            sid = str(event.session.sid)
        except Exception:
            return
        is_dm = not getattr(event, "is_group_message", lambda: True)()
        self._get_presence(is_dm).note_bot_reply(sid, time.time())
        # bot_speech 额外信号（仅群聊：私聊 bot 发言频率由自身主动逻辑控制；
        # 私聊对应开关 dm_* 不适用于 bot 自身发言）
        if not self.detect_bot_speech or is_dm:
            return
        now = time.time()
        if not self.harass.is_ignored(sid, "bot", "bot_speech", now):
            self._extra_counts[sid]["bot_speech"].append((now, "bot"))
            n = sum(1 for ts, _ in self._extra_counts[sid]["bot_speech"] if ts >= now - self.bot_speech_window)
            if n >= self.bot_speech_threshold:
                self._extra_counts[sid]["bot_speech"].clear()
                self.merger.queue(sid, self._build_extra_notice("bot_speech", "bot", n, self.bot_speech_window, self.bot_speech_threshold))

    # ---- 评分补正（宿主概率触发处调用） ----

    def score_gate(self, sid: str, prob_hit: bool, scope: str = "default", is_dm: bool = False) -> bool:
        """评分补正：返回是否应触发（累计加分机制）。

        三个通路（default/sustain/dm_sustain）各有 deny（门槛过滤）和 boost（补偿触发）独立开关。
        默认全关 → 评分系统未激活，原样返回概率命中结果。

        deny  概率命中 + 评分不足 → 作废（分数保留继续攒）
        boost 概率未命中 + 评分够 → 强制触发（必补），触发后清零
        deny+boost 同时开启 → 两者都生效（分不够拦、分够了补）
        """
        deny, boost = self._score_gate_flags(scope)
        if not deny and not boost:
            return prob_hit
        now = time.time()
        presence = self._get_presence(is_dm)
        if presence.score_threshold <= 0:
            return prob_hit
        score = presence.score(sid, now)
        # 闲时加分：静默超该会话历史平均 × idle_bonus_ratio 时 +idle_bonus
        idle_added = 0.0
        if presence.idle_bonus > 0 and presence.idle_bonus_ok(sid, now):
            score += presence.idle_bonus
            idle_added = presence.idle_bonus
        if prob_hit and score < presence.score_threshold:
            # 概率命中 + 评分不足 → deny 开启时拦下
            if deny:
                logger.info(
                    f"[Enhance] 评分补正({scope}{'|DM' if is_dm else ''} {sid}): "
                    f"概率命中但评分不足 → 抑制触发 deny=on "
                    f"(评分 {score:.1f}{f'(含闲时+{idle_added:.0f})' if idle_added else ''} < 阈值 {presence.score_threshold:.0f})"
                )
                return False
        if score >= presence.score_threshold:
            # 评分够 → boost 开启时（或 deny+boost 同时开）触发并清零
            if boost:
                presence.consume_score(sid)
                logger.info(
                    f"[Enhance] 评分补正({scope}{'|DM' if is_dm else ''} {sid}): "
                    f"评分达标 → 补触发 boost=on（概率未命中也算）"
                    f"(评分 {score:.1f}{f'(含闲时+{idle_added:.0f})' if idle_added else ''} ≥ 阈值 {presence.score_threshold:.0f}，已清零)"
                )
                return True
        logger.debug(
            f"[Enhance] 评分补正({scope}{'|DM' if is_dm else ''} {sid}): "
            f"评分不介入结果（概率命中={prob_hit}，评分 {score:.1f}，阈值 {presence.score_threshold:.0f}）"
        )
        return prob_hit

    def _score_gate_flags(self, scope: str) -> tuple[bool, bool]:
        """返回 (deny, boost) 用于给定 scope。"""
        if scope == "sustain":
            return self.sustain_score_gate_deny, self.sustain_score_gate_boost
        elif scope == "dm_sustain":
            return self.dm_sustain_score_gate_deny, self.dm_sustain_score_gate_boost
        elif scope == "mentioned":
            return self.mentioned_score_gate_deny, self.mentioned_score_gate_boost
        elif scope == "mentioned_dm":
            return self.mentioned_dm_score_gate_deny, self.mentioned_dm_score_gate_boost
        return self.score_gate_deny, self.score_gate_boost

    def k_prob(self, sid: str, is_dm: bool = False) -> float:
        return self._get_presence(is_dm).k_prob(sid, time.time())

    def idle_bonus(self, sid: str) -> float:
        is_dm = False  # caller doesn't have context; group fallback
        p = self._get_presence(is_dm)
        return p.idle_bonus if p.idle_bonus_ok(sid, time.time()) else 0.0
