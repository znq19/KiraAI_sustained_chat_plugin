import asyncio
import base64
import io
import os
import random
import re
import sys
import time
import wave
from collections import defaultdict
from typing import Optional, List, Dict, Any

# 插件管理器用 spec_from_file_location 加载 main.py，不会把插件目录加入 sys.path；
# 显式加入以便导入同目录的 queue_merge 模块（独立插件部署必需）
_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
if _PLUGIN_DIR not in sys.path:
    sys.path.insert(0, _PLUGIN_DIR)

# 热重载只重新 import main.py，sys.modules 里缓存的同目录模块不会更新；
# 强制重载，避免改了 queue_merge / media_recognize / chat_enhance 后热重载不生效（AttributeError 等）
import importlib
for _m in ("queue_merge", "media_recognize", "chat_enhance"):
    if _m in sys.modules:
        try:
            importlib.reload(sys.modules[_m])
        except Exception:
            pass

from core.plugin import BasePlugin, logger, on, Priority, register
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat import KiraIMMessage, User, Group, Session, MessageChain
from core.provider import LLMRequest, LLMResponse
from core.chat.message_elements import Text, Image, Reply, Sticker, Forward, Record
from queue_merge import BatchMergeScheduler
from media_recognize import ParallelMediaRecognizer
from chat_enhance import ChatEnhanceEngine, _safe_int, _safe_float

try:
    from croniter import croniter
except ImportError:
    croniter = None
    logger.warning("croniter not installed, cron schedule disabled")


class DebouncePlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)

        # ========== 从 section_basic 读取基础配置 ==========
        basic = cfg.get("section_basic", {})
        self.waking_words = basic.get("waking_words", [])
        self.receive_unmentioned = basic.get("receive_unmentioned", True)
        self.max_unmentioned_messages = _safe_int(basic.get("max_unmentioned_messages"), 5)
        self.group_chat_prompt = basic.get("group_chat_prompt", '### 群聊环境说明\r\n\r\n当前为群聊环境，你需要聚焦于**和你有直接关联**或**你十分感兴趣**的消息，对于仅显示为[动画表情]或[图片]的消息不用互动，注意不要刷屏，可以选择不回复任何消息，直接输出<msg/>即可。\r\n\r\n## 消息感知\r\n\r\n你可能会同时收到多条消息，请根据上下文自主决策该回复哪些消息，注意不要刷屏，也可以选择不回复任何消息，直接输出<msg/>即可。\r\n你可以使用 <reasoning>reasoning_content</reasoning> 的标签格式来输出推理内容放在整个输出的最前面，用于推理应该回复哪些消息，回复语气，回复条数，消息分段情况等。\r\n<reasoning>标签和<msg>标签同级，**禁止**将次标签放到<msg>标签内。\r\n**符合以上规则的情况下**确保你想发的聊天消息在<text>标签内，不要遗漏。\r\n')
        self.group_proactive_chat = basic.get("group_proactive_chat", False)
        self.group_proactive_chat_probability = _safe_float(basic.get("group_proactive_chat_probability"), 0.1)
        self.proactive_k_prob_enabled = basic.get("proactive_k_prob_enabled", True)
        self.proactive_score_gate_deny = basic.get("proactive_score_gate_deny", True)
        self.proactive_score_gate_boost = basic.get("proactive_score_gate_boost", True)
        self.proactive_scope_sessions = set(
            str(x) for x in (basic.get("proactive_scope_sessions") or [])
        )
        # 主动屏蔽工具开关（manage_ignore）：关闭后 bot 不再能主动屏蔽骚扰
        self.enable_manage_ignore = basic.get("enable_manage_ignore", True)

        # ========== 从 section_media 读取媒体处理配置 ==========
        media = cfg.get("section_media", {})
        self.image_recognition_only_on_mention = media.get("image_recognition_only_on_mention", False)
        self.image_recognition_probability = _safe_float(media.get("image_recognition_probability"), 1.0)
        self.max_images_per_message = _safe_int(media.get("max_images_per_message"), 3)
        self.forward_recognition_only_on_mention = media.get("forward_recognition_only_on_mention", True)
        self.voice_recognition_only_on_mention = media.get("voice_recognition_only_on_mention", False)
        self.voice_private_need_mention = media.get("voice_private_need_mention", False)
        self.voice_max_duration = _safe_int(media.get("voice_max_duration"), 0)

        # ========== 从 section_group_sustain 读取群聊持续对话配置 ==========
        group_sustain = cfg.get("section_group_sustain", {})
        self.sustain_enabled = group_sustain.get("sustain_enabled", False)
        self.sustain_window_seconds = _safe_float(group_sustain.get("sustain_window_seconds"), 180)
        self.sustain_reply_probability = _safe_float(group_sustain.get("sustain_reply_probability"), 0.5)
        self.max_sustain_replies = _safe_int(group_sustain.get("max_sustain_replies"), -1)
        self.sustain_stop_keywords = group_sustain.get("sustain_stop_keywords", [])
        self.stop_on_ai_keywords = group_sustain.get("stop_on_ai_keywords", ['晚安', '再见', '拜拜', '下次再聊', '下次聊', '不聊了', '不想理', '不理你', '不说了'])
        self.stop_on_ai_empty = group_sustain.get("stop_on_ai_empty", True)
        self.sustain_mode = group_sustain.get("sustain_mode", "per_round")
        # 新增：群聊持续对话作用域（白名单/黑名单）与 LLM 请求时开窗判定
        self.sustain_allowed_sessions = group_sustain.get("sustain_allowed_sessions", [])
        self.sustain_denied_sessions = group_sustain.get("sustain_denied_sessions", [])
        self.sustain_judge_timing = group_sustain.get("sustain_judge_timing", "either")
        # 空 msg 后评分补上再触发（默认关）：bot 空 msg 只是"这次不回"，不是"这轮结束"
        self.sustain_retry_on_empty = bool(group_sustain.get("sustain_retry_on_empty", False))
        self.sustain_k_prob_enabled = group_sustain.get("sustain_k_prob_enabled", False)

        # ========== 从 section_dm_sustain 读取私聊持续对话配置 ==========
        dm_sustain = cfg.get("section_dm_sustain", {})
        self.dm_sustain_enabled = dm_sustain.get("dm_sustain_enabled", False)
        self.dm_sustain_window_range = dm_sustain.get("dm_sustain_window_range", "60s/45s")
        self.dm_sustain_reply_probability = _safe_float(dm_sustain.get("dm_sustain_reply_probability"), 0.3)
        self.dm_max_sustain_replies = _safe_int(dm_sustain.get("dm_max_sustain_replies"), -1)
        self.dm_sustain_mode = dm_sustain.get("dm_sustain_mode", "per_retry")
        self.dm_max_retry_attempts = _safe_int(dm_sustain.get("dm_max_retry_attempts"), 3)
        self.dm_sustain_stop_keywords = dm_sustain.get("dm_sustain_stop_keywords", [])
        self.dm_stop_on_ai_keywords = dm_sustain.get("dm_stop_on_ai_keywords", ['晚安', '再见', '拜拜', '下次再聊', '下次聊', '不聊了', '不想理', '不理你', '不说了'])
        self.dm_stop_on_ai_empty = dm_sustain.get("dm_stop_on_ai_empty", True)
        self.dm_allowed_users = dm_sustain.get("dm_allowed_users", [])
        self.dm_denied_users = dm_sustain.get("dm_denied_users", [])
        self.dm_proactive_prompt = dm_sustain.get(
            "dm_proactive_prompt",
            "请根据当前对话上下文，自然地主动发送一条消息，可以随意开启新话题或延续之前的聊天。不要提及这是主动触发。"
        )
        # 新增：私聊主动触发的工具黑名单
        self.dm_tool_blacklist = dm_sustain.get("dm_tool_blacklist", [])
        self.dm_tool_blacklist_mode = dm_sustain.get("dm_tool_blacklist_mode", "partial")
        # 新增：控制停止词是否触发重试
        self.dm_retry_on_user_stop = dm_sustain.get("dm_retry_on_user_stop", True)
        self.dm_retry_on_ai_stop = dm_sustain.get("dm_retry_on_ai_stop", True)
        # 空 msg 后评分补上再触发（默认关）：bot 空 msg 只是"这次不回"，不是"这轮结束"
        self.dm_retry_on_empty = bool(dm_sustain.get("dm_retry_on_empty", False))
        self.dm_k_prob_enabled = dm_sustain.get("dm_k_prob_enabled", False)

        # ========== 从 section_scheduled 读取定时任务配置 ==========
        scheduled = cfg.get("section_scheduled", {})
        self.scheduled_enabled = scheduled.get("scheduled_enabled", False)
        self.scheduled_sessions = scheduled.get("scheduled_sessions", [])
        self.scheduled_max_per_round = _safe_int(scheduled.get("scheduled_max_per_round"), 1)
        self.scheduled_type = scheduled.get("scheduled_type", "interval")
        self.scheduled_interval_expression = scheduled.get("scheduled_interval_expression", "5m/270s")
        self.scheduled_cron = scheduled.get("scheduled_cron", "0 */1 * * *")
        self.scheduled_context_count = _safe_int(scheduled.get("scheduled_context_count"), 10)
        self.scheduled_fetch_history = scheduled.get("scheduled_fetch_history", True)
        self.scheduled_initial_history_count = _safe_int(scheduled.get("scheduled_initial_history_count"), 10)
        self.scheduled_prompt = scheduled.get("scheduled_prompt", '根据人设和上下文自然发言与使用你想用的工具并返回结果，严禁暴露这是定时任务')
        self.scheduled_tool_blacklist = scheduled.get("scheduled_tool_blacklist", [])
        self.scheduled_tool_blacklist_mode = scheduled.get("scheduled_tool_blacklist_mode", "partial")

        # ========== 原有状态变量 ==========
        self.session_events: dict[str, asyncio.Event] = {}
        self.session_tasks: dict[str, asyncio.Task] = {}
        bot_cfg = ctx.config["bot_config"].get("bot", {})
        self.debounce_interval = _safe_float(bot_cfg.get("max_message_interval"), 1.5)
        self.max_buffer_messages = _safe_int(bot_cfg.get("max_buffer_messages"), 3)

        # 群聊持续状态
        self.sustain_until = defaultdict(float)
        self.sustain_count = defaultdict(int)
        self.sustain_tasks: dict[str, asyncio.Task] = {}
        self.sustain_judged = defaultdict(bool)
        # 本轮终止标志：AI 空消息 / AI 停止词 / 用户停止词 / 达上限停窗后置位；
        # LLM 请求兜底开窗据此不再重开窗口，直到下次真实唤醒（@/唤醒词/引用回复）解除
        self.sustain_stopped: dict[str, bool] = {}
        # 持续命中消息的 message_id 集合（按 sid）：用于区分「持续命中触发」与
        # 「真实唤醒」，停窗时仅丢弃前者产生的积压批；新一轮唤醒时清空
        self.sustain_hit_ids: dict[str, set] = {}

        # 私聊持续状态
        self.dm_sustain_until = defaultdict(float)
        self.dm_sustain_count = defaultdict(int)
        self.dm_sustain_tasks: dict[str, asyncio.Task] = {}
        self.dm_sustain_retry_count = defaultdict(int)
        self.dm_sustain_active = defaultdict(bool)

        # 定时任务状态
        self._scheduler_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # 队列合并 / 积压处理（BatchMergeScheduler）
        self.merge_scheduler = BatchMergeScheduler(ctx, cfg, bot_cfg)
        # 并行媒体识别（ParallelMediaRecognizer）
        self.media_recognizer = ParallelMediaRecognizer(ctx, cfg, bot_cfg)
        # ========== 聊天增强引擎（存在感节流/骚扰感知化/休眠状态机/通知合并） ==========
        # 引擎内 PresenceThrottle/DormantState 读扁平键，HarassDetector 读 section_* 键，
        # 因此配置需同时保留 section 结构 + 拍平 presence/dormant 键
        _enhance_cfg = dict(cfg)
        for _sec in ("section_presence", "section_dormant"):
            _sec_cfg = cfg.get(_sec, {}) or {}
            for _k, _v in _sec_cfg.items():
                _enhance_cfg[_k] = _v
        # 防骚扰/休眠作用域与白名单（section_harass_scope / section_dormant 内）
        _hscope = cfg.get("section_harass_scope", {}) or {}
        for _k in ("harass_scope_sessions", "harass_whitelist_users", "harass_whitelist_sessions"):
            if _k in _hscope:
                _enhance_cfg[_k] = _hscope[_k]
        _dscope = cfg.get("section_dormant", {}) or {}
        for _k in ("dormant_scope_sessions", "dormant_whitelist_users", "dormant_whitelist_sessions"):
            if _k in _dscope:
                _enhance_cfg[_k] = _dscope[_k]
        # 评分补正独立开关（门槛过滤 deny / 补偿触发 boost，三个通路各自独立）
        _gscope = cfg.get("section_group_sustain", {}) or {}
        for _k in ("sustain_score_gate_deny", "sustain_score_gate_boost"):
            if _k in _gscope:
                _enhance_cfg[_k] = _gscope[_k]
        _dmscope = cfg.get("section_dm_sustain", {}) or {}
        for _k in ("dm_sustain_score_gate_deny", "dm_sustain_score_gate_boost"):
            if _k in _dmscope:
                _enhance_cfg[_k] = _dmscope[_k]
        # 群聊主动概率的评分补正（从 section_basic 透传至引擎 default scope）
        _basic = cfg.get("section_basic", {}) or {}
        # proactive_score_gate_deny/boost → 引擎的 score_gate_deny/boost（default scope）
        _enhance_cfg["score_gate_deny"] = _basic.get("proactive_score_gate_deny", True)
        _enhance_cfg["score_gate_boost"] = _basic.get("proactive_score_gate_boost", True)
        # 私聊独立参数（从 section_dm_sustain 透传）
        _dmparams = cfg.get("section_dm_sustain", {}) or {}
        for _k in ("dm_presence_enabled", "dm_presence_window_size", "dm_presence_target_ratio",
                   "dm_presence_k_min", "dm_presence_k_max",
                   "dm_score_threshold", "dm_score_increment", "dm_score_penalty",
                   "dm_score_cap", "dm_idle_bonus_score", "dm_idle_bonus_ratio"):
            if _k in _dmparams:
                _enhance_cfg[_k] = _dmparams[_k]
        # 提及消息评分补正（从 section_presence 透传，已 flatten）
        # 注释：section_presence 的密钥已在上面 flatten 循环中透传
        self.enhance = ChatEnhanceEngine(ctx, _enhance_cfg, self, merge_seconds=self.debounce_interval)

    async def initialize(self):
        logger.info("[Debounce] 插件已初始化")
        logger.info(f"[Debounce] 唤醒词: {self.waking_words}")
        logger.info(f"[Debounce] 接收非唤醒消息: {self.receive_unmentioned}")
        if self.sustain_enabled:
            logger.info(f"[Debounce] 群聊持续对话已启用: mode={self.sustain_mode}, window={self.sustain_window_seconds}s, prob={self.sustain_reply_probability}, max={self.max_sustain_replies}")
        if self.dm_sustain_enabled:
            logger.info(f"[Debounce] 私聊持续对话已启用: mode={self.dm_sustain_mode}, window_range={self.dm_sustain_window_range}, prob={self.dm_sustain_reply_probability}, max={self.dm_max_sustain_replies}, retry_max={self.dm_max_retry_attempts}")
            if self.dm_allowed_users:
                logger.info(f"[Debounce] 私聊白名单: {self.dm_allowed_users}")
            if self.dm_denied_users:
                logger.info(f"[Debounce] 私聊黑名单: {self.dm_denied_users}")
            logger.info(f"[Debounce] 私聊主动提示词: {self.dm_proactive_prompt[:50]}...")
            if self.dm_tool_blacklist:
                logger.info(f"[Debounce] 私聊工具黑名单: {self.dm_tool_blacklist} (mode={self.dm_tool_blacklist_mode})")
            logger.info(f"[Debounce] 私聊重试配置: user_stop_retry={self.dm_retry_on_user_stop}, ai_stop_retry={self.dm_retry_on_ai_stop}")
        if self.scheduled_enabled:
            logger.info(f"[Debounce] 定时任务已启用: type={self.scheduled_type}, sessions={len(self.scheduled_sessions)}, max_per_round={self.scheduled_max_per_round}")
            if self.scheduled_type == "interval":
                logger.info(f"[Debounce] 间隔表达式: {self.scheduled_interval_expression}")
            else:
                logger.info(f"[Debounce] Cron: {self.scheduled_cron}")

        if self.scheduled_enabled:
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

        # 启动聊天增强引擎（存在感/骚扰/休眠/通知合并）
        self.enhance.start()
        # 接管互斥：检测独立防骚扰插件是否已加载，已加载则提示停用（本插件内置同能力）
        try:
            _pm = self.ctx.plugin_mgr
            if _pm is not None:
                _loaded = set()
                try:
                    # 框架 PluginManager 无 get_loaded_plugin_ids，用 list_plugins 取 plugin_id
                    _infos = _pm.list_plugins() if hasattr(_pm, "list_plugins") else []
                    _loaded = set(getattr(i, "plugin_id", "") for i in (_infos or []))
                except Exception:
                    pass
                if any("anti-harass" in str(pid).lower() for pid in _loaded):
                    logger.warning(
                        "[Enhance] 检测到独立防骚扰插件已加载，本插件已内置完整骚扰屏蔽能力，"
                        "建议停用独立防骚扰插件避免重复检测/重复通知"
                    )
        except Exception:
            pass

    async def terminate(self):
        for sid, task in list(self.session_tasks.items()):
            if not task.done():
                task.cancel()
        if self.session_tasks:
            await asyncio.gather(*self.session_tasks.values(), return_exceptions=True)
        self.session_tasks.clear()
        self.session_events.clear()

        for task in self.sustain_tasks.values():
            if not task.done():
                task.cancel()
        if self.sustain_tasks:
            await asyncio.gather(*self.sustain_tasks.values(), return_exceptions=True)
        self.sustain_tasks.clear()

        for task in self.dm_sustain_tasks.values():
            if not task.done():
                task.cancel()
        if self.dm_sustain_tasks:
            await asyncio.gather(*self.dm_sustain_tasks.values(), return_exceptions=True)
        self.dm_sustain_tasks.clear()

        if self._scheduler_task and not self._scheduler_task.done():
            self._shutdown_event.set()
            try:
                await asyncio.wait_for(self._scheduler_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._scheduler_task.cancel()

        # 清理合并调度器（重发 pending + 取消 tick）
        await self.merge_scheduler.shutdown()
        # 关闭聊天增强引擎（await 等待 prune 任务退出）
        await self.enhance.shutdown()
        logger.info("[Debounce] 插件已终止")

    # ========== 工具函数 ==========
    @staticmethod
    def _parse_duration(expr: str) -> int:
        expr = expr.strip()
        if not expr:
            return 0
        try:
            return int(expr)
        except ValueError:
            pass
        # 兼容 schema 提示中的 1min/30s、1h、30s 等写法
        match = re.match(r'^(\d+)\s*(hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)$', expr, re.IGNORECASE)
        if match:
            val, unit = match.groups()
            val = int(val)
            unit = unit.lower()
            if unit.startswith('h'):
                return val * 3600
            if unit.startswith('m'):
                return val * 60
            return val
        return 0

    def _parse_window_range(self, range_str: str) -> tuple[int, int]:
        if '/' not in range_str:
            base = self._parse_duration(range_str)
            return base, 0
        parts = range_str.split('/', 1)
        base_str, var_str = parts[0].strip(), parts[1].strip()
        base = self._parse_duration(base_str)
        var = self._parse_duration(var_str)
        return base, var

    def _get_dm_window_seconds(self) -> int:
        base, var = self._parse_window_range(self.dm_sustain_window_range)
        if var == 0:
            return max(1, base)
        low = max(1, base - var)
        high = base + var
        return random.randint(low, high)

    def _is_dm_allowed(self, sid: str) -> bool:
        if self.dm_allowed_users:
            return sid in self.dm_allowed_users
        if self.dm_denied_users:
            return sid not in self.dm_denied_users
        return True

    def _filter_tools(self, tool_set, blacklist: List[str], mode: str):
        """按黑名单过滤 ToolSet。

        官方 ToolSet.tools 是 BaseTool 实例列表（见 core/agent/tool.py），
        不是 OpenAI function dict；应使用 tool.name + tool_set.remove。
        """
        if not blacklist or not tool_set or not getattr(tool_set, "tools", None):
            return
        to_remove: list[str] = []
        for tool in list(tool_set.tools):
            name = getattr(tool, "name", None) or ""
            if not name:
                continue
            if mode == "partial":
                if any(kw in name for kw in blacklist):
                    to_remove.append(name)
            else:
                if name in blacklist:
                    to_remove.append(name)
        if to_remove:
            tool_set.remove(*to_remove)
            logger.debug(f"[Proactive] 已从 tool_set 移除工具: {to_remove}")

    # ========== 骚扰屏蔽 XML tag（戳/at/关键词/引用） ==========

    @register.tag(name="wake_extend", description="休眠唤醒后主动续窗。输出 <wake_extend>yes</wake_extend> 延长维持期（受 wake_max_extensions 限制）。")
    async def handle_wake_extend(self, value: str, **kwargs) -> list:
        try:
            sid = self._last_ignore_sid
        except AttributeError:
            sid = None
        if sid is None:
            return []
        if (value or "").strip().lower() == "yes":
            result = self.enhance.dormant.extend(sid, __import__("time").time())
            if result:
                logger.info(f"[Enhance] 主动续窗: {result}")
        return []

    @register.tag(name="poke_ignore", description="屏蔽戳一戳骚扰。输出 <poke_ignore>user|duration:N</poke_ignore> 屏蔽目标用户，<poke_ignore>all|duration:N</poke_ignore> 屏蔽所有用户，<poke_ignore>none</poke_ignore> 不屏蔽。duration 为秒，留空用默认值。")
    async def handle_poke_ignore(self, value: str, **kwargs) -> list:
        return self._apply_ignore_tag("poke", value)

    @register.tag(name="ignore", description="拉黑用户：屏蔽后该用户/会话的所有消息不再进入（含戳一戳/at/关键词/引用/刷屏）。输出 <ignore>user|duration:N</ignore> 拉黑目标用户，<ignore>all|duration:N</ignore> 拉黑所有用户，<ignore>none</ignore> 不屏蔽。duration 为秒，留空用默认值。")
    async def handle_ignore(self, value: str, **kwargs) -> list:
        return self._apply_ignore_tag("all", value)

    def _apply_ignore_tag(self, kind: str, value: str) -> list:
        """解析骚扰屏蔽 tag 值并执行屏蔽。返回空列表（tag 不产生消息输出）。"""
        try:
            sid = self._last_ignore_sid
        except AttributeError:
            sid = None
        if sid is None:
            return []
        result = self.enhance.harass.apply_ignore_from_tag(sid, kind, value)
        if result:
            logger.info(f"[Enhance] {kind} 屏蔽: {result}")
        return []

    @register.tool(
        name="manage_ignore",
        description="管理骚扰屏蔽：屏蔽某个用户/会话/某种唤醒方式，或提前解除屏蔽。bot 觉得被骚扰、或人设要求时调用。",
        params={
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["block", "unblock", "list"],
                    "description": "block=屏蔽，unblock=解除屏蔽，list=查看当前屏蔽列表",
                },
                "target_type": {
                    "type": "string",
                    "enum": ["user", "session", "all"],
                    "description": "屏蔽对象：user=某个用户，session=某个会话，all=全局（所有用户所有会话）",
                },
                "target_id": {
                    "type": "string",
                    "description": "目标 ID：target_type=user 时是用户 ID，=session 时是会话 ID，=all 时留空",
                },
                "block_type": {
                    "type": "string",
                    "enum": ["poke", "all"],
                    "description": "屏蔽类型：poke=只屏蔽戳一戳（其他形式正常），all=拉黑（该用户/会话所有消息不再进入，含戳一戳）",
                    "default": "all",
                },
                "duration": {
                    "type": "integer",
                    "description": "屏蔽时长（秒）。留空用默认值；-1 表示永久",
                    "default": 0,
                },
            },
            "required": ["action", "target_type"],
        },
    )
    async def manage_ignore(self, event, action: str, target_type: str, target_id: str = "",
                            block_type: str = "all", duration: int = 0) -> str:
        """bot 主动管理骚扰屏蔽。"""
        try:
            sid = str(event.session.sid)
        except Exception:
            sid = str(getattr(event, "sid", ""))
        if action == "list":
            return self.enhance.harass.list_ignored(sid)
        if action == "unblock":
            if target_type == "all":
                return "请指定要解除的用户或会话"
            return self.enhance.harass.unblock(sid, target_id, block_type)
        # block
        if target_type == "all":
            return self.enhance.harass.apply_ignore("*", "*", block_type, duration)
        if target_type == "session":
            return self.enhance.harass.apply_ignore(sid, "*", block_type, duration)
        return self.enhance.harass.apply_ignore(sid, target_id, block_type, duration)

    def _is_empty_msg(self, xml: str) -> bool:
        pattern = r'^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$'
        return bool(re.match(pattern, xml))

    def _check_stop_keywords(self, text: str, keywords: List[str]) -> bool:
        if not keywords:
            return False
        text_lower = text.lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                return True
        return False

    # ========== 群聊持续对话 ==========
    def _is_proactive_allowed(self, sid: str) -> bool:
        """群聊积极概率作用域检查：scope 非空时仅这些会话生效（空=全部）。"""
        if not self.proactive_scope_sessions:
            return True
        return sid in self.proactive_scope_sessions

    def _is_sustain_allowed(self, sid: str) -> bool:
        """群聊持续对话作用域检查：白名单非空时仅白名单内生效；白名单为空时排除黑名单。

        群不在作用域内时顺带清理该群的残留状态（窗口/计数/判定标记），
        避免群被移出白名单后 count 等脏状态一直留在内存。
        """
        if self.sustain_allowed_sessions:
            allowed = sid in self.sustain_allowed_sessions
        elif self.sustain_denied_sessions:
            allowed = sid not in self.sustain_denied_sessions
        else:
            allowed = True
        if not allowed:
            self._clear_sustain_state(sid)
            self.sustain_stopped.pop(sid, None)
            self.sustain_hit_ids.pop(sid, None)
        return allowed

    def _is_in_sustain_window(self, sid: str) -> bool:
        # 用 .get 避免 defaultdict 隐式建键（与 _is_in_dm_sustain 一致）
        return time.time() < self.sustain_until.get(sid, 0.0)

    def _start_sustain_window(self, sid: str):
        deadline = time.time() + self.sustain_window_seconds
        self.sustain_until[sid] = deadline
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks[sid] = asyncio.create_task(self._end_sustain_window(sid))
        if self.sustain_mode == "per_round":
            self.sustain_judged[sid] = False

    async def _end_sustain_window(self, sid: str):
        try:
            await asyncio.sleep(self.sustain_window_seconds)
        except asyncio.CancelledError:
            return
        # 超时后清除窗口但保留连续计数（count 只在真实唤醒时清零），
        # 避免长 LLM 处理（工具循环超过窗口时长）绕过 max_sustain_replies 限制
        # 若窗口已被提前关闭/刷新，deadline 会变化或消失，避免误清
        deadline = self.sustain_until.get(sid, 0)
        if deadline and time.time() >= deadline:
            # 直接清理字段，不走 _clear_sustain_window（避免 cancel 自身任务）。
            # 任务身份校验：仅当本任务仍是当前窗口任务时才 pop，避免陈旧任务
            # 误删新窗口任务的引用（与 _dm_sustain_loop 身份校验一致）
            if self.sustain_tasks.get(sid) is asyncio.current_task():
                self.sustain_tasks.pop(sid, None)
            self.sustain_until.pop(sid, None)
            self.sustain_judged.pop(sid, None)
            logger.debug(f"[Sustain] 群 {sid} 持续窗口超时结束（保留计数 {self.sustain_count.get(sid, 0)}）")

    def _clear_sustain_window(self, sid: str, keep_count: bool = False):
        """清除群聊持续窗口状态。

        keep_count=True 时保留连续回复计数（命中后关闭窗口、等 AI 再开窗时使用）。
        keep_count=False 时连同计数一并清除（超时/停止词/达上限等）。
        """
        self.sustain_until.pop(sid, None)
        if not keep_count:
            self.sustain_count.pop(sid, None)
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks.pop(sid, None)
        self.sustain_judged.pop(sid, None)

    def _clear_sustain_state(self, sid: str):
        """完全清除群聊持续状态（含连续计数）。"""
        self._clear_sustain_window(sid, keep_count=False)

    async def _stop_sustain_round(self, sid: str):
        """明确终止本轮群聊持续对话（AI 空消息 / AI 停止词 / 用户停止词 / 达上限）。

        除清除窗口与计数外：
        - 置位 sustain_stopped：LLM 请求兜底开窗不再重开窗口，直到下次真实唤醒
          （否则停窗后 count 归零，兜底开窗会以「连续次数 0」开启全新一轮，停止形同虚设）；
        - 丢弃 QueueMerge pending 中「仅由持续命中消息触发」的积压批，
          避免停窗前已命中的消息在停窗后仍被追加回复（真实唤醒的批次保留）。
        sustain_hit_ids 不在此处清除：停窗后姗姗来迟的纯持续命中批次（debounce
        尚未 flush）仍需凭它在批次入口拦截；由下次真实唤醒统一清空。
        """
        self._clear_sustain_state(sid)
        self.sustain_stopped[sid] = True
        dropped = await self.merge_scheduler.drop_sustain_pending(sid, self.sustain_hit_ids.get(sid))
        if dropped:
            logger.debug(f"[Sustain] 群 {sid} 停窗，丢弃持续命中积压批次 {dropped} 个")

    # ========== 私聊持续对话 ==========
    def _is_in_dm_sustain(self, sid: str) -> bool:
        return self.dm_sustain_active.get(sid, False) and time.time() < self.dm_sustain_until.get(sid, 0)

    def _cancel_dm_sustain(self, sid: str):
        """仅取消私聊持续窗口，保留主动回复计数与重试计数。"""
        if sid in self.dm_sustain_tasks and not self.dm_sustain_tasks[sid].done():
            self.dm_sustain_tasks[sid].cancel()
        self.dm_sustain_tasks.pop(sid, None)
        self.dm_sustain_active[sid] = False
        self.dm_sustain_until.pop(sid, None)

    def _reset_dm_sustain_count(self, sid: str):
        """重置私聊主动回复计数与重试计数（用户真实发言 / 停止词结束一轮时）。"""
        prev = self.dm_sustain_count.get(sid, 0)
        self.dm_sustain_count.pop(sid, None)
        self.dm_sustain_retry_count.pop(sid, None)
        if prev:
            logger.debug(f"[DM Sustain] 重置主动回复计数: {sid}（原 {prev}）")

    def _clear_dm_sustain_state(self, sid: str):
        """完全清除私聊持续状态（窗口 + 计数）。"""
        self._cancel_dm_sustain(sid)
        self._reset_dm_sustain_count(sid)

    def _start_dm_sustain_window(self, sid: str):
        # 检查是否为私聊会话
        parts = sid.split(":", 2)
        if len(parts) != 3 or parts[1] != "dm":
            logger.debug(f"[DM Sustain] 跳过非私聊会话: {sid}")
            return

        if not self.dm_sustain_enabled:
            return
        if not self._is_dm_allowed(sid):
            return
        current_count = self.dm_sustain_count.get(sid, 0)
        if self.dm_max_sustain_replies != -1 and current_count >= self.dm_max_sustain_replies:
            logger.debug(
                f"[DM Sustain] 已达最大主动回复次数 {self.dm_max_sustain_replies}，不再开窗: {sid}"
            )
            return
        # 取消旧窗口
        self._cancel_dm_sustain(sid)
        wait_seconds = self._get_dm_window_seconds()
        deadline = time.time() + wait_seconds
        self.dm_sustain_until[sid] = deadline
        self.dm_sustain_active[sid] = True
        # 注意：不重置 dm_sustain_retry_count / dm_sustain_count，由外部控制
        task = asyncio.create_task(self._dm_sustain_loop(sid, wait_seconds))
        self.dm_sustain_tasks[sid] = task
        logger.debug(
            f"[DM Sustain] 窗口启动: {sid}, 等待 {wait_seconds}s, "
            f"主动次数 {current_count}/{self.dm_max_sustain_replies if self.dm_max_sustain_replies != -1 else '∞'}, "
            f"重试计数 {self.dm_sustain_retry_count.get(sid, 0)}"
        )

    def _handle_dm_failure(self, sid: str, reason: str = ""):
        """处理私聊主动触发失败（概率未命中、停止词等），根据模式决定重试或取消"""
        if self.dm_sustain_mode == "per_round":
            self._cancel_dm_sustain(sid)
            logger.debug(f"[DM Sustain] per_round 模式，失败后取消窗口: {sid} ({reason})")
            return

        # per_retry 模式
        retry_count = self.dm_sustain_retry_count.get(sid, 0) + 1
        self.dm_sustain_retry_count[sid] = retry_count
        if retry_count >= self.dm_max_retry_attempts:
            logger.debug(f"[DM Sustain] 达到最大重试次数 {self.dm_max_retry_attempts}，停止窗口: {sid} ({reason})")
            self._cancel_dm_sustain(sid)
        else:
            logger.debug(f"[DM Sustain] 失败重试 {retry_count}/{self.dm_max_retry_attempts}: {sid} ({reason})")
            # 取消当前窗口（如果还在运行）
            self._cancel_dm_sustain(sid)
            # 启动新窗口（保持重试计数）
            self._start_dm_sustain_window(sid)

    async def _dm_sustain_loop(self, sid: str, wait_seconds: int):
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            logger.debug(f"[DM Sustain] 窗口取消: {sid}")
            return

        if not self.dm_sustain_active.get(sid, False):
            return

        rand_val = random.random()
        # 休眠期内不主动触发（休眠时段不主动）。
        # 注意：休眠期不消耗重试计数（per_retry 模式下反复重开窗会烧光
        # dm_max_retry_attempts，休眠结束后不再主动触发）。直接取消窗口，
        # 保留 retry_count，休眠结束后由用户消息/on_llm_response 重新开窗。
        if self.enhance.dormant.in_dormant(self.enhance._now_hhmm(), sid):
            logger.debug(f"[DM Sustain] 休眠期内不主动触发: {sid}")
            self._cancel_dm_sustain(sid)
            return
        # 存在感节流：概率 × k_prob（回少提高/回多降低）+ 评分补正
        _dm_prob = self.dm_sustain_reply_probability
        if self.dm_k_prob_enabled:
            _dm_prob *= self.enhance.k_prob(sid, is_dm=True)
        _dm_hit = rand_val < _dm_prob
        if self.enhance.score_gate(sid, _dm_hit, scope="dm_sustain", is_dm=True):
            self.dm_sustain_count[sid] += 1
            count = self.dm_sustain_count[sid]
            # 成功发送后重置重试计数（保留主动次数）
            self.dm_sustain_retry_count[sid] = 0
            logger.info(
                f"[DM Sustain] 触发主动回复: {sid} "
                f"(概率 {rand_val:.2f} < {_dm_prob:.2f})，"
                f"连续主动次数 {count}"
                + (
                    f"/{self.dm_max_sustain_replies}"
                    if self.dm_max_sustain_replies != -1
                    else ""
                )
            )
            await self._trigger_dm_proactive(sid)
            # 只关窗，保留 count，供后续 on_llm_response / 下次开窗判断 max。
            # 注意：await 期间 on_llm_response 可能已对同一 sid 开新窗口，
            # 必须校验任务身份，避免误杀新窗口。
            if self.dm_sustain_tasks.get(sid) is asyncio.current_task():
                self._cancel_dm_sustain(sid)
        else:
            logger.debug(f"[DM Sustain] 未命中: {sid} (概率 {rand_val:.2f} >= {_dm_prob:.2f})")
            self._handle_dm_failure(sid, "概率未命中")

    async def _trigger_dm_proactive(self, sid: str):
        parts = sid.split(":", 2)
        if len(parts) != 3 or parts[1] != "dm":
            logger.error(f"[DM Sustain] 非私聊会话，跳过: {sid}")
            return
        adapter_name, session_type, session_id = parts

        adapter = self.ctx.adapter_mgr.get_adapter(adapter_name)
        if not adapter:
            logger.error(f"[DM Sustain] 无法获取适配器: {adapter_name}")
            return

        # 对齐官方 PluginContext.publish_notice 的事件构造方式
        # （core/plugin/plugin_context.py），再覆盖 session 到真实私聊 sid
        cur_time = int(time.time())
        user = User(user_id="system_proactive_dm", nickname="系统主动触发")
        chain = MessageChain([Text(self.dm_proactive_prompt)])

        event = KiraMessageEvent(
            adapter=adapter.info,
            message_types=adapter.message_types,
            message=KiraIMMessage(
                timestamp=cur_time,
                sender=user,
                message_id="system_proactive",
                self_id=str(adapter.config.get("self_id", "") or ""),
                chain=chain,
                is_notice=False,
                is_mentioned=True,
            ),
            timestamp=cur_time,
        )
        # __post_init__ 会按 sender 生成错误 session，必须覆盖为真实私聊会话
        event.session = Session(
            adapter_name=adapter_name,
            session_type="dm",
            session_id=session_id,
        )

        try:
            await self.ctx.message_processor.handle_im_message(event)
            logger.info(f"[DM Sustain] 主动触发事件已发布: {sid}")
        except Exception as e:
            logger.error(f"[DM Sustain] 触发主动回复失败: {e}")

    # ========== 定时任务 ==========
    def _parse_interval_expression(self, expr: str) -> int:
        base, var = self._parse_window_range(expr)
        if var == 0:
            return max(1, base)
        low = max(1, base - var)
        high = base + var
        return random.randint(low, high)

    async def _scheduler_loop(self):
        if not croniter and self.scheduled_type == "cron":
            logger.error("[Scheduler] croniter 未安装，无法使用 cron 调度")
            return

        while not self._shutdown_event.is_set():
            try:
                if self.scheduled_type == "interval":
                    wait_seconds = self._parse_interval_expression(self.scheduled_interval_expression)
                    logger.debug(f"[Scheduler] 下次间隔等待 {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
                else:
                    now = time.time()
                    cron = croniter(self.scheduled_cron, now)
                    next_time = cron.get_next(float)
                    wait_seconds = next_time - now
                    if wait_seconds < 0:
                        wait_seconds = 0
                    logger.debug(f"[Scheduler] 下次 Cron 时间: {next_time}, 等待 {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)

                if self._shutdown_event.is_set():
                    break

                await self._run_scheduled_task()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Scheduler] 调度循环异常: {e}")

    async def _run_scheduled_task(self):
        if not self.scheduled_sessions:
            return
        sessions = list(self.scheduled_sessions)
        random.shuffle(sessions)
        selected = sessions[:self.scheduled_max_per_round]
        logger.info(f"[Scheduler] 本轮选中会话: {selected}")

        for sid in selected:
            try:
                await self._process_scheduled_session(sid)
            except Exception as e:
                logger.error(f"[Scheduler] 处理会话 {sid} 失败: {e}")

    async def _process_scheduled_session(self, sid: str):
        parts = sid.split(":", 2)
        if len(parts) != 3:
            logger.error(f"[Scheduler] 无效的 sid: {sid}")
            return
        adapter_name, session_type, session_id = parts

        adapter = self.ctx.adapter_mgr.get_adapter(adapter_name)
        if not adapter:
            logger.error(f"[Scheduler] 无法获取适配器: {adapter_name}")
            return

        # 如果会话无历史，尝试拉取
        history = self.ctx.session_mgr.fetch_memory(sid)
        if not history and self.scheduled_fetch_history:
            logger.info(f"[Scheduler] 会话 {sid} 无历史，尝试拉取")
            try:
                fetched = await self._fetch_history_from_api(adapter, session_type, session_id, self.scheduled_initial_history_count)
                if fetched:
                    chunks = self._flat_to_chunks(fetched)
                    if chunks:
                        self.ctx.session_mgr.write_memory(sid, chunks)
                        history = self.ctx.session_mgr.fetch_memory(sid)
                        logger.info(f"[Scheduler] 拉取并写入 {len(chunks)} 条消息")
            except Exception as e:
                logger.error(f"[Scheduler] 拉取历史失败: {e}")

        if not history:
            logger.warning(f"[Scheduler] 会话 {sid} 无历史，跳过")
            return

        # 对齐官方 publish_notice 构造；群聊必须带 group，否则 is_group_message() 会错
        cur_time = int(time.time())
        user = User(user_id="system_scheduled", nickname="定时任务")
        prompt_text = self.scheduled_prompt or "请根据当前对话上下文，自然地发送一条消息。"
        chain = MessageChain([Text(prompt_text)])
        group = Group(group_id=session_id) if session_type == "gm" else None

        event = KiraMessageEvent(
            adapter=adapter.info,
            message_types=adapter.message_types,
            message=KiraIMMessage(
                timestamp=cur_time,
                sender=user,
                group=group,
                message_id="scheduled_task",
                self_id=str(adapter.config.get("self_id", "") or ""),
                chain=chain,
                is_notice=False,
                is_mentioned=True,
            ),
            timestamp=cur_time,
        )
        # 覆盖为配置中的真实会话（__post_init__ 对 system 用户可能生成错误 sid）
        event.session = Session(
            adapter_name=adapter_name,
            session_type=session_type,
            session_id=session_id,
        )

        try:
            await self.ctx.message_processor.handle_im_message(event)
            logger.info(f"[Scheduler] 定时任务事件已发布: {sid}")
        except Exception as e:
            logger.error(f"[Scheduler] 处理定时任务失败: {e}")

    async def _fetch_history_from_api(self, adapter, session_type: str, session_id: str, count: int) -> List[Dict[str, Any]]:
        client = adapter.get_client()
        if not client:
            return []
        try:
            if session_type == "dm":
                resp = await client.send_action("get_friend_msg_history", {"user_id": int(session_id), "count": count})
            else:
                resp = await client.send_action("get_group_msg_history", {"group_id": int(session_id), "count": count})
            if resp.get("status") != "ok":
                return []
            messages = resp.get("data", {}).get("messages", [])
            result = []
            for msg in reversed(messages):
                content = msg.get("raw_message", "")
                sender = msg.get("sender", {})
                nickname = sender.get("nickname", "")
                result.append({
                    "role": "user",
                    "content": f"[{nickname}]: {content}" if nickname else content
                })
            return result
        except Exception as e:
            logger.error(f"[Scheduler] 拉取历史 API 失败: {e}")
            return []

    def _flat_to_chunks(self, flat: List[dict]) -> List[List[dict]]:
        chunks = []
        cur = []
        for msg in flat:
            if msg.get("role") == "user":
                if cur:
                    chunks.append(cur)
                cur = [msg]
            else:
                cur.append(msg)
        if cur:
            chunks.append(cur)
        return chunks

    # ========== 消息处理钩子 ==========
    @on.im_message(priority=Priority.HIGH)
    async def handle_msg(self, event: KiraMessageEvent, *_):
        # --- 修复：过滤机器人自己的私聊消息 ---
        if not event.is_group_message():
            # 正确获取 self_id
            self_id = str(event.message.self_id) if hasattr(event.message, 'self_id') and event.message.self_id is not None else None
            sender_id = str(event.message.sender.user_id) if event.message.sender else None
            if self_id and sender_id and self_id == sender_id:
                logger.debug(f"[Debounce] 忽略机器人自己的私聊消息: {event.message.message_id}")
                event.discard()
                return

        # === 拉黑拦截：被屏蔽的用户/会话消息完全不进 LLM（不 buffer/flush/不触发） ===
        try:
            _sid = event.session.sid
            _uid = str(event.message.sender.user_id) if event.message.sender else "unknown"
            if self.enhance.harass.is_blocked(_sid, _uid, time.time()):
                logger.debug(f"[Enhance] 拉黑拦截: {_sid} 用户 {_uid} 的消息不进 LLM")
                event.discard()
                return
        except Exception:
            pass

        # 唤醒词检测（区分真 @ 与唤醒词命中：框架在循环前已标记真 @）
        _was_mentioned = bool(getattr(event, "is_mentioned", False))
        for m in event.message.chain:
            if isinstance(m, Text) and any(w in m.text for w in self.waking_words):
                event.message.is_mentioned = True
                if not _was_mentioned:
                    event._wake_source = "keyword"
                break
        if _was_mentioned:
            event._wake_source = "at"

        if event.is_group_message():
            is_mentioned = event.is_mentioned
            self._process_media(event.message.chain, is_mentioned, is_private=False)
            if not is_mentioned and not self.image_recognition_only_on_mention:
                self._limit_media_count(event.message.chain, self.max_images_per_message)
        else:
            is_mentioned = event.is_mentioned
            self._process_media(event.message.chain, is_mentioned, is_private=True)

        sid = event.session.sid
        # 评分补正对提及消息的影响（存在感节流下独立控制）
        if event.is_mentioned and not self.enhance.dormant.in_dormant(self.enhance._now_hhmm(), sid):
            is_dm = not event.is_group_message()
            scope = "mentioned_dm" if is_dm else "mentioned"
            mentioned_gate = self.enhance.score_gate(sid, True, scope=scope, is_dm=is_dm)
            if not mentioned_gate:
                # deny 生效：评分不足时阻止触发，降级为未提及
                event.is_mentioned = False

        # 注意：不在此记录 _last_ignore_sid（旧实现）。ignore/wake_extend tag 是
        # LLM 回复的输出，on_llm_response 已记录本次回复所属会话；handle_msg 里
        # 记录会被任意新消息（含不触发 LLM 的围观消息）覆盖，造成 tag 作用到
        # 错误会话的竞态。

        # === 聊天增强引擎：存在感记录 + 骚扰检测 + 休眠判定 ===
        self.enhance.on_im_message(event)
        # 休眠期内起夜未命中：抑制触发（不推送 LLM）
        if getattr(event, "_enhance_dormant_blocked", False):
            event.discard()
            return
        # 强制通路超额抑制：占比超标且评分不足时，被唤醒也抑制（等评分补上）
        if getattr(event, "_enhance_force_suppressed", False):
            event.discard()
            return

        # === 私聊持续对话：用户消息处理 ===
        if self.dm_sustain_enabled and not event.is_group_message():
            sender_id = str(event.message.sender.user_id) if event.message.sender else ""
            # 系统主动触发消息不参与用户侧逻辑（避免刚 +1 的 count 被清掉）
            is_proactive_msg = sender_id in ("system_proactive_dm", "system_scheduled")

            if not is_proactive_msg:
                text_content = "".join(
                    elem.text for elem in event.message.chain if isinstance(elem, Text)
                )
                in_window = self._is_in_dm_sustain(sid)

                # 仅在持续窗口内处理停止词（避免非窗口期误丢弃正常消息）
                if in_window and self._check_stop_keywords(
                    text_content, self.dm_sustain_stop_keywords
                ):
                    if self.dm_sustain_mode == "per_retry" and self.dm_retry_on_user_stop:
                        # 算失败重试，不重置主动次数
                        self._handle_dm_failure(sid, "用户停止词")
                        event.discard()
                        return
                    self._clear_dm_sustain_state(sid)
                    logger.debug(f"[DM Sustain] 用户停止词触发，结束本轮: {sid}")
                    event.discard()
                    return

                # 真实用户发言：取消等待中的主动窗口，并重置主动次数（开启新一轮）
                # 这样 max 不会在用户回来后永久卡死
                if in_window or self.dm_sustain_count.get(sid, 0):
                    logger.debug(
                        f"[DM Sustain] 用户消息到达，取消窗口并重置计数: {sid} "
                        f"（原主动次数 {self.dm_sustain_count.get(sid, 0)}）"
                    )
                self._cancel_dm_sustain(sid)
                self._reset_dm_sustain_count(sid)

        # === 群聊持续对话 ===
        # 真实唤醒（@ / 唤醒词）：开启新一轮，重置连续计数
        if self.sustain_enabled and event.is_group_message() and event.is_mentioned and self._is_sustain_allowed(sid):
            if self.sustain_count.get(sid, 0) or self._is_in_sustain_window(sid):
                logger.debug(f"[Sustain] 群 {sid} 真实唤醒，重置连续计数（原 {self.sustain_count.get(sid, 0)}）")
            self.sustain_count[sid] = 0
            self._clear_sustain_window(sid, keep_count=True)
            # 新一轮开始：解除终止标志，清空上一轮命中标记
            self.sustain_stopped.pop(sid, None)
            self.sustain_hit_ids.pop(sid, None)

        if self.sustain_enabled and event.is_group_message() and not event.is_mentioned and self._is_sustain_allowed(sid):
            if self._is_in_sustain_window(sid):
                if self.max_sustain_replies != -1 and self.sustain_count.get(sid, 0) >= self.max_sustain_replies:
                    await self._stop_sustain_round(sid)
                else:
                    text_content = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text))
                    if self._check_stop_keywords(text_content, self.sustain_stop_keywords):
                        await self._stop_sustain_round(sid)
                        event.discard()
                        return

                    if self.sustain_mode == "per_message":
                        # 窗口内每条非唤醒消息都独立判断；
                        # 未命中：窗口继续，下一条仍可判；
                        # 命中：回复并关窗（保留计数），等 AI 回复后再开新窗
                        # 存在感节流：概率 × k_prob（回少提高/回多降低）+ 评分补正
                        # 休眠期内不介入（休眠时段不主动触发）
                        if self.enhance.dormant.in_dormant(self.enhance._now_hhmm(), sid):
                            logger.debug(f"[Sustain] 群 {sid} 休眠期内不介入（per_message）")
                        else:
                            _sustain_prob = self.sustain_reply_probability
                            if self.sustain_k_prob_enabled:
                                _sustain_prob *= self.enhance.k_prob(sid)
                            _sustain_hit = random.random() < _sustain_prob
                            if self.enhance.score_gate(sid, _sustain_hit, scope="sustain"):
                                event.message.is_mentioned = True
                                self.sustain_count[sid] += 1
                                _mid = getattr(event.message, "message_id", None)
                                if _mid is not None:
                                    self.sustain_hit_ids.setdefault(sid, set()).add(_mid)
                                self._clear_sustain_window(sid, keep_count=True)
                                logger.debug(
                                    f"[Sustain] 群 {sid} 持续对话命中（per_message），连续次数 {self.sustain_count[sid]}"
                                )
                                if (
                                    self.max_sustain_replies != -1
                                    and self.sustain_count[sid] >= self.max_sustain_replies
                                ):
                                    logger.debug(
                                        f"[Sustain] 群 {sid} 已达最大持续次数 {self.max_sustain_replies}，"
                                        f"AI 回复后将不再开窗"
                                    )
                    else:
                        # per_round：窗口内只判断第一条；
                        # 未命中：本窗口不再判断；命中：关窗保留计数，等 AI 再开窗
                        if not self.sustain_judged.get(sid, False):
                            self.sustain_judged[sid] = True
                            # 休眠期内不介入（休眠时段不主动触发）
                            if self.enhance.dormant.in_dormant(self.enhance._now_hhmm(), sid):
                                logger.debug(f"[Sustain] 群 {sid} 休眠期内不介入（per_round）")
                            else:
                                # 存在感节流：概率 × k_prob + 评分补正
                                _sustain_prob = self.sustain_reply_probability
                                if self.sustain_k_prob_enabled:
                                    _sustain_prob *= self.enhance.k_prob(sid)
                                _sustain_hit = random.random() < _sustain_prob
                                if self.enhance.score_gate(sid, _sustain_hit, scope="sustain"):
                                    event.message.is_mentioned = True
                                    self.sustain_count[sid] += 1
                                    _mid = getattr(event.message, "message_id", None)
                                    if _mid is not None:
                                        self.sustain_hit_ids.setdefault(sid, set()).add(_mid)
                                    self._clear_sustain_window(sid, keep_count=True)
                                    logger.debug(
                                        f"[Sustain] 群 {sid} 持续对话命中（per_round），连续次数 {self.sustain_count[sid]}"
                                    )
                                    if (
                                        self.max_sustain_replies != -1
                                        and self.sustain_count[sid] >= self.max_sustain_replies
                                    ):
                                        logger.debug(
                                            f"[Sustain] 群 {sid} 已达最大持续次数 {self.max_sustain_replies}，"
                                            f"AI 回复后将不再开窗"
                                        )
                                else:
                                    logger.debug(f"[Sustain] 群 {sid} 持续对话未命中，本窗口不再判断")

        # === 消息缓冲逻辑 ===
        if not event.is_mentioned:
            if self.receive_unmentioned:
                buffer = self.ctx.get_buffer(str(event.session))
                if buffer.get_length() >= self.max_unmentioned_messages:
                    buffer.pop(count=buffer.get_length()-self.max_unmentioned_messages+1)
                event.buffer()
                if self.group_proactive_chat and event.is_group_message() \
                        and not self.enhance.dormant.in_dormant(self.enhance._now_hhmm(), sid) \
                        and self._is_proactive_allowed(sid):
                    # 存在感节流：概率 × k_prob（回少提高/回多降低）
                    prob = self.group_proactive_chat_probability
                    if self.proactive_k_prob_enabled:
                        prob *= self.enhance.k_prob(sid)
                    prob_hit = random.random() < prob
                    # 评分补正：评分不足概率命中作废；评分够概率未命中补触发
                    if self.enhance.score_gate(sid, prob_hit):
                        logger.info("[Chat] Triggered proactive chat")
                        event.flush()
            else:
                event.discard()
            return

        event.buffer()
        buffer_len = self.ctx.message_processor.get_session_buffer_length(sid)
        if buffer_len + 1 >= self.max_buffer_messages:
            event.flush()
            return

        if sid not in self.session_events:
            self.session_events[sid] = asyncio.Event()
        if sid not in self.session_tasks:
            self.session_tasks[sid] = asyncio.create_task(self._debounce_loop(sid))
        self.session_events[sid].set()

    async def _debounce_loop(self, sid: str):
        event = self.session_events[sid]
        try:
            while True:
                await event.wait()
                event.clear()
                try:
                    await asyncio.sleep(self.debounce_interval)
                except asyncio.CancelledError:
                    break
                if event.is_set() and not self.receive_unmentioned:
                    continue
                buffer_len = self.ctx.message_processor.get_session_buffer_length(sid)
                if buffer_len == 0:
                    continue
                try:
                    await self.ctx.message_processor.flush_session_messages(sid)
                except Exception:
                    logger.exception(f"[Debounce] Error flushing session {sid}")
        except asyncio.CancelledError:
            logger.debug(f"[Debounce] Debounce loop for session {sid} cancelled")
        finally:
            self.session_tasks.pop(sid, None)
            self.session_events.pop(sid, None)

    # ========== LLM 响应钩子 ==========
    @on.llm_response(priority=Priority.HIGH)
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse, *_):
        sid = event.sid

        # 官方 AgentExecutor 在每一步都会触发 ON_LLM_RESPONSE（含 tool_calls 中间步）。
        # 持续对话只应在「最终文本回复」时处理，否则会误开窗/误判停止词。
        if resp.tool_calls:
            return

        # 聊天增强引擎：存在感记录 + 休眠维持期（仅最终文本回复时）
        self.enhance.on_llm_response(event, resp)
        # 休眠维持期次数限制：达上限则结束维持期（wake_max_rounds 生效）
        if not self.enhance.dormant.can_reply(sid):
            self.enhance.dormant._awake_until.pop(sid, None)
            logger.debug(f"[Enhance] 休眠维持期达最大互动次数，结束: {sid}")

        ai_text = (resp.text_response or "").strip()

        # 记录本次 LLM 回复所属会话（ignore/wake_extend tag 处理器用）。
        # 框架 tag 处理器签名只有 (value, **attrs) 无 event 上下文（core/tag/base.py），
        # _last_ignore_sid 是唯一通道。在最终文本回复时写入已把竞态窗口缩到最小
        # （on_llm_response 返回后框架才解析 XML 执行 tag，期间其他会话的
        # on_llm_response 可能覆盖——多会话并发回复时概率触发，已知限制）。
        self._last_ignore_sid = sid

        # provider 全挂时框架返回 "[ProviderError] ..." 错误文本（无 tool_calls，
        # agent_executor 标记 is_final=True 直接收尾）。它不是真实 AI 回复：若按正常
        # 回复处理会误开持续窗口，在 provider 恢复前反复主动触发。识别后静默结束，
        # 同时关闭 LLM 请求时兜底开的窗口，避免错误响应后窗口残留期间持续误判
        if ai_text.startswith("[ProviderError]"):
            self._clear_sustain_window(sid, keep_count=True)
            logger.debug(f"[Sustain] provider 全挂错误响应，关闭窗口不开窗: {sid}")
            return

        # === 私聊持续对话 ===
        if not event.is_group_message() and self.dm_sustain_enabled:
            if self._is_dm_allowed(sid):
                current_count = self.dm_sustain_count.get(sid, 0)
                if self.dm_max_sustain_replies != -1 and current_count >= self.dm_max_sustain_replies:
                    logger.debug(
                        f"[DM Sustain] AI 回复完成但已达最大主动次数 "
                        f"{current_count}/{self.dm_max_sustain_replies}，不再开窗: {sid}"
                    )
                else:
                    should_stop = False
                    stop_reason = ""
                    if self.dm_stop_on_ai_empty and self._is_empty_msg(ai_text):
                        should_stop = True
                        stop_reason = "空消息"
                    elif self._check_stop_keywords(ai_text, self.dm_stop_on_ai_keywords):
                        should_stop = True
                        stop_reason = "AI停止关键词"

                    if should_stop:
                        if stop_reason == "空消息" and self.dm_retry_on_empty:
                            # 空 msg 只是"这次不回"：重新开窗，评分补上时再给一次触发机会
                            # （不结束本轮，不重置主动次数）
                            self._start_dm_sustain_window(sid)
                            logger.debug(f"[DM Sustain] 私聊 {sid} AI 空消息（dm_retry_on_empty），重开窗口等评分补上")
                        elif self.dm_sustain_mode == "per_retry" and self.dm_retry_on_ai_stop:
                            # 视为失败，重试或取消（不重置主动次数）
                            self._handle_dm_failure(sid, f"AI {stop_reason}")
                        else:
                            # 明确结束本轮
                            self._clear_dm_sustain_state(sid)
                            logger.debug(f"[DM Sustain] AI {stop_reason}，结束本轮: {sid}")
                    else:
                        # 正常回复，启动新窗口（count 已在主动触发时 +1，此处不改 count）
                        self._start_dm_sustain_window(sid)
                        logger.debug(
                            f"[DM Sustain] AI 回复完成，启动窗口: {sid} "
                            f"（当前主动次数 {current_count}）"
                        )

        # === 群聊持续对话 ===
        if event.is_group_message() and self.sustain_enabled and self._is_sustain_allowed(sid):
            should_stop = False
            if self.stop_on_ai_empty and self._is_empty_msg(ai_text):
                if self.sustain_retry_on_empty:
                    # 空 msg 只是"这次不回"：重新开窗，评分补上时再给一次触发机会
                    # （不调用 _stop_sustain_round，不 drop pending，窗口继续）
                    # 清空旧 hit_ids，避免陈旧 id 把后续真实唤醒批次误判为纯持续命中
                    self.sustain_hit_ids.pop(sid, None)
                    self._start_sustain_window(sid)
                    logger.debug(f"[Sustain] 群 {sid} AI 空消息（retry_on_empty），重开窗口等评分补上")
                    return
                should_stop = True
                logger.debug(f"[Sustain] AI 输出空消息，停止窗口: {sid}")
            elif self._check_stop_keywords(ai_text, self.stop_on_ai_keywords):
                should_stop = True
                logger.debug(f"[Sustain] AI 回复包含停止关键词，停止窗口: {sid}")

            if should_stop:
                await self._stop_sustain_round(sid)
                return

            if self.sustain_stopped.get(sid):
                # 本轮已在本批次处理期间被终止（如用户停止词/达上限），
                # 在途回复不再重开窗口，等下次真实唤醒解除
                logger.debug(f"[Sustain] 群 {sid} 本轮已终止，在途回复不开窗")
                return

            if self.max_sustain_replies == -1 or self.sustain_count.get(sid, 0) < self.max_sustain_replies:
                if self.sustain_judge_timing == "llm_processing":
                    # 判定仅在 LLM 处理期间进行，回复后关闭兜底窗，
                    # 避免 LLM 请求时开的固定时长窗口在回复后剩余时间内继续判定
                    self._clear_sustain_window(sid, keep_count=True)
                    logger.debug(f"[Sustain] 群 {sid} timing=llm_processing，回复后关闭窗口")
                elif self.sustain_judge_timing == "either" and self._is_in_sustain_window(sid):
                    # either：兜底窗口仍在则延续不重置（一轮只判一次），
                    # 由兜底窗口自然结束；窗口不在才开新窗
                    logger.debug(f"[Sustain] 群 {sid} timing=either，窗口延续不重置")
                else:
                    self._start_sustain_window(sid)
                    logger.debug(
                        f"[Sustain] 群 {sid} AI 回复完成，启动窗口 "
                        f"（连续次数 {self.sustain_count.get(sid, 0)}）"
                    )
            else:
                logger.debug(
                    f"[Sustain] 群 {sid} 已达最大持续次数 "
                    f"{self.sustain_count.get(sid, 0)}/{self.max_sustain_replies}，不再开窗"
                )

    # ========== LLM 请求钩子（工具黑名单过滤） ==========
    @on.llm_request(priority=Priority.HIGH)
    async def filter_proactive_tools(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        # 聊天增强引擎：注入合并通知（骚扰/唤醒/存在感状态）
        self.enhance.on_llm_request(event, req)

        # 主动屏蔽工具开关：关闭时从 tool_set 移除 manage_ignore（bot 不再能主动屏蔽）
        if not self.enable_manage_ignore:
            self._filter_tools(req.tool_set, ["manage_ignore"], "exact")
            logger.debug("[Enhance] manage_ignore 工具已禁用（enable_manage_ignore=false）")

        if not hasattr(event, 'messages') or not event.messages:
            return

        proactive = False
        blacklist = []
        mode = "partial"
        for msg in event.messages:
            sender_id = getattr(msg.sender, 'user_id', '')
            if sender_id in ("system_proactive_dm", "system_scheduled"):
                proactive = True
                if sender_id == "system_proactive_dm":
                    blacklist = self.dm_tool_blacklist
                    mode = self.dm_tool_blacklist_mode
                else:
                    blacklist = self.scheduled_tool_blacklist
                    mode = self.scheduled_tool_blacklist_mode
                break

        if proactive and blacklist:
            self._filter_tools(req.tool_set, blacklist, mode)
            logger.debug(f"[Proactive] 已过滤工具: {blacklist} (mode={mode})")

    @on.llm_request(priority=Priority.MEDIUM)
    async def inject_group_prompt(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if event.is_group_message() and self.group_chat_prompt:
            for p in req.system_prompt:
                if p.name == "chat_env":
                    p.content += self.group_chat_prompt
                    break

    @on.llm_request(priority=Priority.MEDIUM)
    async def sustain_open_on_llm_request(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        """LLM 请求时兜底开窗：覆盖 LLM 处理期间（含工具循环）到达的消息。

        sustain_judge_timing 控制判定时机：
        - both / either / llm_processing：LLM 请求时若窗口不在则保底开窗，
          窗口内的消息可正常判定/命中；窗口在时完全不动（不刷新不关闭）
        - after_reply：处理期间只接住消息不判定，不开窗
        窗口续期由 on_llm_response 在最终回复时按 timing 策略处理。
        """
        if not self.sustain_enabled:
            return
        if self.sustain_judge_timing == "after_reply":
            return
        if not event.is_group_message():
            return
        sid = event.sid
        if not self._is_sustain_allowed(sid):
            return
        if self.sustain_stopped.get(sid):
            # 本轮已被明确终止（AI空消息/AI停止词/用户停止词/达上限），
            # 兜底不再重开窗口，直到下次真实唤醒
            return
        if self._is_in_sustain_window(sid):
            return
        if self.max_sustain_replies != -1 and self.sustain_count.get(sid, 0) >= self.max_sustain_replies:
            return
        self._start_sustain_window(sid)
        logger.debug(f"[Sustain] 群 {sid} LLM 请求兜底开窗（连续次数 {self.sustain_count.get(sid, 0)}）")

    # ================= 队列合并 / 积压处理（转发给 BatchMergeScheduler） =================

    @on.im_batch_message(priority=Priority.HIGH)
    async def on_queue_merge_batch(self, event: KiraMessageBatchEvent, *_):
        # 停窗后迟到的「纯持续命中」批次直接拦截：其触发完全来自持续命中，
        # 不应在 AI 已终止本轮后再引起一次回复（消息仍在缓冲，上下文不丢）
        if event.is_group_message() and self.sustain_stopped.get(event.sid):
            hit_ids = self.sustain_hit_ids.get(event.sid) or set()
            mentioned = [m for m in (getattr(event, "messages", None) or [])
                         if getattr(m, "is_mentioned", False)]
            if mentioned and all(getattr(m, "message_id", None) in hit_ids for m in mentioned):
                logger.debug(f"[Sustain] 群 {event.sid} 已停窗，拦截纯持续命中批次 {event.event_id}")
                event.stop()
                return
        await self.merge_scheduler.on_batch_message(event)

    @on.llm_response(priority=Priority.HIGH)
    async def on_queue_merge_resp(self, event: KiraMessageBatchEvent, resp, *_):
        await self.merge_scheduler.on_llm_response(event, resp)

    @on.step_result(priority=Priority.HIGH)
    async def on_queue_merge_step(self, event: KiraMessageBatchEvent, *_):
        await self.merge_scheduler.on_step_result(event)

    # ================= 并行媒体识别（转发给 ParallelMediaRecognizer） =================
    # 注意：im_message 钩子必须定义在 handle_msg 之后（同优先级按注册顺序执行），
    #       保证"非唤醒不识别"配置先由 handle_msg 处理（兼容前提）

    @on.im_message(priority=Priority.HIGH)
    async def on_media_rec_im(self, event: KiraMessageEvent, *_):
        await self.media_recognizer.on_im_message(event)

    @on.im_batch_message(priority=Priority.HIGH)
    async def on_media_rec_batch(self, event: KiraMessageBatchEvent, *_):
        await self.media_recognizer.on_im_batch_message(event)

    @on.llm_request(priority=Priority.HIGH)
    async def on_media_rec_llm(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        await self.media_recognizer.on_llm_request(event, req)

    # ========== 私有辅助 ==========
    # MP3 码率表（kbps）：MPEG1 Layer III / MPEG2&2.5 Layer III
    _MP3_BR_V1 = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0]
    _MP3_BR_V2 = [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0]

    def _record_bytes(self, elem) -> Optional[bytes]:
        """尽力取出语音的原始字节（url 不做同步下载，返回 None）"""
        try:
            ft = getattr(elem, "file_type", "")
            if ft == "base64":
                return base64.b64decode(elem.file)
            if ft == "data_url":
                _, _, b64 = elem.file.partition(",")
                return base64.b64decode(b64) if b64 else None
            if ft == "path" and os.path.exists(elem.file):
                if os.path.getsize(elem.file) <= 50 * 1024 * 1024:
                    with open(elem.file, "rb") as f:
                        return f.read()
            return None
        except Exception:
            return None

    def _estimate_mp3_duration(self, data: bytes) -> int:
        """按第一个有效帧头的码率估算 MP3 时长（秒），失败返回 0"""
        try:
            offset = 0
            if data[:3] == b"ID3" and len(data) >= 10:
                tag_size = ((data[6] & 0x7F) << 21) | ((data[7] & 0x7F) << 14) \
                    | ((data[8] & 0x7F) << 7) | (data[9] & 0x7F)
                offset = 10 + tag_size
            limit = min(len(data) - 4, offset + 65536)
            i = offset
            while i < limit:
                if data[i] == 0xFF and (data[i + 1] & 0xE0) == 0xE0:
                    version = (data[i + 1] >> 3) & 0x03
                    layer = (data[i + 1] >> 1) & 0x03
                    br_idx = (data[i + 2] >> 4) & 0x0F
                    if layer == 1 and version in (0, 2, 3) and br_idx not in (0, 15):
                        table = self._MP3_BR_V1 if version == 3 else self._MP3_BR_V2
                        br = table[br_idx]
                        if br:
                            return round(len(data) * 8 / (br * 1000))
                    i += 1
                else:
                    i += 1
            return 0
        except Exception:
            return 0

    def _estimate_record_duration(self, elem) -> int:
        """Record 缺少 duration 元数据时尽力估算时长（秒），失败返回 0。

        典型场景：机器人自己发出的语音被用户引用回来时不带 duration，
        导致长语音限制被绕过。QQ 适配器会把语音统一转成 mp3 base64，
        本地 TTS 文件多为 wav，二者都可估算。
        """
        data = self._record_bytes(elem)
        if not data:
            return 0
        try:
            if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WAVE":
                with wave.open(io.BytesIO(data)) as wf:
                    rate = wf.getframerate()
                    return round(wf.getnframes() / rate) if rate else 0
            return self._estimate_mp3_duration(data)
        except Exception:
            return 0

    def _get_record_duration(self, elem) -> int:
        """优先读元数据 duration；缺失时从音频字节估算（如被引用的机器人自己的语音）"""
        try:
            duration = int(float(getattr(elem, "duration", 0) or 0))
        except (TypeError, ValueError):
            duration = 0
        if duration <= 0:
            duration = self._estimate_record_duration(elem)
        return duration

    def _process_media(self, chain, is_mentioned: bool, is_private: bool = False):
        for i, elem in enumerate(chain.message_list):
            if isinstance(elem, (Image, Sticker)):
                if is_mentioned:
                    continue
                if self.image_recognition_only_on_mention:
                    chain.message_list[i] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")
                else:
                    if random.random() >= self.image_recognition_probability:
                        chain.message_list[i] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")
            elif isinstance(elem, Forward):
                # only_on_mention=True：仅唤醒消息保留转发；False：全部保留
                if self.forward_recognition_only_on_mention and not is_mentioned:
                    chain.message_list[i] = Text("[转发消息]")
            elif isinstance(elem, Record):
                duration = self._get_record_duration(elem)
                if self.voice_max_duration > 0 and duration > self.voice_max_duration:
                    chain.message_list[i] = Text(f"[长语音 {duration}秒]")
                    continue
                should_try_stt = False
                if is_private:
                    if self.voice_private_need_mention:
                        should_try_stt = is_mentioned
                    else:
                        should_try_stt = True
                else:
                    if self.voice_recognition_only_on_mention:
                        should_try_stt = is_mentioned
                    else:
                        should_try_stt = True
                if should_try_stt:
                    pass
                else:
                    chain.message_list[i] = Text("[语音]")
            elif isinstance(elem, Reply) and elem.chain:
                self._process_media(elem.chain, is_mentioned, is_private)

    def _limit_media_count(self, chain, max_count: int):
        if self.image_recognition_only_on_mention:
            return
        media_indices = [i for i, e in enumerate(chain.message_list) if isinstance(e, (Image, Sticker))]
        if len(media_indices) <= max_count:
            return
        for idx in reversed(media_indices[max_count:]):
            elem = chain.message_list[idx]
            chain.message_list[idx] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")
