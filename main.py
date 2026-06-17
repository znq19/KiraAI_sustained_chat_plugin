import asyncio
import random
import re
import time
from collections import defaultdict
from typing import Optional, List, Dict, Any

from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.provider import LLMRequest, LLMResponse
from core.chat.message_elements import Text, Image, Reply, Sticker, Forward, Record
from core.agent.message import OpenAIMessage
from core.prompt_manager import Prompt

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
        self.max_unmentioned_messages = int(basic.get("max_unmentioned_messages", 5))
        self.group_chat_prompt = basic.get("group_chat_prompt", "")
        self.group_proactive_chat = basic.get("group_proactive_chat", False)
        self.group_proactive_chat_probability = float(basic.get("group_proactive_chat_probability", 0.1))

        # ========== 从 section_media 读取媒体处理配置 ==========
        media = cfg.get("section_media", {})
        self.image_recognition_only_on_mention = media.get("image_recognition_only_on_mention", True)
        self.image_recognition_probability = float(media.get("image_recognition_probability", 0.5))
        self.max_images_per_message = int(media.get("max_images_per_message", 3))
        self.forward_recognition_only_on_mention = media.get("forward_recognition_only_on_mention", True)
        self.voice_recognition_only_on_mention = media.get("voice_recognition_only_on_mention", True)
        self.voice_private_need_mention = media.get("voice_private_need_mention", True)
        self.voice_max_duration = int(media.get("voice_max_duration", 0))

        # ========== 从 section_group_sustain 读取群聊持续对话配置 ==========
        group_sustain = cfg.get("section_group_sustain", {})
        self.sustain_enabled = group_sustain.get("sustain_enabled", False)
        self.sustain_window_seconds = float(group_sustain.get("sustain_window_seconds", 180))
        self.sustain_reply_probability = float(group_sustain.get("sustain_reply_probability", 0.5))
        self.max_sustain_replies = int(group_sustain.get("max_sustain_replies", -1))
        self.sustain_stop_keywords = group_sustain.get("sustain_stop_keywords", [])
        self.stop_on_ai_keywords = group_sustain.get("stop_on_ai_keywords", [])
        self.stop_on_ai_empty = group_sustain.get("stop_on_ai_empty", True)
        self.sustain_mode = group_sustain.get("sustain_mode", "per_message")

        # ========== 从 section_dm_sustain 读取私聊持续对话配置 ==========
        dm_sustain = cfg.get("section_dm_sustain", {})
        self.dm_sustain_enabled = dm_sustain.get("dm_sustain_enabled", False)
        self.dm_sustain_window_range = dm_sustain.get("dm_sustain_window_range", "30s/10s")
        self.dm_sustain_reply_probability = float(dm_sustain.get("dm_sustain_reply_probability", 0.3))
        self.dm_max_sustain_replies = int(dm_sustain.get("dm_max_sustain_replies", -1))
        self.dm_sustain_mode = dm_sustain.get("dm_sustain_mode", "per_round")
        self.dm_max_retry_attempts = int(dm_sustain.get("dm_max_retry_attempts", 3))
        self.dm_sustain_stop_keywords = dm_sustain.get("dm_sustain_stop_keywords", [])
        self.dm_stop_on_ai_keywords = dm_sustain.get("dm_stop_on_ai_keywords", [])
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

        # ========== 从 section_scheduled 读取定时任务配置 ==========
        scheduled = cfg.get("section_scheduled", {})
        self.scheduled_enabled = scheduled.get("scheduled_enabled", False)
        self.scheduled_sessions = scheduled.get("scheduled_sessions", [])
        self.scheduled_max_per_round = int(scheduled.get("scheduled_max_per_round", 1))
        self.scheduled_type = scheduled.get("scheduled_type", "interval")
        self.scheduled_interval_expression = scheduled.get("scheduled_interval_expression", "5m/30s")
        self.scheduled_cron = scheduled.get("scheduled_cron", "0 */1 * * *")
        self.scheduled_context_count = int(scheduled.get("scheduled_context_count", 10))
        self.scheduled_fetch_history = scheduled.get("scheduled_fetch_history", True)
        self.scheduled_initial_history_count = int(scheduled.get("scheduled_initial_history_count", 10))
        self.scheduled_prompt = scheduled.get("scheduled_prompt", "")
        self.scheduled_tool_blacklist = scheduled.get("scheduled_tool_blacklist", [])
        self.scheduled_tool_blacklist_mode = scheduled.get("scheduled_tool_blacklist_mode", "partial")

        # ========== 原有状态变量 ==========
        self.session_events: dict[str, asyncio.Event] = {}
        self.session_tasks: dict[str, asyncio.Task] = {}
        bot_cfg = ctx.config["bot_config"].get("bot", {})
        self.debounce_interval = float(bot_cfg.get("max_message_interval", 1.5))
        self.max_buffer_messages = int(bot_cfg.get("max_buffer_messages", 3))

        # 群聊持续状态
        self.sustain_until = defaultdict(float)
        self.sustain_count = defaultdict(int)
        self.sustain_tasks = defaultdict(asyncio.Task)
        self.sustain_judged = defaultdict(bool)

        # 私聊持续状态
        self.dm_sustain_until = defaultdict(float)
        self.dm_sustain_count = defaultdict(int)
        self.dm_sustain_tasks: dict[str, asyncio.Task] = {}
        self.dm_sustain_retry_count = defaultdict(int)
        self.dm_sustain_active = defaultdict(bool)

        # 定时任务状态
        self._scheduler_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

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
        self.sustain_tasks.clear()

        for task in self.dm_sustain_tasks.values():
            if not task.done():
                task.cancel()
        self.dm_sustain_tasks.clear()

        if self._scheduler_task and not self._scheduler_task.done():
            self._shutdown_event.set()
            try:
                await asyncio.wait_for(self._scheduler_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._scheduler_task.cancel()
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
        match = re.match(r'^(\d+)\s*(h|m|s)$', expr, re.IGNORECASE)
        if match:
            val, unit = match.groups()
            val = int(val)
            if unit.lower() == 'h':
                return val * 3600
            elif unit.lower() == 'm':
                return val * 60
            else:
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
        if not blacklist or not tool_set:
            return
        to_remove = []
        for tool in tool_set.tools:
            name = tool.get("function", {}).get("name", "")
            if mode == "partial":
                if any(kw in name for kw in blacklist):
                    to_remove.append(tool)
            else:
                if name in blacklist:
                    to_remove.append(tool)
        for t in to_remove:
            tool_set.tools.remove(t)

    def _is_empty_msg(self, xml: str) -> bool:
        pattern = r'^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$'
        return bool(re.match(pattern, xml))

    def _check_ai_stop_keywords(self, text: str, keywords: List[str]) -> bool:
        if not keywords:
            return False
        text_lower = text.lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                return True
        return False

    def _check_user_stop_keywords(self, text: str, keywords: List[str]) -> bool:
        if not keywords:
            return False
        text_lower = text.lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                return True
        return False

    # ========== 群聊持续对话 ==========
    def _is_in_sustain_window(self, sid: str) -> bool:
        return time.time() < self.sustain_until[sid]

    def _start_sustain_window(self, sid: str):
        deadline = time.time() + self.sustain_window_seconds
        self.sustain_until[sid] = deadline
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks[sid] = asyncio.create_task(self._end_sustain_window(sid))
        if self.sustain_mode == "per_round":
            self.sustain_judged[sid] = False

    async def _end_sustain_window(self, sid: str):
        await asyncio.sleep(self.sustain_window_seconds)
        if self._is_in_sustain_window(sid):
            self.sustain_until.pop(sid, None)
            self.sustain_count.pop(sid, None)
            self.sustain_judged.pop(sid, None)

    def _clear_sustain_state(self, sid: str):
        if sid in self.sustain_until:
            self.sustain_until.pop(sid, None)
        if sid in self.sustain_count:
            self.sustain_count.pop(sid, None)
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks.pop(sid, None)
        self.sustain_judged.pop(sid, None)

    # ========== 私聊持续对话 ==========
    def _is_in_dm_sustain(self, sid: str) -> bool:
        return self.dm_sustain_active.get(sid, False) and time.time() < self.dm_sustain_until.get(sid, 0)

    def _cancel_dm_sustain(self, sid: str):
        if sid in self.dm_sustain_tasks and not self.dm_sustain_tasks[sid].done():
            self.dm_sustain_tasks[sid].cancel()
        self.dm_sustain_tasks.pop(sid, None)
        self.dm_sustain_active[sid] = False
        self.dm_sustain_until.pop(sid, None)
        # 不重置重试计数，保留以便记录

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
        if self.dm_max_sustain_replies != -1 and self.dm_sustain_count[sid] >= self.dm_max_sustain_replies:
            return
        # 取消旧窗口
        self._cancel_dm_sustain(sid)
        wait_seconds = self._get_dm_window_seconds()
        deadline = time.time() + wait_seconds
        self.dm_sustain_until[sid] = deadline
        self.dm_sustain_active[sid] = True
        # 注意：不重置 dm_sustain_retry_count，由外部控制
        task = asyncio.create_task(self._dm_sustain_loop(sid, wait_seconds))
        self.dm_sustain_tasks[sid] = task
        logger.debug(f"[DM Sustain] 窗口启动: {sid}, 等待 {wait_seconds}s, 截止 {deadline:.1f}, 当前重试计数 {self.dm_sustain_retry_count.get(sid, 0)}")

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
        if rand_val < self.dm_sustain_reply_probability:
            logger.info(f"[DM Sustain] 触发主动回复: {sid} (概率 {rand_val:.2f} < {self.dm_sustain_reply_probability})")
            self.dm_sustain_count[sid] += 1
            # 成功发送后重置重试计数
            self.dm_sustain_retry_count[sid] = 0
            await self._trigger_dm_proactive(sid)
            self._cancel_dm_sustain(sid)
        else:
            logger.debug(f"[DM Sustain] 未命中: {sid} (概率 {rand_val:.2f} >= {self.dm_sustain_reply_probability})")
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

        from core.chat import KiraMessageEvent, KiraIMMessage, User, Session, MessageChain
        user = User(user_id="system_proactive_dm", nickname="系统主动触发")
        chain = MessageChain([Text(self.dm_proactive_prompt)])

        event = KiraMessageEvent(
            adapter=adapter.info,
            message_types=adapter.message_types,
            message=KiraIMMessage(
                timestamp=int(time.time()),
                sender=user,
                message_id="system_proactive",
                self_id=str(adapter.config.get("self_id", "")),
                chain=chain,
                is_notice=False,
                is_mentioned=True
            ),
            timestamp=int(time.time())
        )
        event.session = Session(
            adapter_name=adapter_name,
            session_type="dm",
            session_id=session_id
        )
        event._is_proactive = True
        event._proactive_blacklist = self.dm_tool_blacklist
        event._proactive_blacklist_mode = self.dm_tool_blacklist_mode

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

        from core.chat import KiraMessageEvent, KiraIMMessage, User, Session, MessageChain
        user = User(user_id="system_scheduled", nickname="定时任务")
        prompt_text = self.scheduled_prompt or "请根据当前对话上下文，自然地发送一条消息。"
        chain = MessageChain([Text(prompt_text)])

        event = KiraMessageEvent(
            adapter=adapter.info,
            message_types=adapter.message_types,
            message=KiraIMMessage(
                timestamp=int(time.time()),
                sender=user,
                message_id="scheduled_task",
                self_id=str(adapter.config.get("self_id", "")),
                chain=chain,
                is_notice=False,
                is_mentioned=True
            ),
            timestamp=int(time.time())
        )
        event.session = Session(
            adapter_name=adapter_name,
            session_type=session_type,
            session_id=session_id
        )
        event._is_proactive = True
        event._proactive_blacklist = self.scheduled_tool_blacklist
        event._proactive_blacklist_mode = self.scheduled_tool_blacklist_mode

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
    async def handle_msg(self, event: KiraMessageEvent):
        # --- 修复：过滤机器人自己的私聊消息 ---
        if not event.is_group_message():
            # 正确获取 self_id
            self_id = str(event.message.self_id) if hasattr(event.message, 'self_id') and event.message.self_id is not None else None
            sender_id = str(event.message.sender.user_id) if event.message.sender else None
            if self_id and sender_id and self_id == sender_id:
                logger.debug(f"[Debounce] 忽略机器人自己的私聊消息: {event.message.message_id}")
                event.discard()
                return

        # 唤醒词检测
        for m in event.message.chain:
            if isinstance(m, Text) and any(w in m.text for w in self.waking_words):
                event.message.is_mentioned = True
                break

        if event.is_group_message():
            is_mentioned = event.is_mentioned
            self._process_media(event.message.chain, is_mentioned, is_private=False)
            if not is_mentioned and not self.image_recognition_only_on_mention:
                self._limit_media_count(event.message.chain, self.max_images_per_message)
        else:
            is_mentioned = event.is_mentioned
            self._process_media(event.message.chain, is_mentioned, is_private=True)

        sid = event.session.sid

        # === 私聊持续对话：用户消息处理 ===
        if self.dm_sustain_enabled and not event.is_group_message():
            # 检查是否在窗口中
            if self._is_in_dm_sustain(sid):
                text_content = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text))
                # 检查用户停止词
                if self._check_user_stop_keywords(text_content, self.dm_sustain_stop_keywords):
                    # 判断是否将用户停止词视为失败并重试
                    if self.dm_sustain_mode == "per_retry" and self.dm_retry_on_user_stop:
                        # 视为失败，重试或取消
                        self._handle_dm_failure(sid, "用户停止词")
                        # 丢弃这条消息，因为已经触发了重试逻辑
                        event.discard()
                        return
                    else:
                        # 直接取消窗口
                        self._cancel_dm_sustain(sid)
                        logger.debug(f"[DM Sustain] 用户停止词触发，取消窗口: {sid}")
                        # 仍然将消息丢弃（不进入缓冲）
                        event.discard()
                        return
                else:
                    # 用户消息到达，取消窗口（正常交互）
                    self._cancel_dm_sustain(sid)
                    logger.debug(f"[DM Sustain] 用户消息到达，取消窗口: {sid}")

        # === 群聊持续对话（原有） ===
        if self.sustain_enabled and event.is_group_message() and not event.is_mentioned:
            if self._is_in_sustain_window(sid):
                if self.max_sustain_replies != -1 and self.sustain_count[sid] >= self.max_sustain_replies:
                    self._clear_sustain_state(sid)
                else:
                    text_content = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text))
                    if self._check_user_stop_keywords(text_content, self.sustain_stop_keywords):
                        self._clear_sustain_state(sid)
                        event.discard()
                        return

                    if self.sustain_mode == "per_message":
                        if random.random() < self.sustain_reply_probability:
                            event.message.is_mentioned = True
                            self.sustain_count[sid] += 1
                            self._clear_sustain_state(sid)
                            logger.debug(f"[Sustain] 群 {sid} 持续对话命中（per_message），连续次数 {self.sustain_count[sid]}")
                    else:
                        if not self.sustain_judged.get(sid, False):
                            self.sustain_judged[sid] = True
                            if random.random() < self.sustain_reply_probability:
                                event.message.is_mentioned = True
                                self.sustain_count[sid] += 1
                                self._clear_sustain_state(sid)
                                logger.debug(f"[Sustain] 群 {sid} 持续对话命中（per_round），连续次数 {self.sustain_count[sid]}")
                            else:
                                logger.debug(f"[Sustain] 群 {sid} 持续对话未命中，本窗口不再判断")

        # === 消息缓冲逻辑 ===
        if not event.is_mentioned:
            if self.receive_unmentioned:
                buffer = self.ctx.get_buffer(str(event.session))
                if buffer.get_length() >= self.max_unmentioned_messages:
                    buffer.pop(count=buffer.get_length()-self.max_unmentioned_messages+1)
                event.buffer()
                if self.group_proactive_chat and event.is_group_message():
                    if random.random() < self.group_proactive_chat_probability:
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
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse):
        sid = event.sid

        # === 私聊持续对话 ===
        if not event.is_group_message() and self.dm_sustain_enabled:
            if self._is_dm_allowed(sid):
                if self.dm_max_sustain_replies == -1 or self.dm_sustain_count[sid] < self.dm_max_sustain_replies:
                    ai_text = resp.text_response.strip()
                    should_stop = False
                    stop_reason = ""
                    if self.dm_stop_on_ai_empty and self._is_empty_msg(ai_text):
                        should_stop = True
                        stop_reason = "空消息"
                    elif self._check_ai_stop_keywords(ai_text, self.dm_stop_on_ai_keywords):
                        should_stop = True
                        stop_reason = "AI停止关键词"

                    if should_stop:
                        if self.dm_sustain_mode == "per_retry" and self.dm_retry_on_ai_stop:
                            # 视为失败，重试或取消
                            self._handle_dm_failure(sid, f"AI {stop_reason}")
                        else:
                            self._cancel_dm_sustain(sid)
                            logger.debug(f"[DM Sustain] AI {stop_reason}，取消窗口: {sid}")
                    else:
                        # 正常回复，启动新窗口
                        self._start_dm_sustain_window(sid)
                        logger.debug(f"[DM Sustain] AI 回复完成，启动窗口: {sid}")

        # === 群聊持续对话（原有） ===
        if event.is_group_message() and self.sustain_enabled:
            ai_text = resp.text_response.strip()
            should_stop = False
            if self.stop_on_ai_empty and self._is_empty_msg(ai_text):
                should_stop = True
                logger.debug(f"[Sustain] AI 输出空消息，停止窗口: {sid}")
            elif self._check_ai_stop_keywords(ai_text, self.stop_on_ai_keywords):
                should_stop = True
                logger.debug(f"[Sustain] AI 回复包含停止关键词，停止窗口: {sid}")

            if should_stop:
                self._clear_sustain_state(sid)
                return

            if not resp.tool_calls:
                if self.max_sustain_replies == -1 or self.sustain_count[sid] < self.max_sustain_replies:
                    self._start_sustain_window(sid)
                    logger.debug(f"[Sustain] 群 {sid} AI 回复完成，启动窗口")

    # ========== LLM 请求钩子（工具黑名单过滤） ==========
    @on.llm_request(priority=Priority.HIGH)
    async def filter_proactive_tools(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
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

    # ========== 私有辅助 ==========
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
                if is_mentioned:
                    if self.forward_recognition_only_on_mention:
                        continue
                    else:
                        chain.message_list[i] = Text("[转发消息]")
                else:
                    chain.message_list[i] = Text("[转发消息]")
            elif isinstance(elem, Record):
                duration = getattr(elem, 'duration', 0)
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
