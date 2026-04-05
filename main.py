import asyncio
import random
import re
import time
from collections import defaultdict
from datetime import datetime

from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.provider import LLMRequest, LLMResponse
from core.chat.message_elements import Text, Image, Reply, Sticker, Forward


class SustainedChatPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        # 原有配置
        self.session_events: dict[str, asyncio.Event] = {}
        self.session_tasks: dict[str, asyncio.Task] = {}
        bot_cfg = ctx.config["bot_config"].get("bot", {})
        self.debounce_interval = float(bot_cfg.get("max_message_interval", 1.5))
        self.max_buffer_messages = int(bot_cfg.get("max_buffer_messages", 3))
        self.max_unmentioned_messages = int(self.plugin_cfg.get("max_unmentioned_messages", 5))
        self.receive_unmentioned = self.plugin_cfg.get("receive_unmentioned", True)
        self.group_chat_prompt = self.plugin_cfg.get("group_chat_prompt", "")
        self.waking_words = cfg.get("waking_words", [])

        # 媒体处理配置
        self.image_recognition_only_on_mention = cfg.get("image_recognition_only_on_mention", True)
        self.image_recognition_probability = float(cfg.get("image_recognition_probability", 0.5))
        self.max_images_per_message = int(cfg.get("max_images_per_message", 3))
        self.forward_recognition_only_on_mention = cfg.get("forward_recognition_only_on_mention", True)

        # 持续回复配置
        self.sustain_window_seconds = float(cfg.get("sustain_window_seconds", 15))
        self.reply_probability = float(cfg.get("reply_probability", 0.8))
        self.max_continuous_replies = int(cfg.get("max_continuous_replies", 3))
        self.stop_on_empty_message = cfg.get("stop_on_empty_message", True)

        # 会话状态
        self.sustain_until = defaultdict(float)
        self.continuous_count = defaultdict(int)
        self.sustain_tasks = defaultdict(asyncio.Task)

    async def initialize(self):
        logger.info(f"[SustainedChat] enabled (group only), stop_on_empty_message={self.stop_on_empty_message}")

    async def terminate(self):
        for task in self.session_tasks.values():
            if not task.done():
                task.cancel()
        self.session_tasks.clear()
        self.session_events.clear()
        for task in self.sustain_tasks.values():
            if not task.done():
                task.cancel()
        self.sustain_tasks.clear()
        logger.info("[SustainedChat] terminated")

    def _format_time(self, timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def _is_in_sustain_window(self, sid: str) -> bool:
        return time.time() < self.sustain_until[sid]

    def _start_sustain_window(self, sid: str):
        deadline = time.time() + self.sustain_window_seconds
        self.sustain_until[sid] = deadline
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks[sid] = asyncio.create_task(self._end_sustain_window(sid))
        logger.info(f"群 {sid} 启动持续回复窗口，截止 {self._format_time(deadline)}")

    async def _end_sustain_window(self, sid: str):
        await asyncio.sleep(self.sustain_window_seconds)
        if self._is_in_sustain_window(sid):
            self.sustain_until.pop(sid, None)
            self.continuous_count.pop(sid, None)
            logger.info(f"群 {sid} 持续回复窗口自然结束")

    def _reset_sustain_window(self, sid: str):
        if not self._is_in_sustain_window(sid):
            return
        deadline = time.time() + self.sustain_window_seconds
        self.sustain_until[sid] = deadline
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks[sid] = asyncio.create_task(self._end_sustain_window(sid))
        logger.info(f"群 {sid} 持续回复窗口重置，新截止 {self._format_time(deadline)}")

    def _clear_sustain_state(self, sid: str):
        if sid in self.sustain_until:
            self.sustain_until.pop(sid, None)
        if sid in self.continuous_count:
            self.continuous_count.pop(sid, None)
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks.pop(sid, None)
        logger.info(f"群 {sid} 清除持续回复状态")

    def _is_empty_message(self, xml: str) -> bool:
        pattern = r'^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$'
        return bool(re.match(pattern, xml))

    # ========== 媒体处理函数（兼容 list 和 MessageChain） ==========
    def _get_message_list(self, chain):
        """返回可修改的消息元素列表"""
        if hasattr(chain, 'message_list'):
            return chain.message_list
        elif isinstance(chain, list):
            return chain
        else:
            raise TypeError(f"Unsupported chain type: {type(chain)}")

    def _process_media(self, chain, is_mentioned: bool):
        message_list = self._get_message_list(chain)
        for i, elem in enumerate(message_list):
            if isinstance(elem, (Image, Sticker)):
                if is_mentioned:
                    continue
                if self.image_recognition_only_on_mention:
                    message_list[i] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")
                else:
                    if random.random() >= self.image_recognition_probability:
                        message_list[i] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")
            elif isinstance(elem, Forward):
                if is_mentioned:
                    if self.forward_recognition_only_on_mention:
                        continue
                    else:
                        message_list[i] = Text("[转发消息]")
                else:
                    message_list[i] = Text("[转发消息]")
            elif isinstance(elem, Reply) and hasattr(elem, 'chain') and elem.chain:
                self._process_media(elem.chain, is_mentioned)

    def _limit_media_count(self, chain, max_count: int):
        if self.image_recognition_only_on_mention:
            return
        message_list = self._get_message_list(chain)
        media_indices = [i for i, e in enumerate(message_list) if isinstance(e, (Image, Sticker))]
        if len(media_indices) <= max_count:
            return
        for idx in reversed(media_indices[max_count:]):
            elem = message_list[idx]
            message_list[idx] = Text("[图片]" if isinstance(elem, Image) else "[动画表情]")

    # ========== 消息处理主入口 ==========
    @on.im_message(priority=Priority.HIGH)
    async def handle_msg(self, event: KiraMessageEvent):
        # 检查唤醒词
        for m in event.message.chain:
            if isinstance(m, Text) and any(w in m.text for w in self.waking_words):
                event.message.is_mentioned = True
                break

        # 仅对群聊进行媒体处理和持续回复逻辑
        if event.is_group_message():
            is_mentioned = event.is_mentioned
            # 注意：event.message.chain 可能是 MessageChain 或 list，_process_media 已兼容
            self._process_media(event.message.chain, is_mentioned)
            if not is_mentioned and not self.image_recognition_only_on_mention:
                self._limit_media_count(event.message.chain, self.max_images_per_message)

            sid = event.session.sid

            if is_mentioned:
                self._clear_sustain_state(sid)
                logger.debug(f"群 {sid} 收到唤醒消息，清除窗口")
            else:
                if self._is_in_sustain_window(sid):
                    if self.max_continuous_replies == -1 or self.continuous_count[sid] < self.max_continuous_replies:
                        if random.random() < self.reply_probability:
                            event.message.is_mentioned = True
                            logger.info(f"群 {sid} 持续回复触发，次数 {self.continuous_count[sid]+1}/{self.max_continuous_replies if self.max_continuous_replies!=-1 else '∞'}")
                        else:
                            event.stop()
                            logger.debug(f"群 {sid} 持续回复概率未命中，忽略")
                            return
                    else:
                        event.stop()
                        logger.debug(f"群 {sid} 已达最大持续回复次数，忽略")
                        return

        # 原有逻辑：非唤醒消息处理
        if not event.is_mentioned:
            if self.receive_unmentioned:
                buffer = self.ctx.get_buffer(str(event.session))
                if buffer.get_length() >= self.max_unmentioned_messages:
                    buffer.pop()
                event.buffer()
            else:
                event.stop()
            return

        # 唤醒消息的处理（原版逻辑）
        sid = event.session.sid
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

    @on.llm_response(priority=Priority.HIGH)
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse):
        if not event.is_group_message():
            return

        sid = event.sid
        if not resp.tool_calls:
            if self.stop_on_empty_message and self._is_empty_message(resp.text_response):
                self._clear_sustain_state(sid)
                logger.info(f"群 {sid} AI 发送空消息，已清除持续回复状态")
                return

            if self.max_continuous_replies == -1 or self.continuous_count[sid] < self.max_continuous_replies:
                self.continuous_count[sid] += 1
                self._start_sustain_window(sid)
                logger.info(f"群 {sid} AI 回复完成，持续回复计数 {self.continuous_count[sid]}/{self.max_continuous_replies if self.max_continuous_replies!=-1 else '∞'}")
            else:
                logger.debug(f"群 {sid} 已达最大持续回复次数，不启动窗口")

    async def _debounce_loop(self, sid: str):
        event = self.session_events[sid]
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
                logger.exception(f"[SustainedChat] Error flushing session {sid}")

    @on.llm_request(priority=Priority.MEDIUM)
    async def inject_group_prompt(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if not event.is_group_message():
            return
        if self.group_chat_prompt:
            for p in req.system_prompt:
                if p.name == "chat_env":
                    p.content += self.group_chat_prompt
                    break