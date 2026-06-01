import asyncio
import random
import re
import time
from collections import defaultdict

from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.provider import LLMRequest, LLMResponse
from core.chat.message_elements import Text, Image, Reply, Sticker, Forward, Record


class DebouncePlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        self.session_events: dict[str, asyncio.Event] = {}
        self.session_tasks: dict[str, asyncio.Task] = {}
        bot_cfg = ctx.config["bot_config"].get("bot", {})
        self.debounce_interval = float(bot_cfg.get("max_message_interval", 1.5))
        self.max_buffer_messages = int(bot_cfg.get("max_buffer_messages", 3))
        self.max_unmentioned_messages = int(self.plugin_cfg.get("max_unmentioned_messages", 5))
        self.receive_unmentioned = self.plugin_cfg.get("receive_unmentioned", False)
        self.group_chat_prompt = self.plugin_cfg.get("group_chat_prompt", "")
        self.group_proactive_chat = self.plugin_cfg.get("group_proactive_chat", False)
        self.group_proactive_chat_probability = self.plugin_cfg.get("group_proactive_chat_probability", 0.1)

        self.waking_words = cfg.get("waking_words", [])

        # 图片/表情/转发消息处理配置
        self.image_recognition_only_on_mention = cfg.get("image_recognition_only_on_mention", True)
        self.image_recognition_probability = float(cfg.get("image_recognition_probability", 0.5))
        self.max_images_per_message = int(cfg.get("max_images_per_message", 3))
        self.forward_recognition_only_on_mention = cfg.get("forward_recognition_only_on_mention", True)

        # 语音消息处理配置
        self.voice_recognition_only_on_mention = cfg.get("voice_recognition_only_on_mention", True)
        self.voice_private_need_mention = cfg.get("voice_private_need_mention", True)
        self.voice_max_duration = int(cfg.get("voice_max_duration", 0))

        # 持续对话配置
        self.sustain_enabled = cfg.get("sustain_enabled", False)
        self.sustain_window_seconds = float(cfg.get("sustain_window_seconds", 15))
        self.sustain_reply_probability = float(cfg.get("sustain_reply_probability", 0.5))
        self.max_sustain_replies = int(cfg.get("max_sustain_replies", -1))
        self.sustain_stop_keywords = cfg.get("sustain_stop_keywords", [])   # 用户消息停止关键词
        self.stop_on_ai_keywords = cfg.get("stop_on_ai_keywords", [])       # AI回复停止关键词
        self.stop_on_ai_empty = cfg.get("stop_on_ai_empty", True)           # AI回复空消息停止
        self.sustain_mode = cfg.get("sustain_mode", "per_message")

        # 持续对话状态
        self.sustain_until = defaultdict(float)
        self.sustain_count = defaultdict(int)
        self.sustain_tasks = defaultdict(asyncio.Task)
        self.sustain_judged = defaultdict(bool)

    async def initialize(self):
        logger.info(f"[Debounce] enabled (group media/forward/voice control, private unchanged)")
        if self.sustain_enabled:
            logger.info(f"[Debounce] 持续对话已启用: mode={self.sustain_mode}, window={self.sustain_window_seconds}s, prob={self.sustain_reply_probability}, max={self.max_sustain_replies}, user_stop={self.sustain_stop_keywords}, ai_stop={self.stop_on_ai_keywords}, ai_empty_stop={self.stop_on_ai_empty}")

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
        logger.debug("[Debounce] All debounce tasks cancelled")

    # ========== 持续对话辅助方法 ==========
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
        logger.debug(f"群 {sid} 启动持续对话窗口，截止 {deadline:.1f}")

    async def _end_sustain_window(self, sid: str):
        await asyncio.sleep(self.sustain_window_seconds)
        if self._is_in_sustain_window(sid):
            self.sustain_until.pop(sid, None)
            self.sustain_count.pop(sid, None)
            self.sustain_judged.pop(sid, None)
            logger.debug(f"群 {sid} 持续对话窗口自然结束")

    def _clear_sustain_state(self, sid: str):
        if sid in self.sustain_until:
            self.sustain_until.pop(sid, None)
        if sid in self.sustain_count:
            self.sustain_count.pop(sid, None)
        if sid in self.sustain_tasks and not self.sustain_tasks[sid].done():
            self.sustain_tasks[sid].cancel()
        self.sustain_tasks.pop(sid, None)
        self.sustain_judged.pop(sid, None)

    def _check_user_stop_keywords(self, text: str) -> bool:
        if not self.sustain_stop_keywords:
            return False
        text_lower = text.lower()
        for kw in self.sustain_stop_keywords:
            if kw.lower() in text_lower:
                return True
        return False

    def _check_ai_stop_keywords(self, text: str) -> bool:
        if not self.stop_on_ai_keywords:
            return False
        text_lower = text.lower()
        for kw in self.stop_on_ai_keywords:
            if kw.lower() in text_lower:
                return True
        return False

    def _is_empty_msg(self, xml: str) -> bool:
        pattern = r'^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$'
        return bool(re.match(pattern, xml))

    # ========== 媒体处理函数 ==========
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
                    try:
                        stt_client = self.ctx.provider_mgr.get_default_stt()
                        if stt_client:
                            pass
                        else:
                            chain.message_list[i] = Text("[语音]")
                    except Exception:
                        chain.message_list[i] = Text("[语音]")
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

    @on.im_message(priority=Priority.HIGH)
    async def handle_msg(self, event: KiraMessageEvent):
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

        # 持续对话：用户消息检测停止关键词
        if self.sustain_enabled and event.is_group_message() and not is_mentioned:
            sid = event.session.sid
            if self._is_in_sustain_window(sid):
                # 检查是否已达最大次数
                if self.max_sustain_replies != -1 and self.sustain_count[sid] >= self.max_sustain_replies:
                    self._clear_sustain_state(sid)
                else:
                    text_content = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text))
                    if self._check_user_stop_keywords(text_content):
                        self._clear_sustain_state(sid)
                        event.discard()
                        return

                    if self.sustain_mode == "per_message":
                        if random.random() < self.sustain_reply_probability:
                            event.message.is_mentioned = True
                            self.sustain_count[sid] += 1
                            self._clear_sustain_state(sid)
                            logger.debug(f"群 {sid} 持续对话命中（per_message），连续次数 {self.sustain_count[sid]}")
                    else:  # per_round
                        if not self.sustain_judged.get(sid, False):
                            self.sustain_judged[sid] = True
                            if random.random() < self.sustain_reply_probability:
                                event.message.is_mentioned = True
                                self.sustain_count[sid] += 1
                                self._clear_sustain_state(sid)
                                logger.debug(f"群 {sid} 持续对话命中（per_round），连续次数 {self.sustain_count[sid]}")
                            else:
                                logger.debug(f"群 {sid} 持续对话未命中，本窗口不再判断")

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

    @on.llm_response(priority=Priority.HIGH)
    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse):
        if not event.is_group_message():
            return

        sid = event.sid
        # 检查 AI 回复是否触发停止
        ai_text = resp.text_response.strip()
        should_stop = False

        # 空消息停止
        if self.stop_on_ai_empty and self._is_empty_msg(ai_text):
            should_stop = True
            logger.debug(f"群 {sid} AI 输出空消息，触发停止")
        # AI 关键词停止
        elif self._check_ai_stop_keywords(ai_text):
            should_stop = True
            logger.debug(f"群 {sid} AI 回复包含停止关键词，触发停止")

        if should_stop:
            self._clear_sustain_state(sid)
            return

        # 持续对话：回复中没有工具调用，启动新窗口
        if self.sustain_enabled and not resp.tool_calls:
            if self.max_sustain_replies == -1 or self.sustain_count[sid] < self.max_sustain_replies:
                self._start_sustain_window(sid)
                logger.debug(f"群 {sid} AI 回复完成，启动持续对话窗口")

    @on.llm_request(priority=Priority.MEDIUM)
    async def inject_group_prompt(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if not event.is_group_message():
            return
        if self.group_chat_prompt:
            for p in req.system_prompt:
                if p.name == "chat_env":
                    p.content += self.group_chat_prompt
                    break
