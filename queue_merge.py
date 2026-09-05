"""队列合并 / 积压处理调度器（v2.4.1 - 修复 _push_pending 防抖在锁内）

v2.4.1 修复：
- _push_pending 防抖等待移到锁外（之前 await asyncio.sleep 在 async with self._lock 内，
  导致 on_batch_message 无法获取锁来更新 _last_arrival，防抖重置永远不生效；
  且该 sid 等待期间阻塞了其他所有 sid 的队列处理）
- _tick 跳过已有 inflight+final 的 sid（让 _push_pending 处理，避免双路径竞争）
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from core.plugin import logger
from core.chat.message_utils import KiraMessageBatchEvent
from core.chat.message_elements import Image, Sticker, Record
from core.provider import LLMResponse


@dataclass
class PendingBatch:
    """待推送批次（记录进入 pending 的时刻，用于超时合并判定）"""
    arrival_ts: float
    batch: KiraMessageBatchEvent


class BatchMergeScheduler:
    """队列合并调度器：作为 mixin 组件挂在聊天插件上，不引用插件私有状态。"""

    def __init__(self, ctx, plugin_cfg: dict, bot_cfg: dict):
        self.ctx = ctx
        sec = plugin_cfg.get("section_queue_merge", {})
        self.enabled = sec.get("enabled", True)
        self.max_merge_seconds = float(sec.get("max_merge_seconds", 0))
        merge_window_cfg = sec.get("merge_window_seconds", 0)
        if merge_window_cfg and float(merge_window_cfg) > 0:
            self.merge_window_seconds = float(merge_window_cfg)
        else:
            self.merge_window_seconds = float(bot_cfg.get("max_message_interval", 1.5))
        self.max_merge_batches_limit = int(sec.get("max_merge_batches_limit", 0))
        self.max_merge_messages = int(sec.get("max_merge_messages", -1))
        self.max_merge_est_tokens = int(sec.get("max_merge_est_tokens", 0))
        self.token_est_ratio = float(sec.get("token_est_ratio", 2.0))
        self.short_merge_max_messages = int(sec.get("short_merge_max_messages", -1))
        self.media_preprocess_enabled = sec.get("media_preprocess_enabled", True)
        self.media_preprocess_max_batches = int(sec.get("media_preprocess_max_batches", 0))
        self.debug_log_enabled = sec.get("debug_log_enabled", False)
        stall_cfg = float(sec.get("inflight_stall_timeout", -1))
        llm_timeout = 120.0
        tool_timeout = 60.0
        try:
            pm = getattr(ctx, "provider_mgr", None)
            if pm is not None and hasattr(pm, "get_default_llm"):
                client = pm.get_default_llm()
                if client is not None:
                    mc = getattr(client.model, "model_config", None) or {}
                    llm_timeout = float(mc.get("timeout", 120) or 120)
            tc = ctx.config.get_config("bot_config.agent.tool_call_timeout")
            if tc:
                tool_timeout = float(tc)
        except Exception:
            pass
        if stall_cfg == 0:
            self.inflight_stall_timeout = 0.0
        elif stall_cfg > 0:
            self.inflight_stall_timeout = stall_cfg
        else:
            self.inflight_stall_timeout = llm_timeout + tool_timeout
        try:
            self.max_steps = int(ctx.config.get_config("bot_config.agent.max_tool_loop"))
        except (TypeError, ValueError):
            self.max_steps = 2
        buffer_cap = int(bot_cfg.get("max_buffer_messages", 5))
        recv_unmentioned = plugin_cfg.get("receive_unmentioned", False)
        unmentioned_cap = int(plugin_cfg.get("max_unmentioned_messages", 5)) if recv_unmentioned else 0
        if self.max_merge_messages == -1:
            self.max_merge_messages = (unmentioned_cap + buffer_cap) * 3
        if self.short_merge_max_messages == -1:
            self.short_merge_max_messages = buffer_cap
        self._inflight: dict[str, str] = {}
        self._inflight_since: dict[str, float] = {}
        self._final_marked: set[str] = set()
        self._pending: dict[str, list[PendingBatch]] = {}
        self._last_arrival: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._merge_task: Optional[asyncio.Task] = None

    def _log(self, sid: str, msg: str):
        if self.debug_log_enabled:
            logger.info(f"[QueueMerge] {sid} {msg}")

    # ================= 钩子 =================

    async def on_batch_message(self, event: KiraMessageBatchEvent, *_):
        if not self.enabled:
            return
        sid = event.session.sid
        async with self._lock:
            if event.extra and event.extra.get("_qm_self"):
                self._inflight[sid] = event.event_id
                self._inflight_since[sid] = time.time()
                self._log(sid, f"自发布批次 {event.event_id} 直接放行（_qm_self）")
                return
            if self._inflight.get(sid) == event.event_id:
                return
            if sid in self._inflight or self._pending.get(sid):
                if (self.media_preprocess_enabled and self.media_preprocess_max_batches > 0
                        and self._has_media(PendingBatch(time.time(), event))
                        and self._count_media_batches(self._pending.get(sid, [])) >= self.media_preprocess_max_batches):
                    self._log(sid, f"媒体批次超积压上限({self.media_preprocess_max_batches})，直接放行独立处理 {event.event_id}")
                    return
                self._pending.setdefault(sid, []).append(PendingBatch(time.time(), event))
                self._last_arrival[sid] = time.time()
                event.stop()
                self._ensure_task_locked()
            else:
                self._inflight[sid] = event.event_id
                self._inflight_since[sid] = time.time()
                self._log(sid, f"放行批次 {event.event_id}")

    async def on_llm_response(self, event: KiraMessageBatchEvent, resp: LLMResponse, *_):
        if not self.enabled:
            return
        async with self._lock:
            sid = event.session.sid
            if self._inflight.get(sid) == event.event_id:
                self._inflight_since[sid] = time.time()
                if not resp.tool_calls:
                    self._final_marked.add(sid)
                    self._log(sid, f"批次 {event.event_id} 进入最后一步（文本收尾）")
                elif self._is_last_step(resp):
                    self._final_marked.add(sid)
                    self._log(sid, f"批次 {event.event_id} 最后一步仍带工具，提前标记收尾")

    async def on_step_result(self, event: KiraMessageBatchEvent, *_):
        """ON_STEP_RESULT：最后一步消息已发送 → _push_pending。"""
        if not self.enabled:
            return
        sid = event.session.sid
        need_push = False
        async with self._lock:
            if self._inflight.get(sid) == event.event_id and sid in self._final_marked:
                need_push = True
        if need_push:
            await self._push_pending(sid, event.event_id)

    def _is_last_step(self, resp: LLMResponse) -> bool:
        try:
            idx = int(getattr(resp, "agent_step_index", 0) or 0)
            return idx >= self.max_steps
        except (TypeError, ValueError):
            return False

    # ================= 推送决策 =================

    async def _push_pending(self, sid: str, done_event_id: str):
        """in-flight 完成时推送 pending。

        防抖窗口移到锁外（v2.4.1 fix）：避免 await asyncio.sleep 在锁内阻塞，
        使 on_batch_message 无法更新 _last_arrival，防抖重置永不生效。
        """
        # 第一次检查：锁内验证 in-flight 匹配
        first_check_pass = False
        async with self._lock:
            if self._inflight.get(sid) != done_event_id:
                self._log(sid, f"忽略过期完成事件 {done_event_id}（in-flight={self._inflight.get(sid)}）")
                return
            first_check_pass = True

        if not first_check_pass:
            return

        # 防抖等待（锁外）：期间新消息到达可自由更新 _last_arrival
        while True:
            async with self._lock:
                last = self._last_arrival.get(sid, 0.0)
            wait = self.merge_window_seconds - (time.time() - last)
            if wait <= 0:
                break
            self._log(sid, f"防抖等待 {wait:.1f}s（窗口 {self.merge_window_seconds}s）")
            await asyncio.sleep(min(wait, 0.5))
            # 每次唤醒后重新计算，等待期间新消息到达会更新 _last_arrival

        # 第二次检查：锁内重新验证 + 决策
        to_publish = None
        async with self._lock:
            # 防抖等待期间 inflight 可能已被 tick 或其他路径处理
            if self._inflight.get(sid) != done_event_id:
                self._log(sid, f"防抖后 in-flight 已变更，跳过: {done_event_id}")
                return
            to_publish = self._decide_and_apply_locked(sid)

        if to_publish is not None:
            n_msgs = len(to_publish.messages)
            self._log(sid, f"发布批次 {to_publish.event_id}（{n_msgs} 条）")
            await self.ctx.event_bus.publish(to_publish)

    def _decide_and_apply_locked(self, sid: str) -> Optional[KiraMessageBatchEvent]:
        """三分支推送决策（须持有 _lock）。"""
        pending = self._pending.pop(sid, [])
        self._inflight.pop(sid, None)
        self._inflight_since.pop(sid, None)
        self._final_marked.discard(sid)
        self._last_arrival.pop(sid, None)
        if not pending:
            return None

        total_msgs = sum(len(pb.batch.messages) for pb in pending)
        est_tokens = self._estimate_tokens(pending)

        if (total_msgs <= self.short_merge_max_messages
                and total_msgs <= self.max_merge_messages
                and (self.max_merge_est_tokens == 0 or est_tokens <= self.max_merge_est_tokens)):
            self._log(sid, f"软合并：{len(pending)}批次/{total_msgs}条")
            to_merge, rest = self._split_by_limits(pending)
        elif time.time() - pending[0].arrival_ts >= self.max_merge_seconds:
            waited = time.time() - pending[0].arrival_ts
            self._log(sid, f"超时合并：攒批 {waited:.1f}s ≥ {self.max_merge_seconds}s，{len(pending)}批次/{total_msgs}条")
            to_merge, rest = self._split_by_limits(pending)
        else:
            self._log(sid, f"独立推送：第1个批次（{len(pending[0].batch.messages)}条），其余 {len(pending)-1} 个留 pending")
            to_merge, rest = [pending[0]], pending[1:]

        if rest:
            self._pending[sid] = rest
        else:
            self._pending.pop(sid, None)
        merged = self._build_merged_batch(to_merge)
        self._inflight[sid] = merged.event_id
        self._inflight_since[sid] = time.time()
        return merged

    async def drop_sustain_pending(self, sid: str, hit_ids) -> int:
        async with self._lock:
            pending = self._pending.get(sid, [])
            before = len(pending)
            kept = [pb for pb in pending if not self._is_sustain_only(pb, hit_ids)]
            dropped = before - len(kept)
            if dropped:
                self._pending[sid] = kept
                self._log(sid, f"丢弃 {dropped} 个持续命中积压批次")
                if not kept:
                    self._pending.pop(sid, None)
            return dropped

    def _is_sustain_only(self, pb: PendingBatch, hit_ids: set) -> bool:
        if not hit_ids:
            return False
        msg_ids = set()
        for m in pb.batch.messages:
            mid = getattr(m, "message_id", None)
            if mid is not None:
                msg_ids.add(str(mid))
        return bool(msg_ids and msg_ids.issubset(hit_ids)) if hit_ids else False

    # ================= 拆分 =================

    def _split_by_limits(self, pending: list[PendingBatch]):
        batches = pending
        merge_msgs = 0
        merge_idx = 0
        for i, pb in enumerate(batches):
            n = len(pb.batch.messages)
            if self.max_merge_batches_limit > 0 and i >= self.max_merge_batches_limit:
                break
            if self.max_merge_messages > 0 and merge_msgs + n > self.max_merge_messages:
                break
            if self.max_merge_est_tokens > 0:
                est = self._estimate_tokens([pb])
                if merge_idx > 0 and merge_msgs > 0 and self._estimate_tokens(batches[:i]) + est > self.max_merge_est_tokens:
                    break
            merge_msgs += n
            merge_idx = i + 1
        return pending[:merge_idx], pending[merge_idx:]

    def _estimate_tokens(self, batches: list[PendingBatch]) -> int:
        total = 0
        for pb in batches:
            for m in pb.batch.messages:
                try:
                    text = getattr(m, "text", "") or ""
                    total += len(text)
                except Exception:
                    total += 50
        return int(total * self.token_est_ratio)

    def _has_media(self, pb: PendingBatch) -> bool:
        for m in pb.batch.messages:
            for elem in getattr(m, "chain", []):
                if isinstance(elem, (Image, Sticker, Record)):
                    return True
        return False

    def _count_media_batches(self, pending: list[PendingBatch]) -> int:
        return sum(1 for pb in pending if self._has_media(pb))

    # ================= 构造合并批次 =================

    def _build_merged_batch(self, batches: list[PendingBatch]) -> KiraMessageBatchEvent:
        msgs = []
        for pb in batches:
            msgs.extend(pb.batch.messages)
        last = batches[-1].batch
        merged = KiraMessageBatchEvent(
            message_types=last.message_types,
            timestamp=int(time.time()),
            adapter=last.adapter,
            session=last.session,
            messages=msgs,
            extra={"merged_from": [b.batch.event_id for b in batches],
                   "_qm_self": True},
        )
        return merged

    # ================= 兜底 tick =================

    def _ensure_task_locked(self):
        if self._merge_task is None or self._merge_task.done():
            self._merge_task = asyncio.create_task(self._tick_loop())

    async def _tick_loop(self):
        try:
            while True:
                await asyncio.sleep(0.5)
                await self._tick()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[QueueMerge] tick loop error")

    async def _tick(self):
        to_publish = []
        async with self._lock:
            now = time.time()
            for sid in list(self._pending):
                pending = self._pending.get(sid)
                if not pending:
                    continue
                inflight = self._inflight.get(sid)
                # 分支 A：in-flight 仍在处理中（未 final）
                if inflight and sid not in self._final_marked:
                    stalled = (
                        self.inflight_stall_timeout > 0
                        and now - self._inflight_since.get(sid, now) >= self.inflight_stall_timeout
                    )
                    if not stalled:
                        if self.max_merge_seconds > 0 and now - pending[0].arrival_ts >= self.max_merge_seconds:
                            self._log(sid, f"超时兜底：in-flight 未收尾且攒批到点，强制推送 pending")
                        else:
                            continue
                    else:
                        self._log(sid, f"in-flight 卡死兜底（>{self.inflight_stall_timeout:.0f}s 无收尾），强制推送 pending")
                else:
                    # 空闲（无 in-flight）/ final 已标记：走防抖窗口
                    # _decide_and_apply_locked 是幂等的（pop pending 为空则返回 None），
                    # _push_pending 和 tick 不会重复推送
                    last = self._last_arrival.get(sid, 0.0)
                    if now - last < self.merge_window_seconds:
                        self._log(sid, f"tick 防抖等待（窗口 {self.merge_window_seconds}s，距最后消息 {now - last:.1f}s）")
                        continue
                merged = self._decide_and_apply_locked(sid)
                if merged is not None:
                    to_publish.append(merged)
        for merged in to_publish:
            n_msgs = len(merged.messages)
            self._log(merged.session.sid, f"发布批次 {merged.event_id}（{n_msgs} 条）")
            await self.ctx.event_bus.publish(merged)

    async def shutdown(self):
        task = None
        to_publish: list[PendingBatch] = []
        async with self._lock:
            task = self._merge_task
            self._merge_task = None
            for sid, pend in self._pending.items():
                to_publish.extend(pend)
            self._pending.clear()
            self._inflight.clear()
            self._inflight_since.clear()
            self._final_marked.clear()
            self._last_arrival.clear()
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        for pb in to_publish:
            chain = pb.batch.messages[0].chain if pb.batch.messages else None
            if chain is None:
                continue
            text_parts = [str(e) for e in chain if hasattr(e, "text")]
            text = "\n".join(text_parts)[:200]
            logger.info(f"[QueueMerge] 插件终止，积压消息转入正常管线: {text}")