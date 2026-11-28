"""Shared on-demand media and nested-forward catalog for S/Z chat plugins.

Only metadata and text trees are persisted. Media bytes stay in the recognizer's
cache or KiraAI's archive. All runtime IDs are source-derived, so the first LLM
request never waits for hashing or recognition merely to obtain an ID.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import weakref
from collections import OrderedDict
from datetime import datetime
from pathlib import Path

from core.chat.message_elements import Text, Image, Sticker, Record, Reply, Forward
from core.utils.path_utils import get_data_path
from forward_raw import (
    RawForward,
    card_ids,
    collect_nested,
    extract_forward_descriptors,
    first_str,
    is_json_forward_card,
    json_forward_id,
    message_object,
    message_payload,
    node_meta,
    node_segments,
    segment_node_parts,
    xml_text,
)

try:
    from core.plugin import logger
except ImportError:  # 测试桩或极简宿主
    import logging
    logger = logging.getLogger(__name__)

try:
    from core.chat.message_elements import At, Emoji, File, Json, Notice, Poke, Video
except ImportError:  # 旧测试桩只声明本插件实际处理的六类元素
    class At: pass
    class Emoji: pass
    class File: pass
    class Json: pass
    class Notice: pass
    class Poke: pass
    class Video: pass

try:
    from core.agent.tool import ToolResult
except ImportError:  # 精简测试桩：真实框架始终走上面的正式实现
    class ToolResult:
        def __init__(self, text="", media_refs=None):
            self.text = text
            self.media_refs = list(media_refs or [])

try:
    from core.utils.media_refs import store_session_media
except ImportError:
    store_session_media = None


MEDIA_ID_RE = re.compile(r"^[isr][0-9a-f]{8,64}$")
FORWARD_ID_RE = re.compile(r"^f[0-9a-f]{8,64}$")
MEDIA_ID_PATTERN = r"^[isr][0-9a-f]{8,64}$"
FORWARD_ID_PATTERN = r"^f[0-9a-f]{8,64}$"

# 原始 OneBot 转发树补全的安全边界：只用于口径异常的协议端补齐，正常路径不触发。
RAW_FORWARD_TIMEOUT = 8.0
RAW_FORWARD_TOTAL_BUDGET = 12.0
RAW_FORWARD_MAX_DEPTH = 64
RAW_FORWARD_MAX_NODES = 2000
RAW_FORWARD_MAX_FETCHES = 32
RAW_FORWARD_RETRY_SECONDS = 60
RAW_MEDIA_OCCURRENCE_BASE = 1_000_000
RAW_TRUNCATED_TEXT = {
    "depth": "已达最大查看深度",
    "cycle": "循环引用",
    "fetch_failed": "内容获取失败，可重试",
    "xml_card": "聊天记录卡片无法展开",
}


class _RawMessage:
    """Minimal message stand-in for hydrating a persisted record after restart."""

    def __init__(self, message_id):
        self.message_id = message_id or "runtime"
        self.chain = []
        self.sender = None
        self.group = None
        self.extra = {}
        self.message_str = None


class _RawBuildContext:
    """Per-hydration budget/cycle/created-record tracking."""

    def __init__(self, manager, sid, message, root_id, message_id):
        self.manager = manager
        self.sid = sid
        self.message = message if message is not None else _RawMessage(message_id)
        self.message_id = message_id or getattr(self.message, "message_id", "runtime")
        self.root_id = root_id
        self.visited: set[str] = set()
        self.active: set[str] = set()
        self.records: dict = {}
        self.entries: set[str] = set()
        self.fetches = 0
        self.nodes = 0
        self.deadline = time.monotonic() + RAW_FORWARD_TOTAL_BUDGET


class MediaAccessManager:
    SHORT_ID_LEN = 8
    MAX_BATCH_MEDIA = 16
    MAX_BATCH_FORWARDS = 8
    RUNTIME_LIMIT = 2048
    MESSAGE_INDEX_LIMIT = 256
    MESSAGE_PIN_LIMIT = 32
    CATALOG_SESSION_LIMIT = 2048
    CATALOG_TOTAL_LIMIT = 16384
    CATALOG_CLEANUP_INTERVAL = 60.0

    def __init__(self, ctx, recognizer, plugin_cfg: dict, plugin_id: str):
        self.ctx = ctx
        self.recognizer = recognizer
        basic = plugin_cfg.get("section_media", {}) or {}
        if isinstance(basic, dict) and isinstance(basic.get("fields"), dict):
            basic = basic["fields"]
        if not isinstance(basic, dict):
            basic = {}
        self.image_tool_enabled = bool(basic.get("media_tool_image_enabled", False))
        self.audio_tool_enabled = bool(basic.get("media_tool_audio_enabled", False))
        self.forward_tool_enabled = bool(basic.get("forward_tool_enabled", True))
        self.prefetch_enabled = bool(basic.get("media_tool_prefetch_enabled", True))
        self.auto_private = bool(basic.get("media_tool_auto_present_private_enabled", True))
        self.auto_group = bool(basic.get("media_tool_auto_present_group_enabled", True))
        try:
            self.auto_max_count = max(1, int(basic.get("media_tool_auto_present_max_count", 1)))
        except (TypeError, ValueError):
            self.auto_max_count = 1
        try:
            self.forward_max_depth = max(1, int(basic.get("forward_max_depth", 4)))
        except (TypeError, ValueError):
            self.forward_max_depth = 4
        self._entries = OrderedDict()
        self._forwards = OrderedDict()
        self._message_entries = OrderedDict()
        self._message_forwards = OrderedDict()
        self._message_refs = OrderedDict()
        # 轻量身份索引覆盖整个磁盘目录；热条目才有界进入运行时 LRU。
        self._catalog: dict[str, dict] = {}
        self._identity_ids: dict[tuple[str, str], str] = {}
        self._bg_tasks: set[asyncio.Task] = set()
        self._reading_ids: dict[str, int] = {}
        self._persisting_ids: set[str] = set()
        # key: Forward ID；value: 正在补全原始树的后台任务。read_forward 会等待它，
        # 但最多等 RAW_FORWARD_TIMEOUT，超时仍按已有轻量树返回，绝不阻塞消息链路。
        self._hydrate_tasks: dict[str, asyncio.Task] = {}
        self._initialized = False
        self._last_catalog_cleanup = 0.0
        self._session_event_bus = None
        root = get_data_path() / "plugin_data" / plugin_id / "media_access"
        self._entry_dir = root / "entries"
        self._forward_dir = root / "forwards"
        self._index_path = root / "identity_index.json"
        self._catalog_max_age_seconds = 0
        try:
            cleanup = ctx.config.get_config("bot_config.message_history_cleanup", {}) or {}
            if cleanup.get("enabled", True):
                self._catalog_max_age_seconds = max(0, int(cleanup.get("max_age_days", 0))) * 86400
        except Exception:
            pass

    @property
    def any_enabled(self) -> bool:
        return self.image_tool_enabled or self.audio_tool_enabled or self.forward_tool_enabled

    @staticmethod
    def _digest(*parts) -> str:
        return hashlib.sha256("\x1f".join(map(str, parts)).encode("utf-8")).hexdigest()

    @staticmethod
    def _message_ref(message):
        try:
            return weakref.ref(message)
        except TypeError:
            return lambda: message

    @staticmethod
    def _message_of(record):
        ref = record.get("message_ref")
        return ref() if ref is not None else None

    @staticmethod
    def _entry_digest(entry):
        digest = entry.get("digest")
        if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest):
            return digest
        if entry.get("kind") in ("Image", "Sticker", "Record"):
            return MediaAccessManager._digest(
                entry.get("sid"), entry.get("message_id"), entry["kind"],
                entry.get("occurrence", 0))
        return ""

    def _load_catalog_sync(self):
        snapshot = {}
        snapshot_dirs = {}
        try:
            raw = self._read_json_sync(self._index_path)
            snapshot_dirs = raw.get("directories", {}) or {}
            if raw.get("version") == 1 and isinstance(raw.get("items"), list):
                for item in raw["items"]:
                    kind, key = item.get("kind"), item.get("id")
                    pattern = MEDIA_ID_RE if kind == "media" else FORWARD_ID_RE
                    if isinstance(key, str) and pattern.fullmatch(key):
                        snapshot[(kind, key)] = item
        except (OSError, ValueError, TypeError, AttributeError):
            snapshot = {}
            snapshot_dirs = {}
        result = []
        for kind, directory in (("media", self._entry_dir), ("forward", self._forward_dir)):
            if not directory.is_dir():
                continue
            pattern = MEDIA_ID_RE if kind == "media" else FORWARD_ID_RE
            paths = list(directory.glob("*.json"))
            snapshot_keys = {key for snapshot_kind, key in snapshot if snapshot_kind == kind}
            current_keys = {path.stem for path in paths if pattern.fullmatch(path.stem)}
            directory_meta = snapshot_dirs.get(kind) or {}
            if (directory_meta and directory.stat().st_mtime == directory_meta.get("mtime")
                    and current_keys == snapshot_keys):
                # 原子写 + 目录 mtime + 文件名集合三重校验，兼容不同文件系统时间粒度。
                for (snapshot_kind, key), cached in snapshot.items():
                    if snapshot_kind != kind:
                        continue
                    file_mtime = cached.get("file_mtime", cached.get("mtime", 0))
                    result.append((kind, key, cached.get("sid"), cached.get("digest", ""),
                                   cached.get("mtime", file_mtime), None,
                                   cached.get("cache_path"), file_mtime))
                continue
            for path in paths:
                key = path.stem
                if not pattern.fullmatch(key):
                    continue
                try:
                    file_mtime = path.stat().st_mtime
                    cached = snapshot.get((kind, key))
                    if cached is not None and cached.get("file_mtime") == file_mtime:
                        result.append((kind, key, cached.get("sid"), cached.get("digest", ""),
                                       cached.get("mtime", file_mtime), None,
                                       cached.get("cache_path"), file_mtime))
                        continue
                    data = self._read_json_sync(path)
                    if data.get("id") != key:
                        continue
                    digest = self._entry_digest(data)
                    result.append((kind, key, data.get("sid"), digest,
                                   file_mtime, data, data.get("cache_path"), file_mtime))
                except (OSError, ValueError, TypeError):
                    continue
        return result

    def _write_catalog_snapshot_sync(self, payload):
        self._index_path.parent.mkdir(parents=True, exist_ok=True)
        directories = {}
        for kind, directory in (("media", self._entry_dir), ("forward", self._forward_dir)):
            try:
                directories[kind] = {"mtime": directory.stat().st_mtime}
            except OSError:
                pass
        payload["directories"] = directories
        self._atomic_json(self._index_path, payload)

    async def _save_catalog_snapshot(self):
        items = []
        for key, meta in self._catalog.items():
            if meta.get("file_mtime") is None:
                continue
            items.append({
                "id": key, "kind": meta.get("kind"), "sid": meta.get("sid"),
                "digest": meta.get("digest", ""), "mtime": meta.get("mtime", 0),
                "cache_path": meta.get("cache_path"), "file_mtime": meta["file_mtime"],
            })
        await asyncio.to_thread(self._write_catalog_snapshot_sync, {"version": 1, "items": items})

    async def initialize(self):
        """Load only identities at startup; heavy entries remain lazy and bounded."""
        if self._initialized:
            return
        rows = await asyncio.to_thread(self._load_catalog_sync)
        rows.sort(key=lambda row: row[4])
        for kind, key, sid, digest, mtime, _data, cache_path, file_mtime in rows:
            self._catalog[key] = {"kind": kind, "sid": sid, "digest": digest,
                                  "mtime": mtime, "cache_path": cache_path,
                                  "file_mtime": file_mtime}
            if digest:
                self._identity_ids[(key[0], digest)] = key
        self._initialized = True
        try:
            bus = getattr(self.ctx, "event_bus", None)
            if bus is not None:
                bus.subscribe("session_deleted", self._on_session_deleted)
                self._session_event_bus = bus
        except Exception:
            self._session_event_bus = None

    def _allocate_id(self, prefix, digest, sid, kind):
        existing = self._identity_ids.get((prefix, digest))
        if existing:
            return existing
        for length in range(self.SHORT_ID_LEN, 65, 2):
            key = prefix + digest[:length]
            other = self._catalog.get(key)
            if other is None:
                self._catalog[key] = {
                    "kind": kind, "sid": sid, "digest": digest, "mtime": time.time(),
                }
                self._identity_ids[(prefix, digest)] = key
                return key
            if other.get("digest") == digest:
                self._identity_ids[(prefix, digest)] = key
                return key
        raise RuntimeError("unable to allocate a unique media access ID")

    def _protected_message_ids(self):
        protected = set()
        if self.MESSAGE_PIN_LIMIT <= 0:
            return protected
        for mapping in (self._message_entries, self._message_forwards):
            for _mid, ids in list(mapping.items())[-self.MESSAGE_PIN_LIMIT:]:
                protected.update(ids)
        return protected

    def _reading_start(self, key):
        self._reading_ids[key] = self._reading_ids.get(key, 0) + 1

    def _reading_stop(self, key):
        count = self._reading_ids.get(key, 0) - 1
        if count > 0:
            self._reading_ids[key] = count
        else:
            self._reading_ids.pop(key, None)

    def _trim_runtime(self):
        protected = self._protected_message_ids() | set(self._reading_ids)
        for mapping in (self._entries, self._forwards):
            while len(mapping) > self.RUNTIME_LIMIT:
                for key in mapping:
                    if key not in protected:
                        mapping.pop(key, None)
                        break
                else:
                    break
        while len(self._message_entries) > self.MESSAGE_INDEX_LIMIT:
            self._message_entries.popitem(last=False)
        while len(self._message_forwards) > self.MESSAGE_INDEX_LIMIT:
            self._message_forwards.popitem(last=False)
        while len(self._message_refs) > self.MESSAGE_INDEX_LIMIT:
            mid, _ref = self._message_refs.popitem(last=False)
            self._message_entries.pop(mid, None)
            self._message_forwards.pop(mid, None)

    def _bind_message(self, message):
        """Prevent stale id(message) entries from joining a recycled object identity."""
        mid = id(message)
        ref = self._message_refs.get(mid)
        if ref is None or ref() is not message:
            self._message_entries[mid] = []
            self._message_forwards[mid] = []
            self._message_refs[mid] = self._message_ref(message)
        self._message_refs.move_to_end(mid)

    def _index_message(self, mapping, message, key):
        self._bind_message(message)
        mid = id(message)
        ids = mapping.setdefault(mid, [])
        if key not in ids:
            ids.append(key)
        mapping.move_to_end(mid)
        self._trim_runtime()

    @staticmethod
    def _is_visual(kind: str) -> bool:
        return kind in ("Image", "Sticker")

    def _enabled_for(self, kind: str) -> bool:
        return self.image_tool_enabled if self._is_visual(kind) else self.audio_tool_enabled

    def allows_prefetch(self, kind: str) -> bool:
        """Non-tool media keep the old behavior; tool media honor pure-on-demand mode."""
        return True if not self._enabled_for(kind) else self.prefetch_enabled

    def _register_media(self, elem, kind: str, sid: str, message,
                        occurrence: int, nested: bool, history_occurrence=None,
                        index: bool = True) -> str:
        raw_message_id = getattr(message, "message_id", None)
        message_id = raw_message_id or f"runtime-{id(message)}"
        prefix = {"Image": "i", "Sticker": "s", "Record": "r"}[kind]
        digest = self._digest(sid, message_id, kind, occurrence)
        media_id = self._allocate_id(prefix, digest, sid, "media")
        entry = self._entries.get(media_id)
        if entry is None:
            entry = {
                "id": media_id, "kind": kind, "sid": sid, "digest": digest,
                "message_id": str(message_id), "occurrence": occurrence,
                "history_occurrence": history_occurrence,
                "nested": nested,
                "cache_path": (self._catalog.get(media_id) or {}).get("cache_path"),
                "volatile": raw_message_id is None,
            }
        entry["elem"] = elem
        entry["message_ref"] = self._message_ref(message)
        self._entries[media_id] = entry
        self._entries.move_to_end(media_id)
        try:
            elem._media_access_id = media_id
        except Exception:
            pass
        if index:
            self._index_message(self._message_entries, message, media_id)
        return media_id

    def prepare_message(self, message, sid: str) -> None:
        """Index media/forwards synchronously; schedule metadata persistence off-loop."""
        if not self.any_enabled or not sid:
            return
        self._bind_message(message)
        counters = {"Image": 0, "Sticker": 0, "Record": 0}
        history_counters = {"Image": 0, "Sticker": 0, "Record": 0}
        forward_ids = []
        # OneBot 原始报文里顶层 forward 段的顺序与框架链中的 Forward 元素一致；
        # 只在顶层消费，Reply.chain 里的 Forward 由 _schedule_reply_hydration 处理。
        raw_forwards = self._extract_raw_forwards(getattr(message, "raw_message", None))
        top_forward_count = sum(1 for elem in (getattr(message, "chain", None) or [])
                                if isinstance(elem, Forward))
        # 只有「框架顶层 Forward 数 == 原始顶层 forward 段数」时才按顺序匹配；
        # 部分段被框架丢弃时宁可不匹配，也不能把 A 的内容套到 B 上。
        map_raw = top_forward_count == len(raw_forwards)
        if raw_forwards and not map_raw and top_forward_count:
            logger.debug(f"[MediaAccess] forward count mismatch: framework={top_forward_count} "
                         f"raw={len(raw_forwards)}; skip raw mapping to avoid mixing records")
        raw_cursor = 0

        def walk(chain, nested=False, path=()):
            nonlocal raw_cursor
            for index, elem in enumerate(chain):
                if isinstance(elem, (Image, Sticker, Record)):
                    if getattr(elem, "_long_voice_seconds", None) is not None:
                        # 超长语音只做 [长语音] 占位，不生成媒体 ID，也不允许 read_media。
                        continue
                    kind = ("Sticker" if isinstance(elem, Sticker)
                            else "Image" if isinstance(elem, Image) else "Record")
                    occurrence = counters[kind]
                    counters[kind] += 1
                    history_occurrence = history_counters[kind]
                    history_counters[kind] += 1
                    self._register_media(
                        elem, kind, sid, message, occurrence, nested, history_occurrence)
                elif isinstance(elem, Reply) and elem.chain is not None:
                    walk(elem.chain, nested, path + ("r", index))
                elif isinstance(elem, Forward):
                    raw = None
                    # 顶层链的 path 为空；Reply.chain 递归后会带 "r" 前缀。
                    if map_raw and not path and raw_cursor < len(raw_forwards):
                        raw = raw_forwards[raw_cursor]
                        raw_cursor += 1
                    forward_id = self._register_forward(
                        elem, sid, message, 1 if not nested else None,
                        path + (index,), counters, set(), raw=raw)
                    forward_ids.append(forward_id)

        walk(message.chain)
        if self.forward_tool_enabled:
            # 回复链里的 Forward 也要补全；先捕获元素，再统一替换为预览文本。
            self._schedule_reply_hydration(message, sid)
            if forward_ids:
                self._replace_forward_elements(message.chain)
            elif (raw_forwards and top_forward_count == 0
                  and not any(getattr(elem, "_media_forward_id", None)
                              for elem in message.chain)
                  and not self._message_forwards.get(id(message))):
                # 框架整体丢了顶层 forward（NapCat 的 node 包裹就是这种情况），
                # 由插件补一条预览，保证 LLM 仍能拿到可读取的 Forward ID。
                for raw in raw_forwards:
                    self._attach_raw_only_forward(message, sid, raw, counters)
        self._schedule_persistence(message)

    def _attach_raw_only_forward(self, message, sid, raw, counters) -> str:
        """Create a root Forward record when the framework dropped the element."""
        dummy = Forward(chains=[])
        forward_id = self._register_forward(
            dummy, sid, message, 1, (raw.index,), counters, set(), raw=raw)
        preview = Text(self.render_forward(forward_id, depth=1, root_summary=True))
        preview._media_forward_id = forward_id
        try:
            target = message.chain.message_list
            target.insert(min(max(raw.index, 0), len(target)), preview)
        except AttributeError:
            try:
                message.chain.append(preview)
            except Exception:
                pass
        return forward_id

    def _register_forward(self, forward: Forward, sid: str, message, depth,
                          path, counters, visiting, raw: RawForward | None = None) -> str:
        actual_depth = depth or 1
        raw_message_id = getattr(message, "message_id", None)
        message_id = raw_message_id or f"runtime-{id(message)}"
        path_text = "/".join(map(str, path))
        digest = self._digest(sid, message_id, "forward", path_text)
        forward_id = self._allocate_id("f", digest, sid, "forward")
        existing = self._forwards.get(forward_id)
        if existing is not None:
            existing["message_ref"] = self._message_ref(message)
            if raw is not None and not existing.get("source_id"):
                existing["source_id"] = raw.id
                existing["source_kind"] = raw.kind
            self._index_message(self._message_forwards, message, forward_id)
            try:
                forward._media_access_forward_id = forward_id
            except Exception:
                pass
            self._schedule_forward_hydration(existing, raw, sid, message)
            return forward_id
        record = {
            "id": forward_id, "depth": actual_depth, "sid": sid, "digest": digest,
            "message_id": str(message_id), "path": path_text, "children": [],
            "volatile": raw_message_id is None,
            "source_id": raw.id if raw is not None else "",
            "source_kind": raw.kind if raw is not None else "",
            "hydrated": False, "retry_after": 0,
        }
        record["message_ref"] = self._message_ref(message)
        self._forwards[forward_id] = record
        self._index_message(self._message_forwards, message, forward_id)
        try:
            forward._media_access_forward_id = forward_id
        except Exception:
            pass
        marker = id(forward)
        if marker in visiting or actual_depth >= RAW_FORWARD_MAX_DEPTH:
            self._schedule_forward_hydration(record, raw, sid, message)
            return forward_id
        visiting.add(marker)
        for index, chain in enumerate(forward.chains or []):
            record["children"].append(self._serialize_chain(
                chain, sid, message, actual_depth, path + (index,), counters, visiting))
        visiting.discard(marker)
        self._schedule_forward_hydration(record, raw, sid, message)
        return forward_id

    def _serialize_chain(self, chain, sid, message, depth, path, counters, visiting):
        result = []
        for index, elem in enumerate(chain):
            if isinstance(elem, Text):
                result.append({"type": "text", "text": elem.text})
            elif isinstance(elem, (Image, Sticker, Record)):
                kind = ("Sticker" if isinstance(elem, Sticker)
                        else "Image" if isinstance(elem, Image) else "Record")
                occurrence = counters[kind]
                counters[kind] += 1
                media_id = self._register_media(elem, kind, sid, message, occurrence, True)
                result.append({"type": "media", "media_id": media_id, "kind": kind})
            elif isinstance(elem, Forward):
                child_id = self._register_forward(
                    elem, sid, message, depth + 1, path + ("f", index),
                    counters, visiting)
                result.append({"type": "forward", "forward_id": child_id})
            elif isinstance(elem, Reply):
                nested = self._serialize_chain(
                    elem.chain, sid, message, depth, path + ("r", index), counters, visiting,
                ) if elem.chain is not None else []
                result.append({"type": "reply", "message_id": elem.message_id,
                               "message_content": elem.message_content, "chain": nested})
            elif isinstance(elem, At):
                result.append({"type": "at", "pid": elem.pid, "nickname": elem.nickname})
            elif isinstance(elem, Emoji):
                result.append({"type": "emoji", "emoji_id": elem.emoji_id,
                               "emoji_desc": elem.emoji_desc})
            elif isinstance(elem, Notice):
                result.append({"type": "notice", "text": elem.text})
            elif isinstance(elem, Poke):
                result.append({"type": "poke", "pid": elem.pid})
            elif isinstance(elem, Json):
                result.append({"type": "json", "text": json.dumps(elem.data, ensure_ascii=False)})
            elif isinstance(elem, (File, Video)):
                result.append({"type": "file", "name": getattr(elem, "name", None)})
            else:
                result.append({"type": "unknown", "text": getattr(elem, "repr", "")})
        return result

    def _replace_forward_elements(self, chain) -> None:
        for index, elem in enumerate(chain):
            if isinstance(elem, Forward):
                forward_id = self._forward_id_for_element(elem)
                if forward_id:
                    preview = Text(self.render_forward(forward_id, depth=1, root_summary=True))
                    preview._media_forward_id = forward_id
                    chain[index] = preview
            elif isinstance(elem, Reply) and elem.chain is not None:
                self._replace_forward_elements(elem.chain)

    def _forward_id_for_element(self, target):
        return getattr(target, "_media_access_forward_id", None)

    # ================= 原始 OneBot 转发树补全 =================
    #
    # 背景（2026-11-28 审计，KiraAI 3.3.0 / 3.4.0 的 QQ 适配器逐字未变）：
    #   - 适配器对嵌套 forward 段使用「内层消息的 message_id」再调 get_forward_msg，
    #     但协议端要求的是记录 resId（NapCat 明确报「内层消息无法获取」）。
    #   - 适配器完全不看 forward 段的 data.content（NapCat/SnowLuma/LLOneBot 的内联节点），
    #     也不看 NapCat get_forward_msg 返回的 {type:"node", data:{...}} 包裹。
    #   - 因此 2.8.0 / 1.11.0 起「保留原树 + read_forward 下钻」在遇到嵌套时只能看到外层，
    #     而旧版靠 _flatten_forwards 把嵌套内容拍平，所以看起来是「升级后丢了嵌套」。
    #
    # 修复策略不碰框架：只在插件侧按协议端原始报文补全插件自己的轻量树。
    # 原有框架解析结果、媒体 ID、首轮预览全部保留；仅补上框架丢掉/放不下的嵌套节点。

    def _extract_raw_forwards(self, raw_message) -> list:
        if not isinstance(raw_message, dict):
            return []
        if isinstance(raw_message.get("message"), list):
            return extract_forward_descriptors(raw_message)
        data = raw_message.get("data")
        if isinstance(data, dict) and isinstance(data.get("message"), list):
            return extract_forward_descriptors(data)
        return []

    def _track_hydration(self, key: str, task) -> None:
        if task is None:
            return
        self._hydrate_tasks[key] = task

        def _done(finished, key=key):
            if self._hydrate_tasks.get(key) is finished:
                self._hydrate_tasks.pop(key, None)

        task.add_done_callback(_done)

    def _schedule_forward_hydration(self, record, raw, sid, message) -> None:
        if not self.forward_tool_enabled or raw is None or record is None:
            return
        if record.get("hydrated"):
            return
        if not (raw.id or raw.content or raw.kind in ("node", "xml")):
            return
        retry_after = record.get("retry_after") or 0
        if retry_after > time.time():
            return
        fid = record.get("id")
        if not fid:
            return
        task = self._hydrate_tasks.get(fid)
        if task is not None and not task.done():
            return
        task = self._schedule_task(self._hydrate_forward(record, raw, sid, message))
        self._track_hydration(fid, task)

    def _schedule_reply_hydration(self, message, sid) -> None:
        for elem in list(getattr(message, "chain", []) or []):
            if not isinstance(elem, Reply) or elem.chain is None:
                continue
            pairs = []
            for forward in elem.chain:
                if isinstance(forward, Forward):
                    fid = getattr(forward, "_media_access_forward_id", None)
                    if fid:
                        pairs.append(fid)
            if not pairs:
                continue
            if all((self._forwards.get(fid) or {}).get("hydrated") for fid in pairs):
                continue
            key = "reply:" + str(id(elem))
            task = self._hydrate_tasks.get(key)
            if task is not None and not task.done():
                continue
            task = self._schedule_task(self._hydrate_reply_forwards(elem, pairs, sid, message))
            self._track_hydration(key, task)

    async def _hydrate_reply_forwards(self, reply, pairs, sid, message) -> None:
        client = self._client_for(sid)
        if client is None:
            return
        try:
            resp = await asyncio.wait_for(
                self._call_get_msg(client, reply.message_id), timeout=RAW_FORWARD_TIMEOUT)
        except Exception:
            return
        descs = [d for d in self._extract_raw_forwards(message_object(resp))
                 if d.kind == "forward"]
        if len(descs) != len(pairs):
            return
        for index, fid in enumerate(pairs):
            record = self._forwards.get(fid)
            if record is None or record.get("hydrated"):
                continue
            if (record.get("retry_after") or 0) > time.time():
                continue
            if index < len(descs):
                await self._hydrate_forward(record, descs[index], sid, message)

    async def _ensure_forward_ready(self, forward_id: str, sid=None):
        record = await self._forward(forward_id)
        if record is None or record.get("hydrated"):
            return record
        if sid and record.get("sid") != sid:
            return record
        if record.get("depth", 1) > min(self.forward_max_depth, RAW_FORWARD_MAX_DEPTH):
            return record
        if record.get("truncated") in ("depth", "cycle"):
            return record
        retry_after = record.get("retry_after") or 0
        if retry_after > time.time():
            return record
        task = self._hydrate_tasks.get(forward_id)
        if task is None or task.done():
            raw = None
            source_id = record.get("source_id")
            if source_id:
                raw = RawForward(id=source_id, kind=record.get("source_kind") or "forward")
            else:
                # 兼容升级前已落盘的旧记录：它们没有 source_id，但保留了 message_id
                # 与顶层 path，可用 get_msg 重新取回原始报文定位 forward 段。
                attempts = int(record.get("source_restore_attempts") or 0)
                if attempts >= 3:
                    return record
                record["source_restore_attempts"] = attempts + 1
                raw = await self._descriptor_from_message_record(record)
                if raw is None:
                    record["retry_after"] = time.time() + RAW_FORWARD_RETRY_SECONDS
                    return record
                record["source_id"] = raw.id
                record["source_kind"] = raw.kind
            task = self._schedule_task(self._hydrate_forward(
                record, raw, record.get("sid"), None))
            if task is None:
                return record
            self._track_hydration(forward_id, task)
        try:
            await asyncio.wait_for(asyncio.shield(task),
                                   timeout=RAW_FORWARD_TOTAL_BUDGET + 3)
        except Exception:
            pass
        return record

    async def _descriptor_from_message_record(self, record):
        """Rebuild a root Forward descriptor for a legacy persisted record."""
        message_id = str(record.get("message_id") or "")
        if not message_id or message_id.startswith("runtime-") or message_id == "None":
            return None
        path_text = str(record.get("path") or "0")
        if "/" in path_text:
            return None  # 只恢复顶层记录；嵌套记录由父记录补全
        try:
            index = int(path_text.split("/", 1)[0])
        except (TypeError, ValueError):
            return None
        client = self._client_for(record.get("sid"))
        if client is None:
            return None
        try:
            resp = await asyncio.wait_for(
                self._call_get_msg(client, message_id), timeout=RAW_FORWARD_TIMEOUT)
        except Exception:
            return None
        descs = self._extract_raw_forwards(message_object(resp))
        for desc in descs:
            if desc.index == index:
                return desc
        # 框架链索引与原始段索引可能因丢弃元素错位；只有一个候选时仍可安全恢复。
        return descs[0] if len(descs) == 1 else None

    def _client_for(self, sid):
        if not sid or self.ctx is None:
            return None
        manager = getattr(self.ctx, "adapter_mgr", None)
        if manager is None:
            return None
        try:
            adapter = manager.get_adapter(sid.split(":", 1)[0])
        except Exception:
            adapter = None
        if adapter is None:
            return None
        getter = getattr(adapter, "get_client", None)
        if callable(getter):
            try:
                client = getter()
                if client is not None:
                    return client
            except Exception:
                pass
        return getattr(adapter, "bot", None)

    async def _call_get_forward(self, client, source_id: str):
        errors = []
        empty_resp = None

        def _valid(resp):
            nonlocal empty_resp
            try:
                messages = message_payload(resp)
                if messages:
                    return True
                if empty_resp is None:
                    empty_resp = resp
                return False
            except Exception:
                return False

        method = getattr(client, "get_forward_msg", None)
        if callable(method):
            try:
                resp = await method(source_id)
                if _valid(resp):
                    return resp
                errors.append("get_forward_msg(message_id) invalid response")
            except Exception as exc:
                errors.append(f"get_forward_msg(message_id): {exc}")
        send = getattr(client, "send_action", None)
        if callable(send):
            for params in ({"message_id": source_id}, {"id": source_id},
                           {"res_id": source_id}):
                try:
                    resp = await send("get_forward_msg", params)
                    if _valid(resp):
                        return resp
                    errors.append(f"send_action({params}) invalid response")
                except Exception as exc:
                    errors.append(f"send_action({params}): {exc}")
        if empty_resp is not None:
            return empty_resp
        raise RuntimeError("; ".join(errors) or "adapter client has no get_forward_msg")

    async def _call_get_msg(self, client, message_id: str):
        method = getattr(client, "get_msg", None)
        if callable(method):
            return await method(message_id)
        send = getattr(client, "send_action", None)
        if callable(send):
            return await send("get_msg", {"message_id": message_id})
        raise RuntimeError("adapter client has no get_msg")

    async def _fetch_forward_nodes(self, source_id: str, ctx) -> list:
        if not source_id:
            return []
        if ctx.fetches >= RAW_FORWARD_MAX_FETCHES:
            raise RuntimeError("raw forward fetch budget exhausted")
        if time.monotonic() > ctx.deadline:
            raise TimeoutError("raw forward hydration deadline exceeded")
        ctx.fetches += 1
        client = self._client_for(ctx.sid)
        if client is None:
            raise RuntimeError("adapter client unavailable")
        resp = await asyncio.wait_for(
            self._call_get_forward(client, source_id), timeout=RAW_FORWARD_TIMEOUT)
        return message_payload(resp)

    async def _raw_nodes_for(self, raw: RawForward, ctx) -> list | None:
        if raw.content:
            content = raw.content
            if content and all(isinstance(x, str) for x in content):
                return [{"message": [{"type": "text", "text": x}]} for x in content]
            if content and all(isinstance(x, dict) for x in content):
                node_like = any(
                    ("sender" in x) or ("message" in x) or ("content" in x)
                    or ("user_id" in x) or ("nickname" in x)
                    for x in content)
                if not node_like:
                    return [{"message": content}]
            return content
        if raw.kind == "node":
            kind, payload = segment_node_parts(raw.data)
            if kind == "nodes":
                return payload
            if kind == "segments":
                meta = node_meta(raw.data)
                return [{
                    "sender": {"nickname": meta["name"], "user_id": meta["uid"]},
                    "time": meta["time"], "message": payload,
                }]
            if raw.id:
                return await self._fetch_forward_nodes(raw.id, ctx)
            return []
        if raw.kind in ("xml", "json"):
            for resid in [raw.id] + card_ids(raw.data):
                if not resid:
                    continue
                try:
                    nodes = await self._fetch_forward_nodes(resid, ctx)
                except Exception:
                    continue
                if nodes:
                    return nodes
            return []
        if raw.id:
            return await self._fetch_forward_nodes(raw.id, ctx)
        return None

    async def _hydrate_forward(self, record, raw: RawForward, sid, message) -> None:
        if record.get("depth", 1) > min(self.forward_max_depth, RAW_FORWARD_MAX_DEPTH):
            record["truncated"] = "depth"
            record["hydrated"] = True
            return
        if raw.kind in ("xml", "json") and not (raw.id or card_ids(raw.data)):
            record["truncated"] = "xml_card"
            record["hydrated"] = True
            return
        ctx = _RawBuildContext(self, sid, message, record["id"], record.get("message_id"))
        if raw.id:
            ctx.visited.add(raw.id)
        try:
            nodes = await self._raw_nodes_for(raw, ctx)
            if nodes is None:
                return
            ctx.active.add(record["id"])
            try:
                record["children"] = await self._merge_raw_nodes(record, nodes, ctx)
            finally:
                ctx.active.discard(record["id"])
            record["hydrated"] = True
            record["truncated"] = ""
            record["source_id"] = raw.id or record.get("source_id", "")
            record["source_kind"] = raw.kind
            record.pop("hydration_error", None)
            record.pop("retry_after", None)
            record.pop("source_restore_attempts", None)
            self._refresh_forward_preview(record, message)
            # 根记录必须落盘：纯文本补全不会有子记录/媒体，但 children 已经变化。
            await self._persist_message(
                sorted(ctx.entries), list(ctx.records) + [record["id"]])
            self._trim_runtime()
        except Exception as exc:
            record["hydration_error"] = f"{type(exc).__name__}: {exc}"
            if raw.kind in ("xml", "json"):
                record["truncated"] = "xml_card"
            elif not record.get("children") and not record.get("truncated"):
                record["truncated"] = "fetch_failed"
            record["retry_after"] = time.time() + RAW_FORWARD_RETRY_SECONDS
            logger.debug(f"[MediaAccess] raw forward hydration failed for "
                         f"{record.get('id')}: {type(exc).__name__}: {exc}")

    async def _merge_raw_nodes(self, record, nodes, ctx) -> list:
        existing = record.get("children") or []
        if not existing:
            return await self._build_raw_children(
                nodes, record.get("depth", 1), (record["id"],), ctx)
        for index, node in enumerate(nodes):
            if index >= len(existing):
                # 原始报文比框架多出的条目只追加，不重建已有链，避免丢框架媒体条目。
                if ctx.nodes >= RAW_FORWARD_MAX_NODES:
                    existing.append([{"type": "text", "text": "[合并记录过长，已截断]"}])
                    break
                ctx.nodes += 1
                existing.append(await self._serialize_raw_node(
                    node, ctx, record.get("depth", 1), (record["id"], index)))
                continue
            chain = existing[index]
            if not isinstance(chain, list):
                continue
            if not chain:
                if ctx.nodes >= RAW_FORWARD_MAX_NODES:
                    existing[index] = [{"type": "text", "text": "[合并记录过长，已截断]"}]
                    continue
                ctx.nodes += 1
                existing[index] = await self._serialize_raw_node(
                    node, ctx, record.get("depth", 1), (record["id"], index))
                continue
            descs = collect_nested(node_segments(node))
            forward_elements = [e for e in chain if e.get("type") == "forward"]
            forward_descs = [d for d in descs if d.kind == "forward"]
            extra_descs = [d for d in descs if d.kind != "forward"]
            for sub, desc in enumerate(forward_descs):
                if sub < len(forward_elements):
                    child = self._forwards.get(forward_elements[sub].get("forward_id"))
                    if (child is not None and not child.get("hydrated")
                            and (child.get("retry_after") or 0) <= time.time()):
                        await self._hydrate_child_record(child, desc, ctx)
                else:
                    chain.extend(await self._make_nested_forward(
                        desc, ctx, record.get("depth", 1),
                        (record["id"], index, "f", sub)))
            for sub, desc in enumerate(extra_descs):
                chain.extend(await self._make_nested_forward(
                    desc, ctx, record.get("depth", 1),
                    (record["id"], index, "x", sub)))
        return existing

    async def _hydrate_child_record(self, record, raw: RawForward, ctx) -> None:
        if record.get("depth", 1) > min(self.forward_max_depth, RAW_FORWARD_MAX_DEPTH):
            record["truncated"] = "depth"
            record["hydrated"] = True
            ctx.records[record["id"]] = record
            return
        if record["id"] in ctx.active:
            record["truncated"] = "cycle"
            record["hydrated"] = True
            ctx.records[record["id"]] = record
            return
        try:
            nodes = await self._raw_nodes_for(raw, ctx)
            if nodes is None:
                return
            ctx.active.add(record["id"])
            try:
                if record.get("children"):
                    record["children"] = await self._merge_raw_nodes(record, nodes, ctx)
                else:
                    record["children"] = await self._build_raw_children(
                        nodes, record.get("depth", 1), (record["id"],), ctx)
            finally:
                ctx.active.discard(record["id"])
            record["hydrated"] = True
            record["truncated"] = ""
            record["source_id"] = raw.id or record.get("source_id", "")
            record["source_kind"] = raw.kind
            ctx.records[record["id"]] = record
        except Exception:
            record["truncated"] = "fetch_failed"
            record["hydrated"] = False
            record["retry_after"] = time.time() + RAW_FORWARD_RETRY_SECONDS
            ctx.records[record["id"]] = record

    async def _build_raw_children(self, nodes, depth, path, ctx) -> list:
        children = []
        for index, node in enumerate(nodes or []):
            if ctx.nodes >= RAW_FORWARD_MAX_NODES:
                children.append([{"type": "text", "text": "[合并记录过长，已截断]"}])
                break
            ctx.nodes += 1
            children.append(await self._serialize_raw_node(
                node, ctx, depth, tuple(path) + (index,)))
        return children

    async def _serialize_raw_node(self, node, ctx, depth, path) -> list:
        meta = node_meta(node)
        segments = node_segments(node)
        chain = [{"type": "text",
                  "text": f"[{self._format_raw_time(meta['time'])}] {self._clean_name(meta['name'])}: "}]
        for index, seg in enumerate(segments):
            chain.extend(await self._serialize_raw_segment(
                seg, ctx, depth, tuple(path) + (index,)))
        if not segments:
            chain.append({"type": "text", "text": "[空消息]"})
        return chain

    async def _serialize_raw_segment(self, seg, ctx, depth, path) -> list:
        if not isinstance(seg, dict):
            return [{"type": "text", "text": str(seg)}] if seg else []
        kind = seg.get("type")
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
        if kind == "text":
            text = data.get("text", "")
            return [{"type": "text", "text": str(text)}] if text else []
        if kind == "image":
            url = self._raw_media_source(data)
            sticker = data.get("sub_type") == 1 or data.get("summary") == "[动画表情]"
            if not url:
                return [{"type": "text", "text": "[动画表情]" if sticker else "[图片]"}]
            try:
                elem = Sticker(sticker=url) if sticker else Image(url)
            except Exception:
                return [{"type": "text", "text": "[动画表情]" if sticker else "[图片]"}]
            media_id = self._register_raw_media(
                elem, "Sticker" if sticker else "Image", ctx, path)
            return [{"type": "media", "media_id": media_id,
                     "kind": "Sticker" if sticker else "Image"}]
        if kind == "record":
            url = self._raw_media_source(data)
            if not url:
                return [{"type": "text", "text": "[语音]"}]
            try:
                elem = Record(url)
            except Exception:
                return [{"type": "text", "text": "[语音]"}]
            media_id = self._register_raw_media(elem, "Record", ctx, path)
            return [{"type": "media", "media_id": media_id, "kind": "Record"}]
        if kind == "video":
            name = first_str(data.get("file"), data.get("name"), "视频")
            return [{"type": "file", "name": name}]
        if kind == "file":
            return [{"type": "file", "name": first_str(data.get("name"), data.get("file"), "文件")}]
        if kind == "at":
            target = first_str(data.get("qq"), data.get("user_id"))
            nickname = "全体成员" if target == "all" else first_str(
                data.get("name"), data.get("nickname"))
            return [{"type": "at", "pid": target, "nickname": nickname}]
        if kind in ("face", "emoji"):
            return [{"type": "emoji", "emoji_id": first_str(data.get("id")),
                     "emoji_desc": first_str(data.get("desc"), data.get("summary"))}]
        if kind == "reply":
            return [{"type": "reply", "message_id": first_str(data.get("id")),
                     "message_content": first_str(
                         data.get("content"), data.get("message_content")), "chain": []}]
        if kind == "json":
            if is_json_forward_card(data):
                desc = RawForward(kind="json", id=json_forward_id(data), data=data)
                return await self._make_nested_forward(
                    desc, ctx, depth, tuple(path) + ("json",))
            return [{"type": "json", "text": self._raw_card_text(data)}]
        if kind == "xml":
            desc = collect_nested([seg])
            if desc and desc[0].kind == "xml":
                return await self._make_nested_forward(desc[0], ctx, depth, tuple(path) + ("xml",))
            return [{"type": "text", "text": "[XML消息]"}]
        if kind == "forward":
            desc = collect_nested([seg])
            if desc:
                return await self._make_nested_forward(desc[0], ctx, depth, tuple(path) + ("f",))
            return [{"type": "text", "text": "[空合并记录]"}]
        if kind == "node":
            node_kind, _payload = segment_node_parts(data)
            if node_kind == "empty":
                if first_str(data.get("id")):
                    desc = RawForward(kind="node", id=first_str(data.get("id")), data=data)
                    return await self._make_nested_forward(
                        desc, ctx, depth, tuple(path) + ("n",))
                return [{"type": "text", "text": "[空消息]"}]
            # 合并记录内部出现的 node 段就是协议端的嵌套记录边界（NapCat 的
            # get_forward_msg 会把嵌套 forward 递归成 node.data.message）。
            desc = RawForward(kind="node", data=data)
            return await self._make_nested_forward(desc, ctx, depth, tuple(path) + ("n",))
        if kind in ("markdown",):
            return [{"type": "text", "text": first_str(data.get("content"), data.get("text"), "[Markdown]")}]
        if kind == "poke":
            return [{"type": "text", "text": "[戳一戳]"}]
        return [{"type": "text", "text": f"[{kind or '未知消息'}]"}]

    async def _make_nested_forward(self, desc: RawForward, ctx, parent_depth, path) -> list:
        depth = parent_depth + 1
        truncated = ""
        nodes = None
        if depth > min(self.forward_max_depth, RAW_FORWARD_MAX_DEPTH):
            truncated = "depth"
        elif desc.kind == "forward":
            if desc.content:
                nodes = desc.content
            elif desc.id:
                if desc.id in ctx.visited:
                    truncated = "cycle"
                else:
                    ctx.visited.add(desc.id)
                    try:
                        nodes = await self._fetch_forward_nodes(desc.id, ctx)
                    except Exception:
                        truncated = "fetch_failed"
                    finally:
                        ctx.visited.discard(desc.id)
            else:
                return [{"type": "text", "text": "[空合并记录]"}]
        elif desc.kind == "node":
            node_kind, payload = segment_node_parts(desc.data)
            if node_kind == "nodes":
                nodes = payload
            elif node_kind == "segments":
                meta = node_meta(desc.data)
                nodes = [{"sender": {"nickname": meta["name"], "user_id": meta["uid"]},
                          "time": meta["time"], "message": payload}]
            elif desc.id and desc.id not in ctx.visited:
                ctx.visited.add(desc.id)
                try:
                    nodes = await self._fetch_forward_nodes(desc.id, ctx)
                except Exception:
                    truncated = "fetch_failed"
                finally:
                    ctx.visited.discard(desc.id)
            elif desc.id:
                truncated = "cycle"
            else:
                nodes = []
        elif desc.kind in ("xml", "json"):
            candidates = []
            for value in [desc.id] + card_ids(desc.data):
                if value and value not in candidates:
                    candidates.append(value)
            if not candidates:
                truncated = "xml_card"
            for resid in candidates:
                if resid in ctx.visited:
                    continue
                ctx.visited.add(resid)
                try:
                    nodes = await self._fetch_forward_nodes(resid, ctx)
                except Exception:
                    nodes = None
                finally:
                    ctx.visited.discard(resid)
                if nodes:
                    break
            if not nodes:
                truncated = "xml_card"
                logger.debug(f"[MediaAccess] {desc.kind} forward card unresolved: "
                             f"ids={candidates} xml_len={len(xml_text(desc.data)) if desc.kind == 'xml' else 0}")
        if nodes is None:
            nodes = []
        record = self._new_raw_forward_record(ctx, depth, path, desc, truncated)
        if nodes and not record.get("children"):
            try:
                record["children"] = await self._build_raw_children(nodes, depth, path, ctx)
                record["truncated"] = ""
                record["hydrated"] = True
            except Exception:
                record["truncated"] = "fetch_failed"
                record["hydrated"] = False
        return [{"type": "forward", "forward_id": record["id"]}]

    def _new_raw_forward_record(self, ctx, depth, path, desc: RawForward, truncated: str = "") -> dict:
        path_text = "/".join(map(str, path))
        source_id = desc.id or ""
        digest = self._digest(ctx.sid, ctx.root_id, "raw-forward", path_text, source_id)
        forward_id = self._allocate_id("f", digest, ctx.sid, "forward")
        record = self._forwards.get(forward_id)
        if record is None:
            record = {
                "id": forward_id, "depth": depth, "sid": ctx.sid, "digest": digest,
                "message_id": str(ctx.message_id or "runtime"),
                "path": path_text, "children": [],
                "volatile": ctx.message_id is None,
            }
            self._forwards[forward_id] = record
        record.update({
            "depth": depth, "sid": ctx.sid, "digest": digest, "path": path_text,
            "source_id": source_id, "source_kind": desc.kind,
            "truncated": truncated,
            "hydrated": truncated not in ("fetch_failed", "xml_card"),
        })
        if truncated in ("fetch_failed", "xml_card"):
            record["retry_after"] = 0
        record.setdefault("children", [])
        if not isinstance(ctx.message, _RawMessage):
            self._index_message(self._message_forwards, ctx.message, forward_id)
        ctx.records[forward_id] = record
        return record

    def _register_raw_media(self, elem, kind, ctx, path) -> str:
        key = "/".join(map(str, path)) + "|" + kind
        occurrence = RAW_MEDIA_OCCURRENCE_BASE + (
            int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:10], 16) % 100_000_000)
        media_id = self._register_media(
            elem, kind, ctx.sid, ctx.message, occurrence, True,
            index=not isinstance(ctx.message, _RawMessage))
        ctx.entries.add(media_id)
        return media_id

    @staticmethod
    def _raw_media_source(data: dict) -> str:
        for key in ("url", "file", "uri", "path"):
            value = data.get(key)
            # 只接受网络/内联数据；不接受 file:/// 或裸路径，避免原始报文
            # 把本地文件带进 read_media。
            if isinstance(value, str) and value.startswith(
                    ("http://", "https://", "data:", "base64://")):
                return value
        return ""

    @staticmethod
    def _raw_card_text(data: dict) -> str:
        raw = data.get("data", data.get("content", ""))
        if isinstance(raw, str):
            try:
                card = json.loads(raw)
            except (TypeError, ValueError):
                return "[卡片消息]"
            meta = card.get("meta") if isinstance(card, dict) else None
            if isinstance(meta, dict):
                for value in meta.values():
                    if isinstance(value, dict):
                        title = first_str(value.get("title"), value.get("desc"))
                        if title:
                            title = re.sub(r"\s+", " ", title).strip()
                            return f"[卡片 {title[:80]}]"
        return "[卡片消息]"

    @staticmethod
    def _format_raw_time(value) -> str:
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            return "时间未知"
        if timestamp <= 0:
            return "时间未知"
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, OverflowError, ValueError):
            return "时间未知"

    @staticmethod
    def _clean_name(value) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        text = text.replace("[", "［").replace("]", "］").replace(":", "：")
        return text or "未知"

    def _refresh_forward_preview(self, record, message) -> None:
        if message is None:
            return
        forward_id = record.get("id")
        new_text = self.render_forward(forward_id, depth=1, root_summary=True)
        for elem in self._iter_texts(getattr(message, "chain", None)):
            if getattr(elem, "_media_forward_id", None) != forward_id:
                continue
            old_text = getattr(elem, "text", "")
            elem.text = new_text
            current = getattr(message, "message_str", None)
            if current and old_text in current:
                message.message_str = current.replace(old_text, new_text, 1)

    @staticmethod
    def _iter_texts(chain, visiting=None):
        if chain is None:
            return
        visiting = set() if visiting is None else visiting
        identity = id(chain)
        if identity in visiting:
            return
        visiting.add(identity)
        try:
            elements = chain.message_list
        except AttributeError:
            elements = chain
        for elem in list(elements or []):
            if isinstance(elem, Reply) and elem.chain is not None:
                yield from MediaAccessManager._iter_texts(elem.chain, visiting)
            elif isinstance(elem, Text):
                yield elem

    def _schedule_task(self, coro):
        try:
            task = asyncio.create_task(coro)
        except RuntimeError:
            return None
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    def _schedule_persistence(self, message) -> None:
        ids = list(self._message_entries.get(id(message), []))
        forward_ids = list(self._message_forwards.get(id(message), []))
        if ids or forward_ids:
            self._schedule_task(self._persist_message(ids, forward_ids))

    def note_media_path(self, elem, path) -> None:
        """Persist an already-known path; never downloads merely for display."""
        media_id = getattr(elem, "_media_access_id", None)
        entry = self._entries.get(media_id)
        if entry is None or not path:
            return
        resolved = str(Path(path).resolve())
        if entry.get("cache_path") == resolved:
            return
        entry["cache_path"] = resolved
        if media_id not in self._persisting_ids:
            self._schedule_task(self._persist_message([media_id], []))

    async def _persist_message(self, entry_ids, forward_ids) -> None:
        self._persisting_ids.update(entry_ids)
        try:
            await self._persist_message_inner(entry_ids, forward_ids)
        finally:
            self._persisting_ids.difference_update(entry_ids)

    async def _persist_message_inner(self, entry_ids, forward_ids) -> None:
        entries = [self._entries[mid] for mid in entry_ids if mid in self._entries]
        forwards = [self._forwards[fid] for fid in forward_ids if fid in self._forwards]
        stable_path = getattr(self.recognizer, "_is_stable_media_path", None)
        for entry in entries:
            elem = entry.get("elem")
            raw = getattr(elem, "_temp_path", None)
            current = entry.get("cache_path")
            if (current and raw and stable_path is not None
                    and not stable_path(current) and current == str(Path(raw).resolve())):
                entry["cache_path"] = None
            if not entry.get("cache_path") and raw:
                if stable_path is None or stable_path(raw):
                    entry["cache_path"] = str(Path(raw).resolve())
        # 先保存轻量目录，慢媒体下载不再阻塞整棵 Forward 文本树。
        mtimes = await asyncio.to_thread(self._persist_sync, entries, forwards)
        await self._mark_persisted(entries, forwards, mtimes)
        changed = []
        for entry in entries:
            if entry.get("cache_path"):
                continue
            elem = entry.get("elem")
            if elem is None:
                continue
            # 纯按需不额外下载顶层 URL；嵌套媒体必须尽力为重启恢复落盘。
            if (not entry.get("nested") and not self.prefetch_enabled
                    and getattr(elem, "file_type", "") == "url"
                    and not getattr(elem, "_temp_path", None)):
                continue
            try:
                persist = getattr(self.recognizer, "_persist_access_media", None)
                if persist is None:
                    persist = self.recognizer._persist_media
                path, _ = await persist(elem)
                if path:
                    entry["cache_path"] = str(Path(path).resolve())
                    changed.append(entry)
            except Exception:
                continue
        if changed:
            mtimes = await asyncio.to_thread(self._persist_sync, changed, [])
            await self._mark_persisted(changed, [], mtimes)

    def _persist_sync(self, entries, forwards) -> dict:
        mtimes = {}
        for directory in (self._entry_dir, self._forward_dir):
            directory.mkdir(parents=True, exist_ok=True)
        for entry in entries:
            payload = {k: v for k, v in entry.items() if k not in ("elem", "message_ref")}
            path = self._entry_dir / f"{entry['id']}.json"
            self._atomic_json(path, payload)
            mtimes[entry["id"]] = path.stat().st_mtime
        for record in forwards:
            payload = {k: v for k, v in record.items() if k != "message_ref"}
            path = self._forward_dir / f"{record['id']}.json"
            self._atomic_json(path, payload)
            mtimes[record["id"]] = path.stat().st_mtime
        return mtimes

    async def _mark_persisted(self, entries, forwards, mtimes=None):
        now = time.time()
        for kind, records in (("media", entries), ("forward", forwards)):
            for record in records:
                key = record["id"]
                self._catalog[key] = {
                    "kind": kind, "sid": record.get("sid"),
                    "digest": self._entry_digest(record), "mtime": now,
                    "cache_path": record.get("cache_path"),
                    "file_mtime": (mtimes or {}).get(
                        key, (self._catalog.get(key) or {}).get("file_mtime")),
                }
                digest = self._entry_digest(record)
                if digest:
                    self._identity_ids[(key[0], digest)] = key
        await self._cleanup_catalog()

    async def _cleanup_catalog(self):
        now = time.monotonic()
        if now - self._last_catalog_cleanup < self.CATALOG_CLEANUP_INTERVAL:
            return
        self._last_catalog_cleanup = now
        protected = self._protected_message_ids() | set(self._reading_ids)
        victims = set()
        if self._catalog_max_age_seconds:
            cutoff = time.time() - self._catalog_max_age_seconds
            victims.update(key for key, meta in self._catalog.items()
                           if meta.get("mtime", 0) < cutoff and key not in protected)
        groups = {}
        for key, meta in self._catalog.items():
            groups.setdefault((meta.get("kind"), meta.get("sid")), []).append((meta.get("mtime", 0), key))
        for records in groups.values():
            records.sort(reverse=True)
            victims.update(key for _mtime, key in records[self.CATALOG_SESSION_LIMIT:]
                           if key not in protected)
        ordered = sorted((meta.get("mtime", 0), key) for key, meta in self._catalog.items())
        excess = max(0, len(self._catalog) - self.CATALOG_TOTAL_LIMIT)
        if excess:
            victims.update(key for _mtime, key in ordered[:excess] if key not in protected)
        if not victims:
            await self._save_catalog_snapshot()
            return
        paths = []
        for key in victims:
            meta = self._catalog.get(key) or {}
            directory = self._forward_dir if meta.get("kind") == "forward" else self._entry_dir
            paths.append((key, directory / f"{key}.json"))
        deleted = await asyncio.to_thread(self._unlink_catalog_paths, paths)
        for key in deleted:
            meta = self._catalog.pop(key, None)
            if meta and meta.get("digest"):
                identity = (key[0], meta["digest"])
                if self._identity_ids.get(identity) == key:
                    self._identity_ids.pop(identity, None)
        await self._save_catalog_snapshot()

    @staticmethod
    def _unlink_catalog_paths(paths):
        deleted = []
        for key, path in paths:
            try:
                path.unlink()
                deleted.append(key)
            except FileNotFoundError:
                deleted.append(key)
            except OSError:
                pass
        return deleted

    async def _on_session_deleted(self, event):
        payload = getattr(event, "payload", {}) or {}
        sid = payload.get("session") if isinstance(payload, dict) else None
        if sid:
            await self.delete_session(str(sid))

    async def delete_session(self, sid):
        """Delete only this session's metadata; shared content-hash bytes stay in LRU."""
        keys = [key for key, meta in self._catalog.items() if meta.get("sid") == sid]
        paths = []
        for key in keys:
            meta = self._catalog[key]
            directory = self._forward_dir if meta.get("kind") == "forward" else self._entry_dir
            paths.append((key, directory / f"{key}.json"))
        deleted = set(await asyncio.to_thread(self._unlink_catalog_paths, paths))
        for key in keys:
            meta = self._catalog.pop(key, None)
            if meta and meta.get("digest"):
                self._identity_ids.pop((key[0], meta["digest"]), None)
            self._entries.pop(key, None)
            self._forwards.pop(key, None)
        for mapping in (self._message_entries, self._message_forwards):
            for mid in list(mapping):
                mapping[mid] = [key for key in mapping[mid] if key not in keys and key not in deleted]
                if not mapping[mid]:
                    mapping.pop(mid, None)
        while len(self._message_refs) > self.MESSAGE_INDEX_LIMIT:
            mid, _ref = self._message_refs.popitem(last=False)
            self._message_entries.pop(mid, None)
            self._message_forwards.pop(mid, None)
        await self._save_catalog_snapshot()

    def protected_paths(self):
        paths = set()
        ids = set(self._reading_ids) | self._protected_message_ids()
        for key in ids:
            entry = self._entries.get(key)
            path = entry.get("cache_path") if entry else None
            if path:
                paths.add(str(path))
        return paths

    @staticmethod
    def _atomic_json(path: Path, payload: dict) -> None:
        tmp = path.with_suffix(f".{os.getpid()}.{id(payload)}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)

    @staticmethod
    def _read_json_sync(path: Path):
        return json.loads(path.read_text(encoding="utf-8"))

    async def _load(self, mapping: dict, directory: Path, key: str):
        pattern = MEDIA_ID_RE if directory == self._entry_dir else FORWARD_ID_RE
        if not isinstance(key, str) or not pattern.fullmatch(key):
            return None
        if key in mapping:
            mapping.move_to_end(key)
            return mapping[key]
        path = directory / f"{key}.json"
        try:
            exists = await asyncio.to_thread(path.is_file)
            if not exists:
                return None
            data = await asyncio.to_thread(self._read_json_sync, path)
            file_mtime = await asyncio.to_thread(lambda: path.stat().st_mtime)
        except (OSError, ValueError):
            return None
        if isinstance(data, dict) and data.get("id") == key:
            mapping[key] = data
            mapping.move_to_end(key)
            kind = "media" if directory == self._entry_dir else "forward"
            digest = self._entry_digest(data)
            self._catalog[key] = {
                "kind": kind, "sid": data.get("sid"), "digest": digest,
                "mtime": time.time(), "cache_path": data.get("cache_path"),
                "file_mtime": file_mtime,
            }
            if digest:
                self._identity_ids[(key[0], digest)] = key
            self._trim_runtime()
            return data
        return None

    async def _entry(self, media_id: str):
        return await self._load(self._entries, self._entry_dir, media_id)

    async def _forward(self, forward_id: str):
        return await self._load(self._forwards, self._forward_dir, forward_id)

    def _child_forward_records(self, record):
        def walk(chain):
            for elem in chain:
                if elem.get("type") == "forward":
                    child = self._forwards.get(elem["forward_id"])
                    if child is not None:
                        yield child
                elif elem.get("type") == "reply" and elem.get("chain"):
                    yield from walk(elem["chain"])
        for chain in record.get("children", []):
            yield from walk(chain)

    def _forward_remaining_depth(self, record, visiting=None) -> int:
        visiting = set() if visiting is None else visiting
        if record["id"] in visiting:
            return 0
        visiting.add(record["id"])
        best = 0
        for child in self._child_forward_records(record):
            best = max(best, 1 + self._forward_remaining_depth(child, visiting))
        visiting.discard(record["id"])
        return best

    def _forward_total_nodes(self, record, visiting=None) -> int:
        visiting = set() if visiting is None else visiting
        if record["id"] in visiting:
            return 0
        visiting.add(record["id"])
        total = len(record.get("children", []))
        for child in self._child_forward_records(record):
            total += self._forward_total_nodes(child, visiting)
        visiting.discard(record["id"])
        return total

    def _known_path(self, entry: dict) -> str:
        """Return an already-known local path without any I/O or download."""
        elem = entry.get("elem")
        path = entry.get("cache_path") or getattr(elem, "_temp_path", None)
        if not path and getattr(elem, "file_type", "") == "path":
            path = getattr(elem, "file", None)
        if not path:
            return ""
        raw = str(path).replace("\\", "/")
        if raw.startswith("data/"):
            return raw
        try:
            p = Path(raw)
            data_dir = get_data_path()
            if p.is_absolute():
                try:
                    return "data/" + p.relative_to(data_dir).as_posix()
                except ValueError:
                    return p.as_posix()
        except Exception:
            pass
        return raw

    async def _actual_media_path(self, entry: dict) -> str:
        known = self._known_path(entry)
        if known:
            return known
        elem = entry.get("elem")
        if elem is None:
            return ""
        try:
            return await self.recognizer._media_path(elem) or ""
        except Exception:
            return ""

    @staticmethod
    def _path_suffix(path: str) -> str:
        return f", file_path: {path}" if path else ""

    def _media_placeholder(self, elem: dict) -> str:
        kind = elem.get("kind")
        media_id = elem.get("media_id")
        label = kind if kind in ("Image", "Sticker", "Record") else "Media"
        entry = self._entries.get(media_id, {})
        path = self._known_path(entry)
        # 保留 S/Z 默认提示词识别的元素类型名，并继续携带已有本地路径。
        return f"[{label} #{media_id}: 可读取{self._path_suffix(path)}]"

    def _chain_preview(self, chain: list, depth: int, indent: str, visiting: set,
                       preview: bool = False) -> tuple[list[str], list[str]]:
        lines, media_ids = [], []
        text = ""
        prefix = ""

        def flush_text():
            nonlocal text, prefix
            if not text:
                return
            raw = text.strip()
            match = re.match(r"^\[[^\]]+\]\s*([^:]+):\s*(.*)$", raw, re.S)
            if match:
                prefix = match.group(1).strip() + "："
                raw = match.group(2)
            if raw:
                if preview and len(raw) > 200:
                    lines.append(indent + prefix + raw[:200] + "…")
                else:
                    # 工具全文读取按行分页前先做有界分片，长文本也不会被静默截断。
                    for offset in range(0, len(raw), 1000):
                        lines.append(indent + prefix + raw[offset:offset + 1000])
            text = ""

        for elem in chain:
            kind = elem.get("type")
            if kind == "text":
                text += elem.get("text") or ""
            elif kind == "media":
                flush_text()
                media_id = elem["media_id"]
                lines.append(indent + prefix + self._media_placeholder(elem))
                media_ids.append(media_id)
            elif kind == "forward":
                flush_text()
                forward_id = elem["forward_id"]
                child = self._forwards.get(forward_id)
                if child is None:
                    lines.append(indent + f"[Forward #{forward_id}，不可恢复]")
                    continue
                trunc_note = RAW_TRUNCATED_TEXT.get(child.get("truncated") or "", "")
                if trunc_note and not child.get("children"):
                    lines.append(indent + prefix + f"[Forward #{forward_id}｜{trunc_note}]")
                    continue
                actual_remaining = self._forward_remaining_depth(child)
                remaining = min(actual_remaining,
                                max(0, self.forward_max_depth - child.get("depth", 1)))
                depth_note = "｜已达最大查看深度" if actual_remaining > remaining else ""
                if trunc_note:
                    depth_note += f"｜{trunc_note}"
                lines.append(indent + prefix + f"[Forward #{forward_id}｜本层{len(child.get('children', []))}条｜还可下钻{remaining}层{depth_note}]")
                # Every nested block uses the same direct-child preview format.
                if depth > 1 and forward_id not in visiting:
                    child_lines, child_media = self._render_children(
                        child, depth - 1, indent + "  ", visiting | {forward_id}, preview)
                    lines.extend(child_lines)
                    media_ids.extend(child_media)
            elif kind == "reply":
                flush_text()
                content = elem.get("message_content") or ""
                lines.append(indent + prefix + f"[引用 #{elem.get('message_id')}" + (f"：{content}]" if content else "]"))
                if elem.get("chain"):
                    child_lines, child_media = self._chain_preview(
                        elem["chain"], depth, indent + "  ", visiting, preview)
                    lines.extend(child_lines)
                    media_ids.extend(child_media)
            elif kind == "at":
                text += f"[@{elem.get('nickname') or elem.get('pid')}]"
            elif kind == "emoji":
                text += f"[表情 {elem.get('emoji_desc') or elem.get('emoji_id')}]"
            elif kind in ("notice", "json", "unknown"):
                text += elem.get("text") or ""
            elif kind == "poke":
                text += "[戳一戳]"
            elif kind == "file":
                text += f"[文件 {elem.get('name') or ''}]"
        flush_text()
        return lines, media_ids

    def _render_children(self, record, depth, indent, visiting, preview=False):
        lines, media_ids = [], []
        for chain in record.get("children", []):
            child_lines, child_media = self._chain_preview(chain, depth, indent, visiting, preview)
            lines.extend(child_lines)
            media_ids.extend(child_media)
        return lines, media_ids

    def render_forward(self, forward_id: str, depth=1, cursor=None, root_summary=False) -> str:
        record = self._forwards.get(forward_id)
        if record is None:
            return f"合并记录 {forward_id} 不存在或已过期"
        note = RAW_TRUNCATED_TEXT.get(record.get("truncated") or "", "")
        if note and not record.get("children"):
            return f"[Forward #{forward_id}｜{note}]"
        try:
            depth = int(depth)
        except (TypeError, ValueError):
            depth = 1
        allowed = max(0, self.forward_max_depth - record.get("depth", 1) + 1)
        if allowed == 0:
            return f"Forward {forward_id} 已超过最大查看深度 {self.forward_max_depth}"
        if depth == -1:
            depth = allowed
        depth = max(1, min(depth, allowed))
        remaining = min(self._forward_remaining_depth(record),
                        max(0, self.forward_max_depth - record.get("depth", 1)))
        count = len(record.get("children", []))
        total = self._forward_total_nodes(record)
        if root_summary:
            if count or record.get("hydrated"):
                header = f"[Forward #{forward_id}｜外层{count}条｜可用 read_forward(\"{forward_id}\") 查看]"
            else:
                # 框架整体丢弃（如 NapCat 的 node 包裹）且补全尚未完成时，
                # 不要显示误导性的「外层0条」；ID 仍然可用。
                header = f"[Forward #{forward_id}｜可用 read_forward(\"{forward_id}\") 查看]"
        else:
            suffix = f"｜{note}" if note else ""
            header = f"[Forward #{forward_id}｜本层{count}条｜还可下钻{remaining}层｜子树{total}节点{suffix}]"
        lines, _ = self._render_children(record, depth, "", {forward_id}, root_summary)
        all_lines = [header, *lines]
        # Line-based pagination preserves every selected node without splitting a line.
        start = 0
        if cursor not in (None, ""):
            try:
                if isinstance(cursor, bool) or int(cursor) < 0 or str(cursor).strip() != str(int(cursor)):
                    raise ValueError
                start = int(cursor)
            except (TypeError, ValueError):
                return f"invalid_cursor：Forward {forward_id} 的分页游标无效，请使用上次返回的 cursor"
        page, size = [], 0
        for line in all_lines[start:]:
            if page and (len(page) >= 120 or size + len(line) + 1 > 6000):
                break
            page.append(line)
            size += len(line) + 1
        next_index = start + len(page)
        if next_index < len(all_lines):
            page.append(f"[结果已分页｜下一页 cursor={next_index}]")
        return "\n".join(page)

    def apply_batch_policy(self, event) -> tuple[set[str], set[str]]:
        """Mark direct vs lazy media for the final batch, per media category."""
        direct, lazy = set(), set()
        candidates = {"visual": [], "record": []}
        is_group = bool(event.is_group_message())
        auto_enabled = self.auto_group if is_group else self.auto_private
        native_mode = bool(self.recognizer._native_mode(event.sid))
        for message in getattr(event, "messages", []) or []:
            for media_id in self._message_entries.get(id(message), []):
                entry = self._entries.get(media_id)
                if not entry or entry.get("nested") or not self._enabled_for(entry["kind"]):
                    continue
                if getattr(entry.get("elem"), "_long_voice_seconds", None) is not None:
                    continue
                elem = entry.get("elem")
                if elem is None:
                    continue
                if not auto_enabled or getattr(elem, "_media_skip", False):
                    lazy.add(media_id)
                elif (not self.prefetch_enabled
                      and not (native_mode and self._is_visual(entry["kind"]))):
                    # 纯按需：VLM/STT 只能由工具触发；native 直图不调用 VLM，仍按自动规则。
                    lazy.add(media_id)
                elif getattr(message, "is_mentioned", False):
                    direct.add(media_id)
                else:
                    candidates["visual" if self._is_visual(entry["kind"]) else "record"].append(media_id)
        for ids in candidates.values():
            if len(ids) <= self.auto_max_count:
                direct.update(ids)
            else:
                lazy.update(ids)
        lazy.difference_update(direct)
        for media_id in direct | lazy:
            elem = self._entries[media_id].get("elem")
            if elem is not None:
                elem._media_tool_lazy = media_id in lazy
                if media_id in lazy and self._is_visual(self._entries[media_id]["kind"]):
                    elem.caption = ""
        return direct, lazy

    def refresh_forward_summaries(self, message) -> None:
        """Refresh known paths after background persistence; no I/O or downloads."""
        for elem in getattr(message, "chain", []):
            forward_id = getattr(elem, "_media_forward_id", None)
            if forward_id:
                old = elem.text
                new = self.render_forward(forward_id, depth=1, root_summary=True)
                elem.text = new
                current = getattr(message, "message_str", None)
                if current and old in current:
                    message.message_str = current.replace(old, new, 1)

    async def render_lazy_message(self, message) -> None:
        self.refresh_forward_summaries(message)
        text = getattr(message, "message_str", None)
        if not text:
            return
        for media_id in self._message_entries.get(id(message), []):
            entry = self._entries.get(media_id)
            if not entry or entry.get("nested"):
                continue
            if getattr(entry.get("elem"), "_long_voice_seconds", None) is not None:
                continue
            elem = entry.get("elem")
            if elem is None or not getattr(elem, "_media_tool_lazy", False):
                continue
            kind = entry["kind"]
            if kind == "Record":
                old_id = getattr(elem, "_pir_short_id", "")
                if old_id:
                    placeholder = f"[Record #{media_id}: 可读取{self._path_suffix(self._known_path(entry))}]"
                    text = text.replace(f"[Record #{old_id}: ]", placeholder)
                continue
            label = "Image" if kind == "Image" else "Sticker"
            placeholder = f"[{label} #{media_id}: 可读取{self._path_suffix(self._known_path(entry))}]"
            patterns = [rf"\[{label} attached\]", rf"\[{label}\s*, file_path: [^\]]+\]",
                        rf"\[{label}\s+#[^\]\s:]+:\s*可读取(?:, file_path: [^\]]+)?\]",
                        rf"\[{label}\s*\]"]
            for pattern in patterns:
                new_text, count = re.subn(pattern, lambda _: placeholder, text, count=1)
                if count:
                    text = new_text
                    break
        message.message_str = text

    async def _resolve_media_element(self, entry: dict):
        elem = entry.get("elem")
        if elem is not None:
            return elem
        cache_path = entry.get("cache_path")
        if cache_path and Path(cache_path).is_file():
            kind = entry["kind"]
            if kind == "Image":
                elem = Image(cache_path)
            elif kind == "Sticker":
                elem = Sticker(sticker=cache_path)
            else:
                elem = Record(cache_path)
            try:
                elem._media_access_id = entry["id"]
            except Exception:
                pass
            stable_path = getattr(self.recognizer, "_is_stable_media_path", None)
            if stable_path is not None and not stable_path(cache_path):
                try:
                    path, _ = await self.recognizer._persist_access_media(elem)
                    if path:
                        entry["cache_path"] = str(Path(path).resolve())
                        elem._temp_path = str(Path(path).resolve())
                        self.note_media_path(elem, path)
                except Exception:
                    pass
            return elem
        history = getattr(self.ctx, "message_history", None)
        if history is None or entry.get("nested"):
            return None
        try:
            record = await history.get_message(
                session_id=entry["sid"], platform_message_id=entry["message_id"])
        except Exception:
            return None
        if record is None:
            return None
        matches = []
        def walk(chain):
            for elem in chain:
                is_sticker = isinstance(elem, Sticker)
                if ((entry["kind"] == "Sticker" and is_sticker)
                        or (entry["kind"] == "Image" and isinstance(elem, Image) and not is_sticker)
                        or (entry["kind"] == "Record" and isinstance(elem, Record))):
                    matches.append(elem)
                elif isinstance(elem, Reply) and elem.chain is not None:
                    walk(elem.chain)
        walk(record.chain)
        index = entry.get("history_occurrence")
        if index is None:
            index = int(entry.get("occurrence", 0))
        index = int(index)
        if index < len(matches):
            entry["elem"] = matches[index]
            return matches[index]
        return None

    async def _store_media(self, elem, sid: str, message_id: str):
        if store_session_media is None:
            raise RuntimeError("当前框架不支持原生媒体引用")
        return await store_session_media(elem, sid, message_id)

    def _batch_candidates(self, event, forward=False):
        result = []
        mapping = self._message_forwards if forward else self._message_entries
        for message in getattr(event, "messages", []) or []:
            for key in mapping.get(id(message), []):
                meta = self._catalog.get(key) or {}
                if meta.get("sid") != getattr(event, "sid", None):
                    continue
                if not forward:
                    enabled = self.image_tool_enabled if key[0] in "is" else self.audio_tool_enabled
                    if not enabled:
                        continue
                if key not in result:
                    result.append(key)
                if len(result) >= 8:
                    return result
        return result

    async def read_media(self, event, media_ids) -> ToolResult:
        if isinstance(media_ids, str):
            ids = [x.strip() for x in media_ids.split(",") if x.strip()]
        elif isinstance(media_ids, (list, tuple)):
            ids = [str(x).strip() for x in media_ids if str(x).strip()]
        else:
            ids = []
        ids = list(dict.fromkeys(ids))
        if not ids:
            return ToolResult(text="invalid_id：请提供至少一个媒体 ID")
        if len(ids) > self.MAX_BATCH_MEDIA:
            return ToolResult(
                text=f"batch_limit：一次最多读取 {self.MAX_BATCH_MEDIA} 个去重后的媒体；本次 {len(ids)} 个，未执行任何读取")

        sid = getattr(event, "sid", None)

        async def one(media_id):
            if not MEDIA_ID_RE.fullmatch(media_id):
                return media_id, None, (
                    f"[Media #{media_id}: invalid_id；ID 格式无效，未执行读取。"
                    "请逐字复制原始消息 # 后、冒号前的完整 ID，不要删除 i/s/r 等任何前缀]"), True
            try:
                entry = await self._entry(media_id)
                if entry is None or entry.get("sid") != sid:
                    return media_id, None, (
                        f"[Media #{media_id}: not_found；当前会话未找到该 ID。"
                        "可能是复制错误、旧摘要或目录已清理；未执行媒体读取]"), True
                if getattr(entry.get("elem"), "_long_voice_seconds", None) is not None:
                    return media_id, None, (
                        f"[Record #{media_id}: not_available；长语音仅占位，不提供按需识别]"), False
                kind = entry["kind"]
                if not self._enabled_for(kind):
                    return media_id, None, f"[{kind} #{media_id}: disabled；该类媒体的按需读取未开启]", False
                self._reading_start(media_id)
                try:
                    elem = await self._resolve_media_element(entry)
                    if elem is None:
                        return media_id, None, (
                            f"[{kind} #{media_id}: source_unavailable；目录存在，但媒体源、缓存和历史归档均不可恢复。"
                            "媒体可能确已清理，可请用户重发]"), False
                    if self._is_visual(kind) and self.recognizer._native_mode(sid):
                        ref = await self._store_media(elem, sid, entry["message_id"])
                        path = (ref or {}).get("path") or await self._actual_media_path(entry)
                        text = f"[{kind} #{media_id}: 已返回原生图片{self._path_suffix(path)}]"
                        return media_id, ref, text, False
                    desc = await self.recognizer.read_access_entry(entry, event)
                    if not desc:
                        return media_id, None, f"[{kind} #{media_id}: recognition_failed；识别失败或结果为空]", False
                    path = await self._actual_media_path(entry)
                    text = f"[{kind} #{media_id}: {desc}{self._path_suffix(path)}]"
                    return media_id, None, text, False
                finally:
                    self._reading_stop(media_id)
            except Exception as exc:
                # 已开始的读取由 inner finally 精确释放一次；开始前异常无需释放。
                return media_id, None, f"[Media #{media_id}: read_failed；{type(exc).__name__}: {exc}]", False

        results = await asyncio.gather(*(one(mid) for mid in ids))
        lines, refs = [], []
        for media_id, ref, text, _retryable in results:
            lines.append(text)
            if ref is not None:
                refs.append(ref)
        if any(item[3] for item in results):
            candidates = self._batch_candidates(event)
            if candidates:
                lines.append("[本批可读取 ID（仅供核对，未自动读取）：" + ", ".join(candidates) + "]")
        unique_refs = {}
        for ref in refs:
            unique_refs[(ref.get("type"), ref.get("path"))] = ref
        return ToolResult(text="\n".join(lines), media_refs=list(unique_refs.values()))

    async def _read_forward_one(self, event, forward_id: str, depth=1, cursor=None):
        record = await self._forward(forward_id)
        if record is None:
            return f"Forward {forward_id} 不存在或目录已过期", [], None

        async def load_tree(rec, visiting):
            if rec["id"] in visiting:
                return
            visiting.add(rec["id"])
            async def walk_chain(chain):
                for elem in chain:
                    if elem.get("type") == "forward":
                        child = await self._forward(elem["forward_id"])
                        if child is not None and child.get("sid") == rec.get("sid"):
                            self._reading_start(child["id"])
                            try:
                                await load_tree(child, visiting)
                            finally:
                                self._reading_stop(child["id"])
                    elif elem.get("type") == "media":
                        entry = await self._entry(elem["media_id"])
                        if entry is not None and entry.get("sid") != rec.get("sid"):
                            self._entries.pop(elem["media_id"], None)
                    elif elem.get("type") == "reply" and elem.get("chain"):
                        await walk_chain(elem["chain"])
            for chain in rec.get("children", []):
                await walk_chain(chain)

        await load_tree(record, set())
        text = self.render_forward(forward_id, depth=depth, cursor=cursor)
        ids = list(dict.fromkeys(re.findall(r"#([isr][0-9a-f]{8,64}): 可读取", text)))

        async def present(media_id):
            entry = await self._entry(media_id)
            if (entry is None or entry.get("sid") != getattr(event, "sid", None)
                    or self._enabled_for(entry["kind"])):
                return media_id, None, None, ""
            self._reading_start(media_id)
            try:
                elem = await self._resolve_media_element(entry)
                if elem is None:
                    return media_id, None, "source_unavailable", ""
                if self._is_visual(entry["kind"]) and self.recognizer._native_mode(event.sid):
                    ref = await self._store_media(elem, event.sid, entry["message_id"])
                    path = (ref or {}).get("path") or await self._actual_media_path(entry)
                    return media_id, ref, "已返回原生图片", path
                desc = await self.recognizer.read_access_entry(entry, event)
                return media_id, None, desc or "recognition_failed", await self._actual_media_path(entry)
            except Exception as exc:
                return media_id, None, f"read_failed: {type(exc).__name__}", ""
            finally:
                self._reading_stop(media_id)

        # 单个 Forward 内的媒体也并行；多个 Forward 由外层 gather 再并行一层。
        presented = await asyncio.gather(*(present(mid) for mid in ids))
        refs = []
        for media_id, ref, desc, path in presented:
            if ref is not None:
                refs.append(ref)
            if desc:
                has_path = f"#{media_id}: 可读取, file_path:" in text
                suffix = "" if has_path else self._path_suffix(path)
                text = text.replace(f"#{media_id}: 可读取", f"#{media_id}: {desc}{suffix}")
        match = re.search(r"下一页 cursor=(\d+)", text)
        return text, refs, match.group(1) if match else None

    @staticmethod
    def _valid_cursor(value):
        if value in (None, ""):
            return True
        if isinstance(value, bool):
            return False
        try:
            return int(value) >= 0 and str(value).strip() == str(int(value))
        except (TypeError, ValueError):
            return False

    async def read_forward(self, event, forward_id=None, depth=1, cursor=None,
                           forward_ids=None) -> ToolResult:
        if not self.forward_tool_enabled:
            return ToolResult(text="disabled：合并记录按需读取未开启")
        ids = []
        if isinstance(forward_ids, str):
            try:
                decoded = json.loads(forward_ids)
                if isinstance(decoded, list):
                    ids.extend(decoded)
                else:
                    ids.extend(x.strip() for x in forward_ids.split(",") if x.strip())
            except ValueError:
                ids.extend(x.strip() for x in forward_ids.split(",") if x.strip())
        elif isinstance(forward_ids, (list, tuple)):
            ids.extend(forward_ids)
        if forward_id:
            ids.insert(0, forward_id)
        ids = list(dict.fromkeys(str(x).strip() for x in ids if str(x).strip()))
        if not ids:
            return ToolResult(text="invalid_id：forward_id 或 forward_ids 至少传一个")
        if len(ids) > self.MAX_BATCH_FORWARDS:
            return ToolResult(
                text=f"batch_limit：一次最多读取 {self.MAX_BATCH_FORWARDS} 个去重后的 Forward；本次 {len(ids)} 个，未执行任何读取")

        # 首次读取时等待原始树补全；补全失败/超时仍按现有轻量树返回，不阻塞工具。
        valid_ids = [fid for fid in ids if FORWARD_ID_RE.fullmatch(fid)]
        if valid_ids:
            await asyncio.gather(*(
                self._ensure_forward_ready(fid, getattr(event, "sid", None))
                for fid in valid_ids))

        current = cursor
        if isinstance(current, str) and current.strip().startswith("{"):
            try:
                current = json.loads(current)
            except ValueError:
                return ToolResult(text="invalid_cursor：多记录游标 JSON 无效，未执行任何读取")
        if isinstance(current, dict):
            if (any(not self._valid_cursor(value) for value in current.values())
                    or any(key not in ids for key in current)):
                return ToolResult(text="invalid_cursor：游标含无效值或不属于本批的 Forward ID，未执行任何读取")
        elif len(ids) > 1 and current not in (None, ""):
            return ToolResult(text="invalid_cursor：多记录分页须传上次返回的游标 JSON")
        elif not self._valid_cursor(current):
            return ToolResult(text="invalid_cursor：请使用上次返回的非负整数 cursor")

        def cursor_for(fid):
            if isinstance(current, dict):
                return current.get(fid)
            return current if len(ids) == 1 else None

        sid = getattr(event, "sid", None)

        async def one(fid):
            if not FORWARD_ID_RE.fullmatch(fid):
                return fid, (f"[Forward #{fid}: invalid_id；请逐字复制 # 后、分隔符前的完整 ID，"
                             "不要删除 f 等任何前缀]", [], None), True
            try:
                record = await self._forward(fid)
                if record is None or record.get("sid") != sid:
                    return fid, (f"[Forward #{fid}: not_found；当前会话未找到该目录，未执行读取]", [], None), True
                self._reading_start(fid)
                try:
                    return fid, await self._read_forward_one(event, fid, depth, cursor_for(fid)), False
                finally:
                    self._reading_stop(fid)
            except Exception as exc:
                return fid, (f"[Forward #{fid}: read_failed；{type(exc).__name__}: {exc}]", [], None), False

        results = await asyncio.gather(*(one(fid) for fid in ids))
        texts, refs, next_cursors = [], [], {}
        for fid, (text, media_refs, next_cursor), _retryable in results:
            texts.append(text)
            refs.extend(media_refs)
            if next_cursor is not None:
                next_cursors[fid] = next_cursor
        unique_refs = {}
        for ref in refs:
            unique_refs[(ref.get("type"), ref.get("path"))] = ref
        if len(ids) > 1 and next_cursors:
            texts.append("[多记录分页游标：" + json.dumps(next_cursors) + "]")
        if any(item[2] for item in results):
            candidates = self._batch_candidates(event, forward=True)
            if candidates:
                texts.append("[本批可读取 Forward ID（仅供核对，未自动读取）：" + ", ".join(candidates) + "]")
        return ToolResult(text="\n\n".join(texts), media_refs=list(unique_refs.values()))

    async def shutdown(self) -> None:
        if self._session_event_bus is not None:
            try:
                self._session_event_bus.unsubscribe("session_deleted", self._on_session_deleted)
            except Exception:
                pass
            self._session_event_bus = None
        self._initialized = False
        for task in list(self._hydrate_tasks.values()):
            task.cancel()
        self._hydrate_tasks.clear()
        tasks = list(self._bg_tasks)
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=3.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._bg_tasks.clear()
        try:
            await self._save_catalog_snapshot()
        except Exception:
            pass


_NATIVE_FILTERS = []


def install_native_lazy_filter() -> int:
    """Exclude explicitly lazy images from native auto-attachment (2.x/3.x)."""
    import importlib
    patched = 0
    for modname, class_name, method_name in (
        ("core.workflow.src.im.native_content", "MessageMediaService", "iter_images"),
        ("core.message_manager", "MessageProcessor", "_iter_message_images"),
    ):
        try:
            module = importlib.import_module(modname)
            owner = getattr(module, class_name)
            original = getattr(owner, method_name)
        except Exception:
            continue
        if getattr(original, "_media_tool_native_filter", False):
            continue
        def filtered(chain, _original=original):
            for elem in _original(chain):
                if not getattr(elem, "_media_tool_lazy", False):
                    yield elem
        filtered._media_tool_native_filter = True
        setattr(owner, method_name, staticmethod(filtered))
        _NATIVE_FILTERS.append((owner, method_name, original))
        patched += 1
    return patched


def uninstall_native_lazy_filter() -> int:
    restored = 0
    while _NATIVE_FILTERS:
        owner, name, original = _NATIVE_FILTERS.pop()
        current = getattr(owner, name, None)
        if getattr(current, "_media_tool_native_filter", False):
            setattr(owner, name, staticmethod(original))
            restored += 1
    return restored
