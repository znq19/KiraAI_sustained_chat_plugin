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
from pathlib import Path

from core.chat.message_elements import Text, Image, Sticker, Record, Reply, Forward
from core.utils.path_utils import get_data_path

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
        # 轻量身份索引覆盖整个磁盘目录；热条目才有界进入运行时 LRU。
        self._catalog: dict[str, dict] = {}
        self._identity_ids: dict[tuple[str, str], str] = {}
        self._bg_tasks: set[asyncio.Task] = set()
        self._reading_ids: dict[str, int] = {}
        self._initialized = False
        self._last_catalog_cleanup = 0.0
        self._session_event_bus = None
        root = get_data_path() / "plugin_data" / plugin_id / "media_access"
        self._entry_dir = root / "entries"
        self._forward_dir = root / "forwards"
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
        result = []
        for kind, directory in (("media", self._entry_dir), ("forward", self._forward_dir)):
            if not directory.is_dir():
                continue
            for path in directory.glob("*.json"):
                try:
                    data = self._read_json_sync(path)
                    key = data.get("id")
                    pattern = MEDIA_ID_RE if kind == "media" else FORWARD_ID_RE
                    if not isinstance(key, str) or not pattern.fullmatch(key):
                        continue
                    if path.name != f"{key}.json":
                        continue
                    digest = self._entry_digest(data)
                    result.append((kind, key, data.get("sid"), digest,
                                   path.stat().st_mtime, data, data.get("cache_path")))
                except (OSError, ValueError, TypeError):
                    continue
        return result

    async def initialize(self):
        """Load only identities at startup; heavy entries remain lazy and bounded."""
        if self._initialized:
            return
        rows = await asyncio.to_thread(self._load_catalog_sync)
        rows.sort(key=lambda row: row[4])
        for kind, key, sid, digest, mtime, _data, cache_path in rows:
            self._catalog[key] = {"kind": kind, "sid": sid, "digest": digest,
                                  "mtime": mtime, "cache_path": cache_path}
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

    def _index_message(self, mapping, message, key):
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
                        occurrence: int, nested: bool, history_occurrence=None) -> str:
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
        self._index_message(self._message_entries, message, media_id)
        return media_id

    def prepare_message(self, message, sid: str) -> None:
        """Index media/forwards synchronously; schedule metadata persistence off-loop."""
        if not self.any_enabled or not sid:
            return
        counters = {"Image": 0, "Sticker": 0, "Record": 0}
        history_counters = {"Image": 0, "Sticker": 0, "Record": 0}
        forward_ids = []

        def walk(chain, nested=False, path=()):
            for index, elem in enumerate(chain):
                if isinstance(elem, (Image, Sticker, Record)):
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
                    forward_id = self._register_forward(
                        elem, sid, message, 1 if not nested else None,
                        path + (index,), counters, set())
                    forward_ids.append(forward_id)

        walk(message.chain)
        if self.forward_tool_enabled and forward_ids:
            self._replace_forward_elements(message.chain)
        self._schedule_persistence(message)

    def _register_forward(self, forward: Forward, sid: str, message, depth,
                          path, counters, visiting) -> str:
        actual_depth = depth or 1
        raw_message_id = getattr(message, "message_id", None)
        message_id = raw_message_id or f"runtime-{id(message)}"
        path_text = "/".join(map(str, path))
        digest = self._digest(sid, message_id, "forward", path_text)
        forward_id = self._allocate_id("f", digest, sid, "forward")
        existing = self._forwards.get(forward_id)
        if existing is not None:
            existing["message_ref"] = self._message_ref(message)
            self._index_message(self._message_forwards, message, forward_id)
            try:
                forward._media_access_forward_id = forward_id
            except Exception:
                pass
            return forward_id
        record = {
            "id": forward_id, "depth": actual_depth, "sid": sid, "digest": digest,
            "message_id": str(message_id), "path": path_text, "children": [],
            "volatile": raw_message_id is None,
        }
        record["message_ref"] = self._message_ref(message)
        self._forwards[forward_id] = record
        self._index_message(self._message_forwards, message, forward_id)
        try:
            forward._media_access_forward_id = forward_id
        except Exception:
            pass
        marker = id(forward)
        if marker in visiting or actual_depth >= 64:
            return forward_id
        visiting.add(marker)
        for index, chain in enumerate(forward.chains or []):
            record["children"].append(self._serialize_chain(
                chain, sid, message, actual_depth, path + (index,), counters, visiting))
        visiting.discard(marker)
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

    def _schedule_task(self, coro):
        try:
            task = asyncio.create_task(coro)
        except RuntimeError:
            return
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

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
        self._schedule_task(self._persist_message([media_id], []))

    async def _persist_message(self, entry_ids, forward_ids) -> None:
        entries = [self._entries[mid] for mid in entry_ids if mid in self._entries]
        forwards = [self._forwards[fid] for fid in forward_ids if fid in self._forwards]
        for entry in entries:
            if not entry.get("cache_path"):
                elem = entry.get("elem")
                raw = getattr(elem, "_temp_path", None)
                if raw:
                    entry["cache_path"] = str(Path(raw).resolve())
        # 先保存轻量目录，慢媒体下载不再阻塞整棵 Forward 文本树。
        await asyncio.to_thread(self._persist_sync, entries, forwards)
        await self._mark_persisted(entries, forwards)
        changed = []
        for entry in entries:
            if entry.get("cache_path"):
                continue
            elem = entry.get("elem")
            if elem is None:
                continue
            # 纯按需不额外下载顶层 URL；嵌套媒体必须尽力为重启恢复落盘。
            if (not entry.get("nested") and not self.prefetch_enabled
                    and getattr(elem, "file_type", "") == "url"):
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
            await asyncio.to_thread(self._persist_sync, changed, [])
            await self._mark_persisted(changed, [])

    def _persist_sync(self, entries, forwards) -> None:
        for directory in (self._entry_dir, self._forward_dir):
            directory.mkdir(parents=True, exist_ok=True)
        for entry in entries:
            payload = {k: v for k, v in entry.items() if k not in ("elem", "message_ref")}
            self._atomic_json(self._entry_dir / f"{entry['id']}.json", payload)
        for record in forwards:
            payload = {k: v for k, v in record.items() if k != "message_ref"}
            self._atomic_json(self._forward_dir / f"{record['id']}.json", payload)

    async def _mark_persisted(self, entries, forwards):
        now = time.time()
        for kind, records in (("media", entries), ("forward", forwards)):
            for record in records:
                key = record["id"]
                self._catalog[key] = {
                    "kind": kind, "sid": record.get("sid"),
                    "digest": self._entry_digest(record), "mtime": now,
                    "cache_path": record.get("cache_path"),
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
                actual_remaining = self._forward_remaining_depth(child)
                remaining = min(actual_remaining,
                                max(0, self.forward_max_depth - child.get("depth", 1)))
                depth_note = "｜已达最大查看深度" if actual_remaining > remaining else ""
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
            header = f"[Forward #{forward_id}｜外层{count}条｜可用 read_forward(\"{forward_id}\") 查看]"
        else:
            header = f"[Forward #{forward_id}｜本层{count}条｜还可下钻{remaining}层｜子树{total}节点]"
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
                return Image(cache_path)
            if kind == "Sticker":
                return Sticker(sticker=cache_path)
            return Record(cache_path)
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
                self._reading_stop(media_id)
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

        current = cursor
        if isinstance(current, str) and current.strip().startswith("{"):
            try:
                current = json.loads(current)
            except ValueError:
                return ToolResult(text="invalid_cursor：多记录游标 JSON 无效，未执行任何读取")
        if isinstance(current, dict):
            if any(not self._valid_cursor(value) for value in current.values()):
                return ToolResult(text="invalid_cursor：多记录游标含无效值，未执行任何读取")
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
        tasks = list(self._bg_tasks)
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=3.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._bg_tasks.clear()


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
