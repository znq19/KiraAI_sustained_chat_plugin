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


class MediaAccessManager:
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
        self.forward_tool_enabled = bool(basic.get("forward_tool_enabled", False))
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
        self._entries: dict[str, dict] = {}
        self._forwards: dict[str, dict] = {}
        self._message_entries: dict[int, list[str]] = {}
        self._bg_tasks: set[asyncio.Task] = set()
        root = get_data_path() / "plugin_data" / plugin_id / "media_access"
        self._entry_dir = root / "entries"
        self._forward_dir = root / "forwards"

    @property
    def any_enabled(self) -> bool:
        return self.image_tool_enabled or self.audio_tool_enabled or self.forward_tool_enabled

    @staticmethod
    def _digest(*parts) -> str:
        return hashlib.sha256("\x1f".join(map(str, parts)).encode("utf-8")).hexdigest()[:12]

    @staticmethod
    def _is_visual(kind: str) -> bool:
        return kind in ("Image", "Sticker")

    def _enabled_for(self, kind: str) -> bool:
        return self.image_tool_enabled if self._is_visual(kind) else self.audio_tool_enabled

    def _register_media(self, elem, kind: str, sid: str, message,
                        occurrence: int, nested: bool) -> str:
        message_id = getattr(message, "message_id", None) or f"runtime-{id(message)}"
        prefix = {"Image": "i", "Sticker": "s", "Record": "r"}[kind]
        media_id = prefix + self._digest(sid, message_id, kind, occurrence)
        entry = self._entries.get(media_id)
        if entry is None:
            entry = {
                "id": media_id, "kind": kind, "sid": sid,
                "message_id": str(message_id), "occurrence": occurrence,
                "nested": nested, "cache_path": None,
            }
            self._entries[media_id] = entry
        entry["elem"] = elem
        entry["message"] = message
        try:
            elem._media_access_id = media_id
        except Exception:
            pass
        ids = self._message_entries.setdefault(id(message), [])
        if media_id not in ids:
            ids.append(media_id)
        return media_id

    def prepare_message(self, message, sid: str) -> None:
        """Index media/forwards synchronously; schedule metadata persistence off-loop."""
        if not self.any_enabled or not sid:
            return
        counters = {"Image": 0, "Sticker": 0, "Record": 0}
        forward_ids = []

        def walk(chain, nested=False, path=()):
            for index, elem in enumerate(chain):
                if isinstance(elem, (Image, Sticker, Record)):
                    kind = ("Sticker" if isinstance(elem, Sticker)
                            else "Image" if isinstance(elem, Image) else "Record")
                    occurrence = counters[kind]
                    counters[kind] += 1
                    self._register_media(elem, kind, sid, message, occurrence, nested)
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
        message_id = getattr(message, "message_id", None) or f"runtime-{id(message)}"
        forward_id = "f" + self._digest(sid, message_id, "forward", "/".join(map(str, path)))
        existing = self._forwards.get(forward_id)
        if existing is not None:
            return forward_id
        record = {
            "id": forward_id, "depth": actual_depth, "sid": sid,
            "message_id": str(message_id), "children": [],
            "message": message,
        }
        self._forwards[forward_id] = record
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
                    chain[index] = Text(self.render_forward(forward_id, depth=1, root_summary=True))
            elif isinstance(elem, Reply) and elem.chain is not None:
                self._replace_forward_elements(elem.chain)

    def _forward_id_for_element(self, target):
        for forward_id, record in self._forwards.items():
            message = record.get("message")
            if message is not None:
                # IDs are also written to the element during the first pass when possible.
                if getattr(target, "_media_access_forward_id", None) == forward_id:
                    return forward_id
        return getattr(target, "_media_access_forward_id", None)

    def _schedule_persistence(self, message) -> None:
        ids = list(self._message_entries.get(id(message), []))
        forward_ids = [fid for fid, rec in self._forwards.items()
                       if rec.get("message") is message]
        if not ids and not forward_ids:
            return
        try:
            task = asyncio.create_task(self._persist_message(ids, forward_ids))
        except RuntimeError:
            return
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _persist_message(self, entry_ids, forward_ids) -> None:
        entries = [self._entries[mid] for mid in entry_ids if mid in self._entries]
        forwards = [self._forwards[fid] for fid in forward_ids if fid in self._forwards]
        # 核心历史只保留 Forward 空壳；嵌套媒体在后台复用现有媒体缓存落盘，
        # 目录仍只保存引用，不复制媒体字节。
        for entry in entries:
            if not entry.get("nested") or entry.get("cache_path"):
                continue
            elem = entry.get("elem")
            if elem is None:
                continue
            try:
                path, _ = await self.recognizer._persist_media(elem)
                if path:
                    entry["cache_path"] = str(Path(path).resolve())
            except Exception:
                continue
        await asyncio.to_thread(self._persist_sync, entries, forwards)

    def _persist_sync(self, entries, forwards) -> None:
        for directory in (self._entry_dir, self._forward_dir):
            directory.mkdir(parents=True, exist_ok=True)
        for entry in entries:
            payload = {k: v for k, v in entry.items() if k not in ("elem", "message")}
            self._atomic_json(self._entry_dir / f"{entry['id']}.json", payload)
        for record in forwards:
            payload = {k: v for k, v in record.items() if k != "message"}
            self._atomic_json(self._forward_dir / f"{record['id']}.json", payload)
        # Metadata is tiny; retain a strict bounded catalog without touching media bytes.
        for directory in (self._entry_dir, self._forward_dir):
            files = sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in files[2048:]:
                try:
                    old.unlink()
                except OSError:
                    pass

    @staticmethod
    def _atomic_json(path: Path, payload: dict) -> None:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)

    @staticmethod
    def _read_json_sync(path: Path):
        return json.loads(path.read_text(encoding="utf-8"))

    async def _load(self, mapping: dict, directory: Path, key: str):
        if key in mapping:
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

    @staticmethod
    def _media_placeholder(elem: dict) -> str:
        kind = elem.get("kind")
        media_id = elem.get("media_id")
        label = kind if kind in ("Image", "Sticker", "Record") else "Media"
        # 保留 S/Z 默认提示词识别的元素类型名，只追加稳定短 ID。
        return f"[{label} #{media_id}: 可读取]"

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
                start = max(0, int(cursor))
            except (TypeError, ValueError):
                start = 0
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

    async def render_lazy_message(self, message) -> None:
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
                    text = text.replace(f"[Record #{old_id}: ]", f"[Record #{media_id}: 可读取]")
                continue
            label = "Image" if kind == "Image" else "Sticker"
            placeholder = f"[{label} #{media_id}: 可读取]"
            patterns = [rf"\[{label} attached\]", rf"\[{label}\s*, file_path: [^\]]+\]",
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
        if history is None:
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
        index = int(entry.get("occurrence", 0))
        if index < len(matches):
            entry["elem"] = matches[index]
            return matches[index]
        return None

    async def _store_media(self, elem, sid: str, message_id: str):
        if store_session_media is None:
            raise RuntimeError("当前框架不支持原生媒体引用")
        return await store_session_media(elem, sid, message_id)

    async def read_media(self, event, media_ids) -> ToolResult:
        if isinstance(media_ids, str):
            ids = [x.strip() for x in media_ids.split(",") if x.strip()]
        elif isinstance(media_ids, (list, tuple)):
            ids = [str(x).strip() for x in media_ids if str(x).strip()]
        else:
            ids = []
        if not ids:
            return ToolResult(text="请提供至少一个媒体 ID")

        async def one(media_id):
            entry = await self._entry(media_id)
            if entry is None:
                return media_id, None, "媒体不存在或已过期"
            kind = entry["kind"]
            if not self._enabled_for(kind):
                return media_id, None, "该类媒体的按需读取未开启"
            elem = await self._resolve_media_element(entry)
            if elem is None:
                return media_id, None, "媒体源不可用，无法恢复"
            try:
                if self._is_visual(kind) and self.recognizer._native_mode(event.sid):
                    ref = await self._store_media(elem, event.sid, entry["message_id"])
                    return media_id, ref, "已返回原生图片"
                desc = await self.recognizer.read_access_entry(entry, event)
                if not desc:
                    return media_id, None, "识别失败或结果为空"
                return media_id, None, desc
            except Exception as exc:
                return media_id, None, f"读取失败：{type(exc).__name__}: {exc}"

        results = await asyncio.gather(*(one(mid) for mid in ids))
        lines, refs = [], []
        for media_id, ref, text in results:
            lines.append(f"[{media_id}] {text}")
            if ref is not None:
                refs.append(ref)
        return ToolResult(text="\n".join(lines), media_refs=refs)

    async def read_forward(self, event, forward_id: str, depth=1, cursor=None) -> ToolResult:
        if not self.forward_tool_enabled:
            return ToolResult(text="合并记录按需读取未开启")
        record = await self._forward(forward_id)
        if record is None:
            return ToolResult(text="合并记录不存在或目录已过期")
        # Persisted trees have no runtime children index; load referenced nodes lazily.
        async def load_tree(rec):
            for chain in rec.get("children", []):
                for elem in chain:
                    if elem.get("type") == "forward":
                        child = await self._forward(elem["forward_id"])
                        if child is not None:
                            await load_tree(child)
                    elif elem.get("type") == "reply" and elem.get("chain"):
                        for item in elem["chain"]:
                            if item.get("type") == "forward":
                                child = await self._forward(item["forward_id"])
                                if child is not None:
                                    await load_tree(child)
        await load_tree(record)
        text = self.render_forward(forward_id, depth=depth, cursor=cursor)
        refs = []
        # If media tools are off, expansion itself presents media in the current mode.
        ids = list(dict.fromkeys(re.findall(r"#([isr][0-9a-f]{12}): 可读取", text)))
        for media_id in ids:
            entry = await self._entry(media_id)
            if entry is None or self._enabled_for(entry["kind"]):
                continue
            elem = await self._resolve_media_element(entry)
            if elem is None:
                continue
            try:
                if self._is_visual(entry["kind"]) and self.recognizer._native_mode(event.sid):
                    refs.append(await self._store_media(elem, event.sid, entry["message_id"]))
                else:
                    desc = await self.recognizer.read_access_entry(entry, event)
                    if desc:
                        text = text.replace(f"#{media_id}: 可读取", f"#{media_id}: {desc}")
            except Exception:
                text = text.replace(f"#{media_id}: 可读取", f"#{media_id}: 读取失败")
        return ToolResult(text=text, media_refs=refs)

    async def shutdown(self) -> None:
        tasks = list(self._bg_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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
