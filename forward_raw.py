"""OneBot raw merged-forward payload helpers (shared by S/Z chat plugins).

KiraAI builds ``Forward.chains`` from ``get_forward_msg`` results, but its
adapter only understands a flat ``{sender, time, message}`` shape and uses the
*inner message id* for a nested ``forward`` segment.  Real protocol
implementations use different shapes:

* ``forward`` segment with inline ``data.content`` (NapCat / SnowLuma / LLOneBot)
* ``forward`` segment with ``data.id`` / ``data.res_id`` / ``data.forward_id``
* ``{type: "node", data: {...}}`` wrappers (NapCat's get_forward_msg)
* ``node`` segments whose ``data.content`` / ``data.message`` / ``data.segments`` is a nested node list
* ``xml`` cards (``viewMultiMsg``) carrying ``m_resid`` / ``m_fileName``

This module only normalizes those payloads.  It performs no I/O and has no
dependency on the plugin classes, so it can be unit-tested in isolation.
"""
from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass, field


@dataclass
class RawForward:
    """One nested forward reference found in a raw OneBot segment list."""

    kind: str = "forward"          # forward | node | xml
    id: str = ""
    content: list | None = None    # inline node list for forward.data.content
    data: dict = field(default_factory=dict)
    index: int = -1


def first_str(*values) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def as_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    return []


def normalize_content(value) -> list | None:
    """Return an inline node list, accepting list / dict / JSON string forms."""
    if isinstance(value, list):
        return value or None
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if not text or text[0] not in "[{":
            return None
        try:
            decoded = json.loads(text)
        except (TypeError, ValueError):
            return None
        if isinstance(decoded, list):
            return decoded or None
        if isinstance(decoded, dict):
            if isinstance(decoded.get("messages"), list):
                return decoded["messages"] or None
            return [decoded]
    return None


def is_node_wrapper(value) -> bool:
    return isinstance(value, dict) and value.get("type") == "node" and isinstance(value.get("data"), dict)


def _unwrap_item(item) -> dict:
    """Return the message/node dict, unwrapping ``{type: node, data: ...}``."""
    if is_node_wrapper(item):
        return item.get("data") or {}
    return item if isinstance(item, dict) else {}


def node_meta(item) -> dict:
    data = _unwrap_item(item)
    sender = data.get("sender") if isinstance(data.get("sender"), dict) else {}
    name = first_str(
        sender.get("nickname"), sender.get("card"), sender.get("name"),
        sender.get("sender_name"),
        data.get("nickname"), data.get("name"), data.get("card"),
        data.get("sender_name"), data.get("user_id"), data.get("uin"),
    )
    uid = first_str(
        sender.get("user_id"), sender.get("uin"),
        data.get("user_id"), data.get("uin"),
    )
    time_value = data.get("time", data.get("timestamp"))
    return {"name": name or "未知", "uid": uid, "time": time_value}


def node_segments(item) -> list:
    """Segments of a message node (``message`` / ``content`` / ``segments``)."""
    data = _unwrap_item(item)
    for key in ("message", "content", "segments"):
        value = data.get(key)
        if isinstance(value, (list, dict)):
            return as_list(value)
    return []


def segment_node_parts(data: dict) -> tuple[str, list]:
    """Classify a raw ``node`` segment payload.

    Returns ``("nodes", list)`` when the payload is a nested node list,
    ``("segments", list)`` when it is one node's message body, else
    ``("empty", [])``.  NapCat puts nested nodes in ``message``; SnowLuma uses
    ``content``; some implementations send both (one may be empty).
    """
    if not isinstance(data, dict):
        return "empty", []
    message = as_list(data.get("message"))
    content = as_list(data.get("content"))
    segments = as_list(data.get("segments"))
    for value in (message, content, segments):
        if value and all(is_node_wrapper(x) for x in value):
            return "nodes", value
    for value in (message, content, segments):
        if value:
            return "segments", value
    return "empty", []


def extract_forward_descriptors(raw_message) -> list[RawForward]:
    """Top-level ``forward`` / forward-XML-card segments of an OneBot message dict."""
    if not isinstance(raw_message, dict):
        return []
    segments = raw_message.get("message")
    if isinstance(segments, str):
        return []
    result = []
    for index, seg in enumerate(as_list(segments)):
        if not isinstance(seg, dict):
            continue
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
        if seg.get("type") == "forward":
            result.append(RawForward(
                kind="forward",
                id=first_str(data.get("id"), data.get("res_id"), data.get("forward_id"), data.get("resid")),
                content=normalize_content(data.get("content") or data.get("messages")),
                data=data,
                index=index,
            ))
        elif seg.get("type") == "xml" and is_forward_card(data):
            result.append(RawForward(
                kind="xml", id=xml_res_id(data), data=data, index=index))
        elif seg.get("type") == "json" and is_json_forward_card(data):
            result.append(RawForward(
                kind="json", id=json_forward_id(data), data=data, index=index))
    return result


def collect_nested(segments) -> list[RawForward]:
    """Nested forward/node/xml-card descriptors inside one message's segments."""
    result = []
    for index, seg in enumerate(as_list(segments)):
        if not isinstance(seg, dict):
            continue
        kind = seg.get("type")
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
        if kind == "forward":
            result.append(RawForward(
                kind="forward",
                id=first_str(data.get("id"), data.get("res_id"), data.get("forward_id"), data.get("resid")),
                content=normalize_content(data.get("content") or data.get("messages")),
                data=data,
                index=index,
            ))
        elif kind == "node":
            result.append(RawForward(kind="node", id=first_str(data.get("id")), data=data, index=index))
        elif kind == "xml" and is_forward_card(data):
            result.append(RawForward(kind="xml", id=xml_res_id(data), data=data, index=index))
        elif kind == "json" and is_json_forward_card(data):
            result.append(RawForward(kind="json", id=json_forward_id(data), data=data, index=index))
    return result


def is_forward_card(data: dict) -> bool:
    xml = xml_text(data)
    return bool(xml) and ("viewmultimsg" in xml.lower() or "聊天记录" in xml)


def _json_card(data: dict):
    if not isinstance(data, dict):
        return None
    raw = data.get("data", data.get("content", data.get("json", "")))
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def is_json_forward_card(data: dict) -> bool:
    card = _json_card(data)
    if not isinstance(card, dict):
        return False
    app = str(card.get("app", "")).lower()
    meta = card.get("meta") if isinstance(card.get("meta"), dict) else {}
    detail = meta.get("detail") if isinstance(meta.get("detail"), dict) else {}
    return "multimsg" in app or bool(detail.get("resid") or detail.get("residStr"))


def json_forward_id(data: dict) -> str:
    card = _json_card(data)
    if not isinstance(card, dict):
        return ""
    meta = card.get("meta") if isinstance(card.get("meta"), dict) else {}
    detail = meta.get("detail") if isinstance(meta.get("detail"), dict) else {}
    return first_str(detail.get("resid"), detail.get("residStr"),
                     detail.get("uniseq"), card.get("resid"))


def xml_text(data: dict) -> str:
    if not isinstance(data, dict):
        return ""
    value = data.get("data", data.get("xml", data.get("content", "")))
    if not isinstance(value, str):
        return ""
    # QQ/SnowLuma may hand back entity-escaped or JSON-escaped XML.
    return html.unescape(value).replace('\\"', '"')


_XML_RESID_RE = re.compile(r"""m_resid\s*=\s*["']([^"']+)["']""", re.I)
_XML_FILENAME_RE = re.compile(r"""m_fileName\s*=\s*["']([^"']+)["']""", re.I)
_XML_BRIEF_RE = re.compile(r"""brief\s*=\s*["']([^"']*)["']""", re.I)
_XML_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def xml_res_id(data: dict) -> str:
    xml = xml_text(data)
    if not xml:
        return ""
    match = _XML_RESID_RE.search(xml) or _XML_FILENAME_RE.search(xml)
    return first_str(match.group(1) if match else "")


def card_ids(data: dict) -> list:
    """All plausible record IDs carried by XML/LightApp forward cards.

    SnowLuma sometimes leaves m_resid empty while m_fileName carries the key,
    and vice versa; try both instead of committing to one.
    """
    ids = []
    xml = xml_text(data)
    if xml:
        for pattern in (_XML_RESID_RE, _XML_FILENAME_RE):
            for match in pattern.finditer(xml):
                value = first_str(match.group(1))
                if value and value not in ids:
                    ids.append(value)
    value = json_forward_id(data)
    if value and value not in ids:
        ids.append(value)
    return ids


def xml_brief(data: dict) -> str:
    xml = xml_text(data)
    if not xml:
        return ""
    match = _XML_BRIEF_RE.search(xml) or _XML_TITLE_RE.search(xml)
    text = match.group(1) if match else ""
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def message_payload(payload) -> list:
    """Normalize a get_forward_msg / get_msg response into a message list."""
    if isinstance(payload, dict):
        retcode = payload.get("retcode")
        if retcode not in (None, 0, "0"):
            raise ValueError(f"get_forward_msg failed: retcode={retcode} {payload.get('message', '')}")
        status = str(payload.get("status", "")).lower()
        if status and status not in ("ok", "success", "async"):
            raise ValueError(f"get_forward_msg failed: status={status} {payload.get('message', '')}")
        data = payload.get("data", payload)
    else:
        data = payload
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("messages"), list):
            return data["messages"]
        if isinstance(data.get("message"), list) and ("sender" in data or "user_id" in data):
            return [data]
        if isinstance(data.get("message"), list) and "message_type" in data:
            return [data]
        if data.get("type") == "node" and isinstance(data.get("data"), dict):
            return [data]
        if "message" in data or "content" in data:
            return [data]
    return []


def message_object(payload) -> dict:
    """Extract a single message dict from a get_msg response."""
    messages = message_payload(payload)
    if not messages:
        return {}
    first = messages[0]
    return _unwrap_item(first)
