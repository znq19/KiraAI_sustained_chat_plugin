"""Raw OneBot nested-forward hydration regression tests (S/Z shared).

These tests reproduce the 2026-11-28 field report: KiraAI 3.3/3.4 only builds
one level of Forward.chains, so ``read_forward(depth=2)`` returned
``还可下钻0层`` even though the original record contained inline/nested nodes.
"""
import asyncio
import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
STUB = HERE / "_stub"
sys.path.insert(0, str(STUB))

from core.chat import Group, KiraIMMessage, MessageChain, Session, User  # noqa: E402
from core.chat.message_elements import Forward, Image, Reply, Text  # noqa: E402
from core.chat.message_utils import KiraMessageBatchEvent  # noqa: E402

SID = "qq:gm:188395693"
results = []


def check(label, condition, detail=""):
    results.append((label, bool(condition)))
    print(("  PASS  " if condition else "  FAIL  ") + label
          + ((" — " + str(detail)) if (detail and not condition) else ""))


class FakeClient:
    def __init__(self, payloads, msg_payloads=None):
        self.payloads = payloads
        self.msg_payloads = msg_payloads or {}
        self.calls = []
        self.msg_calls = []

    async def get_forward_msg(self, mid):
        self.calls.append(str(mid))
        if str(mid) not in self.payloads:
            raise RuntimeError("missing forward " + str(mid))
        return {"retcode": 0, "data": {"messages": self.payloads[str(mid)]}}

    async def get_msg(self, mid):
        self.msg_calls.append(str(mid))
        if str(mid) not in self.msg_payloads:
            raise RuntimeError("missing msg " + str(mid))
        return {"retcode": 0, "data": self.msg_payloads[str(mid)]}


class FakeAdapter:
    def __init__(self, client):
        self.client = client

    def get_client(self):
        return self.client


class FakeAdapterMgr:
    def __init__(self, adapter):
        self.adapter = adapter

    def get_adapter(self, _name):
        return self.adapter


class FakeCtx:
    def __init__(self, client):
        self.adapter_mgr = FakeAdapterMgr(FakeAdapter(client))


class FakeRecognizer:
    def __init__(self):
        self.calls = []

    def _native_mode(self, sid=None):
        return False

    async def read_access_entry(self, entry, event):
        self.calls.append(entry["id"])
        return f"{entry['kind']}识别结果"

    async def _persist_media(self, elem):
        return None, ""


def config(**kw):
    media = {
        "media_tool_image_enabled": True,
        "media_tool_audio_enabled": True,
        "forward_tool_enabled": True,
        "forward_max_depth": 4,
    }
    media.update(kw)
    return {"section_media": media}


def message(chain, raw, mid="m1"):
    return KiraIMMessage(
        message_id=mid, self_id="bot", timestamp=1,
        sender=User("u", "周武"), group=Group("g", "群"),
        chain=chain, raw_message=raw, is_mentioned=True,
    )


def batch(messages):
    return KiraMessageBatchEvent(
        timestamp=1,
        session=Session(adapter_name="qq", session_type="gm", session_id="188395693"),
        messages=messages,
    )


def text_chain(text):
    return MessageChain([Text(text)])


def flat(sender, when, body, mid="x"):
    return {
        "sender": {"nickname": sender}, "time": when, "message_id": mid,
        "message": [{"type": "text", "data": {"text": body}}],
    }


def make_manager(module, client, **cfg):
    rec = FakeRecognizer()
    manager = module.MediaAccessManager(
        FakeCtx(client), rec, config(**cfg), "test-forward-raw")
    tmp = Path(tempfile.mkdtemp(prefix="kira-forward-raw-"))
    manager._entry_dir = tmp / "entries"
    manager._forward_dir = tmp / "forwards"
    return manager, rec


async def settle(manager):
    for _ in range(20):
        pending = [t for t in manager._hydrate_tasks.values() if not t.done()]
        if not pending:
            break
        await asyncio.gather(*pending, return_exceptions=True)
    if manager._bg_tasks:
        await asyncio.gather(*list(manager._bg_tasks), return_exceptions=True)


async def run(plugin_dir: Path):
    if str(plugin_dir) not in sys.path:
        sys.path.insert(0, str(plugin_dir))
    import media_access as module

    # ---------------- 1. inline data.content (the reported shape) ----------------
    outer = [
        flat("A", 1700000001, "第一层"),
        {"sender": {"nickname": "B"}, "time": 1700000002, "message_id": "m2",
         "message": [
             {"type": "text", "data": {"text": "看嵌套："}},
             {"type": "forward", "data": {"content": [
                 flat("C", 1700000003, "内联深层"),
                 {"sender": {"nickname": "D"}, "time": 1700000004,
                  "message": [{"type": "forward", "data": {"id": "NEST2"}}]},
             ]}},
         ]},
    ]
    client = FakeClient({"OUTER": outer, "NEST2": [flat("E", 1700000005, "id 深层")]})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[text_chain("[2026] A: 第一层"),
                           text_chain("[2026] B: 看嵌套：")])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "OUTER"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    root_id = root._media_access_forward_id
    text = (await manager.read_forward(batch([obj]), root_id, depth=-1)).text
    check("inline content：嵌套文本可见", "内联深层" in text, text)
    check("inline content：id 深层继续可见", "id 深层" in text, text)
    check("inline content：仍可下钻", "还可下钻0层" not in text.split("\n", 1)[0], text)
    check("inline content：外层条数不变", "本层2条" in text, text)
    check("inline content：只拉了外层与 NEST2 两次",
          client.calls == ["OUTER", "NEST2"], client.calls)
    await manager.shutdown()

    # ---------------- 2. nested data.id mapped to framework's empty Forward ----------------
    nested = Forward(chains=[])
    outer2 = Forward([
        text_chain("[2026] A: 第一层"),
        MessageChain([Text("[2026] B: "), nested]),
    ])
    client = FakeClient({"OUTER2": [
        flat("A", 1700000001, "第一层"),
        {"sender": {"nickname": "B"}, "time": 1700000002, "message_id": "m2",
         "message": [{"type": "forward", "data": {"id": "NEST-ID"}}]},
    ], "NEST-ID": [flat("N", 1700000003, "id 嵌套内容")]})
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([outer2]), {"message": [
        {"type": "forward", "data": {"id": "OUTER2"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), outer2._media_access_forward_id, depth=-1)).text
    check("data.id：框架空 Forward 被补全", "id 嵌套内容" in text, text)
    check("data.id：没有重复追加 forward 元素",
          sum(1 for e in manager._forwards[outer2._media_access_forward_id]["children"][1]
              if e.get("type") == "forward") == 1)
    await manager.shutdown()

    # ---------------- 2b. partially resolved framework tree keeps drilling ----------------
    n2 = Forward(chains=[text_chain("[2026] C: 只到第二层")])
    n1 = Forward(chains=[MessageChain([Text("[2026] B: "), n2])])
    root_partial = Forward(chains=[MessageChain([Text("[2026] A: "), n1])])
    client = FakeClient({
        "P-ROOT": [{"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "text", "data": {"text": "第一层"}},
            {"type": "forward", "data": {"id": "P-N1"}},
        ]}],
        "P-N1": [{"sender": {"nickname": "B"}, "time": 1700000002, "message": [
            {"type": "forward", "data": {"id": "P-N2"}}]}],
        "P-N2": [{"sender": {"nickname": "C"}, "time": 1700000003, "message": [
            {"type": "forward", "data": {"id": "P-N3"}}]}],
        "P-N3": [flat("D", 1700000004, "最深层正文")],
    })
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([root_partial]),
                  {"message": [{"type": "forward", "data": {"id": "P-ROOT"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), root_partial._media_access_forward_id, depth=-1)).text
    check("框架只解析到第二层：继续补到最深层", "最深层正文" in text, text)
    await manager.shutdown()

    # ---------------- 3. NapCat node-wrapped outer (framework drops it entirely) ----------------
    napcat_outer = [
        {"type": "node", "data": {"user_id": 1, "nickname": "A",
                                  "message": [{"type": "text", "data": {"text": "NapCat 外层"}}],
                                  "content": []}},
        {"type": "node", "data": {"user_id": 2, "nickname": "B", "message": [
            {"type": "node", "data": {"user_id": 3, "nickname": "C", "message": [
                {"type": "text", "data": {"text": "NapCat 嵌套"}}], "content": []}},
            {"type": "text", "data": {"text": "同条文字"}},
        ], "content": []}},
    ]
    client = FakeClient({"WRAP": napcat_outer})
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([Text("前缀")]), {"message": [
        {"type": "forward", "data": {"id": "WRAP"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    fids = list(manager._message_forwards.get(id(obj)) or [])
    root_fid = next((getattr(x, "_media_forward_id", None) for x in obj.chain
                     if isinstance(x, Text) and getattr(x, "_media_forward_id", None)), None)
    check("NapCat 包裹：框架丢弃后仍生成 Forward ID", bool(root_fid) and root_fid in fids, fids)
    check("NapCat 包裹：消息链出现预览", any(isinstance(x, Text) and getattr(x, "_media_forward_id", None)
                                             for x in obj.chain))
    preview = next((x.text for x in obj.chain
                    if isinstance(x, Text) and getattr(x, "_media_forward_id", None) == root_fid), "")
    check("NapCat 包裹：补全后预览更新为真实条数", "外层2条" in preview, preview)
    text = (await manager.read_forward(batch([obj]), root_fid, depth=-1)).text if root_fid else ""
    check("NapCat 包裹：外层内容可读", "NapCat 外层" in text, text)
    check("NapCat 包裹：嵌套 node 成为子 Forward", "NapCat 嵌套" in text and "还可下钻" in text, text)
    await manager.shutdown()

    # ---------------- 3b. LLOneBot get_forward_msg uses `content` ----------------
    client = FakeClient({
        "LL-OUT": [{
            "sender": {"nickname": "A", "user_id": 1}, "time": 1700000001,
            "content": [
                {"type": "text", "data": {"text": "LLOneBot 外层"}},
                {"type": "forward", "data": {"id": "LL-NEST"}},
            ],
        }],
        "LL-NEST": [{
            "sender": {"nickname": "B", "user_id": 2}, "time": 1700000002,
            "content": [{"type": "text", "data": {"text": "LLOneBot 深层"}}],
        }],
    })
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([Text("前缀")]), {"message": [
        {"type": "forward", "data": {"id": "LL-OUT"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    root_fid = next((getattr(x, "_media_forward_id", None) for x in obj.chain
                     if isinstance(x, Text) and getattr(x, "_media_forward_id", None)), None)
    text = (await manager.read_forward(batch([obj]), root_fid, depth=-1)).text if root_fid else ""
    check("LLOneBot content：外层可读", "LLOneBot 外层" in text, text)
    check("LLOneBot content：嵌套可读", "LLOneBot 深层" in text, text)
    await manager.shutdown()

    # ---------------- 3c. SnowLuma XML card: try m_fileName if m_resid fails ----------------
    card_xml = ('<msg serviceID="35" action="viewMultiMsg" m_resid="BAD-RES" '
                'm_fileName="GOOD-RES">聊天记录</msg>')
    client = FakeClient({
        "CARD-OUT": [{"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "xml", "data": {"data": card_xml}}]}],
        "GOOD-RES": [flat("B", 1700000002, "SnowLuma 卡片深层")],
    })
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([Text("前缀")]), {"message": [
        {"type": "forward", "data": {"id": "CARD-OUT"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    root_fid = next((getattr(x, "_media_forward_id", None) for x in obj.chain
                     if isinstance(x, Text) and getattr(x, "_media_forward_id", None)), None)
    card_text = (await manager.read_forward(batch([obj]), root_fid, depth=-1)).text
    check("SnowLuma XML：m_resid 失败后回退 m_fileName",
          "SnowLuma 卡片深层" in card_text, card_text)
    await manager.shutdown()

    # ---------------- 3d. LightApp / JSON multimsg card ----------------
    card_json = json.dumps({
        "app": "com.tencent.multimsg",
        "meta": {"detail": {"resid": "JSON-RES", "uniseq": "u1"}},
    }, ensure_ascii=False)
    client = FakeClient({
        "JSON-OUT": [{"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "json", "data": {"data": card_json}}]}],
        "JSON-RES": [flat("B", 1700000002, "JSON 卡片深层")],
    })
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([Text("前缀")]), {"message": [
        {"type": "forward", "data": {"id": "JSON-OUT"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    root_fid = next((getattr(x, "_media_forward_id", None) for x in obj.chain
                     if isinstance(x, Text) and getattr(x, "_media_forward_id", None)), None)
    json_text = (await manager.read_forward(batch([obj]), root_fid, depth=-1)).text
    check("LightApp JSON 卡片：按 meta.detail.resid 展开",
          "JSON 卡片深层" in json_text, json_text)
    await manager.shutdown()

    # ---------------- 4. cycle is bounded ----------------
    client = FakeClient({"CYCLE": [
        {"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "forward", "data": {"id": "CYCLE"}}]},
    ]})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "CYCLE"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), root._media_access_forward_id, depth=-1)).text
    check("循环引用：有界且提示原因", "循环引用" in text, text)
    await manager.shutdown()

    # ---------------- 5. depth cap ----------------
    client = FakeClient({
        "D1": [{"sender": {"nickname": "A"}, "time": 1, "message": [
            {"type": "forward", "data": {"id": "D2"}}]}],
        "D2": [{"sender": {"nickname": "B"}, "time": 2, "message": [
            {"type": "text", "data": {"text": "超过上限"}}]}],
    })
    manager, _ = make_manager(module, client, forward_max_depth=1)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "D1"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), root._media_access_forward_id, depth=-1)).text
    check("最大深度：第 3 层不再展开", "已达最大查看深度" in text and "超过上限" not in text, text)
    check("最大深度：没有拉取越界记录", "D2" not in client.calls, client.calls)
    await manager.shutdown()

    # ---------------- 6. API failure keeps the framework tree ----------------
    class BrokenClient(FakeClient):
        async def get_forward_msg(self, mid):
            self.calls.append(str(mid))
            raise RuntimeError("boom")

    client = BrokenClient({})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[text_chain("[2026] A: 原本可见")])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "BROKEN"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    record = manager._forwards.get(root._media_access_forward_id) or {}
    text = (await manager.read_forward(
        batch([obj]), root._media_access_forward_id, depth=1)).text
    check("补全失败：原树不丢", "原本可见" in text, text)
    check("补全失败：记录错误但不崩", bool(record.get("hydration_error")), record)
    await manager.shutdown()

    # ---------------- 7. nested media is registered + readable ----------------
    client = FakeClient({"MEDIA": [
        {"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "forward", "data": {"content": [
                {"sender": {"nickname": "B"}, "time": 1700000002, "message": [
                    {"type": "image", "data": {"url": "https://example.test/a.png"}}]},
            ]}}]},
    ]})
    manager, rec = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "MEDIA"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), root._media_access_forward_id, depth=-1)).text
    check("嵌套图片：生成可读取媒体 ID", "Image #" in text, text)
    import re
    match = re.search(r"#([isr][0-9a-f]{8,64}): 可读取", text)
    if match:
        read = await manager.read_media(batch([obj]), [match.group(1)])
        check("嵌套图片：read_media 可读", "Image识别结果" in read.text, read.text)
    else:
        check("嵌套图片：read_media 可读", False, text)
    await manager.shutdown()

    # ---------------- 7b. flat text-only tree still persists ----------------
    client = FakeClient({"FLAT": [flat("A", 1700000001, "扁平正文")]})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "FLAT"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    rid = root._media_access_forward_id
    restored = module.MediaAccessManager(None, FakeRecognizer(), config(), "test-forward-raw")
    restored._entry_dir = manager._entry_dir
    restored._forward_dir = manager._forward_dir
    flat_text = (await restored.read_forward(batch([obj]), rid, depth=-1)).text
    check("纯文本补全也落盘", "扁平正文" in flat_text, flat_text)
    await restored.shutdown()
    await manager.shutdown()

    # ---------------- 8. persisted tree survives restart ----------------
    client = FakeClient({"PERSIST": [
        {"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "forward", "data": {"content": [
                {"sender": {"nickname": "B"}, "time": 1700000002,
                 "message": [{"type": "text", "data": {"text": "持久化深层"}}]},
            ]}}]},
    ]})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "PERSIST"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    rid = root._media_access_forward_id
    restored = module.MediaAccessManager(None, FakeRecognizer(), config(), "test-forward-raw")
    restored._entry_dir = manager._entry_dir
    restored._forward_dir = manager._forward_dir
    restored_text = (await restored.read_forward(batch([obj]), rid, depth=-1)).text
    check("重启后深层树可恢复", "持久化深层" in restored_text, restored_text)
    await restored.shutdown()
    await manager.shutdown()

    # ---------------- 8a. legacy persisted record without source_id ----------------
    legacy_raw = {"message_id": "legacy-msg", "message_type": "group", "group_id": 1,
                  "sender": {"nickname": "L"}, "time": 1700000000,
                  "message": [{"type": "text", "data": {"text": "前面的文字"}},
                              {"type": "forward", "data": {"id": "LEGACY"}}]}
    legacy_payload = [{
        "sender": {"nickname": "A"}, "time": 1700000001,
        "message": [{"type": "forward", "data": {"content": [{
            "sender": {"nickname": "B"}, "time": 1700000002,
            "message": [{"type": "text", "data": {"text": "旧记录恢复深层"}}],
        }]}}],
    }]
    client = FakeClient({"LEGACY": legacy_payload},
                        msg_payloads={"legacy-msg": legacy_raw})
    manager, _ = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([Text("前面的文字"), root]), legacy_raw, mid="legacy-msg")
    manager.prepare_message(obj, SID)
    await settle(manager)
    rid = root._media_access_forward_id
    # 模拟升级前落盘：没有 source_id / hydrated，只剩外层浅树。
    record = manager._forwards[rid]
    record["source_id"] = ""
    record["hydrated"] = False
    record["truncated"] = ""
    record["children"] = record["children"][:1]
    restored = module.MediaAccessManager(FakeCtx(client), FakeRecognizer(), config(),
                                         "test-forward-raw")
    restored._entry_dir = manager._entry_dir
    restored._forward_dir = manager._forward_dir
    restored._forwards[rid] = record
    text = (await restored.read_forward(batch([obj]), rid, depth=-1)).text
    check("旧记录无 source_id：按 get_msg 恢复", "旧记录恢复深层" in text, text)
    check("旧记录恢复调用 get_msg", "legacy-msg" in client.msg_calls, client.msg_calls)
    await restored.shutdown()
    await manager.shutdown()

    # ---------------- 8b. XML viewMultiMsg card with m_resid ----------------
    card_xml = ('<msg serviceID="35" action="viewMultiMsg" m_resid="CARD-RES" '
                'm_fileName="card"><item><title>聊天记录</title></item></msg>')
    client = FakeClient({
        "CARD": [{"sender": {"nickname": "A"}, "time": 1700000001, "message": [
            {"type": "xml", "data": {"data": card_xml}},
        ]}],
        "CARD-RES": [flat("C", 1700000003, "按 m_resid 取回的深层")],
    })
    manager, _ = make_manager(module, client)
    root = Forward(chains=[])
    obj = message(MessageChain([root]), {"message": [
        {"type": "forward", "data": {"id": "CARD"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), root._media_access_forward_id, depth=-1)).text
    check("XML 卡片：按 m_resid 展开", "按 m_resid 取回的深层" in text, text)
    await manager.shutdown()

    # ---------------- 8c. top-level XML viewMultiMsg card (framework drops it) ----------------
    client = FakeClient({"TOP-CARD": [flat("T", 1700000001, "顶层卡片深层")]})
    manager, _ = make_manager(module, client)
    obj = message(MessageChain([Text("前缀")]), {"message": [
        {"type": "xml", "data": {"data": '<msg action="viewMultiMsg" m_resid="TOP-CARD">聊天记录</msg>'}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    root_fid = next((getattr(x, "_media_forward_id", None) for x in obj.chain
                     if isinstance(x, Text) and getattr(x, "_media_forward_id", None)), None)
    text = (await manager.read_forward(batch([obj]), root_fid, depth=-1)).text if root_fid else ""
    check("顶层 XML 卡片：框架丢弃后仍可读取", "顶层卡片深层" in text, text)
    await manager.shutdown()

    # ---------------- 9. quoted forward hydration via get_msg ----------------
    quoted = {"sender": {"nickname": "Q"}, "time": 1700000001, "message": [
        {"type": "forward", "data": {"id": "QUOTED"}}]}
    client = FakeClient({"QUOTED": [flat("Z", 1700000002, "引用里的深层")]},
                        msg_payloads={"reply-1": quoted})
    manager, _ = make_manager(module, client)
    reply_forward = Forward(chains=[])
    try:
        reply = Reply("reply-1", chain=MessageChain([reply_forward]))
    except TypeError:  # 测试桩的 Reply 只接受 chain
        reply = Reply(chain=MessageChain([reply_forward]))
        reply.message_id = "reply-1"
    obj = message(MessageChain([reply]), {"message": [
        {"type": "reply", "data": {"id": "reply-1"}}]})
    manager.prepare_message(obj, SID)
    await settle(manager)
    text = (await manager.read_forward(
        batch([obj]), reply_forward._media_access_forward_id, depth=-1)).text
    check("引用转发：get_msg 补全", "引用里的深层" in text, text)
    check("引用转发：调用 get_msg", client.msg_calls == ["reply-1"], client.msg_calls)
    await manager.shutdown()

    return all(ok for _label, ok in results)


async def main():
    dirs = [Path(d).resolve() for d in (sys.argv[1:] or [".."])]
    ok = True
    for directory in dirs:
        print(f"=== {directory.name} ===")
        ok = await run(directory) and ok
    print("ALL PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
