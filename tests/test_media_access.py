"""On-demand media and nested Forward catalog tests (offline stubs)."""
import asyncio
import sys
import tempfile
from pathlib import Path

from test_debounce_arrival_race import SID
from core.chat import Group, KiraIMMessage, MessageChain, Session, User
from core.chat.message_elements import Forward, Image, Record, Sticker, Text
from core.chat.message_utils import KiraMessageBatchEvent


class Recognizer:
    def __init__(self):
        self.native = False
        self.calls = []

    def _native_mode(self, sid=None):
        return self.native

    async def read_access_entry(self, entry, event):
        self.calls.append(entry["id"])
        return f"{entry['kind']}识别结果"

    async def _persist_media(self, elem):
        return getattr(self, "persist_path", None), "md5"


def media_config(**kw):
    cfg = {
        "media_tool_image_enabled": True,
        "media_tool_audio_enabled": True,
        "forward_tool_enabled": True,
        "media_tool_auto_present_private_enabled": True,
        "media_tool_auto_present_group_enabled": True,
        "media_tool_auto_present_max_count": 1,
        "forward_max_depth": 4,
    }
    cfg.update(kw)
    return {"section_media": cfg}


def message(mid, chain, group=True, mentioned=False):
    return KiraIMMessage(
        message_id=str(mid), self_id="bot", timestamp=1,
        sender=User("u", "周武"), group=Group("g", "群") if group else None,
        chain=chain, is_mentioned=mentioned,
    )


def event(messages, group=True):
    return KiraMessageBatchEvent(
        timestamp=1,
        session=Session(adapter_name="qq", session_type="gm" if group else "dm",
                        session_id="g" if group else "u"),
        messages=messages,
    )


def chain(text):
    return MessageChain([Text(text)])


async def run(plugin_dir: Path):
    if str(plugin_dir) not in sys.path:
        sys.path.insert(0, str(plugin_dir))
    import media_access as module
    results = []
    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if not condition else ""))

    rec = Recognizer()
    manager = module.MediaAccessManager(None, rec, media_config(), "test-media-access")
    temp = Path(tempfile.mkdtemp(prefix="kira-media-access-"))
    rec.persist_path = str(temp / "nested-media.bin")
    Path(rec.persist_path).write_bytes(b"media")
    manager._entry_dir = temp / "entries"
    manager._forward_dir = temp / "forwards"

    # Four outer nodes: image, nested record, text, nested record.
    deepest = Forward([chain("[2026-01-01 00:00:00] 周武: 晚安"),
                       MessageChain([Text("[2026-01-01 00:00:01] 周武: "), Image("base64://aW1n")])])
    nested = Forward([
        chain("[2026-01-01 00:00:00] 周武: 在吗"),
        MessageChain([Text("[2026-01-01 00:00:01] 周武: "), Sticker(sticker="base64://c3Rp")]),
        MessageChain([Text("[2026-01-01 00:00:02] 周武: "), deepest]),
    ])
    other = Forward([chain("[2026-01-01 00:00:03] 周武: 明天见")])
    root = Forward([
        MessageChain([Text("[2026-01-01 00:00:00] 周武: "), Image("base64://aW1n")]),
        MessageChain([Text("[2026-01-01 00:00:01] 周武: "), nested]),
        chain("[2026-01-01 00:00:02] 周武: 我爱你"),
        MessageChain([Text("[2026-01-01 00:00:03] 周武: "), other]),
    ])
    message_obj = message("root", MessageChain([root]))
    manager.prepare_message(message_obj, SID)
    root_id = root._media_access_forward_id
    summary = manager.render_forward(root_id, depth=1, root_summary=True)
    check("外层逐节点显示发送者、Image与文本", "周武" in summary and "Image #" in summary
          and "我爱你" in summary and "Forward #" in summary)
    check("外层摘要不使用中文媒体类型名", "图片 #" not in summary and "语音 #" not in summary)

    nested_id = nested._media_access_forward_id
    deep_id = deepest._media_access_forward_id
    expanded = manager.render_forward(nested_id, depth=2)
    check("嵌套块也显示直接子节点预览", "在吗" in expanded and "晚安" in expanded
          and deep_id in expanded)
    check("嵌套预览保留Sticker类型和ID", "Sticker #" in expanded, expanded)

    all_text = manager.render_forward(root_id, depth=-1)
    check("depth=-1展开允许范围内整棵树", "明天见" in all_text and "晚安" in all_text)
    check("最大深度默认4", manager.forward_max_depth == 4)

    overdeep = Forward([chain("[2026-01-01 00:00:00] 周武: L1")])
    cursor_forward = overdeep
    for level in range(2, 6):
        child = Forward([chain(f"[2026-01-01 00:00:0{level}] 周武: L{level}")])
        cursor_forward.chains.append(MessageChain([child]))
        cursor_forward = child
    overdeep_message = message("overdeep", MessageChain([overdeep]))
    manager.prepare_message(overdeep_message, SID)
    overdeep_text = manager.render_forward(overdeep._media_access_forward_id, depth=-1)
    check("超过4层时明确标记最大查看深度", "已达最大查看深度" in overdeep_text,
          overdeep_text)

    cycle = Forward([])
    cycle.chains.append(MessageChain([cycle]))
    cycle_message = message("cycle", MessageChain([cycle]))
    manager.prepare_message(cycle_message, SID)
    cycle_text = manager.render_forward(cycle._media_access_forward_id, depth=-1)
    check("循环Forward不崩溃且有界", "Forward" in cycle_text)

    long_root = Forward([chain("[2026-01-01 00:00:00] 周武: " + "长" * 7000)])
    long_message = message("long", MessageChain([long_root]))
    manager.prepare_message(long_message, SID)
    long_id = long_root._media_access_forward_id
    long_summary = manager.render_forward(long_id, root_summary=True)
    full_page = manager.render_forward(long_id, depth=1)
    check("长文本首轮有界预览", "…" in long_summary and len(long_summary) < 500)
    check("工具可分页读全文且不静默截断", "cursor=" in full_page)
    cursor = full_page.rsplit("cursor=", 1)[1].rstrip("]")
    rest = manager.render_forward(long_id, depth=1, cursor=cursor)
    check("下一页cursor可用", rest and "cursor=" not in rest)

    # Persistence: a fresh manager must load the same tree lazily.
    if manager._bg_tasks:
        await asyncio.gather(*manager._bg_tasks, return_exceptions=True)
    restored = module.MediaAccessManager(None, rec, media_config(), "test-media-access")
    restored._entry_dir = manager._entry_dir
    restored._forward_dir = manager._forward_dir
    restored_result = await restored.read_forward(event([]), root_id, depth=-1)
    check("重启后轻量目录可恢复", "我爱你" in restored_result.text and "晚安" in restored_result.text)
    deep_media_id = next(
        elem["media_id"] for ch in restored._forwards[deep_id]["children"]
        for elem in ch if elem.get("type") == "media")
    restored_media = await restored.read_media(event([]), [deep_media_id])
    check("重启后嵌套媒体仍可读取", "Image识别结果" in restored_media.text,
          restored_media.text)
    await restored.shutdown()

    # One image + one voice: each category is below N=1, both direct.
    mixed = message("mixed", MessageChain([
        Image("base64://aW1n"), Record("base64://dm9p")]), group=True)
    manager.prepare_message(mixed, SID)
    direct, lazy = manager.apply_batch_policy(event([mixed]))
    check("1图+1语音分别计数并直接呈现", len(direct) == 2 and not lazy)

    two_images = message("two", MessageChain([
        Image("base64://aW1n"), Image("base64://aW1n")]), group=True)
    manager.prepare_message(two_images, SID)
    direct, lazy = manager.apply_batch_policy(event([two_images]))
    check("群聊两张图超过N时按需读取", not direct and len(lazy) == 2)

    awakened = message("wake", MessageChain([
        Image("base64://aW1n"), Image("base64://aW1n")]), group=True, mentioned=True)
    manager.prepare_message(awakened, SID)
    direct, lazy = manager.apply_batch_policy(event([awakened]))
    check("唤醒消息不受小批次阈值限制", len(direct) == 2 and not lazy)

    private_off = module.MediaAccessManager(None, rec, media_config(
        media_tool_auto_present_private_enabled=False), "test-private-off")
    single = message("private", MessageChain([Image("base64://aW1n")]), group=False)
    private_off.prepare_message(single, "qq:dm:u")
    direct, lazy = private_off.apply_batch_policy(event([single], group=False))
    check("私聊开关可独立关闭自动呈现", not direct and len(lazy) == 1)

    # Unified media tool returns real results and native refs without duplicate calls.
    calls_before = len(rec.calls)
    read_result = await manager.read_media(event([mixed]), list(manager._message_entries[id(mixed)]))
    check("统一read_media批量返回真实识别结果", "Image识别结果" in read_result.text
          and "Record识别结果" in read_result.text)
    check("混合媒体一次调用", len(rec.calls) == calls_before + 2)

    rec.native = True
    async def fake_store(elem, sid, mid):
        return {"type": "kira_image_ref", "path": "session_media/x", "mime_type": "image/png"}
    module.store_session_media = fake_store
    native_result = await manager.read_media(event([mixed]), [manager._message_entries[id(mixed)][0]])
    check("原生工具返回media_refs真图", len(native_result.media_refs) == 1)

    await manager.shutdown()
    await private_off.shutdown()
    return all(results)


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
