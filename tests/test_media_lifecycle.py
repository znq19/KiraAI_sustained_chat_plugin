"""Short-ID collision, precise errors, recovery and lifecycle regression tests."""
import asyncio
import base64
import gc
import hashlib
import json
import sys
import tempfile
import weakref
from pathlib import Path

from test_media_access import event, media_config, message
from core.chat import MessageChain
from core.chat.message_elements import Forward, Image, Text


class Recognizer:
    def __init__(self):
        self.calls = []
        self.persist_path = None
        self.native = False

    def _native_mode(self, sid=None):
        return self.native

    async def read_access_entry(self, entry, evt):
        self.calls.append(entry["id"])
        return "识别成功"

    async def _persist_media(self, elem):
        return self.persist_path, "md5"


async def drain(manager):
    for _ in range(4):
        tasks = list(manager._bg_tasks)
        if not tasks:
            break
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)


def make_manager(module, tmp, rec=None, ctx=None, plugin_id="lifecycle"):
    manager = module.MediaAccessManager(ctx, rec or Recognizer(), media_config(), plugin_id)
    manager._entry_dir = tmp / "entries"
    manager._forward_dir = tmp / "forwards"
    return manager


def legacy_id(sid, mid, kind="Image", occurrence=0):
    digest = hashlib.sha256("\x1f".join(map(str, (sid, mid, kind, occurrence))).encode()).hexdigest()
    return "i" + digest[:12], digest


async def run(plugin_dir: Path):
    if str(plugin_dir) not in sys.path:
        sys.path.insert(0, str(plugin_dir))
    import media_access as module
    results = []

    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if detail else ""))

    tmp = Path(tempfile.mkdtemp(prefix="kira-media-lifecycle-"))
    sid = "qq:gm:188395693"
    evt = event([], group=True)
    evt.session.session_id = "188395693"

    # The screenshot ID is a valid legacy 12-hex ID, not a foreign "id" prefix.
    old_id, full_digest = legacy_id(sid, "103653866")
    check("截图GIF旧ID可由当前算法精确复算", old_id == "id390a8720aa8", old_id)
    cached = tmp / "legacy.gif"
    cached.write_bytes(b"gif")
    manager = make_manager(module, tmp)
    old_payload = {
        "id": old_id, "kind": "Image", "sid": sid, "digest": full_digest,
        "message_id": "103653866", "occurrence": 0, "history_occurrence": 0,
        "nested": False, "cache_path": str(cached),
    }
    manager._entry_dir.mkdir(parents=True, exist_ok=True)
    (manager._entry_dir / f"{old_id}.json").write_text(json.dumps(old_payload), encoding="utf-8")
    await manager.initialize()
    gif = Image("base64://Z2lm")
    gif_msg = message("103653866", MessageChain([gif]))
    manager.prepare_message(gif_msg, sid)
    check("旧12位ID注册时保持稳定", gif._media_access_id == old_id, gif._media_access_id)
    gif_evt = event([gif_msg], group=True)
    gif_evt.session.session_id = "188395693"
    wrong = await manager.read_media(gif_evt, ["390a8720aa8"])
    check("剥离id后返回invalid_id", "invalid_id" in wrong.text and "not_found" not in wrong.text,
          wrong.text)
    check("错误参数附本批正确候选且不识别", old_id in wrong.text and not manager.recognizer.calls,
          wrong.text)
    right = await manager.read_media(gif_evt, [old_id])
    check("完整旧ID仍可读取", "识别成功" in right.text and "file_path:" in right.text,
          right.text)

    new_msg = message("new-message", MessageChain([Image("base64://bmV3")]))
    manager.prepare_message(new_msg, sid)
    new_id = new_msg.chain[0]._media_access_id
    check("新媒体默认i加8位hex", len(new_id) == 9 and module.MEDIA_ID_RE.fullmatch(new_id), new_id)
    await drain(manager)

    # Forced short-prefix collision: no overwrite, extended ID is stable after restart/order change.
    collision_tmp = Path(tempfile.mkdtemp(prefix="kira-media-collision-"))
    collision = make_manager(module, collision_tmp)
    d1 = "abcdef12" + "11" * 28
    d2 = "abcdef12" + "22" * 28
    collision._digest = lambda *parts: d1 if parts[1] == "m1" else d2
    m1 = message("m1", MessageChain([Image("base64://bTE=")]))
    m2 = message("m2", MessageChain([Image("base64://bTI=")]))
    collision.prepare_message(m1, sid)
    collision.prepare_message(m2, sid)
    id1 = m1.chain[0]._media_access_id
    id2 = m2.chain[0]._media_access_id
    check("前8位碰撞时第二条扩展且首条不被覆盖", id1 == "iabcdef12" and id2 == "iabcdef1222"
          and collision._entries[id1]["message_id"] == "m1"
          and collision._entries[id2]["message_id"] == "m2", f"{id1}, {id2}")
    await drain(collision)
    restored = make_manager(module, collision_tmp)
    await restored.initialize()
    restored._digest = collision._digest
    r2 = message("m2", MessageChain([Image("base64://bTI=")]))
    r1 = message("m1", MessageChain([Image("base64://bTE=")]))
    restored.prepare_message(r2, sid)
    restored.prepare_message(r1, sid)
    check("重启并交换顺序后扩展ID不漂移", r2.chain[0]._media_access_id == id2
          and r1.chain[0]._media_access_id == id1)
    check("身份索引同时保留两条碰撞条目", len(restored._catalog) == 2)

    # Session isolation for media and Forward.
    other_evt = event([], group=True)
    other_evt.session.session_id = "other"
    before = len(manager.recognizer.calls)
    cross = await manager.read_media(other_evt, [old_id])
    check("跨会话媒体返回not_found且不识别", "not_found" in cross.text
          and len(manager.recognizer.calls) == before, cross.text)
    fwd = Forward([MessageChain([Text("会话秘密")])])
    fwd_msg = message("private-forward", MessageChain([fwd]))
    manager.prepare_message(fwd_msg, sid)
    fwd_id = next(iter(manager._message_forwards[id(fwd_msg)]))
    cross_forward = await manager.read_forward(other_evt, forward_id=fwd_id)
    check("跨会话Forward不泄露内容", "not_found" in cross_forward.text
          and "会话秘密" not in cross_forward.text, cross_forward.text)
    repeated_forward = message("private-forward", MessageChain([Forward([MessageChain([Text("会话秘密")])])]))
    manager.prepare_message(repeated_forward, sid)
    check("重复Forward刷新消息索引与弱引用", manager._message_forwards[id(repeated_forward)] == [fwd_id]
          and manager._message_of(manager._forwards[fwd_id]) is repeated_forward)

    # Top-level known local path is persisted and usable without history after restart.
    top = Image("base64://dG9w")
    top_path = tmp / "top.jpg"
    top_path.write_bytes(b"top")
    top._temp_path = str(top_path)
    top_msg = message("top-path", MessageChain([top]))
    manager.prepare_message(top_msg, sid)
    top_id = top._media_access_id
    await drain(manager)
    top_payload = json.loads((tmp / "entries" / f"{top_id}.json").read_text())
    check("顶层已知路径真实写入目录", top_payload.get("cache_path") == str(top_path.resolve()),
          json.dumps(top_payload, ensure_ascii=False))
    no_history = make_manager(module, tmp)
    top_restored = await no_history.read_media(evt, [top_id])
    check("无history重启仍读取顶层缓存", "识别成功" in top_restored.text, top_restored.text)

    # Forward media consumes global occurrence but not history occurrence.
    history_tmp = Path(tempfile.mkdtemp(prefix="kira-media-history-"))
    history_rec = Recognizer()  # no bytes persisted: force core-history fallback
    history_manager = make_manager(module, history_tmp, history_rec)
    nested = Image("base64://bmVzdA==")
    top_after = Image("base64://dG9w")
    ordered = message("ordered", MessageChain([Forward([MessageChain([nested])]), top_after]))
    history_manager.prepare_message(ordered, sid)
    ordered_id = top_after._media_access_id
    entry = history_manager._entries[ordered_id]
    check("Forward后顶层媒体拥有独立history_occurrence", entry["occurrence"] == 1
          and entry["history_occurrence"] == 0)
    await drain(history_manager)
    record = type("Record", (), {"chain": MessageChain([Forward(), Image("base64://dG9w")])})()
    history = type("History", (), {})()
    async def get_message(**kwargs):
        return record
    history.get_message = get_message
    ctx = type("Ctx", (), {"message_history": history})()
    with_history = make_manager(module, history_tmp, Recognizer(), ctx)
    recovered = await with_history.read_media(evt, [ordered_id])
    check("Forward后的顶层媒体从历史正确恢复", "识别成功" in recovered.text, recovered.text)

    # Real recognizer persistence: base64/data_url/path nested media survive restart.
    from media_recognize import ParallelMediaRecognizer
    real_tmp = Path(tempfile.mkdtemp(prefix="kira-media-real-types-"))
    real_cache = real_tmp / "cache"
    real = ParallelMediaRecognizer.__new__(ParallelMediaRecognizer)
    real.media_cache_enabled = True
    real.download_timeout = 2
    real._media_cache_dir = lambda: real_cache
    real._ensure_mc_cleanup = lambda: None
    real._phash_of_bytes = lambda data: None
    real._native_mode = lambda sid=None: False
    real_manager = make_manager(module, real_tmp, real)
    real.access = real_manager

    class LocalImage(Image):
        def __init__(self, source, file_type):
            super().__init__("base64://dA==")
            self.file = source
            self.file_type = file_type
            self.mime = "image/png"
            self._temp_path = None

        async def to_base64(self):
            if self.file_type == "path":
                return base64.b64encode(Path(self.file).read_bytes()).decode()
            return self.file

    payload = base64.b64encode(b"real-image").decode()
    original = real_tmp / "original.bin"
    original.write_bytes(b"real-image")
    local_images = [
        LocalImage(payload, "base64"),
        LocalImage("data:image/png;base64," + payload, "data_url"),
        LocalImage(str(original), "path"),
    ]
    real_msg = message("real-types", MessageChain([Forward([MessageChain(local_images)])]))
    real_manager.prepare_message(real_msg, sid)
    real_ids = list(real_manager._message_entries[id(real_msg)])
    await drain(real_manager)
    check("真实识别器为三种嵌套媒体写入内容缓存", len(real_ids) == 3
          and all(real_manager._entries[key].get("cache_path") for key in real_ids)
          and len(list(real_cache.glob("*"))) == 1)
    original.unlink()  # 证明读取不再依赖原始临时路径
    concurrent_payload = base64.b64encode(b"concurrent-image").decode()
    await asyncio.gather(
        real._persist_access_media(LocalImage(concurrent_payload, "base64")),
        real._persist_access_media(LocalImage(concurrent_payload, "base64")),
    )
    check("相同内容并发落盘无临时文件竞态", not list(real_cache.glob("*.tmp"))
          and len(list(real_cache.glob("*"))) == 2)
    real_restored = make_manager(module, real_tmp, Recognizer())
    real_result = await real_restored.read_media(evt, real_ids)
    check("三种非URL嵌套媒体重启后均可恢复", real_result.text.count("识别成功") == 3,
          real_result.text)
    await real_manager.shutdown()
    await real_restored.shutdown()

    # Duplicate IDs execute once; limits and invalid cursor fail before recognition.
    calls_before = len(manager.recognizer.calls)
    await manager.read_media(evt, [old_id, old_id])
    check("重复媒体ID去重执行", len(manager.recognizer.calls) == calls_before + 1)
    limited = await manager.read_media(evt, [f"i{i:08x}" for i in range(17)])
    check("超媒体批量上限明确拒绝", "batch_limit" in limited.text
          and len(manager.recognizer.calls) == calls_before + 1)
    bad_cursor = await manager.read_forward(evt, forward_id=fwd_id, cursor="not-a-cursor")
    check("非法cursor明确报错", "invalid_cursor" in bad_cursor.text)

    # Runtime-only message IDs and genuine source expiry are reported precisely.
    volatile_tmp = Path(tempfile.mkdtemp(prefix="kira-media-volatile-"))
    volatile_manager = make_manager(module, volatile_tmp)
    volatile_msg = message(None, MessageChain([Image("base64://dm9s")]))
    volatile_msg.message_id = None
    volatile_manager.prepare_message(volatile_msg, sid)
    volatile_id = volatile_msg.chain[0]._media_access_id
    await drain(volatile_manager)
    check("无平台message_id明确标记volatile", volatile_manager._entries[volatile_id]["volatile"] is True)
    volatile_restored = make_manager(module, volatile_tmp)
    volatile_result = await volatile_restored.read_media(evt, [volatile_id])
    check("无法恢复的volatile媒体返回source_unavailable", "source_unavailable" in volatile_result.text,
          volatile_result.text)
    missing = await volatile_restored.read_media(evt, ["i00000000"])
    check("合法但未知ID与真实源过期分离", "not_found" in missing.text
          and "source_unavailable" not in missing.text)
    await volatile_manager.shutdown()
    await volatile_restored.shutdown()

    too_many_forward = await manager.read_forward(
        evt, forward_ids=[f"f{i:08x}" for i in range(9)])
    check("超Forward批量上限明确拒绝", "batch_limit" in too_many_forward.text)

    # Event-bus session deletion is subscribed and safely unsubscribed.
    class Bus:
        def __init__(self):
            self.handlers = []
        def subscribe(self, name, handler):
            self.handlers.append(handler)
        def unsubscribe(self, name, handler):
            self.handlers.remove(handler)
    bus = Bus()
    bus_ctx = type("Ctx", (), {"event_bus": bus, "config": None})()
    bus_tmp = Path(tempfile.mkdtemp(prefix="kira-media-bus-"))
    bus_manager = make_manager(module, bus_tmp, ctx=bus_ctx)
    await bus_manager.initialize()
    check("初始化订阅session_deleted", len(bus.handlers) == 1)
    await bus_manager._on_session_deleted(type("Event", (), {"payload": {"session": sid}})())
    await bus_manager.shutdown()
    check("关闭时退订session_deleted", not bus.handlers)

    # Shutdown drains the initial metadata write even if byte persistence is slower.
    class SlowRecognizer(Recognizer):
        async def _persist_media(self, elem):
            await asyncio.sleep(0.1)
            return self.persist_path, "md5"
    slow_tmp = Path(tempfile.mkdtemp(prefix="kira-media-shutdown-"))
    slow = SlowRecognizer()
    slow_manager = make_manager(module, slow_tmp, slow)
    slow_msg = message("slow", MessageChain([Image("base64://c2xvdw==")]))
    slow_manager.prepare_message(slow_msg, sid)
    slow_id = slow_msg.chain[0]._media_access_id
    await slow_manager.shutdown()
    check("shutdown有界等待目录落盘", (slow_tmp / "entries" / f"{slow_id}.json").is_file())

    # Runtime LRU keeps the light identity catalog after hot entries are evicted.
    lru_tmp = Path(tempfile.mkdtemp(prefix="kira-media-lru-"))
    lru = make_manager(module, lru_tmp)
    lru.RUNTIME_LIMIT = 2
    lru.MESSAGE_PIN_LIMIT = 0
    lru.MESSAGE_INDEX_LIMIT = 2
    live = []
    for index in range(3):
        item = message(f"lru-{index}", MessageChain([Image("base64://bHJ1")]))
        live.append((item, weakref.ref(item)))
        lru.prepare_message(item, sid)
    check("运行时媒体热条目有界", len(lru._entries) <= 2, len(lru._entries))
    check("轻量目录不随热LRU丢失", len(lru._catalog) >= 3, len(lru._catalog))
    first_ref = live[0][1]
    live[0] = None
    gc.collect()
    check("目录不长期强引用消息对象", first_ref() is None)
    await drain(lru)

    # Per-session and total catalog retention, without touching shared media bytes.
    lru.CATALOG_SESSION_LIMIT = 2
    lru.CATALOG_TOTAL_LIMIT = 3
    lru.CATALOG_CLEANUP_INTERVAL = 0
    lru._last_catalog_cleanup = 0
    await lru._cleanup_catalog()
    check("目录按会话与总量有界", len(lru._catalog) <= 3, len(lru._catalog))
    check("目录清理不删除共享媒体字节", cached.exists() and top_path.exists())

    # Session deletion removes only that session's metadata.
    await manager.delete_session(sid)
    check("会话删除清除其媒体与Forward目录", not manager._catalog
          and not list((tmp / "entries").glob("*.json")))
    check("会话删除仍不删除共享媒体字节", cached.exists() and top_path.exists())

    await manager.shutdown()
    await collision.shutdown()
    await restored.shutdown()
    await no_history.shutdown()
    await history_manager.shutdown()
    await with_history.shutdown()
    await lru.shutdown()
    print(f"LIFECYCLE CHECKS={len(results)} FAILURES={results.count(False)}")
    if not all(results):
        raise AssertionError("media lifecycle regression failed")
    print("ALL PASSED")


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
    asyncio.run(run(target))
