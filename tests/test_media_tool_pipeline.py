"""Host-level media tool presentation and lazy VLM pipeline tests."""
import asyncio
import sys
import tempfile
from pathlib import Path

from test_adaptive_integration import config
from test_debounce_arrival_race import Ctx, SEvent, UrlImage, make_msg, session, shutdown
from core.chat import MessageChain
from core.chat.message_elements import Image, Text
from test_queue_timing import batch


def plugin_config(tool=True, max_count=1, prefetch=True):
    cfg = config(0.2)
    cfg["section_media"] = {
        "media_tool_image_enabled": tool,
        "media_tool_audio_enabled": False,
        "media_tool_prefetch_enabled": prefetch,
        "media_tool_auto_present_private_enabled": True,
        "media_tool_auto_present_group_enabled": True,
        "media_tool_auto_present_max_count": max_count,
        "forward_tool_enabled": False,
        "forward_max_depth": 4,
        "image_recognition_only_on_mention": False,
        "image_recognition_probability": 1.0,
        "max_images_per_message": 0,
        "voice_recognition_only_on_mention": False,
        "voice_private_need_mention": False,
    }
    cfg["section_queue_merge"] = {"media_preprocess_enabled": False}
    return cfg


async def prepare(mod, context, plug, mid, image_count, mentioned=True):
    message = make_msg(mid, "看图", mentioned=mentioned,
                       chain=[UrlImage(payload=b"img") for _ in range(image_count)])
    event = SEvent(message, session(), mentioned=mentioned)
    await plug.handle_msg(event)
    await plug.guard_official_vlm(event)
    await plug.on_media_rec_im(event)
    paths = [await plug.media_recognizer._media_path(elem) for elem in message.chain]
    message.message_str = "\n".join(f"[Image , file_path: {p}]" for p in paths)
    batch_event = batch(message)
    await plug.on_media_rec_batch(batch_event)
    return message, batch_event


async def run(plugin_dir: Path):
    results = []
    def check(name, condition, detail=""):
        results.append(bool(condition))
        print(("PASS " if condition else "FAIL ") + name + (f" — {detail}" if not condition else ""))

    # load_plugin registers a unique module for each host variant.
    from test_debounce_arrival_race import load_plugin
    mod = load_plugin(plugin_dir)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
    plug = mod.DebouncePlugin(context, plugin_config())
    async def fake_describe(*args, **kwargs):
        return "一只猫"
    plug.media_recognizer._describe_image = fake_describe
    message, batch_event = await prepare(mod, context, plug, "single", 1)
    check("单图直接呈现真实描述", "一只猫" in (message.message_str or ""), message.message_str)
    check("单图不残留工具占位", "可读取" not in (message.message_str or ""))
    await shutdown(plug)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
    plug = mod.DebouncePlugin(context, plugin_config())
    async def fake_describe(*args, **kwargs):
        return "一只猫"
    plug.media_recognizer._describe_image = fake_describe
    message, batch_event = await prepare(mod, context, plug, "double", 2, mentioned=False)
    check("双图首轮保留Image类型和稳定ID", (message.message_str or "").count("Image #") == 2,
          message.message_str)
    check("双图首轮不直接注入描述", "一只猫" not in (message.message_str or ""))
    ids = list(plug.media_recognizer.access._message_entries[id(message)])
    result = await plug.media_recognizer.access.read_media(batch_event, ids)
    check("统一工具批量返回真实描述", result.text.count("一只猫") == 2, result.text)
    await shutdown(plug)

    # Wake bypasses the small-batch threshold without changing tool registration.
    context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
    plug = mod.DebouncePlugin(context, plugin_config(max_count=1))
    async def fake_describe(*args, **kwargs):
        return "一只猫"
    plug.media_recognizer._describe_image = fake_describe
    message, batch_event = await prepare(mod, context, plug, "wake-double", 2)
    check("唤醒双图仍直接并行呈现", (message.message_str or "").count("一只猫") == 2,
          message.message_str)
    await shutdown(plug)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
    plug = mod.DebouncePlugin(context, plugin_config(prefetch=False))
    async def fake_describe(*args, **kwargs):
        return "一只猫"
    plug.media_recognizer._describe_image = fake_describe
    message, batch_event = await prepare(mod, context, plug, "pure", 1)
    check("纯按需唤醒单图也不绕过工具识别", "可读取" in (message.message_str or "")
          and "一只猫" not in message.message_str, message.message_str)
    ids = list(plug.media_recognizer.access._message_entries[id(message)])
    result = await plug.media_recognizer.access.read_media(batch_event, ids)
    check("纯按需调用工具后返回真实描述与路径", "一只猫" in result.text
          and "file_path:" in result.text, result.text)
    await shutdown(plug)

    # 语音按需 + 仅唤醒：非唤醒语音必须保留可读占位，不能被提前替换成 [语音]
    from test_debounce_arrival_race import FakeRecord
    context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
    cfg = plugin_config(tool=False)
    cfg["section_media"]["media_tool_audio_enabled"] = True
    cfg["section_media"]["voice_recognition_only_on_mention"] = True
    plug = mod.DebouncePlugin(context, cfg)
    async def fake_read(entry, ev):
        return "语音内容"
    plug.media_recognizer.read_access_entry = fake_read
    rec = FakeRecord()
    message = make_msg("voice-lazy", "听语音", mentioned=False, chain=[rec])
    event = SEvent(message, session(), mentioned=False)
    await plug.handle_msg(event)
    await plug.guard_official_vlm(event)
    await plug.on_media_rec_im(event)
    if plug.media_recognizer._stage1_pending:
        await asyncio.gather(*list(plug.media_recognizer._stage1_pending.values()),
                             return_exceptions=True)
    message.message_str = "".join(e.text for e in message.chain if hasattr(e, "text"))
    batch_event = batch(message)
    await plug.on_media_rec_batch(batch_event)
    check("按需语音：非唤醒保留可读占位", "[语音]" not in (message.message_str or "")
          and "Record #" in (message.message_str or ""), message.message_str)
    voice_ids = list(plug.media_recognizer.access._message_entries[id(message)])
    voice_result = await plug.media_recognizer.access.read_media(batch_event, voice_ids)
    check("按需语音：read_media 可读", "语音内容" in voice_result.text, voice_result.text)
    await shutdown(plug)

    # 超长语音：无论语音工具开关，始终 [长语音 X秒] 占位；只补路径，不 STT/不可读
    from test_debounce_arrival_race import FakeRecord as _FR
    for voice_tool in (True, False):
        context = Ctx({"bot_config.capabilities.image_recognition.mode": "vlm_description"})
        cfg = plugin_config(tool=False)
        cfg["section_media"]["media_tool_audio_enabled"] = voice_tool
        cfg["section_media"]["voice_max_duration"] = 10
        plug = mod.DebouncePlugin(context, cfg)
        _stt_called = False
        async def _no_stt(*a, **k):
            nonlocal _stt_called
            _stt_called = True
        plug.media_recognizer._transcribe_one = _no_stt
        class _PathRecord(_FR):
            async def to_path(self):
                return self._temp_path
        rec = _PathRecord(); rec.duration = 999
        _tmp = Path(tempfile.mkdtemp(prefix="kira-long-voice-")) / "voice.mp3"
        _tmp.write_bytes(b"fakeaudio")
        rec._temp_path = str(_tmp)
        message = make_msg(f"long-voice-{voice_tool}", "长语音", mentioned=False, chain=[rec])
        event = SEvent(message, session(), mentioned=False)
        await plug.handle_msg(event)
        await plug.guard_official_vlm(event)
        await plug.on_media_rec_im(event)
        if plug.media_recognizer._stage1_pending:
            await asyncio.gather(*list(plug.media_recognizer._stage1_pending.values()),
                                 return_exceptions=True)
        message.message_str = "".join(e.text for e in message.chain if hasattr(e, "text"))
        batch_event = batch(message)
        await plug.on_media_rec_batch(batch_event)
        check(f"超长语音占位（语音工具={voice_tool}）",
              "[长语音 999秒" in (message.message_str or "")
              and "Record #" not in (message.message_str or ""), message.message_str)
        check(f"超长语音路径（语音工具={voice_tool}）",
              "file_path:" in (message.message_str or ""), message.message_str)
        check(f"超长语音无 read_media ID（语音工具={voice_tool}）",
              not (plug.media_recognizer.access._message_entries.get(id(message)) or []))
        check(f"超长语音不触发 STT（语音工具={voice_tool}）", not _stt_called)
        await shutdown(plug)

    # native 仅唤醒：工具开启时保留可读，关闭时占位
    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    cfg = plugin_config(tool=True)
    cfg["section_media"]["image_recognition_only_on_mention"] = True
    plug = mod.DebouncePlugin(context, cfg)
    native_chain = MessageChain([Image("base64://aW1n")])
    await plug._process_media(native_chain, False, False)
    check("native 仅唤醒+工具：非唤醒图保留可读元素",
          isinstance(native_chain[0], Image)
          and getattr(native_chain[0], "_media_skip_reason", "") == "mention",
          type(native_chain[0]).__name__)
    await shutdown(plug)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    cfg = plugin_config(tool=False)
    cfg["section_media"]["image_recognition_only_on_mention"] = True
    plug = mod.DebouncePlugin(context, cfg)
    native_chain = MessageChain([Image("base64://aW1n")])
    await plug._process_media(native_chain, False, False)
    check("native 仅唤醒+工具关闭：非唤醒图占位",
          isinstance(native_chain[0], Text) and "[Image attached]" in native_chain[0].text,
          type(native_chain[0]).__name__)
    await shutdown(plug)

    # native 识图概率：非唤醒消息也必须按概率生效（工具开时保留可读，关时占位）
    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    cfg = plugin_config(tool=True)
    cfg["section_media"]["image_recognition_only_on_mention"] = False
    cfg["section_media"]["image_recognition_probability"] = 0.0
    plug = mod.DebouncePlugin(context, cfg)
    native_chain = MessageChain([Image("base64://aW1n")])
    await plug._process_media(native_chain, False, False)
    check("native 概率+工具：非唤醒图保留可读元素",
          isinstance(native_chain[0], Image)
          and getattr(native_chain[0], "_media_skip_reason", "") == "probability",
          type(native_chain[0]).__name__)
    await shutdown(plug)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    cfg = plugin_config(tool=False)
    cfg["section_media"]["image_recognition_only_on_mention"] = False
    cfg["section_media"]["image_recognition_probability"] = 0.0
    plug = mod.DebouncePlugin(context, cfg)
    native_chain = MessageChain([Image("base64://aW1n")])
    await plug._process_media(native_chain, False, False)
    check("native 概率+工具关闭：非唤醒图占位",
          isinstance(native_chain[0], Text) and "[Image attached]" in native_chain[0].text,
          type(native_chain[0]).__name__)
    await shutdown(plug)

    # native 超限 + 图片工具：超限图必须保留元素供 read_media，而不是换成 Text
    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    plug = mod.DebouncePlugin(context, plugin_config(tool=True))
    native_chain = MessageChain([Image("base64://aW1n"), Image("base64://aW1n")])
    plug._limit_media_count(native_chain, 1)
    check("native 超限+工具：超限图保留可读元素",
          isinstance(native_chain[1], Image) and getattr(native_chain[1], "_media_skip", False)
          and getattr(native_chain[1], "_media_skip_reason", "") == "cap",
          type(native_chain[1]).__name__)
    await shutdown(plug)

    context = Ctx({"bot_config.capabilities.image_recognition.mode": "native"})
    plug = mod.DebouncePlugin(context, plugin_config(tool=False))
    native_chain = MessageChain([Image("base64://aW1n"), Image("base64://aW1n")])
    plug._limit_media_count(native_chain, 1)
    check("native 超限+工具关闭：超限图占位",
          isinstance(native_chain[1], Text) and "[Image attached]" in native_chain[1].text,
          type(native_chain[1]).__name__)
    await shutdown(plug)
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
