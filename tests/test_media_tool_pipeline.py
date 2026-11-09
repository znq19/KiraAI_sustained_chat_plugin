"""Host-level media tool presentation and lazy VLM pipeline tests."""
import asyncio
import sys
from pathlib import Path

from test_adaptive_integration import config
from test_debounce_arrival_race import Ctx, SEvent, UrlImage, make_msg, session, shutdown
from core.chat.message_elements import Image
from test_queue_timing import batch


def plugin_config(tool=True, max_count=1):
    cfg = config(0.2)
    cfg["section_media"] = {
        "media_tool_image_enabled": tool,
        "media_tool_audio_enabled": False,
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
