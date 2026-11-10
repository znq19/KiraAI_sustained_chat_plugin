# -*- coding: utf-8 -*-
"""S/Z 聊天插件 — 框架真实加载器测试（2.x / 3.0）。

不用桩上下文，而是让**框架自己的 PluginManager** 走真实加载链路：
  load_plugin_from_dir → spec 导入（包式）→ schema 默认配置（含 section_jev 校验）
  → 实例化（DebouncePlugin __init__：BatchMergeScheduler/ParallelMediaRecognizer/
  ChatEnhanceEngine/JEV 状态）→ initialize() → terminate()。

用法（任意机器）：
  python3 real_loader_test.py --core2x /path/to/KiraAI-2.x --core30 /path/to/KiraAI-3.0
  也可用环境变量 KIRA_CORE_2X / KIRA_CORE_30。插件目录 = 本仓库根（自动定位）。
"""
import argparse
import asyncio
import dataclasses
import os
import sys
import types
import traceback
from pathlib import Path

_DEFAULT_ROOT = Path(__file__).resolve().parent.parent

def resolve_cores():
    ap = argparse.ArgumentParser()
    ap.add_argument("--core2x", default=os.environ.get("KIRA_CORE_2X", ""))
    ap.add_argument("--core30", default=os.environ.get("KIRA_CORE_30", ""))
    ap.add_argument("--plugin-dir", default=os.environ.get("KIRA_CHAT_PLUGIN_DIR", ""))
    args, _ = ap.parse_known_args()
    global PLUGIN_DIR
    PLUGIN_DIR = (args.plugin_dir or "").strip() or str(_DEFAULT_ROOT)
    cores = {}
    for gen, p in (("2.x", args.core2x), ("3.0", args.core30)):
        p = (p or "").strip()
        if p and os.path.isdir(os.path.join(p, "core", "plugin")):
            cores[gen] = p
        elif p:
            print(f"⚠️  {gen} 核心路径无效：{p} ⇒ 跳过", flush=True)
    return cores

RESULTS = []

def check(gen, name, cond, detail=""):
    RESULTS.append((gen, name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] [{gen}] {name}" + (f" :: {detail}" if detail and not cond else ""), flush=True)

def swap_core(root):
    if root in sys.path:
        sys.path.remove(root)
    for m in list(sys.modules):
        if m == "core" or m.startswith("core.") or m == "plugins" or m.startswith("plugins."):
            del sys.modules[m]
    sys.path.insert(0, root)
    os.chdir(root)
    os.makedirs(os.path.join(root, "data"), exist_ok=True)

class _Cfg(dict):
    """dict 配置 + get_config（点分路径）——满足 ctx.config['bot_config'] 与
    ctx.config.get_config 两种访问。"""
    def get_config(self, key, default=None):
        cur = self
        for part in str(key).split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

class _StubBuffer:
    def __init__(self):
        self.buffer = []
        import asyncio as _a
        self.lock = _a.Lock()

    def add(self, ev):
        self.buffer.append(ev)

    def get_length(self):
        return len(self.buffer)

    def pop(self, count=1):
        popped = self.buffer[:count]
        del self.buffer[:count]
        return popped

class _StubProcessor:
    def __init__(self):
        self.session_buffer = types.SimpleNamespace(get_buffer=lambda sid: _StubBuffer())

    async def flush_session_messages(self, sid, **kw):
        return False

    def get_session_buffer_length(self, sid):
        return 0

    async def handle_im_message(self, event):
        return None

class _StubProviderMgr:
    def get_default_llm(self):
        return None

    def get_default_vlm(self):
        return None

    def get_model_info(self, pid, mid, model_type=None):
        return types.SimpleNamespace(model_id=mid, model_name="stub模型",
                                     provider_id=pid, provider_name="stub提供商",
                                     provider_config={"base_url": "https://example.invalid",
                                                      "api_key": "sk-stub"},
                                     model_config={})

def build_ctx(gen, PluginContext):
    fields = {f.name for f in dataclasses.fields(PluginContext)}
    kwargs = {name: None for name in fields}
    for name in fields:
        if name == "config":
            kwargs[name] = _Cfg({
                "bot_config": {
                    "bot": {"max_message_interval": 1.5, "max_buffer_messages": 3},
                    "agent": {"max_tool_loop": 2, "tool_call_timeout": 60},
                    "capabilities": {},
                },
                "locale": {"lang": "zh"},
                "providers": {},
            })
        elif name == "message_processor":
            kwargs[name] = _StubProcessor()
        elif name == "provider_mgr":
            kwargs[name] = _StubProviderMgr()
        elif name == "session_mgr":
            kwargs[name] = types.SimpleNamespace(
                fetch_memory=lambda sid: [],
                write_memory=lambda sid, chunks: None,
                delete_session=lambda sid: None,
                get_session_info=lambda sid: types.SimpleNamespace(sid=sid, session_description=""),
            )
        elif name == "event_bus":
            kwargs[name] = types.SimpleNamespace(publish=lambda ev: asyncio.sleep(0))
        elif name == "adapter_mgr":
            kwargs[name] = types.SimpleNamespace(get_adapter=lambda name: None)
        elif name == "tool_mgr":
            kwargs[name] = types.SimpleNamespace(
                register_tool=lambda *a, **k: None,
                unregister_tool=lambda *a, **k: None,
                get_tool=lambda *a, **k: None,
            )
        elif name == "sticker_manager":
            kwargs[name] = types.SimpleNamespace(sticker_dict={}, sticker_folder="/tmp")
    return PluginContext(**kwargs)

async def run_gen(gen, core):
    swap_core(core)
    from core.plugin import PluginManager, PluginContext

    ctx = build_ctx(gen, PluginContext)
    mgr = PluginManager(ctx)
    ctx.plugin_mgr = mgr

    expect_pid = "sustained-chat" if "sustained" in Path(PLUGIN_DIR).name or \
        json_pid(PLUGIN_DIR) == "sustained-chat" else json_pid(PLUGIN_DIR)

    pid = await mgr.load_plugin_from_dir(Path(PLUGIN_DIR), auto_install=False)
    check(gen, f"真实加载器返回插件 ID（{expect_pid}）", pid == expect_pid, str(pid))

    inst = mgr.plugin_instances.get(expect_pid) or mgr.get_plugin_inst(expect_pid)
    check(gen, "插件实例已注册", inst is not None)
    if inst is None:
        return
    check(gen, "initialize 完成（合并调度器就绪）", getattr(inst, "merge_scheduler", None) is not None)
    check(gen, "媒体识别器就绪", getattr(inst, "media_recognizer", None) is not None)
    check(gen, "增强引擎就绪", getattr(inst, "enhance", None) is not None)
    check(gen, "JEV 默认关闭且客户端为空", getattr(inst, "jev_enabled", False) is False
          and getattr(inst, "_jev_client", None) is None)
    check(gen, "JEV 配置面已解析（模式默认 off）",
          getattr(inst, "jev_proactive_mode", "off") == "off")
    # manifest 版本号
    import json as _json
    man = _json.loads((Path(PLUGIN_DIR) / "manifest.json").read_text(encoding="utf-8"))
    check(gen, "manifest 版本为目标版", man.get("version") in ("2.8.1", "1.11.1"), man.get("version"))

    # 钩子注册进框架 registry
    try:
        if gen == "3.0":
            from core.plugin import registry as reg3
            comp = reg3._plugin_components.get(expect_pid)
            hooks = getattr(comp, "hooks", {}) if comp else {}
            check(gen, "事件钩子已注册(3.0)", len(hooks) >= 6, str(list(hooks))[:100])
        else:
            from core.plugin import plugin_registry as reg2
            comp = reg2._plugin_components.get(expect_pid)
            hooks = getattr(comp, "hooks", {}) if comp else {}
            check(gen, "事件钩子已注册(2.x)", len(hooks) >= 6, str(list(hooks))[:100])
    except Exception as e:
        check(gen, "钩子注册检查异常", False, repr(e))

    await inst.terminate()
    check(gen, "terminate 完成", True)

def json_pid(plugin_dir):
    import json as _json
    try:
        return _json.loads((Path(plugin_dir) / "manifest.json").read_text(encoding="utf-8")).get("plugin_id", "")
    except Exception:
        return ""

def main():
    cores = resolve_cores()
    if not cores:
        print("用法：python3 real_loader_test.py --core2x /path/to/2.x --core30 /path/to/3.0\n"
              "或环境变量 KIRA_CORE_2X / KIRA_CORE_30。")
        sys.exit(2)
    for gen, core in cores.items():
        print(f"\n===== 真实加载器 [{gen}]（{core}）=====", flush=True)
        try:
            asyncio.run(run_gen(gen, core))
        except Exception:
            check(gen, "套件异常", False, traceback.format_exc()[-800:])
    fails = [r for r in RESULTS if not r[2]]
    print(f"\n===== 真实加载器汇总：{len(RESULTS)-len(fails)}/{len(RESULTS)} 通过 =====")
    if fails:
        sys.exit(1)
    print("REAL LOADER TEST PASSED")

if __name__ == "__main__":
    main()
