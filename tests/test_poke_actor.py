"""戳一戳归属回归：bot 自己的戳 / 用户戳 bot 必须分得清。

背景（2026-11-18 线上日志）：
  SnowLuma 私聊 poke notice 的 `user_id` 是**会话对端**（不是发起者），
  真正发起者在 `sender_id`：

    user_id   = peerUin    （私聊里永远是好友）
    sender_id = senderUin  （真正的发起者）
    target_id = targetUin  （被戳者）

  NapCat / LLOneBot 私聊只有 user_id（=发起者）；群聊三家都是 user_id=发起者。

  旧行为：`_is_system_event` 把所有 poke 都当"真实用户互动"，
  发起者取 `message.sender.user_id`（框架按 user_id 构造）= 对端
  ⇒ **bot 主动戳人会被记成"该用户戳了一下"**：存在感 +1、骚扰 poke 计数 +1。

修复：poke 用 `sender_id` 优先解析发起者；发起者 == self_id 时整条按系统事件跳过。

用法: python3 tests/test_poke_actor.py [<plugin_dir>]
"""
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGIN = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else HERE.parent
sys.path.insert(0, str(HERE / "_stub"))

BOT = "583893217"
USER = "769690776"
GROUP = "999888777"
RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" :: {detail}" if detail and not cond else ""), flush=True)


def load_engine():
    """按路径加载插件模块（不触发插件框架导入）。"""
    path = PLUGIN / "chat_enhance.py"
    spec = importlib.util.spec_from_file_location("s_z_chat_enhance_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Event:
    """最小事件替身：只实现插件实际读取的字段/方法（与框架 KiraMessageEvent 同构）。"""

    def __init__(self, message, session, self_id):
        self.message = message
        self.session = session
        self.adapter = None
        self.self_id = self_id

    @property
    def is_notice(self):
        return bool(getattr(self.message, "is_notice", False))

    @property
    def raw_message(self):
        return getattr(self.message, "raw_message", None)

    def is_group_message(self):
        return getattr(self.message, "group", None) is not None


def make_event(mod, raw, sender_uid, is_notice=True, self_id=BOT, chain_text=""):
    """构造最小事件（与框架 notice 事件同构：sender.user_id 取 raw.user_id）。"""
    from core.chat import Group, KiraIMMessage, MessageChain, Session, User

    chain = MessageChain()
    if chain_text:
        chain.text(chain_text)
    msg = KiraIMMessage(
        timestamp=1786000000,
        message_id="None" if is_notice else "1764283028",
        group=Group(group_id=GROUP, group_name="测试群"),
        sender=User(user_id=str(sender_uid), nickname="某人"),
        self_id=self_id,
        chain=chain,
        is_notice=is_notice,
        is_mentioned=False,
        raw_message=raw,
    )
    sess = Session(session_type="gm", session_id=GROUP)
    return _Event(msg, sess, self_id)


def main():
    mod = load_engine()
    Engine = getattr(mod, "ChatEnhanceEngine", None) or getattr(mod, "Enhance")
    enh = Engine.__new__(Engine)          # 只测纯判定方法，不初始化重量级状态

    # 1) SnowLuma 私聊：bot 自己戳用户（user_id=对端，sender_id=bot，target_id=对端）
    raw = {"post_type": "notice", "notice_type": "notify", "sub_type": "poke",
           "self_id": BOT, "user_id": USER, "sender_id": BOT, "target_id": USER,
           "time": 1786000000}
    ev = make_event(mod, raw, sender_uid=USER, is_notice=True)
    check("SnowLuma 私聊 bot 自己戳 → 判定为系统事件（跳过统计）",
          enh._is_system_event(ev) is True)
    # on_im_message 必须提前返回（未初始化的状态若被访问会直接抛异常）
    try:
        enh.on_im_message(ev)
        check("bot 自己戳 → on_im_message 直接返回（不触达统计）", True)
    except Exception as e:
        check("bot 自己戳 → on_im_message 直接返回（不触达统计）", False, repr(e))

    # 2) NapCat 私聊：只有 user_id（bot 自己戳）
    raw2 = {"post_type": "notice", "notice_type": "notify", "sub_type": "poke",
            "self_id": BOT, "user_id": BOT, "target_id": USER, "time": 1786000000}
    ev2 = make_event(mod, raw2, sender_uid=BOT, is_notice=True)
    check("NapCat 私聊 bot 自己戳 → 系统事件", enh._is_system_event(ev2) is True)

    # 3) 私聊：用户戳 bot（真实互动，必须保留）
    raw3 = {"post_type": "notice", "notice_type": "notify", "sub_type": "poke",
            "self_id": BOT, "user_id": USER, "sender_id": USER, "target_id": BOT,
            "time": 1786000000}
    ev3 = make_event(mod, raw3, sender_uid=USER, is_notice=True)
    check("私聊 用户戳 bot → 不是系统事件", enh._is_system_event(ev3) is False)
    check("私聊 用户戳 bot → 发起者=用户", enh._sender_id(ev3) == USER, enh._sender_id(ev3))
    check("私聊 用户戳 bot → kind=poke", enh._detect_kind(ev3) == "poke", enh._detect_kind(ev3))

    # 4) 群聊：用户戳用户（三家 user_id=发起者）
    raw4 = {"post_type": "notice", "notice_type": "notify", "sub_type": "poke",
            "self_id": BOT, "user_id": USER, "target_id": "10001",
            "group_id": GROUP, "time": 1786000000}
    ev4 = make_event(mod, raw4, sender_uid=USER, is_notice=True)
    check("群聊 用户戳人 → 不是系统事件", enh._is_system_event(ev4) is False)
    check("群聊 用户戳人 → 发起者=用户", enh._sender_id(ev4) == USER, enh._sender_id(ev4))

    # 5) 群聊：bot 自己戳人
    raw5 = dict(raw4, user_id=BOT, target_id=USER)
    ev5 = make_event(mod, raw5, sender_uid=BOT, is_notice=True)
    check("群聊 bot 自己戳 → 系统事件", enh._is_system_event(ev5) is True)

    # 6) 非 poke notice（禁言）保持原样：系统事件
    raw6 = {"post_type": "notice", "notice_type": "group_ban", "sub_type": "ban",
            "self_id": BOT, "user_id": "10001", "operator_id": "10002",
            "group_id": GROUP, "duration": 60, "time": 1786000000}
    ev6 = make_event(mod, raw6, sender_uid="10001", is_notice=True)
    check("禁言 notice 仍是系统事件（行为不变）", enh._is_system_event(ev6) is True)

    # 7) 普通消息不受影响
    ev7 = make_event(mod, {}, sender_uid=USER, is_notice=False, chain_text="晚上好")
    check("普通消息不是系统事件", enh._is_system_event(ev7) is False)
    check("普通消息 sender 不变", enh._sender_id(ev7) == USER, enh._sender_id(ev7))

    failed = [n for n, ok in RESULTS if not ok]
    print()
    print(f"戳一戳归属: {len(RESULTS) - len(failed)} 通过, {len(failed)} 失败")
    if failed:
        for n in failed:
            print(f"  FAIL {n}")
        sys.exit(1)
    print("=== 全部通过 ===")


if __name__ == "__main__":
    main()
