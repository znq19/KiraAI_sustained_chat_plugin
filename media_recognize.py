"""并行媒体识别模块（v2.3.2）—— 图片 VLM + 音频 STT 并行预处理

设计要点（对齐方案文档 KiraAI并行媒体识别模块对齐方案.md v1.1）：
- 三阶段架构（stage1 ON_IM_MESSAGE / stage2 ON_IM_BATCH_MESSAGE / stage3 ON_LLM_REQUEST）
- v2.3.2 核心变更（Plus-One 复读兼容 + 官方格式对齐，用户拍板）：
    * Image/Sticker **元素保留在 chain 中**（不再替换为标识符删除）——Plus-One 复读
      表情包依赖 Sticker 元素；图片元素保留则纯图片消息天然不参与复读（Plus-One 只认
      Text/Sticker）。转发消息的媒体同样保留。
    * "识别/不识别"通过**预置 elem.caption** 表达：缓存命中 → desc（框架渲染官方
      [Image desc, file_path: p] / [Sticker desc]（官方无路径，本模块增强追加路径））；
      未命中且宿主标记不识别（仅唤醒/概率未中/超限）→ caption=""（官方空占位
      [Image , file_path: p] / [Sticker ]，阻止框架自动 VLM，LLM 知道有媒体未识别）。
    * 只有需要识别的媒体暂存 _pir_media，stage2 并行 VLM 后回填 message_str（锚点
      替换官方空占位）与 elem.caption。
    * Record 语音照旧替换 [Record #id: ] 标识符（阻止框架自动 STT，走本模块限流+缓存）。
- 原生多模态：native 模式运行时实时检测（_native_mode()）——图片由框架直传，本模块
  不预置/不识别；语音 STT 归本模块照旧。
- PIR 互斥（pir_auto_disable 默认开）：检测到 parallel_image_reader 启用 → 自动关闭
  （本模块已覆盖其全部能力）；竞态/关闭失败降级让位，绝不双重处理。
- 缓存：复用框架 image_desc_cache 表；VLM 描述词跟随 WebUI desc_prompt 配置。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import re
from io import BytesIO
from typing import Optional

from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat.message_elements import Text, Image, Sticker, Record, Reply, Forward
from core.provider import LLMRequest
from core.utils.common_utils import get_default_vlm_prompt, speech_to_text

# 专用日志器：与框架 core/utils/common_utils.py 的 `get_logger("llm", "purple")`、
# 以及并行识图插件的 `get_logger("parallel_vlm", "purple")` 保持一致 —— 识图/VLM
# 相关日志统一紫色，前缀 [MediaRecognize]，便于与框架自身的 desc_img 日志对照。
# 注意：logger 名字即日志前缀，故消息正文里不再重复写 [MediaRecognize] 标签。
try:
    from core.logging_manager import get_logger

    logger = get_logger("MediaRecognize", "purple")
except Exception:  # 极端兼容：拿不到框架日志器时退回插件 logger
    from core.plugin import logger

# 标识符匹配（内容三态：空 / 描述 / (未识别) (已过期)）
_IMAGE_RE = re.compile(r"\[Image #([^\]\s:]+): ([^\]]*)\]")
_RECORD_RE = re.compile(r"\[Record #([^\]\s:]+): ([^\]]*)\]")
_ALL_RE = re.compile(r"\[(?:Image|Record) #([^\]\s:]+): ([^\]]*)\]")

# 官方「空占位」：caption 为空时框架渲染成 [Image , file_path: xxx] / [Image ] / [Sticker ]。
# 已有描述的形式（[Image 描述, file_path: …] / [Sticker 描述]）不会命中本正则。
# 用途见 _fill_empty_official_by_path()：第三方插件把被回复消息的媒体重新下载成
# **另一个临时文件**后，链上已无对应元素，只剩这段文本 → 只能按文件内容找描述。
_EMPTY_OFFICIAL_RE = re.compile(r"\[(Image|Sticker)\s*(?:,\s*file_path:\s*([^\]\n]+?)\s*)?\]")

# 感知哈希索引（进程内、跨会话）：md5 不同但画面相同的副本（重新下载 / 重新压缩）也能命中。
# 只存「我们自己描述过」的图，键是 64bit dHash、值是描述；有界，超限丢最旧一半。
_PHASH_INDEX: dict[str, str] = {}
_PHASH_INDEX_MAX = 512
_PHASH_BITS = 64


class ParallelMediaRecognizer:
    """并行媒体识别：作为 mixin 组件挂在聊天插件上，与 queue_merge 解耦。"""

    def __init__(self, ctx, plugin_cfg: dict, bot_cfg: dict):
        self.ctx = ctx
        sec = plugin_cfg.get("section_media_recognition", {})
        self.enabled = sec.get("enabled", True)
        # 三层并发限制（VLM / STT 各自独立），按 批次级 → 会话级 → 全局级 依次获取：
        #   ① 批次级（max_parallel_images / max_parallel_audios）：单个批次内同时识别的最大数，
        #      防"一批 10 张图一次全轰出去"的突发；每个批次使用独立临时信号量
        #   ② 会话级（vlm/stt_max_parallel_per_session）：单个会话累积的最大并行数
        #   ③ 全局级（vlm/stt_max_parallel_global）：所有会话合计的最大并行数
        self.max_parallel_images = int(sec.get("max_parallel_images", 3))
        self.max_parallel_audios = int(sec.get("max_parallel_audios", 3))
        self.vlm_max_parallel_per_session = int(sec.get("vlm_max_parallel_per_session", 15))
        self.vlm_max_parallel_global = int(sec.get("vlm_max_parallel_global", 40))
        self.stt_max_parallel_per_session = int(sec.get("stt_max_parallel_per_session", 15))
        self.stt_max_parallel_global = int(sec.get("stt_max_parallel_global", 40))
        self.media_timeout = float(sec.get("media_timeout", 60.0))
        # 并行识图插件（PIR）自动互斥（默认开）：检测到 PIR 处于启用状态时自动关闭它，
        # 图片识别完全由本模块接管（本模块能力已覆盖 PIR：并行 VLM + 缓存 + 限流 + 转发拍平 + 语音）。
        # 运行时实时检测（与 _pir_active 同理），PIR 热插拔/手动开启后自动再次关闭。
        self.pir_auto_disable = bool(sec.get("pir_auto_disable", True))
        # 官方 VLM 保护网（guard_framework_vlm，默认开）：见 guard_captions()。
        # 目的只有一个 —— 把「官方 VLM 唯一触发条件 `ele.caption is None`」提前堵掉：
        # 框架的渲染发生在所有批次钩子之前，一旦某条消息的图片没被预置 caption，
        # 官方就会付费识图，事后无法挽回。关掉即恢复"框架自己识图"的原始行为
        # （配合 bot_config.capabilities.image_recognition.enabled 一起用）。
        self.guard_enabled = bool(sec.get("guard_framework_vlm", True))
        # ── 预取池（真·预处理）────────────────────────────────────────────
        # 场景：消息已确定进入批次，而上一个批次的 LLM 还在跑 / 本批次还在队列里排队。
        # 这段空窗不该浪费 —— 立刻在后台把 VLM/STT 跑掉；放行时 stage2 直接命中结果，
        # 本批次的关键路径上零识别开销。
        #   _results_pool[sid][media_id] = desc（与 stage2 的 results 同格式，可直接打底）
        #   _pf_tasks[media_id] = Task（去重 + 放行时收尾等待）
        # 预取总开关：由 queue_merge 的「媒体预处理合并限制」控制（关掉 = 彻底不做预处理）
        self.prefetch_enabled = True
        self._results_pool: dict[str, dict] = {}
        self._pf_tasks: dict[str, "asyncio.Task"] = {}
        self._pf_infos: dict[str, dict] = {}
        self.quality_enabled = sec.get("quality_enabled", False)
        self.quality_value = int(sec.get("quality_value", 85))

        self._global_img_sem = asyncio.Semaphore(max(1, self.vlm_max_parallel_global))
        self._global_aud_sem = asyncio.Semaphore(max(1, self.stt_max_parallel_global))
        # 每会话信号量（惰性创建，热重载后自动重建）
        self._session_img_sems: dict[str, asyncio.Semaphore] = {}
        self._session_aud_sems: dict[str, asyncio.Semaphore] = {}

        # VLM 描述语言：读全局 locale.lang；未设置默认中文（对齐并行识图插件中文 DESC_PROMPT）。
        # 实际 prompt 优先取 WebUI 配置 desc_prompt（§_describe_image），此处 lang 仅作默认兜底
        self._vlm_lang = "zh"
        try:
            if hasattr(ctx, "config") and ctx.config is not None:
                cfg_lang = ctx.config.get_config("locale.lang")
                if cfg_lang:
                    self._vlm_lang = str(cfg_lang)
        except Exception:
            pass

        # 原生多模态模式（KiraAI v2.31.0+）：bot_config.capabilities.image_recognition.mode == "native"
        # 时，图片由框架原生多模态直接传给模型（官方压缩 + kira_image_ref 持久化引用），
        # 本模块只做音频 STT，stage1 不再替换 Image/Sticker —— 否则 _build_native_content
        # 遍历 chain 找不到图片元素，原生多模态内容为空（模型收不到图），且 stage2 仍会
        # 调用 VLM 描述，与 native 模式"省 VLM token 直传图片"的初衷冲突。
        # 注意：非唤醒消息的图片仍由宿主 handle_msg 按"非唤醒不识别"策略替换为 [图片] 占位，
        # 只有唤醒消息的图片会保留并走原生多模态 —— 与 z/s 版省 token 设计一致。
        # 模式检测不做 __init__ 快照，改由 _native_mode() 每次事件实时读取（见下）：
        # 用户在 WebUI 直接切换 mode 而不重启/重载时，快照会过时——切到 native 后 stage1 仍
        # 替换图片（原生多模态收不到图）、切回 vlm_description 后图片无人识别（VLM 被跳过）。
        # 实时读配置走内存缓存（微秒级），无卡顿无延迟，WebUI 保存后立即生效。

        # 动态属性挂载名（沿用并行识图插件协议语义）
        self._media_attr = "_pir_media"
        # 当前回合暂存原媒体的 id 索引（stage3 现场识别用）。
        # 按 sid 分层：多会话并发处理时互不串扰
        self._round_media: dict[str, dict[str, dict]] = {}

    # ================= 调试日志 =================

    def _log(self, msg: str):
        logger.debug(msg)

    def _pir_active(self) -> bool:
        """运行时实时检测并行识图插件（PIR）是否已加载，且未被自动互斥关闭。

        语义 v2.3.2 起简化（用户确认）：不再"装了就让位/只做音频"——本模块已覆盖并超越
        PIR（并行 VLM、缓存、三层限流、转发拍平、语音 STT 全都有），PIR 的 stage1 会把
        Image/Sticker 替换为 [Image #id: ] 标识符并删除原元素，破坏 Plus-One 复读表情包。
        因此 pir_auto_disable=True（默认）时：检测到 PIR 启用 → 自动 set_plugin_enabled(False)
        关闭它，图片完全归本模块；关闭失败/竞态（本轮事件 PIR 已先替换）时降级为旧语义
        （图片归 PIR、本模块只做音频），绝不双重处理。
        """
        try:
            pm = getattr(self.ctx, "plugin_mgr", None)
            if pm is None:
                return False
            inst = pm.get_plugin_inst("parallel_image_reader")
            if inst is None:
                return False
            # PIR 已加载：auto-disable 开启则尝试自动关闭（只关一次，防每事件重复 terminate）
            if self.pir_auto_disable:
                if not getattr(self, "_pir_disable_attempted", False):
                    self._pir_disable_attempted = True
                    asyncio.create_task(self._auto_disable_pir())
                # 本轮事件：PIR 处于启用态，stage1 可能已先替换——降级让位，避免双重处理
                return True
            return True  # 互斥关闭：图片归 PIR，本模块只做音频（旧语义）
        except Exception:
            return False

    async def _auto_disable_pir(self):
        """自动关闭并行识图插件（pir_auto_disable=True 时，任务启动后只执行一次）。"""
        if not getattr(self, "pir_auto_disable", False):
            return  # 防御：开关关闭时绝不操作（_pir_active 已保证，双保险）
        try:
            pm = getattr(self.ctx, "plugin_mgr", None)
            if pm is None:
                return
            try:
                enabled = await pm.is_plugin_enabled("parallel_image_reader")
            except TypeError:
                enabled = pm.is_plugin_enabled("parallel_image_reader")
            if enabled:
                try:
                    await pm.set_plugin_enabled("parallel_image_reader", False)
                except TypeError:
                    pm.set_plugin_enabled("parallel_image_reader", False)
                logger.info(
                    "检测到并行识图插件已启用，已自动禁用（pir_auto_disable，"
                    "图片识别由本模块全权接管；如需恢复 PIR 请在 WebUI 关闭本插件的自动互斥开关）"
                )
        except Exception as e:
            logger.warning(f"自动禁用并行识图插件失败（不影响识别）: {type(e).__name__}: {e}")

    def _effective_image_caps(self, sid: Optional[str] = None) -> dict:
        """解析「会话级生效」的 image_recognition 能力，严格对齐框架行为。

        框架 handle_im_batch_message 用的是：
            capabilities = session_mgr.get_effective_capabilities(sid, bot_config.capabilities)
            image_recognition = capabilities.get("image_recognition", {})
        即**会话级覆盖优先于全局**。此前本模块只读全局
        `bot_config.capabilities.image_recognition`，一旦某会话用 WebUI 单独覆盖了
        image_recognition.mode，两边判定就会分叉：
          · 全局 native + 会话 vlm_description → 我们跳过、caption 保持 None
            → 框架认为该 VLM 并自行识别（我们的回填逻辑整条失效）；
          · 全局 vlm_description + 会话 native → 我们照常识别，而框架走原生直传
            （ele.caption 被覆盖为 "attached image"）→ 本次 VLM 完全白跑。
        这里按框架口径取有效能力，消除分叉。
        """
        try:
            global_caps = self.ctx.config.get_config("bot_config.capabilities", {}) or {}
        except Exception:
            global_caps = {}
        if not isinstance(global_caps, dict):
            global_caps = {}
        caps = global_caps
        if sid:
            sm = getattr(self.ctx, "session_mgr", None)
            if sm is not None and hasattr(sm, "get_effective_capabilities"):
                try:
                    eff = sm.get_effective_capabilities(sid, global_caps)
                    if isinstance(eff, dict):
                        caps = eff
                except Exception:
                    pass
        ir = caps.get("image_recognition", {}) if isinstance(caps, dict) else {}
        return ir if isinstance(ir, dict) else {}

    def _native_mode(self, sid: Optional[str] = None) -> bool:
        """运行时实时检测原生多模态模式（KiraAI v2.31.0+）。

        与 _pir_active 同理，不做 __init__ 一次性快照：用户可能在 WebUI 直接切换
        image_recognition.mode 而不重启 Kira / 重载插件，快照会过时。每次事件实时
        读取（框架配置走内存缓存，微秒级），WebUI 保存后立即生效。

        sid 提供时按「会话级生效能力」判定（与框架一致）；未提供时退化为全局配置。
        """
        try:
            ir = self._effective_image_caps(sid)
            mode = ir.get("mode")
            if mode is None:
                mode = self.ctx.config.get_config(
                    "bot_config.capabilities.image_recognition.mode", "vlm_description"
                )
            return str(mode or "").lower() == "native"
        except Exception:
            pass
        return False

    # ================= 三级并发限流（批次级 + 每会话 + 全局） =================

    def _session_sem(self, sems: dict, sid: str, limit: int) -> asyncio.Semaphore:
        """惰性获取/创建某会话的信号量。

        与 _round_media 同样做有界清理：会话数很多时（大群 + 众多私聊），信号量字典
        若无上限会长期缓慢增长。超过 128 个 sid 时按插入顺序淘汰最旧的一半
        （被淘汰的会话再次出现时会重建信号量，代价可忽略）。
        """
        sem = sems.get(sid)
        if sem is None:
            sem = asyncio.Semaphore(max(1, limit))
            if len(sems) > 128:
                for old_sid in list(sems)[: len(sems) - 64]:
                    sems.pop(old_sid, None)
            sems[sid] = sem
        return sem

    # ================= stage1：拍平嵌套 Forward + 替换为标识符 =================

    # 递归遍历/拍平的深度上限：防恶意超深嵌套（Forward 层层套娃）触发 RecursionError。
    # 超深时安全降级——深层 Forward 保留原样，由核心过滤兜底（内容无痕省略但不崩溃）。
    _MAX_CHAIN_DEPTH = 64

    @staticmethod
    def _flatten_forwards(chain, stack=None, depth=0, max_depth=None):
        """就地拍平嵌套 Forward（借鉴并行识图插件 _flatten_forwards，main.py:234-285）。

        KiraAI 核心 message_format_to_text 渲染 Forward 时会过滤嵌套 Forward 元素
        （`[x for x in chain if not isinstance(x, Forward)]`，message_manager.py:371，防无限递归），
        导致嵌套转发的内容（含图片标识符）不进 message_str，LLM 看不到。stage1 先把嵌套
        Forward 展开为平铺元素，保证嵌套内容完整渲染。

        语义：depth=0 的顶层 Forward（消息本身是转发）保留壳；depth>0 的嵌套 Forward
        逐层展开为其子链内容。覆盖路径：Forward.chains 与 Reply.chain。防环：stack 记录
        当前展开路径上的 chain（id），环中子链保留 Forward 元素（核心过滤兜底）。
        深度上限 max_depth（默认 _MAX_CHAIN_DEPTH）：超限不展开（深层内容无痕省略）。
        """
        if max_depth is None:
            max_depth = ParallelMediaRecognizer._MAX_CHAIN_DEPTH
        if stack is None:
            stack = set()
        cid = id(chain)
        if cid in stack:
            return  # 环：同一展开路径上再次出现
        stack.add(cid)
        i = 0
        while i < len(chain):
            ele = chain[i]
            if isinstance(ele, Reply) and ele.chain is not None:
                if depth < max_depth:
                    ParallelMediaRecognizer._flatten_forwards(
                        ele.chain, stack, depth + 1, max_depth)
            elif isinstance(ele, Forward) and ele.chains:
                if depth < max_depth:
                    for c in ele.chains:
                        ParallelMediaRecognizer._flatten_forwards(
                            c, stack, depth + 1, max_depth)
                if depth > 0 and depth < max_depth:
                    # 嵌套 Forward：展开为其子链内容（平铺替换元素本身）
                    expanded = []
                    for c in ele.chains:
                        if id(c) in stack:
                            continue  # 环：跳过该子链（内容无痕省略）
                        expanded.extend(c)
                    if expanded:
                        chain[i:i + 1] = expanded
                        i += len(expanded) - 1
            i += 1
        stack.remove(cid)

    async def guard_captions(self, event: KiraMessageEvent) -> int:
        """官方 VLM 保护网（最早执行，只做一件事：把 caption 从 None 占住）。

        背景：框架的官方 VLM 全项目**只有两个触发点**，条件都是 `ele.caption is None`
        （core/message_manager.py 的 Image / Sticker 分支），而它的渲染发生在
        **所有批次钩子之前**：

            for message in event.messages:                      # ← 先渲染（可能付费识图）
                message_str = await self.message_format_to_text(...)
            for handler in ON_IM_BATCH_MESSAGE handlers: ...     # ← 插件层才轮到

        也就是说：QueueMerge/Midflight 的拦截、本模块 stage2/stage3 的抢救，全都发生在
        "钱已经花掉"之后 —— 插件唯一能阻止官方 VLM 的位置，就是**消息刚到达时**把
        caption 预先占住。stage1 本来就在做这件事，但它跑在 handle_msg 之后（HIGH），
        且会被更早的钩子 stop 掉；一旦漏掉一条，官方就会付费识图，事后无法挽回。

        因此本方法在 SYS_HIGH（比所有 HIGH 钩子都早）做一次**纯占位**：
        caption is None → caption = ""（即官方空占位 `[Image ]` / `[Sticker ]`）。

        刻意不做的事（保证对其它插件零影响）：
          · 不暂存 _pir_media、不预取、不发起任何识别 —— 识别仍由 handle_msg + stage1
            按"仅唤醒识别/概率/超限"决定，省 VLM 的语义完全不变；
          · 不改任何消息策略（不 buffer/discard/stop），不删不换元素；
          · 只在 caption **是 None** 时写 ""，绝不覆盖已有描述；
          · PIR 接管图片 / 原生多模态模式下一律不碰（与 stage1 的跳过条件一致）；
          · 本模块整体关闭（section_media_recognition.enabled=false）时不做，
            这种配置下"框架自己识图"本来就是期望行为。

        返回值 = 占位的媒体个数（供测试/日志用）。
        """
        if not self.enabled or not self.guard_enabled:
            return 0
        try:
            sid = getattr(getattr(event, "session", None), "sid", None)
            # 与 stage1 保持同一套"谁负责图片"的判定：PIR 接管 / native 直传时不插手
            if self._pir_active() or self._native_mode(sid):
                return 0
            n = 0
            for elem in self._iter_media_elems(getattr(getattr(event, "message", None), "chain", None)):
                try:
                    if getattr(elem, "caption", None) is None:
                        elem.caption = ""
                        n += 1
                except Exception:
                    continue
            return n
        except Exception:
            return 0

    async def on_im_message(self, event: KiraMessageEvent, *_):
        """ON_IM_MESSAGE：先拍平嵌套 Forward（防核心渲染丢内容），再处理媒体。

        核心设计 v2.3.2（复读兼容 + 官方格式对齐）：
        - Image/Sticker 元素**保留在 chain 中**（不再替换为标识符/删除）——Plus-One
          复读表情包依赖 chain 里存在 Sticker 元素；图片元素保留则纯图片消息天然
          不参与复读（Plus-One 只认 Text/Sticker）。
        - "识别/不识别"通过**预置 elem.caption** 表达：缓存命中 → desc（框架渲染
          官方 [Image desc, file_path: p]）；未命中 → ""（阻止框架自动 VLM，渲染
          官方空占位 [Image , file_path: p]，LLM 知道有媒体但未识别）。
        - 宿主 handle_msg 已按"仅唤醒识别/识别概率"给不识别媒体打 _media_skip 标记
          （caption=""），本阶段尊重标记：跳过的不暂存不 VLM；唤醒/概率命中的
          未命中媒体才暂存 _pir_media 供 stage2 并行 VLM 后回填官方格式。
        - Record 语音照旧替换为 [Record #id: ] 标识符（阻止框架自动 STT、走本模块
          三层限流 + 缓存；语音不进复读判定，替换无副作用）。
        """
        if not self.enabled:
            return
        try:
            self._flatten_forwards(event.message.chain)
            media: dict[str, dict] = {}
            _sid = getattr(getattr(event, "session", None), "sid", None)
            await self._walk_chain(
                event.message.chain, media, set(),
                is_mentioned=bool(getattr(event, "is_mentioned", False)),
                sid=_sid,
            )
            if media:
                # 合并而非覆盖：并行识图插件（PIR）可能已先写入 Image 索引，
                # 直接覆盖会让它 stage2/stage3 拿不到图片（图片标识符永远空）
                existing = getattr(event.message, self._media_attr, None) or {}
                setattr(event.message, self._media_attr, {**existing, **media})
                # 同时登记到「本会话本回合暂存索引」：stage3 兜底据此在 LLM 请求前
                # 抢救未被回填的媒体。必须在本阶段就登记（不能只在 stage2 登记）——
                # stage2 所在批次可能被第三方插件 stop 掉而根本不执行，那样 stage3
                # 将无从得知有哪些待识别媒体，官方空占位 [Image , file_path: p] /
                # [Sticker ] 就会原样送到 LLM（= 看不见图）。
                if _sid:
                    bucket = self._round_media.setdefault(_sid, {})
                    bucket.update(media)
                    # 有界清理（与 stage2 同范式）：最多保留 128 个 sid 的索引
                    if len(self._round_media) > 128:
                        for old_sid in list(self._round_media)[: len(self._round_media) - 64]:
                            self._round_media.pop(old_sid, None)
        except Exception:
            logger.exception("stage1 error")

    async def _walk_chain(self, chain, media: dict, visited: set, is_mentioned: bool = False,
                          sid: Optional[str] = None):
        """递归遍历 chain（含 Reply.chain / Forward.chains，带环检测）。嵌套 Forward 已拍平。"""
        if chain is None:
            return
        cid = id(chain)
        if cid in visited:
            return
        visited.add(cid)
        for idx, elem in enumerate(chain):
            if isinstance(elem, Text):
                continue
            if isinstance(elem, (Image, Sticker)):
                # 并行识图插件接管中（自动互斥关闭未生效/竞态降级）：图片归它，本模块不碰。
                # 运行时实时检测（不是 __init__ 快照），PIR 热重载/启停后自动生效
                if self._pir_active():
                    continue
                # 原生多模态模式（KiraAI v2.31.0+）：元素保留在 chain 中，
                # 由框架 _build_native_content 收集并直传模型（官方压缩 + 持久化引用）。
                # 本模块不预置 caption、不识别图片，只做音频 STT。
                if self._native_mode(sid):
                    continue
                mtype = "Image" if isinstance(elem, Image) else "Sticker"
                await self._prefill_media(elem, mtype, media)
            elif isinstance(elem, Record):
                replaced = await self._replace_media(elem, "Record", media)
                if replaced is not None:
                    chain[idx] = replaced
            elif isinstance(elem, Reply):
                await self._walk_chain(getattr(elem, "chain", None), media, visited, is_mentioned, sid)
            elif isinstance(elem, Forward):
                for sub in (getattr(elem, "chains", None) or []):
                    await self._walk_chain(sub, media, visited, is_mentioned, sid)

    async def _prefill_media(self, elem, mtype: str, media: dict):
        """图片/表情包 → 预置 caption（元素保留，不替换、不删除）。

        对齐官方渲染（core/message_manager.py：Image → [Image {caption}, file_path: {p}]；
        Sticker → [Sticker {caption}]），并按"仅唤醒识别/概率"省 VLM：
        - 缓存命中 → elem.caption = desc：框架渲染官方带描述格式，零 VLM、零暂存；
        - 未命中且宿主标记 _media_skip（非唤醒仅唤醒开 / 概率未中 / 超限）→ caption=""
          （官方空占位 [Image , file_path: p] / [Sticker ]，LLM 知道有媒体但未识别），不暂存不 VLM；
        - 未命中且未标记（唤醒 / 概率命中）→ caption="" + 暂存 _pir_media，
          stage2 并行 VLM 后回填 message_str（官方格式）与 elem.caption。
        Sticker 与 Image 同规则：元素永远保留 → Plus-One 复读表情包不受识别影响。
        """
        # 宿主 handle_msg 已做"仅唤醒/概率"决策：_media_skip=True = 本次不识别（省 VLM）
        if getattr(elem, "_media_skip", False):
            elem.caption = ""  # 官方空占位 + 阻止框架自动 VLM（caption 非 None）
            return
        try:
            md5 = await elem.hash_image()
        except Exception:
            md5 = None
        short_id = md5[:8] if md5 else f"noid_{id(elem)}"
        # 把本阶段使用的键钉在元素上：框架 handle_im_batch_message 会在渲染前调用
        # compress_image_element()（media.md5 = None + 换文件），随后 message_format_to_text
        # 又会重新 hash_image() → 元素 md5 与 stage1 记录的键不再一致；若 stage2 仍从
        # elem.md5 反推键，就会查不到 results → 识别结果无法合并（VLM 白跑、LLM 看不见图）。
        try:
            elem._pir_short_id = short_id
        except Exception:
            pass
        if md5:
            desc = await self._cache_get(md5) or ""
            if desc and not self._is_valid_desc(desc):
                desc = ""
            if desc:
                # 缓存命中：直接预置官方描述（零 VLM）。不进 media（_done 隐含），
                # 同一批消息重发时无需再处理——stage2 只认 _pir_media 里的媒体。
                elem.caption = desc
                return
        # 未命中：暂存原元素供 stage2 并行识别（唤醒/概率命中路径）
        elem.caption = ""  # 先阻止框架自动 VLM，stage2 识别完成后回填官方格式
        media[short_id] = {"md5": md5, "elem": elem, "type": mtype, "_done": False}

    async def _replace_media(self, elem, mtype: str, media: dict) -> Optional[Text]:
        """语音 Record → 标识符 Text（仅供 Record 使用；图片/表情包走 _prefill_media）。

        语音替换为 [Record #id: ] 标识符：阻止框架自动 STT（串行、无限流），改由
        stage2 并行 STT（三层限流 + image_desc_cache 缓存复用），语义与旧版一致。
        _done 标记：缓存命中（已含内容）或已识别过 → 重发跳过，防重复 STT/429。
        """
        try:
            md5 = await self._record_md5(elem)
        except Exception:
            md5 = None
        if md5:
            short_id = md5[:8]
            desc = await self._cache_get(md5) or ""
            if desc and not self._is_valid_desc(desc):
                desc = ""
        else:
            short_id = f"noid_{id(elem)}"
            desc = ""
        try:
            elem._pir_short_id = short_id  # 同 _prefill_media：把键钉在元素上
        except Exception:
            pass
        media[short_id] = {"md5": md5, "elem": elem, "type": mtype, "_done": bool(desc)}
        if desc:
            # 缓存命中：直接带 file_path（to_path 幂等，_temp_path 已缓存不重复下载）
            p = await self._media_path(elem)
            if p:
                return Text(f"[Record #{short_id}: {desc}, file_path: {p}]")
        return Text(f"[Record #{short_id}: {desc}]")

    async def _media_path(self, elem) -> Optional[str]:
        """对齐原版 message_format_to_text：to_path 落盘后转 data/ 相对路径。

        原版（core/message_manager.py Image 分支）：to_path() → relative_to(data_dir)
        → "data/xxx"，失败降级绝对路径。本模块 stage1 把媒体替换为标识符绕过了
        原版渲染，这里补回 file_path，让 LLM 能拿到本地路径做图生图/上传等。
        """
        try:
            from pathlib import Path
            from core.utils.path_utils import get_data_path
            path = Path(await elem.to_path())
            data_dir = get_data_path()
            try:
                rel = path.relative_to(data_dir)
                return f"data/{rel}"
            except ValueError:
                return str(path)
        except Exception:
            return None

    async def _record_md5(self, elem) -> Optional[str]:
        """音频指纹：to_base64 后取 md5（Record 无 hash_image）。"""
        try:
            b64 = await elem.to_base64()
            if b64.startswith("data:"):
                b64 = b64.split(",", 1)[1]
            return hashlib.md5(base64.b64decode(b64)).hexdigest()
        except Exception:
            return None

    # ================= stage2：并行识别 + 填充（核心） =================

    async def on_im_batch_message(self, event: KiraMessageBatchEvent, *_):
        """ON_IM_BATCH_MESSAGE：收集批次暂存媒体，VLM 与 STT 混合 gather 并行识别，填充。"""
        if not self.enabled:
            return
        try:
            tasks = []  # [(message, media)]
            for message in event.messages:
                media = getattr(message, self._media_attr, None)
                if media:
                    tasks.append((message, media))
            if not tasks:
                return

            # 当前回合原媒体索引（stage3 用）；按 sid 分层防多会话并发串扰，
            # 同一 sid 的并发批次用 setdefault+update 合并，避免后到批次清掉先到批次
            sess_sid = event.session.sid
            self._round_media.setdefault(sess_sid, {})
            for _, media in tasks:
                for short_id, info in media.items():
                    self._round_media[sess_sid][short_id] = info
            # 防无界增长：最多保留 128 个 sid 的索引，超出清最旧
            if len(self._round_media) > 128:
                for old_sid in list(self._round_media)[: len(self._round_media) - 64]:
                    self._round_media.pop(old_sid, None)

            # 本批次里正在预取的媒体：先等它收尾（正常此时早已完成，等待开销≈0）。
            # 这一步保证「预取还没跑完也不会漏」—— 绝不退回空占位。
            _pf_wait = [self._pf_tasks[mid] for _, md in tasks for mid in md if mid in self._pf_tasks]
            if _pf_wait:
                await asyncio.gather(*_pf_wait, return_exceptions=True)

            # 判据 = 结果池里有没有它（不看 _done）：已有描述则跳过（重发同一批不重复 VLM/STT）；
            # 没有描述（含"尝试过但失败/结果被淘汰"）则重新识别 → 保证不会退回空占位
            pending_tasks = [
                (message, {k: v for k, v in media.items() if not self._has_desc(sess_sid, k)})
                for message, media in tasks
            ]
            pending_tasks = [(m, md) for m, md in pending_tasks if md]
            # 原生多模态模式：图片/表情包已由框架直传模型，stage2 只做音频 STT
            if self._native_mode(sess_sid):
                pending_tasks = [
                    (m, {k: v for k, v in md.items() if v.get("type") not in ("Image", "Sticker")})
                    for m, md in pending_tasks
                ]
                pending_tasks = [(m, md) for m, md in pending_tasks if md]

            # 混合并行：图片 VLM 与 音频 STT 同一 gather，各自限流互不阻塞。
            # 批次级信号量：每批次临时创建，限制本批次内同时识别的数量（突发保护）
            batch_img_sem = asyncio.Semaphore(max(1, self.max_parallel_images))
            batch_aud_sem = asyncio.Semaphore(max(1, self.max_parallel_audios))
            # 预取池打底：排队 / 上一批次期间已完成的预取结果直接命中 → 关键路径零开销
            results: dict[str, str] = dict(self._results_pool.get(sess_sid) or {})
            coros = []
            for _, media in pending_tasks:
                for short_id, info in media.items():
                    # 创建 coro 前就设 _done=True：防并发 stage2 重复建 coro
                    info["_done"] = True
                    if info["type"] in ("Image", "Sticker"):
                        coros.append(self._describe_one(sess_sid, short_id, info, results, batch_sem=batch_img_sem))
                    else:
                        coros.append(self._transcribe_one(sess_sid, short_id, info, results, batch_sem=batch_aud_sem))
            await asyncio.gather(*coros, return_exceptions=True)

            # 预取 file_path（to_path 落盘 + data/ 相对路径），填充时带上，
            # 对齐原版 message_format_to_text 的 [Image desc, file_path: xxx] 格式
            paths: dict[str, str] = {}
            for _, media in tasks:
                for sid, info in media.items():
                    if sid in results and sid not in paths and info.get("elem") is not None:
                        p = await self._media_path(info["elem"])
                        if p:
                            paths[sid] = p

            # 填充 message_str 与 chain
            for message, media in tasks:
                hit = any(sid in results for sid in media)
                if hit:
                    if message.message_str:
                        message.message_str = self._fill_message_str(
                            message.message_str, results, paths, message.chain)
                    self._fill_chain(message.chain, results, paths)
        except Exception:
            logger.exception("stage2 error")

    def _fill_message_str(self, text: str, results: dict, paths: dict,
                          chain=None) -> str:
        """填充 message_str：以 chain 里 Image/Sticker 元素（官方空占位锚点）优先，
        找不到时按 [Media #id: ] 标识符兜底（语音 Record / 历史遗留标识符）。

        chain 优先：官方格式占位的 file_path 与 chain 元素逐位对应，按序替换
        （同一消息多个 [Image , file_path: data/x] 各自独立、互不误伤）。
        chain 不可得/无匹配时退回 _fill_text（Record 标识符与旧格式兼容）。
        """
        if chain is not None:
            # 递归遍历 chain 中所有 Image/Sticker（含 Reply.chain / Forward.chains），
            # 按官方空占位顺序逐一回填——嵌套引用/转发里的媒体同样生效
            replaced = False
            for elem in self._iter_media_elems(chain):
                filled = self._fill_official(elem, results, paths)
                if not filled or not filled[0]:
                    continue
                short_id, desc, p = filled
                mtype = "Image" if isinstance(elem, Image) else "Sticker"
                new_text = self._fill_official_text(text, mtype, desc, p)
                if new_text != text:
                    text = new_text
                    replaced = True
            if replaced:
                return text
        # chain 无 Image/Sticker 命中：退回标识符填充（Record / 嵌套链 / 兼容）
        return self._fill_text(text, results, paths)

    @staticmethod
    def _iter_media_elems(chain):
        """递归 yield chain 内所有 Image/Sticker（含 Reply.chain / Forward.chains，防环）。"""
        seen = set()
        def _walk(c):
            if c is None:
                return
            cid = id(c)
            if cid in seen:
                return
            seen.add(cid)
            for ele in c:
                if isinstance(ele, (Image, Sticker)):
                    yield ele
                elif isinstance(ele, Reply):
                    yield from _walk(getattr(ele, "chain", None))
                elif isinstance(ele, Forward):
                    for sub in (getattr(ele, "chains", None) or []):
                        yield from _walk(sub)
        yield from _walk(chain)

    def _batch_media_ids(self, event) -> set:
        """本批次消息链里**实际存在**的媒体 id 集合（stage3 抢救的准入条件）。

        键与 stage1 一致（`elem._pir_short_id`，stage1 在 _prefill_media /
        _replace_media 里钉在元素上）。用它而不是文本锚点来判定「这条媒体在不在
        这次请求里」——官方空占位的锚点是通配的（见 on_llm_request ① 处注释）。
        """
        ids = set()
        for m in (getattr(event, "messages", None) or []):
            for elem in self._iter_media_elems(getattr(m, "chain", None)):
                sid_ = getattr(elem, "_pir_short_id", None)
                if sid_:
                    ids.add(sid_)
        return ids

    # ============ 预取（真·预处理）：排队 / 上一批次还在跑时先识别 ============

    def _has_desc(self, sid: str, media_id: str) -> bool:
        """该媒体**是否已有描述结果**（唯一判据 = 结果池里有它）。

        刻意不看 _done：_done 只代表"尝试过"。识别失败或结果被淘汰时，
        不该让这张图永远拿不到描述（那就会退回空占位）—— 池里没有就再试一次。
        """
        return media_id in (self._results_pool.get(sid) or {})

    def cancel_prefetch(self, media_ids) -> int:
        """取消这些媒体的**在飞**预取（批次被丢弃时调用：这批不进 LLM 了，别再烧 VLM）。

        已完成的识别结果**保留**在结果池 —— 按 md5 命中，之后同图再出现时免费复用。
        """
        n = 0
        for mid in list(media_ids or ()):
            task = self._pf_tasks.pop(mid, None)
            if task is not None and not task.done():
                task.cancel()
                # 取消 ≠ 已识别：必须把 _done 复位，否则该媒体之后进了别的批次时
                # stage2 会认为"已处理"而跳过 → 退回空占位（这才是真正的漏图）
                info = self._pf_infos.pop(mid, None)
                if isinstance(info, dict):
                    info["_done"] = False
                n += 1
        return n

    @staticmethod
    def collect_prefetch_ids(messages) -> set:
        """取出一批消息里「预取用到的 media_id」（丢批次时用它取消）。"""
        ids = set()
        for m in (messages or []):
            for k in (getattr(m, "_pir_media", None) or {}):
                ids.add(k)
            stack = [getattr(m, "chain", None)]
            seen = set()
            while stack:
                ch = stack.pop()
                if ch is None or id(ch) in seen:
                    continue
                seen.add(id(ch))
                for ele in ch:
                    sid_ = getattr(ele, "_pir_short_id", None)
                    if sid_:
                        ids.add(sid_)
                    sub = getattr(ele, "chain", None)
                    if sub is not None:
                        stack.append(sub)
                    for fwd in (getattr(ele, "chains", None) or []):
                        stack.append(fwd)
        return ids

    def schedule_prefetch(self, sid: str, messages) -> None:
        """非阻塞入口：把这些消息里的媒体丢给后台识别。

        调用时机 = 「消息已确定进入批次」（handle_msg 里 event.buffer() 之后）：
        此时它一定会被送进 LLM，识别不会白做；而这段时间多半正是
        「上一个批次的 LLM 还在跑 / 本批次在队列里排队」的空窗 —— 正好用掉。
        被 discard 的消息走不到这里 → 不会浪费 VLM。
        """
        if not self.enabled or not self.prefetch_enabled:
            return
        msgs = [m for m in (messages or []) if m is not None]
        if not msgs:
            return
        try:
            asyncio.create_task(self._prefetch_worker(sid, msgs))
        except Exception as e:
            logger.debug(f"prefetch schedule failed: {type(e).__name__}: {e}")

    async def _prefetch_worker(self, sid: str, messages) -> None:
        """收集待识别媒体 → 起后台识别任务（与 stage2 共用缓存 / 限流 / 结果池）。"""
        try:
            if self._pir_active() or self._native_mode(sid):
                return
            media: dict = {}
            for m in messages:
                # 只取 stage1 已登记的「待识别」媒体（含已被替换掉的 Record 语音）。
                # 被显式判定不识别的媒体（仅唤醒识别的非唤醒媒体 / 概率未中 / 超上限）
                # 不在这里 —— 那是配置要求的省 VLM，预取它等于绕过用户的开关。
                for k, v in (getattr(m, "_pir_media", None) or {}).items():
                    if isinstance(v, dict) and not v.get("_done") and k not in media:
                        media[k] = v
            if not media:
                return
            pool = self._results_pool.setdefault(sid, {})
            # 防无界增长：最多保留 128 个会话的结果池
            if len(self._results_pool) > 128:
                for old in list(self._results_pool)[: len(self._results_pool) - 64]:
                    self._results_pool.pop(old, None)
            # 单会话结果池同样有界（防长时间运行无界增长）
            if len(pool) > 512:
                for old in list(pool)[: len(pool) - 256]:
                    pool.pop(old, None)
            batch_img_sem = asyncio.Semaphore(max(1, self.max_parallel_images))
            batch_aud_sem = asyncio.Semaphore(max(1, self.max_parallel_audios))
            for media_id, info in media.items():
                if self._has_desc(sid, media_id) or media_id in self._pf_tasks:
                    continue          # 已有描述 → 不重复；正在飞 → 去重（失败后允许再试）
                info["_done"] = True
                if info["type"] in ("Image", "Sticker"):
                    coro = self._describe_one(sid, media_id, info, pool, batch_sem=batch_img_sem)
                else:
                    coro = self._transcribe_one(sid, media_id, info, pool, batch_sem=batch_aud_sem)
                task = asyncio.ensure_future(coro)
                self._pf_tasks[media_id] = task
                self._pf_infos[media_id] = info
                task.add_done_callback(
                    lambda t, mid=media_id, inf=info, pl=pool: self._prefetch_done(mid, inf, pl)
                )
        except Exception as e:
            logger.debug(f"prefetch worker failed: {type(e).__name__}: {e}")

    def _prefetch_done(self, media_id: str, info: dict, pool: dict) -> None:
        """预取收尾：写回 elem.caption（渲染即为带描述格式），并清任务表。"""
        self._pf_tasks.pop(media_id, None)
        self._pf_infos.pop(media_id, None)
        # _done 仅用于"同批次内防并发重复建 coro"；是否算"已处理"一律以结果池为准
        # （见 _has_desc）：失败/被淘汰的媒体会在后续阶段自动再试一次，不会留下空占位。
        # 取消路径复位 _done，保证语义一致。
        try:
            desc = pool.get(media_id)
            elem = info.get("elem")
            if desc and elem is not None and not (getattr(elem, "caption", None) or "").strip():
                elem.caption = desc      # 既阻止框架自动 VLM，又让渲染直接带描述
        except Exception:
            pass

    async def _describe_one(self, sess_sid: str, media_id: str, info: dict, results: dict,
                            batch_sem: Optional[asyncio.Semaphore] = None):
        md5 = info["md5"]
        cached = await self._cache_get(md5) if md5 else None
        if cached:
            info["_done"] = True
            results[media_id] = cached
            return
        try:
            sess_sem = self._session_sem(self._session_img_sems, sess_sid, self.vlm_max_parallel_per_session)
            # 三层限流：批次级 → 会话级 → 全局级（固定获取顺序，无死锁）
            if batch_sem is not None:
                async with batch_sem, sess_sem, self._global_img_sem:
                    desc = await asyncio.wait_for(self._describe_image(info["elem"], sess_sid), self.media_timeout)
            else:
                async with sess_sem, self._global_img_sem:
                    desc = await asyncio.wait_for(self._describe_image(info["elem"], sess_sid), self.media_timeout)
            # 无论成功失败都标记已处理：同一条消息重发不再重复识别（防 429 风暴）
            info["_done"] = True
            if desc and self._is_valid_desc(desc):
                if md5:
                    await self._cache_set(md5, desc)
                # 记下 dHash：同图被第三方插件重新下载/重压缩成另一字节流时，仍能命中描述
                await self._phash_remember(info.get("elem"), desc)
                results[media_id] = desc
            else:
                logger.warning(f"image VLM returned empty/invalid desc id={media_id} md5={md5[:8] if md5 else 'n/a'}")
                results[media_id] = "(未识别)"
        except Exception as e:
            info["_done"] = True
            logger.warning(f"image describe failed id={media_id}: {type(e).__name__}: {e}")
            results[media_id] = "(未识别)"

    async def _transcribe_one(self, sess_sid: str, media_id: str, info: dict, results: dict,
                              batch_sem: Optional[asyncio.Semaphore] = None):
        md5 = info["md5"]
        cached = await self._cache_get(md5) if md5 else None
        if cached:
            info["_done"] = True
            results[media_id] = cached
            return
        try:
            provider_mgr = getattr(self.ctx, "provider_mgr", None)
            stt_client = provider_mgr.get_default_stt() if provider_mgr is not None else None
            if stt_client is None:
                info["_done"] = True
                logger.warning(f"STT client unavailable (no default STT model) id={media_id}")
                results[media_id] = "(未识别)"
                return
            sess_sem = self._session_sem(self._session_aud_sems, sess_sid, self.stt_max_parallel_per_session)
            # 三层限流：批次级 → 会话级 → 全局级（固定获取顺序，无死锁）
            if batch_sem is not None:
                async with batch_sem, sess_sem, self._global_aud_sem:
                    text = await asyncio.wait_for(
                        speech_to_text(client=stt_client, record=info["elem"]), self.media_timeout)
            else:
                async with sess_sem, self._global_aud_sem:
                    text = await asyncio.wait_for(
                        speech_to_text(client=stt_client, record=info["elem"]), self.media_timeout)
            # 无论成功失败都标记已处理：同一条消息重发不再重复识别（防 429 风暴）
            info["_done"] = True
            if text and self._is_valid_desc(text):
                if md5:
                    await self._cache_set(md5, text)
                results[media_id] = text
            else:
                logger.warning(f"STT returned empty/invalid text id={media_id}")
                results[media_id] = "(未识别)"
        except Exception as e:
            info["_done"] = True
            logger.warning(f"STT failed id={media_id}: {type(e).__name__}: {e}")
            results[media_id] = "(未识别)"

    async def _describe_image(self, elem, sid: Optional[str] = None) -> str:
        """图片 VLM：统一 to_data_url → vlm.chat 路径（对齐并行识图插件已验证路径）；
        to_data_url 失败时 fallback 直接 httpx 下载（带 UA + pixiv Referer，覆盖图床防盗链）；
        quality_enabled 时 JPEG 压缩。失败返回 ""（调用方降级为 (未识别) 并打日志）。"""
        try:
            vlm = self.ctx.provider_mgr.get_default_vlm()
            if vlm is None:
                logger.warning("get_default_vlm() returned None")
                return ""
            # 可观测性：框架的 desc_img() 会打 "Describing image using …"，而我们直接
            # vlm.chat()（绕过了那层包装）→ 成功时不打任何日志，日志里无法分辨一次识图
            # 是本插件发起还是框架自己发起的。这里补一条**与官方同款文案 + 同款紫色**的
            # 日志（本模块 logger 名为 "MediaRecognize"、颜色 purple，故前缀即
            # [MediaRecognize]），便于对照排查（谁在识图、用的是哪个模型）。
            try:
                _mdl = vlm.model
                logger.info(
                    f"Describing image using {_mdl.model_id} ({_mdl.provider_name})"
                )
            except Exception:
                pass
            data_url = None
            try:
                data_url = await elem.to_data_url()
            except Exception as e:
                logger.debug(f"to_data_url failed ({type(e).__name__}), try direct download")
                data_url = await self._try_direct_download(elem)
            if not data_url:
                logger.warning(
                    f"cannot fetch image data: "
                    f"file_type={getattr(elem, 'file_type', '?')} "
                    f"file={str(getattr(elem, 'file', ''))[:80]}"
                )
                return ""
            if self.quality_enabled:
                _, _, b64 = data_url.partition(",")
                if not b64:
                    logger.warning("empty base64 after to_data_url")
                    return ""
                img = _open_image(base64.b64decode(b64))
                q = max(10, min(100, self.quality_value))
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=q)
                data_url = f"data:image/jpeg;base64,{base64.b64encode(buf.getvalue()).decode()}"
            prompt = self._vlm_prompt(sid)
            request = LLMRequest(messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
                    {"type": "text", "text": prompt},
                ],
            }])
            resp = await vlm.chat(request)
            return (resp.text_response or "").strip() if resp else ""
        except Exception as e:
            logger.warning(f"describe image failed: {type(e).__name__}: {e}")
            return ""

    def _vlm_prompt(self, sid: Optional[str] = None) -> str:
        """VLM 描述词：跟随 WebUI 配置 image_recognition.desc_prompt（对齐框架
        message_format_to_text 行为：框架同样从「会话级生效能力」里取 desc_prompt）；
        未配置/为空时用 locale.lang 语言默认 prompt。"""
        try:
            desc_prompt = (self._effective_image_caps(sid).get("desc_prompt") or "") or ""
            if desc_prompt.strip():
                return desc_prompt.strip()
        except Exception:
            pass
        return get_default_vlm_prompt(self._vlm_lang)

    async def _try_direct_download(self, elem) -> Optional[str]:
        """to_data_url 失败时：直接 httpx 下载图片（带 UA，pixiv 图床补 Referer），返回 data_url 或 None。"""
        url = getattr(elem, "file", None) or getattr(elem, "image", None)
        if not url or not str(url).startswith(("http://", "https://")):
            return None
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            }
            async with httpx.AsyncClient(follow_redirects=True, timeout=self.media_timeout) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code != 200:
                    # pixiv 图床防盗链：补 Referer 重试
                    resp = await client.get(url, headers={**headers, "Referer": "https://www.pixiv.net/"})
                if resp.status_code == 200 and resp.content:
                    return "data:image/jpeg;base64," + base64.b64encode(resp.content).decode()
        except Exception as e:
            logger.debug(f"direct download failed: {type(e).__name__}: {e}")
        return None

    # ================= stage3：历史/残留标识符兜底 =================

    async def on_llm_request(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        """ON_LLM_REQUEST：扫描 req.user_prompt 残留空标识符：缓存命中填、有原媒体现场识别、否则 (已过期)。"""
        if not self.enabled:
            return
        try:
            need: dict[str, str] = {}  # sid -> 标识符/占位形态
            for p in getattr(req, "user_prompt", []) or []:
                text = getattr(p, "content", "") or ""
                for m in _ALL_RE.finditer(text):
                    if not m.group(2).strip():
                        need[m.group(1)] = m.group(0)
            # —— 官方空占位兜底 ——
            # 本模块把「待识别」表达为 caption=""（框架渲染成 [Image , file_path: p] /
            # [Sticker ]）。若 stage2 因异常或第三方插件 stop 批次而未回填，上面的标识符
            # 匹配抓不到这类占位 → LLM 会收到毫无描述的空占位（表现为"看不见图"）。
            # 这里按本会话暂存索引反查「caption 仍为空」的媒体，只要请求文本里确实带着
            # 对应空占位，就一并纳入抢救（有原媒体 → 现场识别；已识别过 → (未识别)）。
            official: dict[str, tuple] = {}   # media_id -> (elem, mtype)
            round_media = self._round_media.setdefault(event.sid, {})
            _texts = [getattr(pp, "content", "") or "" for pp in (getattr(req, "user_prompt", []) or [])]

            def _anchor_of(elem, mtype, path, mid=None):
                # 锚点必须跟随元素**当前**的 caption：渲染用的是 `[Image {caption}, file_path: p]`，
                # caption 为空 → `[Image , file_path: p]`；失败占位 → `[Image (未识别), file_path: p]`。
                # 否则「失败后重试」会因为锚点对不上而永远救不回来。
                _cap = str(getattr(elem, "caption", None) or "")
                if mtype == "Image":
                    return f"[Image {_cap}, file_path: {path}]" if path else f"[Image {_cap}]"
                if mtype == "Record":
                    # 语音走标识符路径（stage1 已把 Record 换成 Text [Record #id: ]）
                    return f"[Record #{mid}: ]" if mid else None
                return f"[Sticker {_cap}]"

            # ① 本会话暂存索引（我方 stage1 认领过的媒体）
            # 准入条件：**元素必须真的出现在本批次消息链里**。
            # 暂存索引是按 ON_IM_MESSAGE 事件登记的，里面可能混入**根本不该被识别**的媒体：
            # 框架的钩子循环只在 stop() 时中断（core/message_manager.py），discard() 不中断，
            # 所以 bot 自己发出、被适配器回显、宿主已判"丢弃"的消息，stage1 照样会登记它。
            # 只按文本锚点反查是不安全的：官方空占位里 Sticker 是 "[Sticker ]"、Image 落盘
            # 失败时是 "[Image ]"，**都是通配的**（无路径也无 id），任意一张未识别媒体都能让
            # 它们命中 → 会把不在请求里的媒体也送去 VLM（识别了不该识别的对象，并把该媒体
            # 的描述/路径填进了别的媒体的空占位）。
            _batch_ids = self._batch_media_ids(event)
            for mid, info in list(round_media.items()):
                if mid in need or not isinstance(info, dict):
                    continue
                if mid not in _batch_ids:
                    self._log(f"{mid} 不在本批次消息链中（如 bot 自身消息被回显），不参与抢救")
                    continue
                elem, mtype = info.get("elem"), info.get("type")
                if elem is None or mtype not in ("Image", "Sticker", "Record"):
                    continue
                if mtype == "Record":
                    # 语音元素已被换成 Text 标识符，用「标识符是否仍为空」判空
                    if not any(f"[Record #{mid}: ]" in t for t in _texts):
                        continue
                else:
                    try:
                        _cap = (getattr(elem, "caption", None) or "").strip()
                        if _cap and _cap != "(未识别)":
                            continue            # 已有真描述才跳过；(未识别) 允许再试一次
                    except Exception:
                        continue
                anchor = _anchor_of(elem, mtype, await self._media_path(elem), mid)
                if anchor and any(anchor in t for t in _texts):
                    official[mid] = (elem, mtype)
                    need[mid] = anchor

            # ② 直接扫描本批次消息链：捕获「我方完全没认领（caption is None / ""）
            #    但请求里仍是空占位」的媒体——例如消息在 ON_IM_MESSAGE 阶段被其它插件
            #    stop 掉，我方 stage1 从未执行；或批次被第三方拦截后消息才进入请求。
            #    这是最后一道保险：凡是请求里出现空占位、而我们又确实拿到了原元素，
            #    就在这里补齐描述，绝不让空占位进 LLM。
            if not self._pir_active() and not self._native_mode(event.sid):
                for _m in (getattr(event, "messages", None) or []):
                    for _elem in self._iter_media_elems(getattr(_m, "chain", None)):
                        if not isinstance(_elem, (Image, Sticker)):
                            continue
                        _cap = str(getattr(_elem, "caption", None) or "").strip()
                        if _cap and _cap != "(未识别)":
                            continue            # 已有真描述才跳过；(未识别) 允许再试一次
                        # PIR 的跳过标记：一律尊重（并行识图插件的分工）
                        if getattr(_elem, "_pir_skip", False):
                            continue
                        # 我方**显式**「不识别」标记，一律尊重：
                        #   mention     = 仅唤醒识别开启时的非唤醒媒体（用户要省这笔 VLM）
                        #   probability = 概率未中（用户掷骰子要省）
                        #   cap         = 超出每消息媒体上限（防图片轰炸）
                        # 这些保持空占位是**配置的既定代价**，不是 bug，绝不能在这里偷偷补。
                        # 这里只救「本该识别却没补上」的：没有跳过标记、caption 仍为空
                        # （stage2 没跑 / md5 键变化 / 批次被第三方插件截断等）。
                        if getattr(_elem, "_media_skip", False):
                            continue
                        mtype = "Sticker" if isinstance(_elem, Sticker) else "Image"
                        anchor = _anchor_of(_elem, mtype, await self._media_path(_elem))
                        if not any(anchor in t for t in _texts):
                            continue
                        _md5 = None
                        try:
                            _md5 = await _elem.hash_image()
                        except Exception:
                            _md5 = None
                        mid = _md5[:8] if _md5 else f"noid_{id(_elem)}"
                        if mid in need:
                            continue
                        round_media.setdefault(mid, {
                            "md5": _md5, "elem": _elem, "type": mtype, "_done": False,
                        })
                        try:
                            _elem._pir_short_id = mid
                        except Exception:
                            pass
                        official[mid] = (_elem, mtype)
                        need[mid] = anchor
            # ③ 文本级兜底：官方空占位按 file_path 的**内容哈希**补齐（零 VLM）。
            #    必须放在 `if not need: return` **之前**——今天的泄露正是这样溜走的：
            #    第三方插件（会话合并/上下文压缩）重建请求后链上已无元素，只剩文本里的
            #    空占位 + 框架已下载的临时文件（同图不同名）→ need 为空 → 直接 return
            #    → 框架 agent 内部渲染该元素时 caption is None → 官方 VLM 付费调用。
            try:
                await self._fill_empty_official_by_path(req, event.sid)
            except Exception as e:
                logger.warning(f"空占位按文件内容补齐失败 <{event.sid}>: {type(e).__name__}: {e}")
            if not need:
                return
            try:
                _rm = self._round_media.setdefault(event.sid, {})
                _why: dict[str, int] = {}
                for _mid in need:
                    _el = (_rm.get(_mid) or {}).get("elem")
                    _r = (getattr(_el, "_media_skip_reason", "") or "stage2未回填") if _el is not None else "stage2未回填"
                    _why[_r] = _why.get(_r, 0) + 1
                _detail = ", ".join(f"{k}×{v}" for k, v in _why.items())
                logger.info(f"空占位兜底 <{event.sid}>：抢救 {len(need)} 个媒体"
                            f"（{_detail}）——这批已进 LLM，不留空占位")
            except Exception:
                pass
            results: dict[str, str] = {}
            # 批次级限流同样作用于 stage3 兜底识别（一个 LLM 请求内的残留标识符 = 一个批次）
            batch_img_sem = asyncio.Semaphore(max(1, self.max_parallel_images))
            batch_aud_sem = asyncio.Semaphore(max(1, self.max_parallel_audios))
            coros = []
            # 只查本会话当前回合暂存的媒体（按 sid 分层，多会话不串扰）
            round_media = self._round_media.get(event.sid, {})
            for media_id in need:
                info = round_media.get(media_id)
                if info and not self._has_desc(event.sid, media_id):
                    # 有原媒体且未识别过 → 现场识别
                    # 注意：Sticker 与 Image 一样走 VLM 描述（与 stage2 的
                    # `type in ("Image","Sticker")` 判定保持一致），只有 Record 走 STT。
                    if info["type"] in ("Image", "Sticker"):
                        # 原生多模态模式：图片不识别，直接标 (未识别) 占位
                        if self._native_mode(event.sid):
                            results[media_id] = "(未识别)"
                            continue
                        coros.append(self._describe_one(event.sid, media_id, info, results, batch_sem=batch_img_sem))
                    else:
                        coros.append(self._transcribe_one(event.sid, media_id, info, results, batch_sem=batch_aud_sem))
                elif info:
                    # 已识别过但占位符仍空（异常路径）：直接标未识别，不重复撞模型
                    results[media_id] = "(未识别)"
                else:
                    results[media_id] = "(已过期)"
            if coros:
                await asyncio.gather(*coros, return_exceptions=True)
            # 预取 file_path（stage3 兜底同样带路径，与 stage1/stage2 格式一致）
            paths: dict[str, str] = {}
            for media_id in need:
                info = round_media.get(media_id)
                if info and info.get("elem") is not None:
                    p = await self._media_path(info["elem"])
                    if p:
                        paths[media_id] = p
            for p in getattr(req, "user_prompt", []) or []:
                text = getattr(p, "content", "") or ""
                new_text = self._fill_text(text, results, paths)
                # 官方空占位替换（stage3 抢救路径专用）
                for mid, (elem, mtype) in official.items():
                    _desc = results.get(mid)
                    if _desc is None:
                        continue
                    new_text = self._fill_official_text(new_text, mtype, _desc, paths.get(mid, ""),
                                                        old_anchor=need.get(mid))
                    try:
                        elem.caption = _desc        # 同步写回元素，避免二次渲染仍是空占位
                    except Exception:
                        pass
                if new_text != text:
                    p.content = new_text
        except Exception:
            logger.exception("stage3 error")
        finally:
            # 无论正常/异常/提前 return 都清理本会话暂存媒体索引，防单 sid 无限累积（内存泄漏）。
            # stage2 的 setdefault+update 是同步原子块，pop 后新批次会重建，无并发风险
            self._round_media.pop(event.sid, None)

    # ================= 填充 =================

    def _fill_text(self, text: str, results: dict, paths: Optional[dict] = None) -> str:
        """按 [Media #id: ] 标识符填充（Record 语音标识符；兼容历史/占位模式遗留标识符）。"""
        for sid, desc in results.items():
            # 用 str.replace 而非 re.sub：replacement 是模板字符串，desc 含 \U/\x 等
            # 反斜杠序列（如 Windows 路径）会抛 bad escape；replace 无转义问题
            fp = ""
            if paths and sid in paths:
                fp = f", file_path: {paths[sid]}"
            text = text.replace(f"[Image #{sid}: ]", f"[Image #{sid}: {desc}{fp}]")
            text = text.replace(f"[Record #{sid}: ]", f"[Record #{sid}: {desc}{fp}]")
        return text

    def _fill_official(self, elem, results: dict, paths: Optional[dict] = None):
        """官方格式回填：把识别结果写回 Image/Sticker 元素（chain 保留原元素）。

        对齐框架渲染（core/message_manager.py）：
          Image  → [Image {caption}, file_path: {p}]
          Sticker→ [Sticker {caption}]（官方无 file_path；本模块增强追加 , file_path: {p}，
                   让 LLM 也能拿到表情包本地路径做图生图/上传——复读不受影响，元素始终保留）
        返回 (short_id, desc, path) 供 _fill_official_text 在 message_str 里锚点替换；
        找不到对应 media 时返回 None。
        """
        # 键来源优先级：
        #   1) 元素上由 stage1 钉下的 _pir_short_id —— 最可靠。框架在渲染前会压缩图片
        #      （media.md5 = None）并在渲染时重新 hash_image()，此时 elem.md5 与 stage1
        #      记录的键已经不同；若从 md5 反推必然查不到，识别结果会被静默丢弃。
        #   2) 兼容未打标记的元素（旧数据/其它 code path）：elem.md5 → noid_{id} 兜底。
        key = getattr(elem, "_pir_short_id", None)
        desc = results.get(key) if key else None
        if desc is None:
            md5 = None
            try:
                md5 = getattr(elem, "md5", None) or None
            except Exception:
                md5 = None
            if md5:
                cand = md5[:8]
                if cand in results:
                    key, desc = cand, results[cand]
        if desc is None:
            alt = f"noid_{id(elem)}"
            if alt in results:
                key, desc = alt, results[alt]
        if desc is None or not key:
            return None
        p = (paths or {}).get(key, "")
        return (key, desc, p)

    def _fill_official_text(self, text: str, mtype: str, desc: str, p: str,
                            old_anchor: Optional[str] = None) -> str:
        """把 message_str 里的官方空占位替换为带描述的官方格式（只替换第一处）。

        空占位形态（caption="" 时框架渲染）：
          Image  → "[Image , file_path: {p}]"（to_path 成功）或 "[Image ]"（落盘失败降级）
          Sticker→ "[Sticker ]"
        识别后形态："[Image {desc}, file_path: {p}]" / "[Sticker {desc}, file_path: {p}]"
        """
        if old_anchor:
            # stage3 抢救：按文本里**实际存在**的锚点精确替换
            # （caption 可能已是 "(未识别)" → 形态为 "[Image (未识别), file_path: p]"）
            if mtype == "Image":
                filled = f"[Image {desc}, file_path: {p}]" if p else f"[Image {desc}]"
            elif mtype == "Sticker":
                filled = f"[Sticker {desc}, file_path: {p}]" if p else f"[Sticker {desc}]"
            else:
                filled = None
            return text.replace(old_anchor, filled, 1) if filled else text
        if mtype == "Image":
            filled = f"[Image {desc}, file_path: {p}]" if p else f"[Image {desc}]"
            if p:
                text = text.replace(f"[Image , file_path: {p}]", filled, 1)
            return text.replace("[Image ]", filled, 1)
        filled = f"[Sticker {desc}, file_path: {p}]" if p else f"[Sticker {desc}]"
        return text.replace("[Sticker ]", filled, 1)

    def _fill_chain(self, chain, results: dict, paths: Optional[dict] = None):
        """回填 chain：Image/Sticker 元素写回 caption（元素保留）；Text 内 Record/历史标识符替换。"""
        if chain is None:
            return
        for elem in chain:
            if isinstance(elem, Text):
                # 与 _fill_text 一致：全文 replace（不依赖 match 只匹配开头），
                # 避免 Text 前有前缀时 chain 漏填而 message_str 已填的不一致
                new_text = self._fill_text(elem.text or "", results, paths)
                if new_text != elem.text:
                    elem.text = new_text
            elif isinstance(elem, (Image, Sticker)):
                filled = self._fill_official(elem, results, paths)
                if filled:
                    short_id, desc, p = filled
                    elem.caption = desc
            elif isinstance(elem, Reply):
                self._fill_chain(getattr(elem, "chain", None), results, paths)
            elif isinstance(elem, Forward):
                for sub in (getattr(elem, "chains", None) or []):
                    self._fill_chain(sub, results, paths)

    # ================= 缓存（复用 image_desc_cache 表） =================

    # ================= 空占位「按文件内容」兜底 =================

    @staticmethod
    async def _read_media_bytes(path) -> Optional[bytes]:
        """读媒体文件字节（兼容 Windows/Linux 两种分隔符写法）。"""
        if not path:
            return None
        raw = str(path).strip()

        def _read():
            for cand in (raw, raw.replace("\\", "/"), raw.replace("/", "\\")):
                try:
                    with open(cand, "rb") as f:
                        return f.read()
                except Exception:
                    continue
            return None

        try:
            return await asyncio.to_thread(_read)
        except Exception:
            return None

    @staticmethod
    def _phash_of_bytes(data: Optional[bytes]) -> Optional[str]:
        """64bit dHash（9x8 灰度相邻差）——抗压缩/缩放，用于匹配「同图不同字节」的副本。"""
        if not data:
            return None
        try:
            from PIL import Image as _PILImage
            im = _PILImage.open(BytesIO(data)).convert("L").resize((9, 8))
            px = list(im.getdata())
            bits = 0
            for row in range(8):
                base = row * 9
                for col in range(8):
                    bits = (bits << 1) | (1 if px[base + col] > px[base + col + 1] else 0)
            if bits in (0, (1 << _PHASH_BITS) - 1):
                return None          # 退化哈希（纯色/纯渐变）：不能作为「同图」的依据
            return f"{bits:016x}"
        except Exception:
            return None

    @staticmethod
    def _phash_nearest(ph: Optional[str]) -> Optional[str]:
        """在索引里找**近似**项：dHash 对重压缩/缩放通常只差 1~2 bit（实测 q40 差 1）。

        精确匹配会漏掉几乎全部"同图不同编码"的副本；索引有界（≤512），线性扫描可忽略。
        """
        if not ph:
            return None
        try:
            v = int(ph, 16)
        except Exception:
            return None
        best, best_d = None, 99
        for k, desc in _PHASH_INDEX.items():
            try:
                d = bin(v ^ int(k, 16)).count("1")
            except Exception:
                continue
            if d < best_d:
                best, best_d = desc, d
                if d == 0:
                    break
        return best if best_d <= 2 else None

    async def _phash_remember(self, elem, desc: str):
        """记住「我们刚描述过」的图的 dHash（进程内、有界），供同图不同字节的副本命中。"""
        if not desc:
            return
        try:
            path = await self._media_path(elem) if elem is not None else None
            data = await self._read_media_bytes(path) if path else None
            ph = self._phash_of_bytes(data)
            if not ph:
                return
            if len(_PHASH_INDEX) >= _PHASH_INDEX_MAX:
                for k in list(_PHASH_INDEX)[: _PHASH_INDEX_MAX // 2]:
                    _PHASH_INDEX.pop(k, None)
            _PHASH_INDEX[ph] = desc
        except Exception:
            pass

    async def _fill_empty_official_by_path(self, req, sid: str) -> int:
        """把请求文本里的**官方空占位**按 file_path 的内容哈希补成描述（零 VLM）。

        场景：会话合并 / 上下文压缩类插件在 llm_request 阶段用历史重建请求时，会把被回复
        消息的媒体**重新下载成另一个临时文件**（download_10.jpg → download_11.jpg）。此刻
        链上已无对应元素，空占位只以**文本**形式留在回复引用的 content 里；框架随后在 agent
        内部渲染该元素、看到 `caption is None` → **触发官方 VLM 付费调用**。

        这里只做**缓存命中**的补齐（md5 优先、dHash 兜底），**不发起任何新识别**：
        拿不到元素就拿不到「跳过标记」，贸然识别会破坏用户"省这笔 VLM"的配置意图。
        """
        fixed = 0
        for p in getattr(req, "user_prompt", []) or []:
            text = getattr(p, "content", "") or ""
            if "[" not in text:
                continue
            matches = list(_EMPTY_OFFICIAL_RE.finditer(text))
            if not matches:
                continue
            out, last = [], 0
            for m in matches:
                kind, path = m.group(1), (m.group(2) or "").strip()
                if not path:
                    continue                      # 无路径无从查证（官方 Sticker 占位无路径）
                data = await self._read_media_bytes(path)
                if not data:
                    continue
                md5 = hashlib.md5(data).hexdigest()
                desc = await self._cache_get(md5)
                if not desc:
                    ph = self._phash_of_bytes(data)
                    desc = self._phash_nearest(ph)
                    if desc:
                        await self._cache_set(md5, desc)   # 顺手把新文件也登记进缓存
                if not desc:
                    continue
                out.append(text[last:m.start()])
                out.append(f"[Sticker {desc}]" if kind == "Sticker"
                           else f"[Image {desc}, file_path: {path}]")
                last = m.end()
                fixed += 1
            if out:
                out.append(text[last:])
                p.content = "".join(out)
        if fixed:
            logger.info(f"空占位按文件内容补齐 <{sid}>：{fixed} 个（命中缓存／同图指纹，零 VLM）")
        return fixed

    async def _cache_get(self, md5: str) -> Optional[str]:
        try:
            row = await self.ctx.db.get_image_desc_cache(md5)
            return row["description"] if row else None
        except Exception:
            return None

    async def _cache_set(self, md5: str, text: str):
        if not md5 or not text:
            return
        try:
            await self.ctx.db.add_image_desc_cache(md5, text, count=1, last_seen=0)
        except Exception:
            pass

    # ================= 校验 =================

    @staticmethod
    def _is_valid_desc(desc: str) -> bool:
        if not desc or not desc.strip():
            return False
        if "\x00" in desc:
            return False
        if "<!--PIR:" in desc:
            return False
        if "[Image #" in desc or "[Record #" in desc:
            return False  # 防嵌套标识符注入缓存并扩散
        return True


def _open_image(data: bytes):
    from PIL import Image as PILImage
    return PILImage.open(BytesIO(data)).convert("RGB")
