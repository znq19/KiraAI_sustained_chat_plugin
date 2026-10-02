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
- v2.3.3 健壮性增强（详见 KiraAI插件全量排查与修复方案.md §1.4/§2.4/§4）：
    * 缓存写入 upsert 带活 last_seen（原 last_seen=0 次日必被框架清理）；
      失败占位文案（(未识别)/(已过期)/(识别超时)/(下载失败)）不再进持久缓存。
    * 失败分类：下载失败 / 识别超时 / 未识别 三类占位可区分；URL 下载独立超时
      （download_timeout，默认 15s），VLM 推理仍由外层 wait_for(media_timeout) 兜底。
    * 到达即落盘：url 型媒体在 stage1 即持久化到 data/plugins_media_cache/ 并设置
      elem._temp_path（URL 失效免疫 + 框架 temp_monitor 60s 误删免疫）；URL 失效时
      凭 message_id 经适配器 get_msg 重取新 URL 重试一次。
    * 预取并发隔离：预取走独立信号量（vlm_prefetch_max_parallel），不占用
      stage2/stage3 关键路径的会话级/全局级信号量；预取先落盘再识别。
    * PIR/native 放行分支也占位 caption=""（堵框架 F1 付费窗口）；
      框架 desc_img 安全接管（缓存命中零 VLM + wait_for 超时兜底，terminate 还原）。
    * stage1/stage3 并行化；PIL/重编码等 CPU 操作移出事件循环（asyncio.to_thread）。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import re
import time
from io import BytesIO
from pathlib import Path
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

# 失败占位文案（统一收口）：这些值被 _is_valid_desc 拒绝——不进持久缓存、不当有效描述；
# 但仍会写进结果池/回填 caption 作为「本轮已处理」占位，防止空占位进 LLM 或重复撞模型。
_PLACEHOLDER_DESCS = ("(未识别)", "(已过期)", "(识别超时)", "(下载失败)")

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
        # URL 下载独立超时（默认 15s，与 VLM 推理预算分离）：媒体字节拉取慢 / URL 失效时
        # 快速失败并归类为 (下载失败)，不再让一次慢下载吃光整个 media_timeout
        # （外层 wait_for(media_timeout) 仍兜底全程，超时归类 (识别超时)）。
        self.download_timeout = float(sec.get("download_timeout", 15.0))
        # 预取并发隔离（默认 4）：预取（排队/在飞空窗的后台预处理）走独立信号量，
        # 不再占用 stage2/stage3 关键路径的会话级/全局级信号量 —— 多会话图片风暴时
        # 预取不会挤占兜底识别的并发槽。
        self.vlm_prefetch_max_parallel = int(sec.get("vlm_prefetch_max_parallel", 4))
        # 预取独立超时（默认 30s，clamp 到 [5s, media_timeout]）：60s 超时的在途项会
        # 长时间占满预取信号量的槽位，饿死后续预取（排队 60s+ 才开始识别）。预取是
        # 「锦上添花」的后台优化，给它更短的预算；stage2/stage3 关键路径仍吃完整
        # media_timeout。
        self.vlm_prefetch_timeout = max(5.0, min(self.media_timeout,
                                                 float(sec.get("vlm_prefetch_timeout", 30.0))))
        # 在途预取任务上限（默认 16）：超限直接跳过新预取（不置 _done，媒体仍由
        # stage2 正常接力识别）——防媒体风暴时在途任务表无界增长、槽位长期被占。
        self.vlm_prefetch_max_queue = int(sec.get("vlm_prefetch_max_queue", 16))
        # 到达即落盘（插件自有媒体缓存，默认开）：url 型媒体在 stage1 即把字节持久化到
        # data/plugins_media_cache/ 并设置 elem._temp_path —— URL 过期后本地字节仍在
        # （识别/渲染/read_file 全链免疫），且框架 temp_monitor 的 60s 保护期清理管不到
        # 插件自有目录。不改 elem.file/file_type，native 模式与框架渲染不受影响。
        # 目录治理：TTL（media_cache_ttl_hours，默认 24h）+ 总量上限（media_cache_max_mb，
        # 默认 512MB，LRU 淘汰最旧），后台任务定期清理（terminate 时取消）；
        # 在飞/待识别条目引用的文件受保护不删（另有 10 分钟宽限期）。
        self.media_cache_enabled = bool(sec.get("media_cache_enabled", True))
        self.media_cache_ttl_hours = float(sec.get("media_cache_ttl_hours", 24))
        self.media_cache_max_mb = int(sec.get("media_cache_max_mb", 512))
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
        # guard 拦截框架 read_file 补读时的在途短等（默认 8s，0=不等直接返回空占位）：
        # 若该媒体此刻正在插件流水线里识别，最多等这几秒拿真描述返回（LLM 补读体验更好）；
        # 等不到也返回 ""——识别由插件负责，绝不放行走框架 VLM 二次付费。
        self.guard_read_file_wait = float(sec.get("guard_read_file_wait", 8.0))
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
        # guard「已知媒体」登记（read_file 收口用，见 _guarded_desc_img）：凡是插件管线
        # 见过（将识别/已识别/已缓存）的媒体指纹都登记在此——框架 agent 的 read_file
        # 补读这些媒体时由 guard 拦截（返回空占位/在途描述），不再触发第二次付费 VLM。
        # 刻意不登记用户配置「不识别」的媒体（_media_skip 分支）：那是省 VLM 的既定语义。
        # 有界 FIFO（md5 cap 4096 / phash cap 2048，超限淘汰最旧一半）。
        self._known_md5: dict[str, None] = {}
        self._known_phash: dict[str, None] = {}
        self.quality_enabled = sec.get("quality_enabled", False)
        self.quality_value = int(sec.get("quality_value", 85))

        self._global_img_sem = asyncio.Semaphore(max(1, self.vlm_max_parallel_global))
        self._global_aud_sem = asyncio.Semaphore(max(1, self.stt_max_parallel_global))
        # 每会话信号量（惰性创建，热重载后自动重建）
        self._session_img_sems: dict[str, asyncio.Semaphore] = {}
        self._session_aud_sems: dict[str, asyncio.Semaphore] = {}
        # 预取专用信号量（并发隔离，见上）与裸 create_task 强引用集（防 GC 提前回收）
        self._prefetch_sem = asyncio.Semaphore(max(1, self.vlm_prefetch_max_parallel))
        self._bg_tasks: set = set()
        # stage1 后台登记任务表（v2.6.0，Fix 3）：id(message) -> Task。
        # ON_IM_MESSAGE 只做零 I/O 的同步收口（语音占位/挂表），下载/md5/DB/落盘
        # 全部挪进后台登记任务；stage2/stage3/预取 worker 开跑前先等它收尾，
        # 保证「登记完成前不会有阶段读到空媒体表」（与 v2.5.18 的调用点修复同语义）。
        self._stage1_pending: dict[int, "asyncio.Task"] = {}
        # 媒体缓存清理任务（懒启动，首个 url 媒体落盘时拉起）；
        # URL 失效重取注册表：media_id -> (message_id, adapter_name)，stage1 登记，
        # 下载失败时凭它经适配器 get_msg 拿新鲜 URL（napcat 的 get_msg 会刷新 rkey）
        self._mc_cleanup_task: Optional["asyncio.Task"] = None
        self._media_source: dict[str, tuple] = {}

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
        """运行时实时检测并行识图插件（PIR）是否处于**启用**状态。

        语义 v2.3.2 起简化（用户确认）：不再"装了就让位/只做音频"——本模块已覆盖并超越
        PIR（并行 VLM、缓存、三层限流、转发拍平、语音 STT 全都有），PIR 的 stage1 会把
        Image/Sticker 替换为 [Image #id: ] 标识符并删除原元素，破坏 Plus-One 复读表情包。
        因此 pir_auto_disable=True（默认）时：检测到 PIR 启用 → 自动 set_plugin_enabled(False)
        关闭它，图片完全归本模块；关闭失败/竞态（本轮事件 PIR 已先替换）时降级为旧语义
        （图片归 PIR、本模块只做音频），绝不双重处理。

        v2.3.3 竞态收口：直接**同步**查插件注册表启用状态（plugin_mgr.is_plugin_enabled
        是纯内存 dict 查询）。框架 set_plugin_enabled(False) 先翻标志位再 terminate 摘
        handler —— 自动关闭任务一旦开始执行，本判定立即返回 False，消除旧实现"已加载但
        handler 未摘除"的误判窗口（该窗口内 guard 放行留 caption=None → 框架 F1 付费识图）。
        """
        try:
            pm = getattr(self.ctx, "plugin_mgr", None)
            if pm is None:
                return False
            inst = pm.get_plugin_inst("parallel_image_reader")
            if inst is None:
                return False
            # 同步查注册表启用状态（内存 dict，微秒级）；接口异常时保守按"启用"让位
            try:
                enabled = pm.is_plugin_enabled("parallel_image_reader")
                if asyncio.iscoroutine(enabled):   # 旧版异步接口：拿不到结果，保守让位
                    try:
                        enabled.close()
                    except Exception:
                        pass
                    enabled = True
            except Exception:
                enabled = True
            # PIR 已加载且启用：auto-disable 开启则自动关闭（只关一次，防每事件重复 terminate）；
            # 裸 task 挂 _bg_tasks 持强引用，防 GC 提前回收
            if enabled and self.pir_auto_disable:
                if not getattr(self, "_pir_disable_attempted", False):
                    self._pir_disable_attempted = True
                    task = asyncio.create_task(self._auto_disable_pir())
                    self._bg_tasks.add(task)
                    task.add_done_callback(self._bg_tasks.discard)
            return bool(enabled)
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
          · 本模块整体关闭（section_media_recognition.enabled=false）时不做，
            这种配置下"框架自己识图"本来就是期望行为。

        PIR/native 放行分支（v2.3.3 收口）：即使 PIR 接管图片 / 原生多模态直传，也照占
        caption="" 再返回 —— PIR"已加载但 handler 未摘除"的竞态窗口里若放行留 None，
        框架渲染就会 F1 付费识图。占 "" 无害：native 模式框架渲染会把 caption 覆写为
        "attached image"；PIR 活着会自己填描述；僵尸 PIR 留 "" 恰好堵住框架 F1。

        返回值 = 占位的媒体个数（供测试/日志用）。
        """
        if not self.enabled or not self.guard_enabled:
            return 0
        try:
            sid = getattr(getattr(event, "session", None), "sid", None)
            # PIR 接管 / native 直传时仍占位（见 docstring）：这里只占位防 F1，
            # 其余处理（暂存/识别）仍由 stage1 的跳过逻辑分流
            self._pir_active()   # 顺带触发 PIR 自动互斥检测（保持原有时机）
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
            _sid = getattr(getattr(event, "session", None), "sid", None)
            targets: list = []
            self._collect_media_targets(event.message.chain, targets, set(), sid=_sid)
            if not targets:
                return
            media: dict[str, dict] = {}
            # ── 同步收口（零 I/O，v2.6.0 / Fix 3）────────────────────────────
            # 语音 Record **立即**替换为 [Record #noid: ] 占位标识符：框架在批次渲染时
            # 会对仍是 Record 的元素做自动 STT（message_format_to_text），异步替换
            # 赶不上「满即推 / trigger / 拦截后快速放行」的批次渲染。占位替换本身
            # 不需要任何 I/O（md5/缓存由后台登记任务补齐），留在同步段。
            for kind, elem, mtype, ch, idx in targets:
                if kind == "record":
                    replaced = self._replace_record_sync(elem, media)
                    ch[idx] = replaced
            # 合并而非覆盖：并行识图插件（PIR）可能已先写入 Image 索引，
            # 直接覆盖会让它 stage2/stage3 拿不到图片（图片标识符永远空）
            existing = getattr(event.message, self._media_attr, None) or {}
            merged = {**existing, **media}
            setattr(event.message, self._media_attr, merged)
            # 重活全部进后台登记任务（下载/落盘/md5/DB 缓存查询），ON_IM_MESSAGE
            # 钩子链对任何媒体消息都不再做网络/DB/磁盘等待：框架立刻就能打印日志、
            # 把消息放进会话缓冲（宿主 Fix 1 的 on_buffered 随之即时武装顺延）。
            # 图片登记结果写进同一个 merged 表；stage2/stage3/预取 worker 开跑前
            # 会先等 _stage1_pending 收尾，不会读到空媒体表。
            _msg_id = getattr(event.message, "message_id", None)
            _ainfo = getattr(event, "adapter", None)
            _aname = getattr(_ainfo, "name", None) or getattr(_ainfo, "adapter_id", None)
            self._launch_stage1_register(_sid, event.message, targets, merged,
                                         _msg_id, _aname)
        except Exception:
            logger.exception("stage1 error")

    def _replace_record_sync(self, elem, media: dict) -> Text:
        """语音 Record → [Record #noid: ] 占位标识符（**同步、零 I/O**）。

        键固定为 noid_{id(elem)}：md5 由后台登记任务补齐（缓存命中时描述直接写进
        结果池与占位文本；未命中保持待识别，stage2 照常接力 STT）。占位必须先于
        批次渲染存在，否则框架会对 Record 元素自动 STT（串行、无限流、无缓存）。
        """
        short_id = f"noid_{id(elem)}"
        try:
            elem._pir_short_id = short_id   # 与 _prefill_media 同约定：把键钉在元素上
        except Exception:
            pass
        info = {"md5": None, "elem": elem, "type": "Record", "_done": False}
        txt = Text(f"[Record #{short_id}: ]")
        info["text_elem"] = txt   # 后台缓存命中时把描述/路径写回占位文本
        media[short_id] = info
        return txt

    def _launch_stage1_register(self, sid, message, targets, bucket: dict,
                                msg_id=None, adapter_name=None) -> None:
        """启动后台登记任务并挂进 _stage1_pending（stage2/stage3/预取会等它收尾）。"""
        try:
            task = asyncio.create_task(
                self._stage1_register(sid, message, targets, bucket, msg_id, adapter_name))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            if len(self._stage1_pending) < 256:
                key = id(message)
                self._stage1_pending[key] = task
                task.add_done_callback(lambda _t, k=key: self._stage1_pending.pop(k, None))
        except Exception as e:
            logger.debug(f"stage1 register schedule failed: {type(e).__name__}: {e}")

    async def _stage1_register(self, sid, message, targets, bucket: dict,
                               msg_id=None, adapter_name=None) -> None:
        """后台登记（原 stage1 的重活部分）：落盘 → md5 → 缓存 → 写登记表 → 按需预取。

        与 v2.5.18 的调用点修复同语义：**登记完成之后**才允许调度预取；
        区别是登记本身也移出了钩子链——识别/落盘与消息顺延窗口并行推进，
        不再阻塞 [message] 日志与消息进缓冲（2026-10-02 引用图 8s 卡钩子链事故）。
        """
        try:
            sem = asyncio.Semaphore(4)

            async def _one(t):
                kind, elem, mtype, ch, idx = t
                async with sem:
                    if kind == "prefill":
                        await self._register_image_one(elem, mtype, bucket)
                    else:
                        await self._register_record_one(sid, elem, bucket)

            await asyncio.gather(*[_one(t) for t in targets], return_exceptions=True)
            # URL 失效重取注册表：记录 media_id → (message_id, adapter_name)，
            # 下载失败时凭 message_id 经适配器 get_msg 拿新鲜 URL（见 _refresh_media_url）
            try:
                if msg_id:
                    if len(self._media_source) > 2048:
                        for _k in list(self._media_source)[: len(self._media_source) - 1024]:
                            self._media_source.pop(_k, None)
                    for _k in bucket:
                        self._media_source[_k] = (str(msg_id), adapter_name)
            except Exception:
                pass
            # 登记到「本会话本回合暂存索引」（stage3 抢救用；语义同旧 stage1）：
            # 批次被第三方 stop、stage2 不跑时，stage3 仍知道有哪些待识别媒体。
            if sid:
                rm = self._round_media.setdefault(sid, {})
                rm.update(bucket)
                if len(rm) > 64:
                    for _k in list(rm)[: len(rm) - 64]:
                        rm.pop(_k, None)
                if len(self._round_media) > 128:
                    for old_sid in list(self._round_media)[: len(self._round_media) - 64]:
                        self._round_media.pop(old_sid, None)
                # ★ 真·预取的调度点（原 stage1 末尾调用点平移到此，语义不变）：
                #   只为「确实会进 LLM」的消息（宿主已打 _batch_entered）预取，
                #   且必须在登记完成之后（否则 worker 读空表，v2.5.13 竞态复辟）。
                if getattr(message, "_batch_entered", False):
                    self.schedule_prefetch(sid, [message], reason="进批次即识别")
        except Exception:
            logger.exception("stage1 register error")

    async def _register_image_one(self, elem, mtype: str, bucket: dict):
        """图片/表情后台登记（原 _prefill_media，Fix 2 单次下载版）。

        顺序：先 _persist_media 落盘（url 型**只下载这一次**，md5 直接取自落盘字节），
        非 url / 落盘失败才退回 _elem_md5 现算 —— 旧实现 hash_image 与 to_base64
        各下载一次（同一 URL 拉两遍）。
        """
        # 宿主 handle_msg 已做"仅唤醒/概率"决策：_media_skip=True = 本次不识别（省 VLM）
        if getattr(elem, "_media_skip", False):
            elem.caption = ""  # 官方空占位 + 阻止框架自动 VLM（caption 非 None）
            return
        # 已有有效描述（同一元素被重复登记等）：不覆盖、不重复识别
        try:
            _cur = (getattr(elem, "caption", None) or "").strip()
            if _cur and self._is_valid_desc(_cur):
                return
        except Exception:
            pass
        md5 = None
        try:
            _p, md5 = await self._persist_media(elem)   # 到达即落盘（url 仅这次下载）
        except Exception:
            md5 = None
        if not md5:
            md5 = await self._elem_md5(elem)
        if md5:
            # guard 已知媒体登记（含下方缓存命中提前 return 的分支）：此后框架 read_file
            # 补读该媒体由 guard 拦截，不再二次付费 VLM
            self._remember_known(md5=md5)
        short_id = md5[:8] if md5 else f"noid_{id(elem)}"
        # 把本阶段使用的键钉在元素上（理由同旧实现：框架渲染前压缩会改 elem.md5）
        try:
            elem._pir_short_id = short_id
        except Exception:
            pass
        if md5:
            desc = await self._cache_get(md5) or ""
            if desc and not self._is_valid_desc(desc):
                desc = ""
            if desc:
                # 缓存命中：直接预置官方描述（零 VLM）。不进登记表（_done 隐含），
                # 同一批消息重发时无需再处理——stage2 只认登记表里的媒体。
                elem.caption = desc
                return
        # 未命中：登记原元素供 stage2 并行识别（唤醒/概率命中路径）
        elem.caption = ""  # 先阻止框架自动 VLM，stage2 识别完成后回填官方格式
        bucket[short_id] = {"md5": md5, "elem": elem, "type": mtype, "_done": False}

    async def _register_record_one(self, sid, elem, bucket: dict):
        """语音后台登记（Fix 2 单次下载版）：md5/缓存补齐到同步段挂的占位条目。

        缓存命中：描述写进结果池（stage2/stage3 直接命中）并回写占位文本
        （含 file_path，格式与旧 _replace_media 一致）；未命中保持待识别，
        stage2 照常接力并行 STT（三层限流 + 缓存）。
        """
        short_id = getattr(elem, "_pir_short_id", None) or f"noid_{id(elem)}"
        info = bucket.get(short_id)
        if not isinstance(info, dict):
            return
        md5 = None
        try:
            _p, md5 = await self._persist_media(elem)   # url 型仅这次下载
        except Exception:
            md5 = None
        if not md5:
            try:
                md5 = await self._record_md5(elem)
            except Exception:
                md5 = None
        if not md5:
            return
        self._remember_known(md5=md5)   # guard 已知媒体登记（同图片分支）
        info["md5"] = md5
        desc = await self._cache_get(md5) or ""
        if desc and not self._is_valid_desc(desc):
            desc = ""
        if not desc:
            return
        info["_done"] = True
        # 结果池打底：stage2 的「已有描述跳过」判据（_has_desc）直接命中
        if sid:
            pool = self._results_pool.setdefault(sid, {})
            pool[short_id] = desc
        # 回写占位文本：批次尚未渲染时，LLM 直接看到带描述的标识符
        txt = info.get("text_elem")
        if txt is not None:
            p = await self._media_path(elem)
            txt.text = (f"[Record #{short_id}: {desc}, file_path: {p}]" if p
                        else f"[Record #{short_id}: {desc}]")

    def _collect_media_targets(self, chain, targets: list, visited: set,
                               sid: Optional[str] = None):
        """递归收集待处理媒体（**同步、纯遍历**，不下载不查库；嵌套 Forward 已拍平）。

        收集为 (kind, elem, mtype, chain, idx) 五元组，由调用方 gather 并行处理
        （v2.3.3 起替代串行的 _walk_chain，消除 ON_IM_MESSAGE 关键路径上的串行
        下载/DB 等待）。kind ∈ {"prefill"（图片/表情）, "record"（语音，需回填替换）}。
        """
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
                targets.append(("prefill", elem, mtype, chain, idx))
            elif isinstance(elem, Record):
                targets.append(("record", elem, "Record", chain, idx))
            elif isinstance(elem, Reply):
                self._collect_media_targets(getattr(elem, "chain", None), targets, visited, sid)
            elif isinstance(elem, Forward):
                for sub in (getattr(elem, "chains", None) or []):
                    self._collect_media_targets(sub, targets, visited, sid)

    async def _elem_md5(self, elem) -> Optional[str]:
        """元素 md5：path 型文件用 asyncio.to_thread 读盘计算（避开框架 hash_image 在
        事件循环里同步 open().read() 全文件堵首 token）；url/base64 型走框架 hash_image。"""
        try:
            cached = getattr(elem, "md5", None)
            if cached:
                return cached
            if getattr(elem, "file_type", "") == "path" and getattr(elem, "file", None):
                def _hash_path():
                    with open(elem.file, "rb") as f:
                        return hashlib.md5(f.read()).hexdigest()
                md5 = await asyncio.to_thread(_hash_path)
                try:
                    elem.md5 = md5
                except Exception:
                    pass
                return md5
            return await elem.hash_image()
        except Exception:
            return None

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

    async def _batch_media_paths(self, elems) -> dict:
        """并行预取媒体本地路径（to_path 落盘，限流 4）：stage3 抢救/批量探测用。

        返回 {id(elem): path or None}。原实现在循环里逐个串行 await _media_path()
        （url 型每次都要下载），多张图时明显拖慢 LLM 首 token。
        """
        out: dict[int, Optional[str]] = {}
        if not elems:
            return out
        sem = asyncio.Semaphore(4)

        async def _probe_one(e):
            async with sem:
                out[id(e)] = await self._media_path(e)

        await asyncio.gather(*[_probe_one(e) for e in elems], return_exceptions=True)
        return out

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
            # Fix 3：先等本批次消息的后台登记任务收尾（正常在顺延窗口里早已完成，
            # 等待开销≈0；快速放行的批次保证不会读到空媒体表）
            _reg = [self._stage1_pending.get(id(m)) for m in event.messages]
            _reg = [t for t in _reg if t is not None and not t.done()]
            if _reg:
                await asyncio.gather(*_reg, return_exceptions=True)
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
            # 桶内限长：每 sid 最多 64 条，超出按插入顺序淘汰最旧
            _rm_bucket = self._round_media[sess_sid]
            if len(_rm_bucket) > 64:
                for _k in list(_rm_bucket)[: len(_rm_bucket) - 64]:
                    _rm_bucket.pop(_k, None)
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
                    # 进入关键路径：摘掉预取标记（改占会话/全局信号量），日志来源标 stage2
                    info.pop("_prefetch", None)
                    info["_mr_source"] = "stage2"
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

        键与 stage1 一致（`elem._pir_short_id`，stage1 在 _register_image_one /
        _replace_record_sync 里钉在元素上）。用它而不是文本锚点来判定「这条媒体在不在
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

    def schedule_prefetch(self, sid: str, messages, reason: str = "") -> None:
        """非阻塞入口：把这些消息里的媒体丢给后台识别。

        调用时机 = 「消息已确定进入批次」**且 stage1 已把媒体登记进 `_pir_media` 之后**
        （media_recognize.on_im_message 末尾，由宿主的 `_batch_entered` 标记触发）。
        这段时间多半正是「上一个批次的 LLM 还在跑 / 本批次在防抖窗口里排队」的空窗
        —— 正好用掉：放行时 stage2 直接命中结果池，关键路径零识别开销；被其它插件
        拦截（不跑 stage2）的批次也能带上描述而不是空占位。

        ⚠️ 不要在 handle_msg（先注册的钩子）里调用：预取 worker 会在 stage1 的第一次
        await 时抢跑，那时 `_pir_media` 还是空的 → worker 读到空直接返回（一次性任务
        不重试）→ 预取失效（v2.5.13~v2.5.17 的实际状况）。
        """
        if not self.enabled or not self.prefetch_enabled:
            return
        msgs = [m for m in (messages or []) if m is not None]
        if not msgs:
            return
        try:
            # 裸 create_task 持强引用（_bg_tasks），防 GC 在任务完成前回收导致静默中断
            task = asyncio.create_task(self._prefetch_worker(sid, msgs, reason))
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except Exception as e:
            logger.debug(f"prefetch schedule failed: {type(e).__name__}: {e}")

    async def _prefetch_worker(self, sid: str, messages, reason: str = "") -> None:
        """收集待识别媒体 → 起后台识别任务（与 stage2 共用缓存 / 限流 / 结果池）。"""
        try:
            if self._pir_active() or self._native_mode(sid):
                return
            # Fix 3：先等这些消息的后台登记收尾（warmup 等早起调度点不会读到空表，
            # 与 v2.5.18「登记完成后再调度」同一语义）
            _reg = [self._stage1_pending.get(id(m)) for m in messages]
            _reg = [t for t in _reg if t is not None and not t.done()]
            if _reg:
                await asyncio.gather(*_reg, return_exceptions=True)
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
            batch_img_sem = None   # 预取走独立信号量（_prefetch_sem），不占批次/会话/全局槽
            batch_aud_sem = None
            started = 0
            skipped = 0
            for media_id, info in media.items():
                if self._has_desc(sid, media_id) or media_id in self._pf_tasks:
                    continue          # 已有描述 → 不重复；正在飞 → 去重（失败后允许再试）
                if len(self._pf_tasks) >= self.vlm_prefetch_max_queue:
                    # 在途预取队列已满：跳过新预取防占槽饥饿。刻意**不置 _done**——
                    # 跳过的媒体仍由 stage2 正常接力识别，不会漏图
                    skipped += 1
                    continue
                info["_done"] = True
                info["_prefetch"] = True        # 预取标记：_describe_one/_transcribe_one 据此
                info["_mr_source"] = "prefetch"  # 走独立信号量 + 先落盘 + 日志来源前缀
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
                started += 1
            if started:
                # 可观测性：确认"进批次即识别"真的启动了（而不是等推批次）
                logger.info(
                    f"[MediaRecognize] 预取启动 {started} 项（{sid}，"
                    f"{reason or '后台识别'}，不阻塞主流程）"
                )
            if skipped:
                logger.debug(
                    f"[MediaRecognize] 预取在途队列已满（上限 {self.vlm_prefetch_max_queue}），"
                    f"跳过 {skipped} 项新预取（媒体仍由 stage2 正常识别）"
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
        source = str(info.get("_mr_source") or "stage2")
        cached = await self._cache_get(md5) if md5 else None
        if cached:
            info["_done"] = True
            results[media_id] = cached
            return
        eff_timeout = self.media_timeout   # 实际生效超时（预取分支用更短的独立预算）
        try:
            if info.get("_prefetch"):
                # 预取并发隔离：先落盘（URL 失效免疫，识别直接吃本地字节），识别只占
                # 预取专用信号量 —— 不占用 stage2/stage3 关键路径的会话级/全局级信号量
                try:
                    await self._persist_media(info.get("elem"), md5)
                except Exception:
                    pass
                # 预取独立超时（默认 30s，短于 media_timeout）：超时项长时间占满预取
                # 槽位会饿死后续预取；stage2/stage3 仍吃完整 media_timeout（见 else 分支）
                eff_timeout = min(self.media_timeout, self.vlm_prefetch_timeout)
                async with self._prefetch_sem:
                    desc = await asyncio.wait_for(
                        self._describe_image(info["elem"], sess_sid, media_id=media_id,
                                             source=source), eff_timeout)
            else:
                sess_sem = self._session_sem(self._session_img_sems, sess_sid, self.vlm_max_parallel_per_session)
                # 三层限流：批次级 → 会话级 → 全局级（固定获取顺序，无死锁）
                if batch_sem is not None:
                    async with batch_sem, sess_sem, self._global_img_sem:
                        desc = await asyncio.wait_for(
                            self._describe_image(info["elem"], sess_sid, media_id=media_id,
                                                 source=source), self.media_timeout)
                else:
                    async with sess_sem, self._global_img_sem:
                        desc = await asyncio.wait_for(
                            self._describe_image(info["elem"], sess_sid, media_id=media_id,
                                                 source=source), self.media_timeout)
            # 无论成功失败都标记已处理：同一条消息重发不再重复识别（防 429 风暴）
            info["_done"] = True
            if desc and self._is_valid_desc(desc):
                if md5:
                    await self._cache_set(md5, desc)
                # 记下 dHash：同图被第三方插件重新下载/重压缩成另一字节流时，仍能命中描述
                await self._phash_remember(info.get("elem"), desc)
                results[media_id] = desc
            elif desc in _PLACEHOLDER_DESCS:
                # 分类占位（(下载失败) 等）：占住结果池防重试/防空占位进 LLM，
                # 但不进持久缓存（_is_valid_desc 已拒绝）
                results[media_id] = desc
            else:
                logger.warning(f"[MediaRecognize:{source}] image VLM returned empty/invalid desc id={media_id} md5={md5[:8] if md5 else 'n/a'}")
                results[media_id] = "(未识别)"
        except asyncio.TimeoutError:
            info["_done"] = True
            logger.warning(f"[MediaRecognize:{source}] image describe timeout id={media_id}（>{eff_timeout:.0f}s）")
            results[media_id] = "(识别超时)"
        except Exception as e:
            info["_done"] = True
            logger.warning(f"[MediaRecognize:{source}] image describe failed id={media_id}: {type(e).__name__}: {e}")
            results[media_id] = "(未识别)"

    async def _transcribe_one(self, sess_sid: str, media_id: str, info: dict, results: dict,
                              batch_sem: Optional[asyncio.Semaphore] = None):
        md5 = info["md5"]
        source = str(info.get("_mr_source") or "stage2")
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
                logger.warning(f"[MediaRecognize:{source}] STT client unavailable (no default STT model) id={media_id}")
                results[media_id] = "(未识别)"
                return
            if info.get("_prefetch"):
                # 预取并发隔离（与 _describe_one 同理）：先落盘 + 只占预取专用信号量
                try:
                    await self._persist_media(info.get("elem"), md5)
                except Exception:
                    pass
                async with self._prefetch_sem:
                    text = await asyncio.wait_for(
                        speech_to_text(client=stt_client, record=info["elem"]), self.media_timeout)
            else:
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
                logger.warning(f"[MediaRecognize:{source}] STT returned empty/invalid text id={media_id}")
                results[media_id] = "(未识别)"
        except asyncio.TimeoutError:
            info["_done"] = True
            logger.warning(f"[MediaRecognize:{source}] STT timeout id={media_id}（>{self.media_timeout:.0f}s）")
            results[media_id] = "(识别超时)"
        except Exception as e:
            info["_done"] = True
            logger.warning(f"[MediaRecognize:{source}] STT failed id={media_id}: {type(e).__name__}: {e}")
            results[media_id] = "(未识别)"

    async def _describe_image(self, elem, sid: Optional[str] = None,
                              media_id: Optional[str] = None, source: str = "stage2") -> str:
        """图片 VLM（三段式）：
        (a) **本地字节优先**：已落盘（_temp_path / path 型 file）则直接读盘，不走 URL；
        (b) 无本地字节才走 URL 下载，下载单独吃 download_timeout（与 VLM 预算分离），
            失败时凭 message_id 经适配器 get_msg 重取新 URL 重试一次，仍失败返回 "(下载失败)"；
        (c) VLM 推理由外层 wait_for(media_timeout) 兜底（超时由调用方归类为 (识别超时)）。
        quality_enabled 时 JPEG 压缩（to_thread，不堵事件循环）。
        其它异常返回 ""（调用方降级为 (未识别) 并打日志）。"""
        try:
            vlm = self.ctx.provider_mgr.get_default_vlm()
            if vlm is None:
                logger.warning(f"[MediaRecognize:{source}] get_default_vlm() returned None")
                return ""
            # 可观测性：框架的 desc_img() 会打 "Describing image using …"，而我们直接
            # vlm.chat()（绕过了那层包装）→ 成功时不打任何日志，日志里无法分辨一次识图
            # 是本插件发起还是框架自己发起的。这里补一条与官方同款文案 + 来源前缀的
            # 日志（[MediaRecognize:prefetch|stage2|stage3]），便于对照排查谁在识图。
            try:
                _mdl = vlm.model
                logger.info(
                    f"[MediaRecognize:{source}] Describing image using {_mdl.model_id} ({_mdl.provider_name})"
                )
            except Exception:
                pass
            data_url = None
            # (a) 本地字节优先
            local = getattr(elem, "_temp_path", None)
            if not local and getattr(elem, "file_type", "") == "path":
                local = getattr(elem, "file", None)
            data = await self._read_media_bytes(local) if local else None
            if data:
                mime = getattr(elem, "mime", None) or "image/jpeg"
                data_url = f"data:{mime};base64,{base64.b64encode(data).decode()}"
            else:
                # (b) URL 下载（独立短超时 + 失效重取一次）
                data_url = await self._fetch_image_data(elem, media_id, source)
                if data_url is None:
                    return "(下载失败)"
            if self.quality_enabled:
                data_url = await asyncio.to_thread(
                    _reencode_jpeg, data_url, max(10, min(100, self.quality_value)))
                if not data_url:
                    logger.warning(f"[MediaRecognize:{source}] empty base64 after re-encode")
                    return ""
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
            logger.warning(f"[MediaRecognize:{source}] describe image failed: {type(e).__name__}: {e}")
            return ""

    async def _fetch_image_data(self, elem, media_id: Optional[str] = None,
                                source: str = "stage2") -> Optional[str]:
        """URL 下载（独立 download_timeout）：to_data_url → 直接 httpx 下载（带 UA +
        pixiv Referer）→ URL 失效重取（get_msg 新 URL）后重试一次。全失败返回 None。"""
        last_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                return await asyncio.wait_for(elem.to_data_url(), timeout=self.download_timeout)
            except Exception as e:
                last_err = e
                logger.debug(f"[MediaRecognize:{source}] to_data_url failed ({type(e).__name__}), try direct download")
                data_url = await self._try_direct_download(elem)
                if data_url:
                    return data_url
                # URL 失效重取：凭 stage1 登记的 message_id 经适配器 get_msg 拿新鲜 URL，
                # 更新 elem.file 后重试一次；能力不存在（非 napcat/无该方法）静默跳过
                if attempt == 0 and await self._refresh_media_url(elem, media_id):
                    logger.info(f"[MediaRecognize:{source}] URL 已失效，经 get_msg 重取新 URL 后重试")
                    continue
                break
        logger.warning(
            f"[MediaRecognize:{source}] image download failed: "
            f"{type(last_err).__name__ if last_err else '?'}: {last_err} "
            f"file_type={getattr(elem, 'file_type', '?')} file={str(getattr(elem, 'file', ''))[:80]}"
        )
        return None

    async def _refresh_media_url(self, elem, media_id: Optional[str] = None) -> bool:
        """URL 失效重取：凭注册表里的 message_id 经适配器客户端 get_msg 拿新鲜 URL。

        napcat 的 get_msg 会返回带新 rkey 的图片/语音 URL（KSM history_tool 已验证）。
        能力不存在（非 napcat / 无 get_msg 方法 / 注册表无记录）时静默返回 False。
        成功时更新 elem.file（Image 同步 elem.image）并清 _temp_path 强制重下。全程兜底。
        """
        try:
            src = self._media_source.get(media_id) if media_id else None
            if not src:
                return False
            message_id, adapter_name = src
            if not message_id:
                return False
            am = getattr(self.ctx, "adapter_mgr", None)
            if am is None:
                return False
            adapter = None
            if adapter_name:
                try:
                    adapter = am.get_adapter(adapter_name)
                except Exception:
                    adapter = None
            if adapter is None:
                # 注册表没带适配器名：单适配器场景兜底取第一个
                try:
                    adapters = getattr(am, "adapters", None) or {}
                    adapter = next(iter(adapters.values()), None)
                except Exception:
                    adapter = None
            client = None
            if adapter is not None and hasattr(adapter, "get_client"):
                try:
                    client = adapter.get_client()
                except Exception:
                    client = None
            if client is None or not hasattr(client, "get_msg"):
                return False
            resp = await client.get_msg(message_id)
            data = (resp or {}).get("data") or {}
            url = self._extract_media_url(data, elem)
            if not url:
                return False
            try:
                elem.file = url
            except Exception:
                pass
            try:
                if hasattr(elem, "image"):
                    elem.image = url
            except Exception:
                pass
            try:
                elem._temp_path = None   # 清掉旧本地指针，强制用新 URL 重下
            except Exception:
                pass
            return True
        except Exception:
            return False

    @staticmethod
    def _extract_media_url(data: dict, elem) -> Optional[str]:
        """从 get_msg 返回的消息段里取与元素类型匹配的媒体新 URL（best-effort）。"""
        try:
            segs = data.get("message") or []
            if isinstance(segs, str):
                return None            # CQ 码字符串形态无从可靠解析，放弃
            want = "record" if elem.__class__.__name__ == "Record" else "image"
            for seg in segs:
                if not isinstance(seg, dict) or seg.get("type") != want:
                    continue
                d = seg.get("data") or {}
                url = d.get("url") or d.get("file")
                if isinstance(url, str) and url.startswith(("http://", "https://")):
                    return url
        except Exception:
            pass
        return None

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
            # 下载吃独立的 download_timeout（与 VLM 推理预算分离）
            async with httpx.AsyncClient(follow_redirects=True, timeout=self.download_timeout) as client:
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
            # Fix 3：同 stage2，先等本批次消息的后台登记收尾（抢救的准入信息才完整）
            _reg = [self._stage1_pending.get(id(m)) for m in (getattr(event, "messages", None) or [])]
            _reg = [t for t in _reg if t is not None and not t.done()]
            if _reg:
                await asyncio.gather(*_reg, return_exceptions=True)
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
            _cands1: list = []   # (mid, info, elem, mtype)
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
                        # 已有真描述才跳过；失败占位（(未识别)/(识别超时)/(下载失败) 等）
                        # 允许再试一次
                        if _cap and self._is_valid_desc(_cap):
                            continue
                    except Exception:
                        continue
                _cands1.append((mid, info, elem, mtype))

            # ② 直接扫描本批次消息链：捕获「我方完全没认领（caption is None / ""）
            #    但请求里仍是空占位」的媒体——例如消息在 ON_IM_MESSAGE 阶段被其它插件
            #    stop 掉，我方 stage1 从未执行；或批次被第三方拦截后消息才进入请求。
            #    这是最后一道保险：凡是请求里出现空占位、而我们又确实拿到了原元素，
            #    就在这里补齐描述，绝不让空占位进 LLM。
            _cands2: list = []   # (_elem, mtype)
            if not self._pir_active() and not self._native_mode(event.sid):
                for _m in (getattr(event, "messages", None) or []):
                    for _elem in self._iter_media_elems(getattr(_m, "chain", None)):
                        if not isinstance(_elem, (Image, Sticker)):
                            continue
                        _cap = str(getattr(_elem, "caption", None) or "").strip()
                        # 同 ①：已有真描述才跳过；失败占位允许再试一次
                        if _cap and self._is_valid_desc(_cap):
                            continue
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
                        _cands2.append((_elem, mtype))

            # 并行预取候选媒体的本地路径（to_path 落盘，限流 4）——原实现循环内逐个
            # 串行 await 下载，多张图时 stage3 抢救明显拖慢 LLM 首 token
            _probe_elems = [c[2] for c in _cands1] + [c[0] for c in _cands2]
            # 文本标识符兜底（历史 [Image #id: ] / [Record #id: ]）的elem也一并预取
            for _mid in need:
                _el = (round_media.get(_mid) or {}).get("elem") \
                    if isinstance(round_media.get(_mid), dict) else None
                if _el is not None and id(_el) not in {id(e) for e in _probe_elems}:
                    _probe_elems.append(_el)
            _probe_paths = await self._batch_media_paths(_probe_elems)

            for mid, info, elem, mtype in _cands1:
                anchor = _anchor_of(elem, mtype, _probe_paths.get(id(elem)), mid)
                if anchor and any(anchor in t for t in _texts):
                    official[mid] = (elem, mtype)
                    need[mid] = anchor

            for _elem, mtype in _cands2:
                anchor = _anchor_of(_elem, mtype, _probe_paths.get(id(_elem)))
                if not any(anchor in t for t in _texts):
                    continue
                # 锚点命中的才算 md5（url 型此处可能触发下载，未命中的候选不浪费流量）
                _md5 = await self._elem_md5(_elem)
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
                    # 进入关键路径：摘掉预取标记（改占会话/全局信号量），日志来源标 stage3
                    info.pop("_prefetch", None)
                    info["_mr_source"] = "stage3"
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
                    # 优先复用并行预取阶段已探测的路径（避免二次下载），未探测的兜底现取
                    p = _probe_paths.get(id(info["elem"]))
                    if p is None:
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
            self._remember_known(phash=ph)   # guard 已知媒体登记（同图重压缩副本收口）
        except Exception:
            pass

    # ================= guard 已知媒体登记（read_file 收口用） =================

    def _remember_known(self, md5: Optional[str] = None, phash: Optional[str] = None):
        """登记「本插件已接管」的媒体指纹（进程内、有界 FIFO，全程静默）。

        登记点：stage1 _register_image_one / _register_record_one（md5 算出即登记，含缓存命中
        分支）、_persist_media（字节在手，顺手 dHash）、_phash_remember（识别成功后）。
        用户配置「不识别」的媒体（_media_skip 提前 return 分支）刻意**不登记**——
        guard 不拦截、read_file 放行原函数，尊重用户省 VLM 的既定语义。
        """
        try:
            if md5:
                if len(self._known_md5) >= 4096:
                    for k in list(self._known_md5)[:2048]:
                        self._known_md5.pop(k, None)
                self._known_md5[md5] = None
            if phash:
                if len(self._known_phash) >= 2048:
                    for k in list(self._known_phash)[:1024]:
                        self._known_phash.pop(k, None)
                self._known_phash[phash] = None
        except Exception:
            pass

    def _phash_known(self, ph: Optional[str]) -> bool:
        """已知 phash 登记的存在性判定（hamming≤2 近似，扫描方式同 _phash_nearest）。"""
        if not ph:
            return False
        try:
            v = int(ph, 16)
        except Exception:
            return False
        for k in self._known_phash:
            try:
                if bin(v ^ int(k, 16)).count("1") <= 2:
                    return True
            except Exception:
                continue
        return False

    async def _known_media_check(self, image) -> tuple:
        """判定该媒体是否「本插件已接管」（guard 收口用，全程防御绝不抛）。

        取数逻辑与 _desc_index_lookup 一致（image.md5 / _temp_path / path 型 file →
        读本地字节现算 md5）。返回 (known, md5, phash)：md5 命中已知登记 →
        (True, md5, None)；否则有本地字节时算 dHash 对已知登记做 hamming≤2 近似匹配
        → (True, md5, ph)；都不中 (False, md5, ph)；任何异常 (False, None, None)。
        """
        try:
            data = None
            md5 = getattr(image, "md5", None) or None
            local = getattr(image, "_temp_path", None)
            if not local and getattr(image, "file_type", "") == "path":
                local = getattr(image, "file", None)
            if not md5 and local:
                data = await self._read_media_bytes(local)
                if data:
                    md5 = hashlib.md5(data).hexdigest()
            if md5 and md5 in self._known_md5:
                return True, md5, None
            if data is None and local:
                data = await self._read_media_bytes(local)
            ph = None
            if data:
                ph = await asyncio.to_thread(self._phash_of_bytes, data)
            if ph and self._phash_known(ph):
                return True, md5, ph
            return False, md5, ph
        except Exception:
            return False, None, None

    async def _await_inflight_desc(self, md5: Optional[str], phash: Optional[str],
                                   timeout: float) -> Optional[str]:
        """guard 拦截 read_file 补读时：短等该媒体的在途识别，拿到真描述就返回。

        shield 包裹在途任务：等待方超时**不取消**识别任务本身（识别仍由插件流水线
        收尾）。随后依次查持久缓存（md5）与 dHash 索引（phash），返回首个有效描述；
        无在途/超时/任何失败返回 None（调用方降级返回空占位）。
        """
        try:
            if md5:
                task = self._pf_tasks.get(md5[:8])
                if task is not None:
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout)
                    except Exception:
                        pass
            if md5:
                desc = await self._cache_get(md5)
                if desc and self._is_valid_desc(desc):
                    return desc
            if phash:
                desc = self._phash_nearest(phash)
                if desc and self._is_valid_desc(desc):
                    return desc
        except Exception:
            pass
        return None

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
                    ph = await asyncio.to_thread(self._phash_of_bytes, data)   # CPU 操作移出事件循环
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
            desc = row["description"] if row else None
            # 占位污染免疫（双保险之读取侧）：框架渲染的失败分支会把 "(未识别)" 等占位
            # 写进持久缓存（message_manager.py），读到一律视为未命中
            if desc and not self._is_valid_desc(desc):
                return None
            return desc
        except Exception:
            return None

    async def _cache_set(self, md5: str, text: str):
        """upsert 写缓存：存在即 update 刷新 last_seen，不存在才 add（带活时间）。

        旧实现固定 add(count=1, last_seen=0) 有两个 bug：
          ① last_seen=0 → 框架清理规则（last_seen < now-15天 且 count<2 即删，
            core/db/service.py）次日必然删掉插件写入的全部缓存项；
          ② add 遇主键冲突静默失败 → 同图重复写永远失败。
        """
        if not md5 or not text:
            return
        now = int(time.time())
        updated = False
        try:
            updated = bool(await self.ctx.db.update_image_desc_cache(
                md5, description=text, last_seen=now))
        except Exception:
            updated = False
        if updated:
            return
        try:
            await self.ctx.db.add_image_desc_cache(md5, text, count=1, last_seen=now)
        except Exception:
            pass

    # ================= 校验 =================

    @staticmethod
    def _is_valid_desc(desc: str) -> bool:
        if not desc or not desc.strip():
            return False
        # 失败占位文案（框架/本模块的降级占位）不是有效描述：拒绝进缓存、不当命中
        if desc.strip() in _PLACEHOLDER_DESCS:
            return False
        if "\x00" in desc:
            return False
        if "<!--PIR:" in desc:
            return False
        if "[Image #" in desc or "[Record #" in desc:
            return False  # 防嵌套标识符注入缓存并扩散
        return True

    # ================= 到达即落盘（插件自有媒体缓存） =================

    @staticmethod
    def _media_cache_dir() -> Path:
        """插件自有媒体缓存目录（data/plugins_media_cache/）。

        不用框架 data/temp：temp_monitor 的保护期只有 60s（core/temp_monitor.py），
        在飞批次滞留超过 60s 时其中的图片文件会被误删 —— 自有目录不受其管辖。
        """
        try:
            from core.utils.path_utils import get_data_path
            return Path(get_data_path()) / "plugins_media_cache"
        except Exception:
            return Path("data") / "plugins_media_cache"

    async def _persist_media(self, elem, md5: Optional[str] = None):
        """到达即落盘：url 型媒体字节持久化到插件自有缓存目录，并设置 elem._temp_path。

        返回 (path, md5v)：md5v 为内容 md5（调用方传入或现算）——Fix 2 让调用方
        「先落盘拿字节、md5 从字节现算」，url 型媒体全程只下载一次（旧实现
        hash_image 与 to_base64 各下载一次）。

        - 不改 elem.file/file_type：native 模式与框架渲染（compress/to_path）完全不受影响，
          to_path() 命中 _temp_path 直接返回本地文件 → 识别/渲染/read_file 全链 URL 失效免疫；
        - 文件名按内容 md5 命名：同图天然去重，重复命中只刷新 mtime（LRU）；
        - 任何失败都静默降级（返回 (None, None)），不影响后续 URL 路径。
        """
        if not self.media_cache_enabled:
            return None, None
        try:
            if getattr(elem, "file_type", "") != "url":
                return None, None
            old = getattr(elem, "_temp_path", None)
            if old and os.path.exists(old):
                return old, md5
            b64 = await asyncio.wait_for(elem.to_base64(), timeout=self.download_timeout)
            if not b64:
                return None, None
            if b64.startswith("data:"):
                b64 = b64.split(",", 1)[-1]
            data = base64.b64decode(b64)
            if not data:
                return None, None
            md5v = md5 or hashlib.md5(data).hexdigest()
            # guard 已知媒体登记：字节已在手，图片/表情顺手算 dHash（同图重压缩副本
            # 也能被 guard 识别为已知媒体）；失败静默，不影响落盘主流程
            try:
                _ph = None
                if elem.__class__.__name__ in ("Image", "Sticker"):
                    _ph = await asyncio.to_thread(self._phash_of_bytes, data)
                self._remember_known(md5=md5v, phash=_ph)
            except Exception:
                pass
            ext = ""
            try:
                mime = (getattr(elem, "mime", None) or "").split(";")[0].strip()
                if mime and "/" in mime:
                    import mimetypes
                    ext = mimetypes.guess_extension(mime) or ""
            except Exception:
                ext = ""
            if not ext:
                ext = ".jpg" if elem.__class__.__name__ in ("Image", "Sticker") else ".bin"
            cache_dir = self._media_cache_dir()
            path = cache_dir / f"{md5v}{ext}"

            def _write():
                cache_dir.mkdir(parents=True, exist_ok=True)
                if path.exists():
                    os.utime(path, None)   # 同内容文件已存在：刷新 mtime（LRU 热度）
                    return
                tmp = cache_dir / f".{md5v}.{os.getpid()}.tmp"
                with open(tmp, "wb") as f:
                    f.write(data)
                os.replace(tmp, path)      # 原子落盘，防并发读半截文件

            await asyncio.to_thread(_write)
            try:
                elem._temp_path = str(path)
            except Exception:
                pass
            self._ensure_mc_cleanup()
            return str(path), md5v
        except Exception:
            return None, None

    def _mc_protected_paths(self) -> set:
        """在飞/待识别条目引用的缓存文件（对应批次还没进 LLM，绝不能删）。"""
        out = set()
        try:
            for bucket in (self._round_media or {}).values():
                for info in (bucket or {}).values():
                    p = getattr((info or {}).get("elem"), "_temp_path", None)
                    if p:
                        out.add(str(p))
            for info in (self._pf_infos or {}).values():
                p = getattr((info or {}).get("elem"), "_temp_path", None)
                if p:
                    out.add(str(p))
        except Exception:
            pass
        return out

    def _ensure_mc_cleanup(self):
        """懒启动媒体缓存清理后台任务（需要运行中的事件循环；terminate 时取消）。"""
        if not self.media_cache_enabled:
            return
        t = self._mc_cleanup_task
        if t is not None and not t.done():
            return
        try:
            task = asyncio.create_task(self._mc_cleanup_loop())
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            self._mc_cleanup_task = task
        except Exception:
            pass

    async def _mc_cleanup_loop(self):
        """媒体缓存治理：每 10 分钟扫一次（TTL 过期 + 总量 LRU），异常不炸任务。"""
        try:
            while True:
                await asyncio.sleep(600)
                try:
                    # 受保护集合在事件循环侧先算好（遍历运行态字典），再进线程做磁盘扫描
                    protected = self._mc_protected_paths()
                    await asyncio.to_thread(self._mc_sweep_sync, protected)
                except Exception:
                    pass
        except asyncio.CancelledError:
            return

    def _mc_sweep_sync(self, protected: set):
        """清理媒体缓存目录：TTL 过期删除；超总量上限按 mtime LRU 淘汰（线程内运行）。

        保护规则：① 在飞/待识别条目引用的文件不删；② 宽限期（10 分钟）内的文件不删
        —— 可能刚落盘还没进索引。
        """
        try:
            d = self._media_cache_dir()
            if not d.exists():
                return
            ttl = max(0.0, float(self.media_cache_ttl_hours)) * 3600
            cap = max(1, int(self.media_cache_max_mb)) * 1024 * 1024
            grace = 600.0
            now = time.time()
            entries = []
            total = 0
            for f in d.iterdir():
                try:
                    if not f.is_file() or f.name.startswith("."):
                        continue
                    st = f.stat()
                except OSError:
                    continue
                entries.append((st.st_mtime, st.st_size, f))
                total += st.st_size

            def _removable(mtime, f):
                return now - mtime > grace and str(f) not in protected

            removed = 0
            if ttl > 0:
                for mtime, size, f in entries:
                    if now - mtime > ttl and _removable(mtime, f):
                        try:
                            f.unlink()
                            total -= size
                            removed += 1
                        except OSError:
                            pass
            if total > cap:
                for mtime, size, f in sorted(entries):
                    if total <= cap:
                        break
                    if not _removable(mtime, f):
                        continue
                    try:
                        f.unlink()
                        total -= size
                        removed += 1
                    except OSError:
                        pass
            if removed:
                logger.info(f"[MediaRecognize] 媒体缓存清理：删除 {removed} 个文件"
                            f"（剩余约 {total // 1024 // 1024}MB）")
        except Exception:
            pass

    # ================= 生命周期（插件 initialize / terminate 调用） =================

    def activate(self):
        """插件 initialize 时调用：启用框架 desc_img 安全接管（幂等，可重复调用）。"""
        try:
            install_desc_img_guard(self)
        except Exception as e:
            logger.warning(f"[MediaRecognize] desc_img 接管启用失败（不影响本模块识别）: {type(e).__name__}: {e}")

    async def shutdown(self):
        """插件 terminate 时调用：还原 desc_img 接管、取消后台任务（清理/预取 worker）。幂等。"""
        try:
            uninstall_desc_img_guard()
        except Exception:
            pass
        t = self._mc_cleanup_task
        self._mc_cleanup_task = None
        if t is not None and not t.done():
            t.cancel()
        for task in list(self._bg_tasks):
            if not task.done():
                task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    # ================= 描述索引查询（框架 desc_img 接管用） =================

    async def _desc_index_lookup(self, image) -> Optional[str]:
        """按 md5 → dHash 查本模块描述索引，命中返回描述（零 VLM）；未命中返回 None。

        md5 取不到时读本地字节（_temp_path / path 型 file）现算；dHash 兜底覆盖
        「同图不同字节」的副本（框架压缩后 md5 与 stage1 原图键分叉，靠画面指纹补救）。
        """
        if not self.enabled:
            return None
        data = None
        md5 = getattr(image, "md5", None) or None
        local = getattr(image, "_temp_path", None)
        if not local and getattr(image, "file_type", "") == "path":
            local = getattr(image, "file", None)
        if not md5 and local:
            data = await self._read_media_bytes(local)
            if data:
                md5 = hashlib.md5(data).hexdigest()
        if md5:
            desc = await self._cache_get(md5)   # 读取侧已过 _is_valid_desc
            if desc:
                return desc
        if data is None and local:
            data = await self._read_media_bytes(local)
        if data:
            ph = await asyncio.to_thread(self._phash_of_bytes, data)
            desc = self._phash_nearest(ph)
            if desc:
                if md5:
                    await self._cache_set(md5, desc)   # 顺手登记新键，下次 md5 直接命中
                return desc
        return None


def _open_image(data: bytes):
    from PIL import Image as PILImage
    return PILImage.open(BytesIO(data)).convert("RGB")


def _reencode_jpeg(data_url: str, quality: int) -> Optional[str]:
    """JPEG 重编码（quality_enabled 时省 token/带宽）。

    PIL 解码/编码是 CPU 操作：调用方必须用 asyncio.to_thread 移出事件循环。
    失败返回 None（调用方按识别失败降级）。
    """
    try:
        _, _, b64 = data_url.partition(",")
        if not b64:
            return None
        img = _open_image(base64.b64decode(b64))
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        return f"data:image/jpeg;base64,{base64.b64encode(buf.getvalue()).decode()}"
    except Exception:
        return None


# ================= 框架 desc_img 安全接管（插件侧 wrap，不改框架文件） =================
#
# 背景（详见 KiraAI插件全量排查与修复方案.md §1.2-G4 / §2.3）：
# 框架的官方 VLM 有 4 个调用点，其中两处经 core.utils.common_utils.desc_img：
#   F1 core/message_manager.py（批次渲染时 ele.caption is None）
#   F2 core/plugin/builtin_plugins/agent/main.py（LLM 用 read_file 补读图片文件）
# 这两处**无超时**（SDK 默认 600s×重试，单图最坏可卡住整个批次管线），且 F2 完全
# 绕开本模块的三级限流 —— 插件识别失败后 LLM 循 file_path 补读 = 同图第二次付费。
# 这里在插件 initialize 时对三个模块的 desc_img **属性引用**做幂等包装
# （from-import 绑定的是模块属性，必须逐模块替换），terminate 时还原：
#   (a) 先查本模块的 md5/dHash 描述索引，命中直接返回（零 VLM）；
#   (b) 未命中调原函数并套 asyncio.wait_for(media_timeout)；
#   (c) 异常/超时返回 ""（与原契约一致：调用方本就按 "" 降级）。
_DESC_IMG_PATCHED: list = []   # [(module, original)]，还原用


def install_desc_img_guard(recognizer) -> int:
    """对框架三处 desc_img 引用做幂等包装（见上）。返回新包装的点数。

    getattr 防御：任一模块/属性不存在就跳过该点；已被包装（含其它实例包装过）
    则跳过（幂等防重入）。
    """
    import importlib
    patched = 0
    for modname in (
        "core.message_manager",
        "core.plugin.builtin_plugins.agent.main",
        "core.utils.common_utils",
    ):
        try:
            mod = importlib.import_module(modname)
        except Exception:
            continue                        # 模块不存在（精简部署）：跳过该点
        orig = getattr(mod, "desc_img", None)
        if orig is None or getattr(orig, "_kira_media_guard", False):
            continue                        # 属性不存在 / 已被包装：幂等跳过
        _orig = orig

        async def _guarded_desc_img(*args, _orig=_orig, **kwargs):
            image = kwargs.get("image")
            if image is None and len(args) >= 2:
                image = args[1]
            # (a) 先查插件 md5/dHash 描述索引，命中直接返回（零 VLM）
            try:
                if image is not None:
                    hit = await recognizer._desc_index_lookup(image)
                    if hit:
                        return hit
            except Exception:
                pass
            # (a2) 已知媒体收口：媒体指纹已被插件登记（将识别/已识别/已缓存），说明
            # 插件流水线已接管它 —— 框架 read_file 补读绝不再触发第二次付费 VLM。
            # 在途识别可短等拿真描述（guard_read_file_wait）；等不到返回空占位。
            # 刻意不拦截：用户配置「不识别」的媒体（未登记）与从未见过的工作区文件
            # —— 前者尊重省 VLM 意图，后者保持 read_file 对任意文件的可用性不变。
            try:
                known, md5, ph = await recognizer._known_media_check(image)
            except Exception:
                known, md5, ph = False, None, None
            if known:
                wait_s = float(getattr(recognizer, "guard_read_file_wait", 8) or 0)
                if wait_s > 0:
                    try:
                        desc = await recognizer._await_inflight_desc(md5, ph, wait_s)
                    except Exception:
                        desc = None
                    if desc:
                        return desc
                logger.info("[MediaRecognize:guard] 拦截框架 VLM 补读：插件已接管该媒体，不重复付费（返回空占位，识别由插件流水线负责）")
                return ""
            # (b) 未命中调原函数并包 wait_for(media_timeout)；
            # (c) 异常/超时返回 ""（与原契约一致：框架调用方本就按 "" 降级）
            try:
                return await asyncio.wait_for(_orig(*args, **kwargs), recognizer.media_timeout)
            except Exception:
                return ""

        _guarded_desc_img._kira_media_guard = True   # 幂等标记
        try:
            setattr(mod, "desc_img", _guarded_desc_img)
            _DESC_IMG_PATCHED.append((mod, _orig))
            patched += 1
        except Exception:
            continue
    if patched:
        logger.info(
            f"[MediaRecognize] 已接管框架 desc_img（{patched} 处引用：缓存命中零 VLM + "
            f"wait_for({recognizer.media_timeout:.0f}s) 超时兜底，terminate 时还原）"
        )
    return patched


def uninstall_desc_img_guard() -> int:
    """还原所有 desc_img 包装（插件 terminate 调用）。返回还原的点数。"""
    restored = 0
    while _DESC_IMG_PATCHED:
        mod, orig = _DESC_IMG_PATCHED.pop()
        try:
            # 只还原仍指向我们包装的引用（不覆盖框架/其它插件后来的改动）
            cur = getattr(mod, "desc_img", None)
            if getattr(cur, "_kira_media_guard", False):
                setattr(mod, "desc_img", orig)
                restored += 1
        except Exception:
            pass
    if restored:
        logger.info(f"[MediaRecognize] 已还原框架 desc_img（{restored} 处）")
    return restored
