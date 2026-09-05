# KiraAI_sustained_chat_plugin/可持续聊天 v2.5.4

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/znq19/KiraAI_sustained_chat_plugin)

# — 让 AI 真“主动”起来

> 这不是一个普通的聊天优化插件，而是一套完整的“社交主动性引擎”。

想象一下：你的 AI 不再是只会被动回答的机器，而是会主动找你聊天、在群聊中自然接话、甚至定时关心你的数字伙伴。

但这一切都有一个前提：**不会刷屏、不会烦人**。插件通过多维度的频率控制机制，让 AI 在保持存在感的同时，始终得体自然。

---

## ✨ 核心亮点

### 1. 群聊持续对话 —— AI 不再高冷，但绝不刷屏

**传统 bot**：被 @ 了才回复，回复完就消失，群里再热闹也与它无关。

**本插件**：AI 回复后，会在一段时间内“在场”。群友接着聊，AI 有一定概率再次介入，就像群里的真实成员一样自然。

最关键的是：**插件通过三层机制保证 AI 不会刷屏**——

| 控制层 | 机制 |
|--------|------|
| **概率控制** | 每条非唤醒消息只以 `sustain_reply_probability`（默认 50%）的概率触发回复 |
| **窗口控制** | 只有 AI 回复后的 `sustain_window_seconds` 窗口内的消息才有机会触发 |
| **次数控制** | `max_sustain_replies` 限制一次唤醒后最多连续回复的次数（可设 -1 无限） |

```yaml
# 一个典型场景
周武: @Shana 你吃饭了吗？
Shana: 吃了！主人今天吃的什么呀？
周武: 我吃了牛肉面
[30秒内的这条消息通过判断并符合Kira原版缓冲机制，AI 可主动接话]
Shana: 牛肉面！听起来好香，我也想吃！
[之后 AI 不会再继续接话，除非再次触发或用户再次@]
```

- ✅ 可配置窗口时间、回复概率、最大连续回复次数
- ✅ 支持 `per_message`（每条消息独立判断）和 `per_round`（窗口内只判断一次）两种模式
- ✅ 支持停止关键词（用户/AI说“别聊了”即可终止）
- ✅ 支持群聊作用域白名单/黑名单（`sustain_allowed_sessions` / `sustain_denied_sessions`），精准控制哪些群启用
- ✅ **判定时机可配置**（`sustain_judge_timing`，默认 `either`）：LLM 处理期间与回复后两个时机都判定，也可只选其一或重叠时一轮只判一次
- ✅ **停止即真停**：AI 空消息 / 停止词（或用户停止词、达上限）终止本轮后，LLM 请求兜底开窗不再重开窗口，停窗前已命中的积压消息也不再追加回复，直到下次真实唤醒（@/唤醒词/引用回复）

> **为什么需要「判定时机」？**
>
> 持续窗口只在 AI 回复后打开时，若 AI 正在处理一条消息（LLM 请求 + 工具循环可能持续数十秒），期间群友发来的消息全部进入缓冲队列——如果之后没有新消息触发 flush，这些消息就永远不会被处理，AI 会“错过”群里的对话。
>
> `sustain_judge_timing` 控制判定时机：
> - `both`：LLM 处理期间（含工具循环）与回复后两个时机都判定，消息不丢且接话自然
> - `either`（默认）：任一时机判定，窗口重叠时一轮只判一次（per_round 下严格保持“一轮只判一次”）
> - `llm_processing`：仅 LLM 处理期间判定，回复后不开判定窗
> - `after_reply`：仅 LLM 回复后判定，处理期间消息只缓冲不判定（持续感最强，本插件传统行为但处理期间消息可能正好错过）

---

### 2. 私聊持续对话 —— AI 会主动找你，但绝不会骚扰

**传统 bot**：私聊中，用户发一句，AI 回一句，用户不发就不回，像一台机器。

**本插件**：AI 回复后，若用户在设定时间内没有新消息，AI 会根据概率主动发起一条新消息。

同样地，**插件通过多层控制确保不会过度骚扰**：

| 控制层 | 机制 |
|--------|------|
| **随机窗口** | 每次等待时间在 `dm_sustain_window_range`（如 `30s/10s`）内随机，避免规律性骚扰 |
| **概率控制** | 超时后只以 `dm_sustain_reply_probability`（默认 30%）的概率触发主动回复 |
| **次数控制** | `dm_max_sustain_replies` 限制连续主动回复次数 |
| **重试控制** | `per_retry` 模式下，`dm_max_retry_attempts` 限制失败后最多重试次数 |
| **停止关键词** | 用户/AI消息含停止词终止窗口 |
| **私聊独立存在感节流** | `dm_presence_enabled`（默认开）：私聊有独立的 k_prob 调节系数与评分参数（窗口 10 条、目标占比 0.7、阈值 30、加分 2 扣分 3），默认值更适合一对一节奏，也可关掉与群聊共享 |

```yaml
# 私聊场景
用户: 今天好累啊
AI: 辛苦了主人，要不要休息一下？
[30 秒后用户没说话]
AI: 对了主人，我刚刚看到一个好笑的视频，想不想看？
[之后如果没有用户响应，AI 不再继续打扰，除非用户再次主动发起对话]
```

- ✅ 可配置等待时间（支持 `30s/10s` 这样的随机范围，显得更自然）
- ✅ 支持 `per_round`（只尝试一次）和 `per_retry`（失败后继续重试，最多 N 次）
- ✅ 支持白名单/黑名单，精准控制哪些私聊启用
- ✅ 可自定义“主动触发提示词”，让 AI 知道如何开口
- ✅ 私聊和群聊的持续对话互相独立，互不干扰

---

### 3. 定时主动任务 —— AI 会“想起来”找你，并阅读上下文

**传统 bot**：只能在用户触发时响应，从未主动“想起”过任何事情。

**本插件**：AI 可以定时（间隔或 Cron 表达式）在指定会话中主动发送消息，就像它自己“想起来”了一样。

**关键能力**：定时任务在执行前会**读取会话的历史记录**（默认最近 10 条消息），确保 AI 说的话**与当前上下文相关**，而不是生硬的模板消息。

```yaml
# 一个有趣的用法
定时任务: 每 2 小时在群里问一句“有人在吗？”
效果: AI 像群里的活跃成员一样，时不时冒个泡，但因为间隔较长，不会造成刷屏
```

| 控制层 | 机制 |
|--------|------|
| **间隔控制** | 支持随机间隔（如 `5m/30s`，4.5~5.5 分钟），避免固定节奏被识破 |
| **数量控制** | `scheduled_max_per_round` 限制每轮最多向多少个会话发送 |
| **会话随机** | 从 `scheduled_sessions` 中随机抽取，避免每次都同一批 |
| **上下文读取** | 执行前读取最近 N 条历史记录，确保发言相关性 |

- ✅ 支持间隔式（如 `5m/30s`，表示 4.5~5.5 分钟随机触发）
- ✅ 支持 Cron 表达式（如 `0 9 * * *`，每天 9 点）
- ✅ 可配置会话列表、每次随机抽取数量（避免所有群同时发）
- ✅ **执行前读取会话历史记录**（`scheduled_context_count` 可配置），确保发言有上下文依据
- ✅ 若会话无历史，可自动从 OneBot API 拉取最近消息作为初始上下文（`scheduled_fetch_history`）
- ✅ 可自定义提示词和工具黑名单

---

### 4. 细粒度的消息处理控制

插件内置了强大的消息预处理能力，对KiraAI默认聊天插件进行优化，让你精确控制 AI 能看到什么：

| 功能 | 说明 |
|------|------|
| **图片控制** | 仅唤醒消息识图 / 按概率识图 / 限制每张消息最多保留图片数 |
| **语音控制** | 群聊仅唤醒消息识别 / 私聊需引用才识别 / 限制最大识别时长 |
| **转发消息控制** | 仅唤醒消息展开转发内容 / 全部替换为 [转发消息] |
| **非唤醒消息缓冲** | 非唤醒消息是否作为上下文 / 最大缓存条数 |

---

### 5. 私聊 & 定时任务的工具黑名单

**痛点**：定时任务或私聊主动触发时，AI 可能会调用某些不该用的工具（如修改数据库、发送外部请求等）。

**解决方案**：为私聊持续对话和定时任务分别独立配置工具黑名单，支持 `partial`（包含关键词即禁用）和 `exact`（完全匹配才禁用）两种模式。

```json
"dm_tool_blacklist": ["memory_add", "github_search"],
"dm_tool_blacklist_mode": "partial"
```

---

### 6. 无需重启，实时生效

所有配置在 WebUI 中修改后，插件会自动重新加载，无需重启 KiraAI。

---

### 7. 新版内置：回复更快、更省 token

除了“主动社交”，这个版本还内置了两项让日常对话更顺滑的能力：

| 能力 | 效果 |
|------|------|
| **队列合并（积压处理）** | LLM 处理慢、消息爆发时，同一会话的积压批次自动合并为一次推送，**上下文只发送一次、LLM 调用次数大幅减少——更省 token**，回复更聚焦 |
| **并行媒体识别** | 图片 VLM 与语音 STT **并行预处理**，积压批次排队期间媒体即识别完成，推送时零等待——**回复更快**，看图听音不卡顿 |

```yaml
# 消息爆发场景示例（开启队列合并后）
用户连发: 消息1 / 消息2 / 消息3（LLM 正在处理上一条）
→ 三条消息合并为一个批次，一次 LLM 调用统一回应
→ 而不是三次调用、三段上下文重复发送（更省 token）
```

- ✅ 队列合并默认"不攒批"（当前批次一完成立即合并推送），软合并/超时合并阈值可配（`section_queue_merge`）
- ✅ 并行媒体识别可配最大并行图片数/语音数、兼容并行识图插件分工（`section_media_recognition`）
- ✅ 两个新模块均默认开启、可独立关闭，关闭后行为与旧版完全一致
- ✅ **媒体识别填充 file_path**（v2.3.2）：识别后的图片标识符带本地文件路径（`[Image #id: 描述, file_path: data/temp/xxx.jpg]`），对齐原版 `message_format_to_text` 行为，LLM 可直接用路径做图生图/上传等操作

### 8. 存在感节流 —— 回少提高、回多降低，永远得体

**痛点**：bot 在群里太活跃会刷屏惹人烦，太安静又像消失。固定概率无法感知群里的“热闹程度”。

**本插件**：实时统计最近 N 条消息里 bot 的发言占比，动态调节触发概率——**回少了提高、回多了降低**，让 bot 始终保持在目标占比附近。

| 控制项 | 机制 |
|--------|------|
| **占比统计** | 取最近 `presence_window_size`（默认 20）条消息计算 bot 发言占比，可加时间衰减（`presence_decay_minutes`，默认 10 分钟） |
| **调节系数 k_prob** | 占比高于 `presence_target_ratio`（默认 0.3）则降概率，低于则提概率；系数钳制在 `presence_k_min`（0.2）~ `presence_k_max`（2.0）之间 |
| **闲时加分** | 静默时长高于该会话历史平均时加分（`idle_bonus_score`，默认 15），活跃群/死群标准不同 |
| **评分补正** | 拆为两个独立模式：`score_gate_deny`（门槛过滤：评分不足时阻止概率命中，分继续攒）+ `score_gate_boost`（补偿触发：评分达标时强制补发，触发后清零）。三条通路各自独立控制（section_presence / section_group_sustain / section_dm_sustain） |
| **强制通路超额抑制** | `force_suppress` 开启后，bot 发言占比过高时，即使被唤醒也降级为评分门槛（分值到了才回） |

```yaml
# 一个典型场景
群里很热闹，bot 已经连回了好几条 → 占比升高 → k_prob 调低 → 触发概率下降
群里冷清，bot 很久没说话 → 占比降低 + 闲时加分 → k_prob 调高 → 更容易接话
```

- ✅ 所有主动触发（群聊持续对话、私聊主动、定时任务）都受存在感节流约束
- ✅ 调节系数实时计算，无需重启，WebUI 改配置即生效

---

### 9. 骚扰感知化 —— bot 会“察觉”被骚扰，并主动屏蔽

**痛点**：有人疯狂戳一戳、连续 @、刷关键词、反复引用唤醒，bot 只能被动回应，无法拒绝。

**本插件**：检测到骚扰信号后，通过 **System 通知** 告知 bot，bot 用 **XML tag** 自主决策是否屏蔽、屏蔽谁、屏蔽多久。

| 信号 | 检测方式 | 决策 tag |
|------|----------|----------|
| **戳一戳** | 时间窗内被戳次数达阈值 | `<poke_ignore>` |
| **连续 at** | 时间窗内被 @ 次数达阈值 | `<at_ignore>` |
| **连续关键词** | 时间窗内命中唤醒词次数达阈值 | `<kw_ignore>` |
| **引用唤醒** | 时间窗内被引用回复次数达阈值 | `<reply_ignore>` |

**XML tag 语法**（值 `user|duration:N` / `all|duration:N` / `none`）：

```xml
<poke_ignore>user|duration:180</poke_ignore>   <!-- 屏蔽某用户戳一戳 180 秒 -->
<at_ignore>all|duration:300</at_ignore>        <!-- 屏蔽所有人 at 300 秒 -->
<kw_ignore>none</kw_ignore>                    <!-- 不屏蔽 -->
```

- ✅ 默认屏蔽 180 秒（`default_duration`），bot 可自设时长（`allow_bot_duration`），钳制到最大 300 秒（`max_duration`）
- ✅ 各信号独立开关、独立窗口/阈值/累计范围（`per_user` 按单用户 / `all` 按会话）
- ✅ **manage_ignore 工具**：bot 可主动调用 `block` / `unblock` / `list` 管理屏蔽名单
- ✅ 屏蔽名单持久化，重启不丢

---

### 10. 休眠时段 —— bot 也会“睡觉”，起夜有概率

**痛点**：深夜群里没人，bot 却还在定时任务/持续对话里冒泡，显得很“假”。

**本插件**：配置休眠时段后，休眠期内 bot 不主动触发；被提及（@/唤醒词/戳一戳）时按**起夜概率**决定是否推送给 LLM。

| 控制项 | 机制 |
|--------|------|
| **休眠时段** | `dormant_ranges` 列表，格式 `HH:MM-HH:MM`，`start>end` 表示跨午夜（如 `23:00-08:00`）；默认空 = 全天活跃 |
| **起夜概率** | `dormant_wake_probability`（默认 0.3），休眠期内被提及推送给 LLM 的概率 |
| **维持模式** | `wake_keep_mode`：`renew`（续窗型，LLM 最后回复完再没人找才计时）/ `once`（一次性型，唤醒后计时到点结束） |
| **维持时长** | `wake_keep_seconds`（默认 300 秒），唤醒后保持可聊的时长 |
| **互动上限** | `wake_max_rounds`（默认 -1 不限），唤醒后最大互动次数 |
| **主动续窗** | `wake_max_extensions`（默认 -1 无限），bot 主动续窗次数上限（0 不能续） |

```yaml
# 一个典型场景
休眠时段: 23:00-08:00
深夜 1 点有人 @bot → 按起夜概率 30% 决定是否回应
回应后进入维持期（renew 模式，300 秒）→ 期间可正常聊
维持期结束 → 回到休眠，不再主动冒泡
```

- ✅ 休眠期内所有主动触发（持续对话、私聊主动、定时任务）全部静默
- ✅ 起夜概率、维持模式、续窗限制均可配，bot 不会在深夜刷屏

---

### 11. 通知合并 —— 骚扰通知不刷屏，一次 LLM 调用全处理

**痛点**：多个骚扰信号同时触发时，若每个都单独通知，会瞬间刷屏。

**本插件**：per-session 挂起队列，通知先进队列，`on_llm_request` 时**统一注入**（一次 LLM 调用处理所有通知）；bot 休眠中长时间无请求时，短窗口（默认跟随 `max_message_interval`）兜底统一 publish。

- ✅ 同一会话的多个通知合并为一条，不刷屏
- ✅ 有 LLM 请求时随请求注入，无请求时短窗口兜底，通知不丢失

---

### 12. queue_merge 升级为 z 版 + 补回 drop_sustain_pending

**自拦截防护双保险**：合并/重放批次打 `_qm_self` 自发布标记，`on_batch_message` 识别后无条件放行；推送决策用 `done_event_id` 双保险，锁内确认 in-flight 才执行——对一切竞态路径（tick、shutdown 重发、重复广播）免疫自拦截。

**补回 `drop_sustain_pending`**：持续对话停止时，丢弃 pending 中「仅由持续命中消息触发」的积压批，避免停止后仍被积压消息复活。

---

## 🎛️ 配置概览

| 模块 | 功能 |
|------|------|
| `section_basic` | 唤醒词、非唤醒消息缓冲、群聊主动发言、**群聊主动概率评分/概率调节** |
| `section_media` | 图片/语音/转发消息的识别与过滤 |
| `section_group_sustain` | 群聊持续对话（窗口、概率、模式、停止词） |
| `section_dm_sustain` | 私聊持续对话 + **私聊独立存在感节流参数** + **评分补正/概率调节开关** |
| `section_scheduled` | 定时主动任务（间隔/Cron、会话列表、工具黑名单、提示词） |
| `section_queue_merge` | 队列合并/积压处理（积压批次合并推送，更省 token） |
| `section_media_recognition` | 并行媒体识别（图片 VLM + 语音 STT 并行预处理，回复更快） |
| `section_presence` | 存在感节流 + **提及消息评分（群聊/私聊）** |
| `section_poke` / `section_at` / `section_keyword` / `section_reply` | 骚扰感知化（戳/at/关键词/引用检测 + XML 决策屏蔽） |
| `section_dormant` | 休眠时段（起夜概率 + 维持期 + 主动续窗限制） |

---

## ⚙️ 时间表达式说明

| 写法 | 含义 |
|------|------|
| `30s/10s` | 等待 20~40 秒（随机） |
| `1min/30s` | 等待 30~90 秒（随机） |
| `5m/30s` | 等待 4.5~5.5 分钟（随机） |
| `1h/15m` | 等待 45~75 分钟（随机） |
| `30s` | 固定等待 30 秒 |

---

## 📦 依赖

```txt
croniter>=1.3.0
```

用于 Cron 表达式解析，安装 `pip install croniter`。

---

## 🚀 快速开始

0. 关闭KiraAI默认聊天插件或其他同类型处理插件
1. 将本插件文件夹放入 `data/plugins/`
2. 在 WebUI 插件设置中配置需要的功能
3. 重启 KiraAI 或禁用/启用插件使配置生效
4. AI 将开始拥有“主动社交”能力！

---

## 💡 设计哲学

这个插件的核心理念是：**AI 应该根据人设有主动的不仅是对话的能力**。

通过群聊持续对话、私聊主动聊天、定时主动任务三个维度的能力组合，AI 可以：

- 在群聊中像真人一样“在场”（持续对话）
- 在私聊中像朋友一样“关心你”（主动触发）
- 在特定时间“想起来”找你（可随机的定时任务）

三者叠加，AI 就从一个被动的工具，变成了一个**主动的数字伙伴**。

**最重要的是**：所有主动行为都受到**多维度频率控制**，确保 AI 有存在感但不烦人，主动但不骚扰——这才是“活”的 AI 应有的样子。

---

## 🙏 致谢

本插件的存在感节流（回少提高/回多降低）、休眠时段（起夜概率 + 维持期）等机制，在设计上参考并致敬了 **NoriEngine Chat**（[skyzhishui/kira-ai-plugin-noriengine-chat](https://github.com/skyzhishui/kira-ai-plugin-noriengine-chat)）的评分引擎思路——它率先用"存在感抑制 + 时段调度"让 KiraAI 在群聊中也有了心跳包的感受，监听全局消息成为可能，融合版在此基础上把语义判断交还给 LLM，规则只做节流与状态管理。感谢 skyzhishui 的先行探索。

---

## 📝 版本信息

- 当前版本：v2.5.4
- 兼容 KiraAI：v2.29.6+（插件图标需 v2.30.0+）
- 作者：KiraAI + znq19

<details>
<summary>更新日志</summary>

### v2.5.2

- **停窗撤销在途批次**：`_stop_sustain_round`（空消息/停止词/达上限停窗）现在会清理 batch_started/batch_count——仍在顺延等待中的"持续命中批次"不再被 flush（保险丝拦截），消息留在前文缓冲等下次真唤醒/概率命中时随前文一起送出
- **背景**：持续命中 → 空消息停止 → 顺延到点的时序下，已命中的消息会被"补刀"再次送进 LLM（空回复循环的余波）；修复后停止语义彻底——bot 闭口后不再误触发
- **安全性**：只清插件侧批次状态，不碰框架 session buffer——消息不丢（前文保留）、不卡回复（保险丝轻量拦截）、不影响真唤醒/满即推/队列合并（drop_sustain_pending 管 QueueMerge 层，本修复管 _debounce_loop 层，双层互补）
- **验证**：命中→停止→顺延到点 flush=0（被拦）；真唤醒后前文（含被撤销消息）完整带出

### v2.5.1

- **修复空消息无限重开循环**：`sustain_retry_on_empty` / `dm_retry_on_empty` 开启时，AI 空消息**不再无条件重开窗口**——改为先检查评分（含 bot 本次回复扣分后），**评分不足阈值 → 停止窗口**，评分达标才重开等评分补上再触发
- **背景**：持续对话概率=1 时，空消息 + 评分不足（1:1 对话下评分恒低）会造成"空消息→重开窗口→下条必命中→又空消息"的无限循环（日志连续次数无限累加）
- **验证**：评分 12 → bot 空回复 -5 → 7≥5 重开 ✓；评分 6 → bot 空回复 -5 → 1<5 停止 ✓
- 窗口超时逻辑复核无影响（群聊 `_end_sustain_window` / 私聊 `_dm_sustain_loop` 正常）

### v2.5.0

- **顺延误触发修复**：非唤醒消息只在**批次已开启**（有真唤醒/持续命中）时才重置顺延计时器；无唤醒来历的非唤醒消息只作为前文缓冲，绝不启动顺延/flush
- **flush 保险丝**：`_debounce_loop` flush 前校验批次唤醒来历，无唤醒来历跳过 flush（前文保留等下次真唤醒）
- **评分/概率/k_prob 判定全链路日志**：`[Enhance] 评分补正(...)`（deny 抑制 / boost 补触发 / 清零，info 级）+ 持续判定/积极概率/私聊判定的"概率×k_prob→有效概率、随机值、评分门"（debug 级）
- **poke 屏蔽拦截**：被屏蔽用户的戳一戳事件不进 LLM（poke 单屏蔽只挡戳，不拉黑普通消息）
- **manage_ignore duration 动态提示**：工具参数描述中的默认时长取自当前配置真实值（不写死）

### v2.4.9

- **消息缓冲模型重构（前文+批次）**：与原版语义对齐并修复丢消息——
  - `max_unmentioned_messages`：唤醒消息**之前**的非唤醒前文上限（超限弹最老前文，唤醒出现后前文锁定不裁剪）
  - `max_buffer_messages`：**从首个唤醒消息起**（含它）进入 buffer 的消息数，达到即满即推；批次内唤醒/普通消息一视同仁（不重置）
  - 推送内容 = 前文 + 批次全部；未满即推则顺延到点推送
- **修复丢消息**：原版"总 buffer 满即推"只数唤醒前存量、且非唤醒到达先裁剪可能弹掉唤醒——现按"前文上限 + 批次计数"模型，唤醒消息随批次完整送出，不再被裁剪丢弃

### v2.4.8

- **修复非唤醒消息不重置顺延**：之前 merge_window_seconds 顺延只被唤醒消息重置，非唤醒消息（receive_unmentioned）到达后计时器不重置——导致顺延形同"首条唤醒消息后固定 N 秒"。现在非唤醒消息也会重置计时器，真正实现"最后一条消息到达后 N 秒无新消息才 flush"

### v2.4.7

- **消息合并顺延默认启用**：`merge_window_seconds` 默认 -1（自动取 WebUI 设置值），新装/升级后立刻体现合并顺延特性
- **顺延调试日志**：`section_basic.debug_log_enabled`（默认关），开启后打印顺延开始/重置/结束日志，便于排查合并时机
- **清理死代码**：`queue_merge.py` 中未使用的 `merge_window_seconds` 字段移除（积压队列合并仍由 `max_merge_seconds` 超时控制）

### v2.4.6

- **队列合并防抖修复**：`_push_pending` 防抖等待移到锁外（之前 await 在锁内阻塞队列处理 + 防抖重置不生效）

### v2.4.6

- **修复：消息合并间隔顺延移到 buffer 层**：之前被错误实现在队列合并层导致 pending 合并前傻等，现移至框架 debounce 层实现真正的防抖重置（新消息到达重置倒计时）

### v2.4.4

- **私聊独立存在感节流**：`dm_presence_enabled`（默认开），私聊有独立评分/k_prob 参数（窗口 10 条、目标占比 0.7、阈值 30、加分 2 扣分 3）
- **概率调节独立开关**：`proactive_k_prob_enabled`（默认开）、`sustain_k_prob_enabled`（默认关）、`dm_k_prob_enabled`（默认关）
- **评分补正细化**：`proactive_score_gate_deny/boost`（section_basic，默认开）+ `sustain_score_gate_deny/boost`（默认关）+ `dm_sustain_score_gate_deny/boost`（默认关）+ `mentioned_score_gate_deny/boost`（群聊提及，默认关）+ `mentioned_dm_score_gate_deny/boost`（私聊提及，默认关）
- **提及消息评分**：`mentioned_score_gate_deny/boost`（群聊）和 `mentioned_dm_score_gate_deny/boost`（私聊），默认全关

### v2.4.2

- 拉黑语义：屏蔽=该用户/会话所有消息不再进入（含戳一戳/at/关键词/引用/刷屏）；poke 单独屏蔽只挡戳一戳
- 累计评分：用户消息 +1、bot 回复 -5，攒到阈值补触发一次后清零（必补）
- tick 防抖：修复积压批次被单独发布不合并的问题
- XML 合并：at_ignore/kw_ignore/reply_ignore 合并为 <ignore>（拉黑）

**存在感节流（`section_presence`）**
- 统计最近 N 条消息的 bot 发言占比，动态调节触发概率：回少提高、回多降低（k_prob 调节系数，钳制在 `presence_k_min`~`presence_k_max`）
- 评分补正（`score_gate_deny` + `score_gate_boost`）：门槛过滤与补偿触发独立控制，三条通路各自独立（section_presence / section_group_sustain / section_dm_sustain）
- 闲时加分（`idle_bonus_score`）：静默时长高于会话历史平均时加分
- 强制通路超额抑制（`force_suppress`）：bot 发言占比过高时，被唤醒也降级为评分门槛

**骚扰感知化（`section_poke` / `section_at` / `section_keyword` / `section_reply`）**
- 戳一戳 / 连续 at / 连续关键词 / 引用唤醒 频率检测 → System 通知 → bot 用 XML tag 决策屏蔽
- tag：`<poke_ignore>` / `<at_ignore>` / `<kw_ignore>` / `<reply_ignore>`，值 `user|duration:N` / `all|duration:N` / `none`
- 默认屏蔽 180s，bot 可自设时长钳制到 300s；`manage_ignore` 工具可主动管理（block/unblock/list）

**休眠时段（`section_dormant`）**
- `dormant_ranges` 休眠时段 list，默认空 = 全天活跃；起夜概率 `dormant_wake_probability`
- 维持期 `wake_keep_mode`（renew/once）+ `wake_keep_seconds` + `wake_max_rounds` + `wake_max_extensions` 主动续窗限制

**通知合并**
- per-session 挂起队列，`on_llm_request` 统一注入，短窗口兜底跟随 `max_message_interval`

**queue_merge 升级为 z 版**
- `_qm_self` 自发布标记 + `done_event_id` 双保险，对竞态路径免疫自拦截
- 补回 `drop_sustain_pending`：持续对话停止时丢弃仅由持续命中消息触发的积压批

### v2.3.4

**修复热重载后子模块不更新（AttributeError）**

- 框架热重载只重新 import `main.py`，`sys.modules` 中缓存的同目录子模块（`queue_merge` / `media_recognize`）不会更新，导致新版 main.py 调用子模块新增方法（如 `drop_sustain_pending`）时热重载后报 `AttributeError`
- `main.py` 导入子模块前对已缓存模块执行 `importlib.reload`，热重载即可加载子模块最新代码；首次加载无行为变化，reload 失败静默忽略
- 注意：从 ≤v2.3.3 升级本次仍需**完整重启一次**（清掉旧模块缓存），之后热重载即可正常生效

### v2.3.3

**修复持续对话停止后被「兜底开窗」复活**

- **根因**：AI 输出空消息 / 命中停止词（或用户停止词、达上限）停窗时窗口与计数被清零，但两点让停止形同虚设：① 停窗前已命中（被标记为唤醒）的积压批次仍留在 QueueMerge pending 中，放行后照常触发一次 LLM 回复；② 该批次的 LLM 请求又触发兜底开窗（连续次数 0），开启全新一轮，连锁产生更多与停止意图相悖的回复
- **修复**：
  1. 新增「本轮已终止」标志：上述停止路径置位，LLM 请求兜底开窗检测到后不再开窗，直到下次真实唤醒（@/唤醒词/引用回复）解除
  2. 持续命中时记录 message_id，用于区分「持续命中触发」与「真实唤醒」；停窗时丢弃 pending 中「仅由持续命中消息触发」的积压批（含真实唤醒消息的批次保留）；停窗后姗姗来迟的纯持续命中批次（debounce 尚未 flush）在批次入口直接拦截
- 覆盖 both / either / llm_processing / after_reply 四种判定时机（after_reply 无兜底开窗问题，但共享积压批清理）；per_message / per_round 同时生效
- 被丢弃批次的消息仍保留在会话缓冲中作为上下文，仅少一次回复，不丢上下文
- 兼容说明：若某适配器消息无 `message_id` 字段，积压批清理自动退化为不生效（不报错、不影响其他逻辑）

### v2.3.2

**媒体识别填充 file_path，对齐原版图片路径**

- 并行媒体识别把 Image/Sticker 替换为标识符后，LLM 拿不到本地文件路径，图生图/上传等工具找不到文件。现在 stage1（缓存命中）/ stage2（识别完成）/ stage3（历史兜底）都会对媒体调用 `to_path()` 落盘并转 `data/` 相对路径，填充为 `[Image #id: 描述, file_path: data/temp/xxx.jpg]`，对齐原版 `message_format_to_text` 行为
- 路径获取失败时降级为旧格式（不带 file_path）；`#id` 前缀保留，stage3 兜底正则、队列合并重放 `_done` 跳过逻辑不受影响
- Record 原版也不带路径，行为不变；File/Video 未被替换，仍走框架原逻辑，不动
- native 多模态模式行为不变（原版 native 也是 `[Image attached]` 不带路径）

### v2.3.1

**修复媒体识别填充崩溃（bad escape）**

- `_fill_text` / `_fill_chain` 的 `re.sub` 改为 `str.replace`：`re.sub` 的 replacement 是模板字符串，VLM/STT 返回的描述含反斜杠序列（如 Windows 路径 `C:\Users\...` 的 `\U`、`\x`）时抛 `re.PatternError: bad escape`，stage2 整批媒体识别崩溃。占位符是精确字面量，`str.replace` 无转义问题

### v2.3.0

**群聊持续对话：作用域控制 + 判定时机可配置**

> ⚠️ **升级提醒**：默认判定时机为 `either`（LLM 处理期间 + 回复后都判，窗口重叠时一轮只判一次），老用户升级后，AI 处理消息期间群友的发言也会触发持续回复（此前只有回复后才判）。如果觉得 bot 变吵，可将 `sustain_judge_timing` 设为 `after_reply` 恢复旧行为。

- **群聊作用域白名单/黑名单**（`sustain_allowed_sessions` / `sustain_denied_sessions`）：白名单非空时仅白名单内群生效；白名单为空时排除黑名单。格式如 `qq:gm:123456`，与私聊黑白名单语义一致
- **判定时机**（`sustain_judge_timing`，默认 `either`）：`both` 两个时机都判定；`either` 窗口重叠时一轮只判一次；`llm_processing` 仅 LLM 处理期间判定（回复后立即关闭兜底窗）；`after_reply` 仅回复后判定。LLM 处理期间兜底开窗覆盖处理中到达的消息——此前这些消息只进缓冲，无新消息触发 flush 时永远不会被处理（AI 会“错过”群聊）。窗口已存在时完全不动（不刷新不关闭），由 AI 最终回复按 timing 策略续期
- **修复长 LLM 处理绕过 max_sustain_replies**：窗口超时清理保留连续计数（count 只在真实唤醒时清零），工具循环超过窗口时长不再导致计数归零
- 作用域检查同时应用于消息判定与 AI 回复开窗，黑白名单外的群完全不受持续对话影响；群被移出作用域时顺带清理残留的窗口/计数状态
- 新增插件图标（`icon.png`，manifest 增加 `icon` 字段，遵循 KiraAI 最新 manifest 图标规范）

> 💡 **注意**：`per_message` 模式 + 高回复概率 + 长工具循环的组合下，LLM 处理期间兜底开窗可能连续命中积压消息（每条命中都会触发一次回复），如不希望这样，建议降低 `sustain_reply_probability` 或改用 `llm_processing` / `after_reply` 时机。

### v2.2.5

**兼容 KiraAI v2.31.0 原生多模态（native 模式）**

- 运行时检测 `bot_config.capabilities.image_recognition.mode == "native"`：图片保留在消息链中，由框架原生多模态直传模型（官方图片压缩 + kira_image_ref 持久化引用），本插件只做音频 STT，不再走 VLM 描述
- stage1 / stage2 / stage3 三阶段均跳过图片处理；非唤醒消息图片仍按「非唤醒不识别」策略替换为占位（省 token 设计不变）
- 默认 `vlm_description` 模式行为完全不变

### v2.2.4

**最后一步带工具即时收尾 + provider 全挂不误开窗**

- **队列合并不再哑 3 分钟**：agent 在最大步数（`max_tool_loop`）仍返回工具调用时，该步工具执行完即结束、无最终文本收尾。此前 `_final_marked` 只认“无 tool_calls 的文本收尾”，此类批次只能等「批次卡死超时」兜底（默认 LLM 超时 + 工具超时 ≈ 180s）才推送 pending，期间新消息全部被拦截（bot 哑 3 分钟，不丢消息）。现在 `on_llm_response` 通过 `resp.agent_step_index >= max_tool_loop` 识别“最后一步仍带工具”，同样标记收尾，由 tick / ON_STEP_RESULT 立即推送
- **provider 全挂时不再误开持续窗口**：框架在所有模型失败时返回 `[ProviderError] ...` 错误文本（无 tool_calls、直接收尾）。此前持续对话判定会把它当成正常 AI 回复而重新开窗，在 provider 恢复前反复主动触发。现在识别该前缀后静默结束，不开窗
- 兼容性：旧框架响应缺 `agent_step_index` 字段时自动退回原行为（等卡死兜底），不出错

### v2.2.3

**队列合并自拦截死锁修复（与 ContextCondensation 等阻塞型插件共存时稳定复现）**

- **根因**：`BatchMergeScheduler._push_pending()` 调用的 `_decide_and_apply_locked()` 会**无条件清空 `_inflight[sid]`**（即使 pending 为空）；而 KiraAI `EventBus.publish()` 只是**异步入队**（`asyncio.Queue.put`，见 `core/event_bus.py`），发布后的合并批次要等事件循环调度才到达 `on_batch_message`。在这个异步窗口内，若同一会话再次触发 `_push_pending`（ON_STEP_RESULT 重复广播、插件 hook 重复注册、tick 竞争等——`core/message_manager.py::send_llm_text()` 在 Agent 每一步都会触发 ON_STEP_RESULT），会把刚发布的合并批次的 inflight 标记清掉，导致该批次到达 `on_batch_message` 时匹配不上 `_inflight`，被误判为外部批次 `event.stop()` 拦截进 pending，会话队列永久死锁
- **日志特征**：`进入最后一步（文本收尾）` 打印两次（同 event_id）；`发布批次 xxx` 后紧跟 `拦截批次 xxx 进 pending（pending=1）`；之后新消息全部 `拦截进 pending` 且数量只增不减
- **修复**：
  1. `_push_pending(sid, done_event_id)` 增加完成批次校验：锁内先确认 `_inflight[sid]` 仍是本次完成的 event_id 才执行推送决策，重复/并发事件直接跳过，不再误清 in-flight 状态
  2. `_build_merged_batch` 为合并批次打 `_qm_self` 自发布标记，`on_batch_message` 识别后无条件放行并恢复 inflight 跟踪——双保险，对一切竞态路径（含 tick、shutdown 重发）免疫自拦截

### v2.2.2

**队列稳定性修复 + 媒体并发控制增强**
- **修复热重载丢消息**：插件终止时积压批次（pending）改为按会话合并为**全新批次**重发（新 event_id、干净 stop 状态）。原实现直接重发原事件对象，而框架 `_is_stopped` 一旦置位无法复位，重进管线会被再次拦停，消息永久丢失
- **新增「批次卡死超时」兜底（`section_queue_merge.inflight_stall_timeout`）**：当前批次自最后一次 LLM 响应起超过阈值仍无动静（LLM 挂起 / 异常崩溃导致收尾事件缺失）时，强制推送积压批次，避免会话队列死锁。默认 `0`=自动跟随默认 LLM 模型超时 + 60s 余量；LLM 慢但每轮有响应不会被误判
- **媒体识别并发限流改为三级**（`section_media_recognition`）：批次级（`max_parallel_images` / `max_parallel_audios`，单批突发保护）+ 会话级（`vlm/stt_max_parallel_per_session`）+ 全局级（`vlm/stt_max_parallel_global`），固定获取顺序无死锁
- **媒体「最多识别一次」**：同一消息内的每个图片/语音成功或失败后标记已处理，队列合并重发时不再重复调用 VLM/STT（防限流/429 风暴）
- **并行识图插件兼容增强**：`compat_mode=auto` 改为运行时实时检测并行识图插件加载状态（热重载/启停即时生效）；媒体积压放行判定同时识别本插件与并行识图插件的暂存属性
- 跨会话状态修复：媒体暂存索引按会话分层，多会话并发处理不再串扰

### v2.2.1

**STT 兼容修复**
- 适配新版 KiraAI：`ctx.llm_api.speech_to_text` 已废弃，改为 `provider_mgr.get_default_stt()` + `core.utils.common_utils.speech_to_text`，修复语音识别失效问题

### v2.2.0

**队列合并 / 积压处理（`section_queue_merge`）—— 更省 token**
- LLM 处理慢、消息爆发时，同一会话的积压批次自动合并为一次推送，上下文只发送一次、LLM 调用次数大幅减少
- 三分支推送决策：软合并（小积压提前合）/ 超时合并（攒批到点必合，默认 0=不攒批）/ 独立推送（都不满足时 1:1）
- 事件配对即时释放（ON_LLM_RESPONSE + ON_STEP_RESULT），无额外等待延迟；工具中间步不误触发
- 阈值防护：单次合并批次数 / 消息条数（-1 自动）/ 估计 token / 媒体批次上限，超限拆批留待下轮
- 开启调试日志（`debug_log_enabled`）可查看每个批次的放行/拦截/合并决策

**并行媒体识别（`section_media_recognition`）—— 回复更快**
- 图片 VLM 与语音 STT 并行预处理（同一 gather 混合并行，图片/语音独立限流），积压批次排队期间媒体即识别完成，推送时零等待
- 三阶段标识符架构：stage1 拍平嵌套转发并替换媒体为标识符（阻止框架串行识别）、stage2 并行识别填充、stage3 历史兜底
- STT 缓存复用框架 `image_desc_cache` 表（音频 md5 去重），重复语音零重复识别
- 兼容并行识图插件（`compat_mode=auto`：装了插件图片归它、本模块只做音频；不装则全权接管）
- VLM 描述词跟随 WebUI 配置（`bot_config.capabilities.image_recognition.desc_prompt`）

### v2.1.0

**语音时长限制修复**
- 修复 `voice_max_duration` 对机器人自己的语音消息无效的问题：当用户引用机器人发出的语音时，该语音缺少 `duration` 元数据，导致时长限制被绕过
- 新增 `_get_record_duration`：优先读取元数据 `duration`，缺失时自动从音频原始字节估算时长
- 新增音频时长估算能力：支持 WAV（通过 `wave` 模块解析帧头）和 MP3（通过首个有效帧头码率推算）两种格式
- 新增 `_record_bytes`：统一从 base64 / data_url / 本地路径三种来源提取音频原始字节
- 现在所有语音消息（含机器人自己的语音被引用）都受 `voice_max_duration` 约束，超长语音统一替换为 `[长语音 N秒]`

### v2.0.3

**群聊持续对话**
- 修复最大持续回复次数（`max_sustain_replies`）不计数的问题：命中后误调用整状态清理，导致计数被立刻清零
- 命中后改为仅关闭窗口并**保留计数**，AI 回复后再开新窗；达上限后不再开窗
- 真实唤醒（@ / 唤醒词）时重置连续计数，避免上一轮 max 卡死
- 明确 `per_message` / `per_round` 语义：两者命中后均关窗再开新窗；差别仅在未命中时是否继续判断后续消息

**私聊持续对话**
- 修复 `dm_max_sustain_replies` 只增不减、达上限后永久不再开窗的问题
- 用户真实发言时重置主动次数；主动触发成功时正确累加并保留计数
- 系统主动消息不再被误判为用户消息而清掉计数
- 日志补充当前主动次数 / 上限，便于排查

**框架对齐与其它修复**
- 对照官方 `core` 修复工具黑名单过滤：`ToolSet.tools` 为 `BaseTool` 实例，不再按 OpenAI function dict 解析
- `ON_LLM_RESPONSE` 跳过含 `tool_calls` 的中间步，仅在最终文本回复时处理持续窗口
- 修复转发消息开关逻辑：`forward_recognition_only_on_mention=false` 时正确保留全部转发
- 定时任务构造群聊事件时补全 `Group`，避免 `is_group_message()` 误判
- 时间表达式兼容 `1min` / `mins` 等写法；修正 `sustain_tasks` 初始化方式
- 补全被截断的 `_limit_media_count` 方法，修复插件无法加载的语法错误

</details>

---

**让 AI 不再被动，从这开始。**
