# KiraAI_sustained_chat_plugin/可持续聊天 v2.5.22

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
| **并行媒体识别** | 图片 VLM 与语音 STT **并行预处理**，积压批次排队期间媒体即识别完成，推送时零等待——**回复更快**，看图听音不卡顿（v2.5.18 修复调度竞态后**真正生效**，见更新日志） |

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
| `section_media_recognition` | 并行媒体识别（图片 VLM + 语音 STT 并行预处理，回复更快）；内含**避免重复识图（省 token）**开关：避免触发原有 VLM 重复识别、浪费 token，默认开，如确有兼容需要可关闭 |
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

0. 直接安装即可——本插件会自动检测并停用功能重叠的插件（框架内置 `default-chat`、独立 `anti-harass`）；若 `default-chat` 已配置唤醒词且本插件唤醒词为空，会自动迁移过来（不覆盖你自己填写的唤醒词）
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

- 兼容 KiraAI：v2.29.6+（插件图标需 v2.30.0+）
- 作者：KiraAI + znq19

<details>
<summary>更新日志</summary>

### v2.5.22

- **修复「AI 已是空 msg 却停不了窗」——判据从「整串」改为「可见输出」**
  - **现象**（用户日志）：bot 已输出空消息，`stop_on_ai_empty` 开着却不停窗，反复重开窗口：
    ```
    [message] LLM -> qq:gm:…: <reasoning>
    …无关消息保持沉默不刷屏，继续空msg。
    </reasoning>
    <msg />
    [plugin] [accel] 一轮结束 … steps=1
    ```
  - **根因**：旧判据 `_is_empty_msg` 用
    `^\s*<msg\s*/>\s*$|^\s*<msg>\s*</msg>\s*$` —— 该正则**本身已正确覆盖两种写法**
    （自闭合 `<msg />` / 空对 `<msg></msg>`，写法覆盖是刻意设计的），但两个 `^…$` 把
    **整串原始输出**框死。而本插件默认提示词要求推理写在同级前面
    （`<reasoning>…</reasoning>`）⇒ 两条分支**同时失配** ⇒ 默认配置下该通路
    **永不触发**（`steps=1` 证明确实走到了停止判定分支，只是判否）。
    真只有空 msg 时本来就能停 —— 是前置的伪 reasoning 标签把判据带偏了。
  - **修复**：`_is_empty_msg` → `is_silent_output`：先用 `visible_output()` 剥掉思考过程，
    再 `ET.fromstring` **结构化**判断「有没有真的发出东西」，不再堆正则
    （对属性/嵌套/多段/空白天然正确，不必为每种写法各加一条正则）。
  - **空 msg 写法覆盖**：原设计两种全部保住，并补齐旧版漏掉的——
    空白子元素 `<msg><text></text></msg>`、多段全空、空 msg + `<ignore>` 等 root 标签、
    空响应 `''`、纯空白、以及**任意写法 + 前置 reasoning**。
  - **口径对齐框架真实产出**：`<text>` 空白**不产出元素**（算空）；
    其它标签（at/reply/poke/img/record/…）空值**也会真的发出一条消息**
    （算非空，保守 —— 绝不误停一个其实发出了 at/poke 的轮次）。
  - **保守边界**：畸形/截断一律判「非静默」（宁可窗口多开一轮，绝不因解析失败误杀窗口，
    且旧行为也是 False ⇒ 畸形输入零回归）。
- **同一口径污染停止词（连带修复）**：停止词判定原本也吃含 reasoning 的整串，
  而默认停止词（不说了 / 不想理 / 晚安 / 再见…）**恰恰最常出现在推理里**
  （模型在解释「他们道晚安了」「这人我不想理」）⇒ 会**误停**。
  现改用同一套 `visible_output()`：只看「说了什么」，不看「想了什么」。
- **接线 bot 发言条数检测（`detect_bot_speech`，防 bot 自己刷屏）**
  - `ChatEnhanceEngine.on_message_sent` 此前**全仓零调用**（方法存在但没注册钩子），
    导致该检测**从未生效**（配置页可见、README 未宣传的未完成残留）。
  - 现注册 `@on.message_sent(priority=LOW)` 转调；**按「条」计数**（设计如此）：
    一次回复里的每个 `<msg>` 分段各算一条（框架对每个 MessageChain 派发一次
    ON_MESSAGE_SENT），窗口内达阈值即通知 bot「你说太多了」。
  - 同时去掉该方法里与 `on_llm_response` **重复**的 `note_bot_reply`：存在感时间线
    按「轮」记更符合语义，按条再记一次会双倍计数（一次回复发 3 段 = 3 条发送事件）。
- **静默轮不再被当成一次「bot 发言」**：`enhance.on_llm_response` 原在 `ai_text` 提取
  **之前**无条件调用，导致 bot 输出空 msg（什么都没发出去）的轮次也被计一次发言
  （存在感扣分 + 休眠维持期次数 +1）。现移到 `ai_text` 之后并传静默标志，
  静默轮直接跳过 —— 静默不是发言。
- **提示词默认值畸形修复**：`dm_proactive_prompt` 的 schema 版含**孤立 `</msg>`**
  （无开头标签）且与代码版**不一致**；它会被 `Text(...)` 当作**用户消息**发给模型，
  既是畸形标签样板又泄漏「这是主动触发」。两处统一为合法写法
  （`直接发送 <msg/> 保持沉默`），并以守护测试锁死一致性。
- **回归测试** `tests/test_empty_stop.py`（T1~T9，49 断言）：日志原文回归、空 msg
  多写法全集、非静默不误停、`visible_output` 剥离口径、停止词不读 reasoning、
  `message_sent` 接线、静默轮不计发言、schema 默认值一致，以及**行为级反向验证**
  （旧判据在这些用例上 8/8 漏判）。
  端到端：同一段日志文本，旧代码窗口保持打开（连续次数 2 不变、`stopped` 未置位），
  新代码 `_stop_sustain_round` 被调用、窗口关闭。
- 版本 v2.5.21 → v2.5.22

### v2.5.21

- **guard 已知媒体收口（read_file 双重付费堵漏）**
  - **现象**（用户日志）：LLM 循占位文本里的 file_path 用 read_file 补读图片 → 框架
    `_describe_image_file` 直接调 desc_img（不看 caption），旧 guard 在插件索引未命中时
    放行原函数 → **框架 VLM 付费识别一次、插件 stage3 又识别一次**（日志里
    `[llm] Describing image using …` 与 `[MediaRecognize:stage3]` 同时出现）。
  - **修复**：插件管线见过的媒体指纹全部登记在案（`_known_md5` cap 4096 /
    `_known_phash` cap 2048，有界 FIFO）——stage1 预填充/语音替换、到达即落盘
    （顺手 dHash）、识别成功后四处登记。guard 拦截 read_file 对**已知媒体**的补读：
    该媒体正在识别时最多短等 `guard_read_file_wait`（默认 8s，0=不等）拿真描述返回，
    等不到返回空占位（识别由插件流水线负责）——**绝不再触发第二次付费 VLM**。
  - **刻意不拦截**：用户配置「不识别」的媒体（_media_skip 分支不登记，保持省 VLM
    意图，read_file 放行原函数）；从未见过的工作区文件照常放行——**read_file 对任意
    文件的可用性不变**。
- **预取占槽饥饿修复**
  - **现象**（用户日志）：60s 超时的在途项长时间占满预取信号量（vlm_prefetch_max_parallel=4），
    新预取「预取启动」到「Describing」排队 69s+ 才开始识别。
  - **修复**：预取独立超时 `vlm_prefetch_timeout`（默认 30s，clamp 到 [5s, media_timeout]，
    新增配置项）——预取是锦上添花的后台优化，给它更短的预算；stage2/stage3 关键路径
    仍吃完整 media_timeout。在途预取任务上限 `vlm_prefetch_max_queue`（默认 16，新增
    配置项）——超限跳过新预取且**不置 _done**，媒体仍由 stage2 正常接力识别，不漏图。
- **回归测试** `tests/test_guard_known_media.py`（T1~T6）：已知 md5/phash 拦截且原函数
  零调用、未知图片放行、在途短等拿真描述、队列上限跳过不置 _done、预取独立超时实际生效。
- 版本 v2.5.20 → v2.5.21（与 z 版 v1.8.12 同步；`media_recognize.py` 两版逐字节一致）

### v2.5.20

- **VLM 泄露收口（多层堵漏）**
  - **PIR/native guard 竞态占位**：官方 VLM 保护网（`guard_captions`）即使检测到 PIR 接管 /
    原生多模态直传，也照占 `caption=""` 再返回 —— PIR「已加载但 handler 未摘除」的竞态窗口里
    若放行留 `None`，框架渲染就会付费识图；`_pir_active()` 同步化（纯内存查注册表），消除旧版
    异步判定的误判窗口。
  - **缓存 last_seen upsert 修复**：`_cache_set` 改为先 `update_image_desc_cache(md5, description=…,
    last_seen=now)`、未命中/失败才 `add` —— 旧实现固定 `add(last_seen=0)`：① `last_seen=0` 次日必被
    框架清理规则删掉；② 主键冲突静默失败，同图重复写永远写不进。
  - **占位污染免疫**：`_PLACEHOLDER_DESCS` 统一收口 "(未识别)/（已过期）/（识别超时）/（下载失败）"，
    `_is_valid_desc` 拒绝进持久缓存，`_cache_get` 读取侧再过滤一遍（框架渲染失败分支写进库的
    占位一律视为未命中）。
  - **框架 desc_img 安全接管**：initialize 时对 `core.message_manager` /
    `core.plugin.builtin_plugins.agent.main` / `core.utils.common_utils` 三处 `desc_img` 引用做
    **幂等包装** —— 先查插件 md5/dHash 描述索引（命中零 VLM），未命中调原函数并套
    `wait_for(media_timeout)`，异常/超时按原契约返回 `""`；terminate 时还原。**纯插件侧改动，
    无需升级框架；包装是幂等的，插件卸载即还原，不改变 read_file 语义。**
- **图片过期免疫**
  - **到达即落盘**：url 型图片/语音在 stage1 即持久化到插件自有目录 `data/plugins_media_cache/`
    并设置本地路径（不改 `file`/`file_type`，native 模式与框架渲染完全不受影响）——URL 过期后
    识别/渲染/read_file 全链仍可用；自有目录不受框架 temp_monitor 60s 保护期误删影响。
  - **URL 失效重取**：下载失败时凭 stage1 登记的 message_id 经适配器 `get_msg` 重取新 URL
    （napcat 会刷新 rkey）重试一次，能力不存在时静默跳过。
- **超时分拆与失败分类**：URL 下载吃独立的 `download_timeout`（默认 15s，新增配置项），
  不再吃光整个 `media_timeout`；VLM 推理仍由外层 `wait_for(media_timeout)` 兜底。
  失败分类返回 "（下载失败）/（识别超时）" 占位，日志来源统一 `[MediaRecognize:prefetch|stage2|stage3]`
  前缀，下载慢与识别慢在日志里可区分。
- **预取并发隔离**：预取走独立信号量 `vlm_prefetch_max_parallel`（默认 4，新增配置项），
  不再占用 stage2/stage3 关键路径的会话级/全局级信号量（防预取风暴挤占兜底识别）。
- **媒体缓存治理**：`media_cache_enabled`（默认开）/ `media_cache_ttl_hours`（默认 24h）/
  `media_cache_max_mb`（默认 512MB，LRU 淘汰最旧）三个新增配置项；后台清理任务懒启动、
  terminate 时取消；在飞/待识别条目引用的文件受保护不删（另有 10 分钟宽限期）。
- **系统/通知消息不计入存在感与骚扰检测**：`system_*` sender（主动回复/定时任务/系统消息）
  与无内容的通知事件不再压低存在感占比、不再误触骚扰通知、休眠期不再被其叫醒；
  poke 戳一戳除外（真实用户互动，骚扰检测需要）。
- **跨会话 ignore 竞态修复**：`<ignore>/<wake_extend>` tag 的会话上下文改为按 sid 精确匹配
  （`_ignore_ctx` dict + `_RESP_SID` contextvar 透传当前响应所属会话 + 60s 过期清理）——
  旧实现单一 `_last_ignore_sid` 标量，多会话并发回复时 A 会话的 `<ignore>` 可能错作用到 B
  （跨会话错拉黑）。
- **性能**：stage1 媒体预填充 gather 并行（原串行 await 下载+查库，k 张图推迟 flush k 倍）；
  stage3 抢救并行化；path 型 md5 / PIL 重编码 / 语音读盘等 IO/CPU 操作全部移出事件循环
  （`asyncio.to_thread`）；定时任务的 `write_memory`（SQLite 同步 IO）同样移出事件循环。
- **队列清场**：in-flight 已被 stop 且无 pending 的悬挂会话由兜底节拍立即清理推送，
  不再白压会话状态多等一跳。
- 裸 `create_task` 全部挂 `_bg_tasks` 强引用（防 GC 提前回收导致预取/清理任务静默中断）。
- **回归测试** `tests/test_media_fixes.py`（19 断言）：T1 占位污染免疫、T2 缓存 upsert
  带活 last_seen（先 update 后 add 调用序）、T3 PIR 竞态窗口 guard 仍占位 `caption=""`。
- 版本 v2.5.19 → v2.5.20（与 z 版 v1.8.11 同步；`media_recognize.py` / `queue_merge.py` /
  `chat_enhance.py` 两版逐字节一致）

### v2.5.19

- **修复：预取作用域写宽 —— "没进批次"的围观图也被预取（白烧 VLM）**
  - **现象**（用户日志）：群里没人 @ 它的图片/表情，到达瞬间就 `预取启动 1 项` +
    `Describing image using …`；那段窗口 bot 一条都没回 —— 这些图**既没进批次、也不会被送进 LLM**，
    识别成本全白付（样本里 4 次预取、1 次还超时 60s，基本 100% 浪费）。
  - **根因**：v2.5.13 起的调用点把"**已进缓冲**"当成了"**会被送进 LLM**"——对**前文阶段**
    （唤醒之前、只作上下文、窗口满会被弹掉）的消息也打了预取标记；该调用点此前被钩子顺序
    竞态打死（预取从未真正跑过），竞态修复后它"活"了 → 作用域问题第一次显形。
  - **修复（两步）**：
    1. **收紧标记**：只有"确定会被送进 LLM"的消息才预取 —— 批次已开启（本条属本批）／唤醒消息／
       本条自己触发主动回复被 `flush()` 推出；**前文阶段不再预取**（真被带上时由 stage2 现场识别）。
    2. **起批预热**：唤醒**起批**那一刻，把缓冲里已有的「前文」交给预取 worker 一起识别 ——
       它们会随本批送进 LLM（唤醒后前文锁定、不再裁剪），而"起批 → 顺延到点"正是等待窗口
       → **既不浪费、也不丢"进批次即识别"的提速**。
  - **判定语义不变**：`_media_skip`（仅唤醒识别／概率未中／超上限）依旧不预取、不识别；
    结果池与在飞任务照旧去重（不会重复烧 VLM）；stage2/stage3 全不动。
  - **日志文案改准确**：`预取启动 N 项（sid，进批次即识别 / 起批预热前文，…）`（原来一律写"消息已进批次"，
    对前文消息是误导）。
  - **回归测试** `tests/test_prefetch_scope.py`（5 场景，用**真实 `handle_msg` + stage1** 驱动）：
    P1 前文图不预取（**在修复前的代码上 FAIL = 反向验证**）、P2 起批预热（本条 + 前文一起识别）、
    P3 批次内非唤醒图仍预取、P4 主动命中被推出时补标记、P5 对照（仅唤醒识别语义不变）。
  - 另：测试桩 `Session` 补 `__str__`（与框架一致返回 sid —— 宿主用 `ctx.get_buffer(str(event.session))`
    取缓冲，缺它会取到错的键）。
- 版本 v2.5.18 → v2.5.19（与 z 版 v1.8.10 同步；`media_recognize.py` 两版逐字节一致）

### v2.5.18

- **提速：去掉兜底节拍的固定 0.5s 等待（改为按截止点睡 + 事件唤醒）**
  - **背景**：QueueMerge 的兜底节拍原来是每 0.5s 盲目醒来做同一件事。它唯一"能被感知"的场景是：
    当前 in-flight 已被别的插件 stop（停止词 / 交棒 / 批次阶段被掐停）后，新到的批次被拦进
    pending，却要等下一拍（≤0.5s）才被推送。（v2.5.16 的停轮检测、v2.5.18 新增的
    `ON_FINAL_RESULT` 兜底已覆盖多数此类情况，这里补的是最后一条边。）
  - **修复**：
    1. 拦截批次进 pending 时，若 in-flight **已被 stop** → **锁外立即走推送决策**
       （0 延迟，实测 <5ms；真正的校验仍在 `_push_pending` 锁内，不会误推）；
    2. `_tick_loop` 不再固定 `sleep(0.5)`：改为 `wait_for(唤醒事件, timeout=_next_watch_delay())` ——
       空闲懒睡 5s；in-flight 已停时 0.05s 复查；其余贴「卡死兜底剩余 / 攒批窗口剩余」的较小值；
       拦截时 `_ensure_task_locked()` 会 set 唤醒事件 → 立刻重算，不会因此晚处理。
       `_tick` 的判断逻辑**一字未动**（只改"什么时候醒来"）。
  - **安全性**：正常收尾路径（step_result / final_result 推送）行为不变；空闲时只是把
    "每 0.5s 一次空转"换成"最多 5s 一次空转"。
  - **回归测试** `tests/test_queue_timing.py`（5 场景）：Q1 立即推送（旧代码上失败=反向验证）、
    Q2 正常轮仍等收尾、Q3 节拍按截止点计算、Q4 有 pending 立即唤醒、Q5 收尾推送不变。
- **修复「真·媒体预处理（预取）」实际未生效 —— 识别一直要等批次被推送**（用户反馈："批次内收到图没有秒启动识别，而是在等开始推批次"）
  - **根因（钩子顺序竞态）**：预取在 `handle_msg`（**先注册**的 im_message 钩子）里 `schedule_prefetch()`，而 `_pir_media` 要等 stage1 `on_media_rec_im`（**后注册**）跑完才写入 —— `create_task` 的预取 worker 会在 stage1 的第一次真正 await（`_cache_get` → 数据库查询；URL 图还要下载）时抢跑，读到**空媒体表**后 `return`（一次性任务，不重试）→ 预取形同虚设，识别只能等批次被推送时的 stage2。被其它插件拦截、stage2 根本不跑的批次（如"随时插话"Midflight 的流入），新图甚至会**永远停在空占位**（只有缓存命中救得回来）。
  - **修复**：`handle_msg` 里只打 `_batch_entered` 标记（消息确定进批次），真正的调度挪到 **stage1 末尾**（媒体登记进 `_pir_media` 之后）。**判定完全不变**：仍只预取 stage1 认定为"待识别"的媒体（`_media_skip` = 仅唤醒识别/概率未中/超限 一律不预取），缓存命中依旧零 VLM——按用户确认，**只改启动时机，不改识别判定**。
  - **效果**：消息一进批次（顺延/防抖窗口、队列排队窗口、上一轮还在跑的空窗）就开始识别，放行时 stage2 直接命中结果池；被拦截流入的消息也能带上描述而不是空占位。
  - **新增 `[MediaRecognize] 预取启动 N 项` 日志**：线上可直接确认"进批次即识别"生效。
  - **回归测试** `tests/test_prefetch_timing.py`（7 场景，含"旧顺序 0 次 VLM"的反向验证）。**在 v2.5.17 上 A/E 失败**（预取 0 次 VLM），本版全过。
- **修复「子代理（SubAgent）跑任务时主聊天的统计/窗口被带偏」**：子代理插件用**携带真实会话 sid 的桩事件**派发 `ON_LLM_RESPONSE`（为了让 file 插件等按会话作用域的管控对子代理与主 AI 一致），本插件会把它当成主 bot 的最终回复——**存在感时间线 +1、扣分**，S 版还会误开/误停持续对话窗口（子代理汇报里出现"再见/拜拜"就直接停窗）。
  - **修复**：`ON_LLM_REQUEST` 登记"框架真实批次"（这个钩子只有框架自己会派发，任何自造桩事件都不会经过），`on_llm_response` 只处理登记过的事件；若桩事件带显式标记（`extra["_subagent_stub"]` / `is_subagent_stub`，子代理插件 v1.1.3 起提供）也直接跳过。真实批次 100% 经过 `ON_LLM_REQUEST` ⇒ **主流程零影响**（判定异常时按"真实批次"放行，宁可照旧工作也不误伤）。
  - **回归测试** `tests/test_foreign_event.py`（G1-G3）：**在 v2.5.17 上 G1/G3 失败**（存在感被污染），本版通过。
- **接入框架 v2.34.4 的 `ON_FINAL_RESULT` 兜底**（KiraAI #308，该钩子此前只定义、从未派发）：QueueMerge 的最后一步发送若在 `AFTER_XML_PARSE` 阶段被 stop，`send_llm_text` 提前 return ⇒ **`ON_STEP_RESULT` 不会派发** ⇒ pending 只能干等 stall（默认约 180s）。现在该钩子一到就补一次推送决策（幂等：in-flight 不匹配 / 无 pending 时无副作用）。`tests/test_foreign_event.py` G4/G5 覆盖。
- 版本 v2.5.17 → v2.5.18（与 z 版 v1.8.9 同步；`queue_merge.py` / `media_recognize.py` 两版逐字节一致）

### v2.5.17
- **紧急修复：官方 VLM 保护网实际未生效（`guard_captions` 漏 `await`）**
  - **问题**：v2.5.16 新增的保护网钩子 `guard_official_vlm` 调用 `guard_captions()` 时**漏了 `await`**——`guard_captions` 是协程，不同步 `await` 就**一行都不会执行**。运行时只留下一条 `RuntimeWarning: coroutine 'ParallelMediaRecognizer.guard_captions' was never awaited`（不抛异常、不影响其它逻辑），所以表现为：**保护网完全失效，官方付费识图照旧发生**，日志里该冒 `[llm] Describing image using …` 还是会冒
  - **修复**：补上 `await`。一个词的改动，但它是保护网能不能跑起来的开关
  - **为什么 v2.5.16 的测试没发现**：原测试是**直接 `await p.guard_captions(...)`**，绕过了真正出问题的钩子 `guard_official_vlm`，所以永远绿。本次新增 `tests/test_guard_await.py` 补上这条缝：
    - **运行时**：通过**真实钩子** `guard_official_vlm` 调用，断言 `caption is None` 的图片/表情真被占成 `""`、已有描述不被动、且**没有 `never awaited` 警告**（bug 时三项全挂）
    - **静态**：AST 扫全部 async 方法名，找出任何"被当同步调用"（未 `await` / 未包装 / 未收集）的点——这类 bug 不会再溜过去
  - 附带逐行核对了 S 版全部 5 处同名方法（`enhance.*` / `merger.shutdown` 等）的调用点，确认**仅此一处**漏 `await`
- 版本 v2.5.16 → v2.5.17

### v2.5.16
- **新增「官方 VLM 保护网」**（`guard_framework_vlm`，默认开）：框架自己那条付费识图链路被彻底堵住
  - **问题**：官方 VLM 全项目只有一个触发条件 —— `ele.caption is None`，而它的**渲染发生在所有批次钩子之前**（`message_manager.handle_im_batch_message` 先渲染批次、后派发 `ON_IM_BATCH_MESSAGE`）。也就是说 QueueMerge 的拦截、Midflight 的拦截、本模块 stage2/stage3 的抢救**全都发生在"钱已经花掉"之后**。只要有一条消息的图片没被预置 caption（第三方插件抢先 `stop()`、钩子顺序异常、stage1 异常……），官方就会付费识图，**事后无法挽回**（日志表现为突然冒出 `[llm] Describing image using …`）
  - **修复**：新增最早执行的钩子（`on.im_message` **SYS_HIGH**，早于所有 HIGH 钩子）`guard_captions()` —— 只做一件事：把链上 `caption is None` 的图片/表情**占成空串**（官方空占位 `[Image ]` / `[Sticker ]`）
  - **只占位，不做别的**：不暂存 `_pir_media`、不预取、不发起任何识别、不改任何消息策略（不 buffer/discard/stop）、不删不换元素；识别仍由 `handle_msg` + stage1 在 HIGH 按「仅唤醒识别/概率/超限」决定，**省 VLM 语义完全不变**；只在 caption **是 None** 时写 `""`，**绝不覆盖已有描述**；PIR 接管图片 / 原生多模态模式下一律不碰（与 stage1 跳过条件一致）；「启用并行媒体识别」关闭时不生效（那种配置下本来就该由框架识图）
  - **兼容性**：框架里 `caption is None` 只用在官方识图那一个判断上；本插件全程用 `(caption or "")`，**空串与 None 完全等价** ⇒ 自己的行为零变化。唯一影响：依赖「`caption is None` = 框架还没识图」这一信号的第三方插件，会看到该字段被提前占位（关掉本开关即可恢复原状）
- **修复「bot 自己发的图/表情被当成识别对象」**：框架的钩子循环**只在 `stop()` 时中断，`discard()` 不中断**（`core/message_manager.py`），所以宿主按"机器人自身消息"`discard()` 之后，stage1 **照样会执行**——把 bot 自己那条消息里的图片/表情设成待识别并登记进本回合暂存索引。而 stage3 判断"这条媒体在不在请求里"用的是**文本锚点**，官方空占位的锚点是**通配**的（`Sticker` 是 `[Sticker ]`，无路径无 id；`Image` 落盘失败是 `[Image ]`）⇒ 只要请求里存在任意一个未识别表情包的空占位，暂存索引里**所有** caption 为空的表情包都会被命中，**包括 bot 自己那张**：白跑一次 VLM，还会把它的描述与 `file_path` 填进**用户那张**的空占位（张冠李戴）
  - **修复**：stage3 抢救新增**准入条件** `_batch_media_ids(event)` —— 只有**元素确实出现在本批次消息链里**（键与 stage1 同源，取 `elem._pir_short_id`）的媒体才允许被抢救；通配文本锚点不再能单独作为"这条在请求里"的证据
- **修复「停窗后持续命中批次被整批掐掉 ⇒ 消息直接丢失」**：`_stop_sustain_round()`（AI 空消息 / AI 停止词 / 用户停止词 / 达上限）之后迟到的「纯持续命中」批次，会在两处被处理 —— ① `on_queue_merge_batch` 里**静默** `event.stop()`（debug 级日志）；② `QueueMerge.drop_sustain_pending()` 丢弃 pending 里的同类批次
  - **问题**：这两处的批次，消息**已经被框架 flush 出会话缓冲**（`flush_session_messages` 会先 `buffer.flush()` 把缓冲弹空）⇒ 只掐不还原就等于**彻底丢消息**：既不会进 LLM、也不在缓冲里，**之后任何一轮都看不到**。`_stop_sustain_round` 注释写的"消息仍在缓冲，上下文不丢"只对**还没 flush** 的批次成立，`_debounce_loop` 的保险丝挡不住"已满即推 / 顺延到点已经 flush"的那些（旧版 `drop_sustain_pending` 的注释其实自己也承认了这一点）
  - **修复**：语义**不变**（仍然不回一轮），但把批次里的消息**原样放回会话缓冲头部**（新增 `restore_messages_to_buffer()`，shim 形状与缓冲内 `KiraMessageEvent` 完全一致）→ 退成「前文」，等下次真实唤醒随批次一起送进 LLM。`drop_sustain_pending()` 同样改为「摘掉批次 + 还原消息」，并**不再取消媒体预取**（这批会进上下文，识别结果依然有用）
  - **举例**：群里 bot 回了空消息 ⇒ 停窗；用户接着说"明天我要请假" ⇒ 旧版这句从上下文里消失（下次有人 @bot 时它完全不知道），现在这句留在前文里，下次唤醒时 bot 看得到
  - 只影响 S 版（Z 版没有持续对话）；`queue_merge.py` 两版保持逐字节一致
- 版本 v2.5.15 → v2.5.16

### v2.5.15
- **修复「第三方插件重建的媒体副本」触发官方 VLM（付费）**：会话合并 / 上下文压缩类插件在 `on_llm_request` 用历史重建请求时，会把被回复消息的媒体**重新下载成另一个临时文件**（`download_10.jpg` → `download_11.jpg`）——此刻链上已无对应元素，空占位只以**文本**形式嵌在 `Reply.content` 里，元素级兜底拿不到元素 → 框架 `if ele.caption is None` 成立 → 官方 VLM 付费调用
- **修复**：新增**文本级兜底** `_fill_empty_official_by_path()`（置于 `if not need: return` 之前）——按空占位里的 `file_path` 取**文件内容 md5** 查描述缓存（与框架 `hash_image()` 的 path 分支同口径），命中**就地替换**文本；md5 不同（重压缩 / 改尺寸）用 **dHash 感知哈希（汉明距离 ≤2）** 兜底；**只做缓存命中补齐、不发起任何新识别**（拿不到元素就拿不到"跳过标记"）
- **效果**：同图换文件名 / 重压缩副本 / 嵌在回复 `content` 内三种形态全部补齐，**0 次新增 VLM**
- 版本 v2.5.14 → v2.5.15

### v2.5.14
- **关键修复**：媒体识别回填被后续插件覆盖
- **根因**：本插件的媒体兜底（stage3）注册在 `Priority.HIGH`（最先执行），而 **KSM 会话合并(-50)** / **CC 上下文压缩(-51)** 等插件会在 `on_llm_request` 里**重建 `req.messages`** —— 我们在它们之前回填，结果被整体覆盖，请求里仍是空占位 `[Image , file_path: p]` / `[Sticker ]`（表现为"看不见图"）。
- **修复**：媒体 stage3 优先级改为 **-60（最后一个执行）**，确保回填落在最终请求文本上；同时保留写回 `elem.caption`，任何后续重渲染也带着描述。
- 版本 v2.5.13 → v2.5.14

### v2.5.13
- **修复空占位「看不见图」+ 真·媒体预处理（预取）**
- **官方怎么做的**：框架 render 里 `if ele.caption is None: desc_img(...)` —— 只要媒体要渲染进 LLM 请求就识别（内置聊天插件从不碰 media）。「跳过识别」是本插件独有的省钱机制。
- **真·预处理（本次新增）**：消息**确定进入批次**（`event.buffer()`）时立刻在后台预取它的媒体 —— 而这段时间正是「上一个批次的 LLM 还在跑 / 本批次在队列里排队」的空窗。放行时 stage2 直接从结果池命中，**本批次关键路径零识别开销**（实测放行后 VLM=0）。被 `discard()` 的消息走不到 buffer ⇒ 不预取、不浪费。私聊同样生效。
  > ⚠ 更正（v2.5.18）：该调度点在 `handle_msg`（先注册的钩子）里，会被 stage1 的钩子顺序竞态打死（worker 抢跑时媒体表还是空的，读到空即退出且不重试）——**本条直到 v2.5.18 才真正生效**，详见 v2.5.18 更新日志。
- **尊重「仅唤醒识别」**：非唤醒消息的媒体（`_media_skip_reason=mention`）、概率未中、超出每消息上限的，**既不预取也不兜底**——空占位是这些开关的既定代价，不是 bug。
- **兜底只救「本该识别却没补上」的**：`on_llm_request` 时仍无描述、且**没有跳过标记**的媒体（stage2 没跑 / md5 键变化 / 批次被第三方插件截断）→ 现场补识别并回填 prompt 与 `caption`。
- **引用链**：唤醒消息里 `Reply.chain` 上的媒体按唤醒处理 → 会识别（「引用那条带图消息 + 叫我」能看图）。
- **顺带修复**：兜底把 `Sticker` 误判为音频走了 STT 分支（应与 stage2 的 `type in ("Image","Sticker")` 一致走 VLM）。
- **不需要任何新配置。**
- 版本 v2.5.12 → v2.5.13

### v2.5.12
- **追加修复（媒体空占位兜底 + 合并批次模型组）**
- **新增「官方空占位」兜底抢救**：此前只能在 LLM 请求前抢救 `[Image #id: ]` 形式的标识符，而图片/表情包实际渲染成官方空占位 `[Image , file_path: p]` / `[Sticker ]`——一旦 stage2 因异常或第三方插件（如批次级拦截插件）**stop 掉批次**而未回填，这类空占位会被原样送进 LLM（表现为"看不见图"），且**没有任何兜底**。现在 stage1 就把待识别索引登记到会话回合表，stage3 在 LLM 请求前反查「caption 仍为空」的媒体并现场补齐。
- **合并批次继承 `model_group`**：此前合并批次未继承原批次的自定义模型组，配置了会话级模型组的批次经队列合并后会**静默改用默认模型**。
- 版本 v2.5.11 → v2.5.12

### v2.5.11
- **新增「评分加减关键词」+ 自己消息过滤修正**
- **评分加减关键词**（存在感节流区新增 4 项配置）：自定义「加分关键词 / 减分关键词」，命中的用户消息直接给累计分加减分，从而影响评分补正的触发。**只统计用户消息，bot 自己的发言一律不计分。**
  - 词与分值都是**标签输入**（输入一个按回车 = 一个标签），按**标签顺序一一对应**：第 1 个分值对第 1 个关键词。
  - 配对规则：只填 1 个分值 → 所有词都用它；分值标签少于词数 → **最后一个分值沿用给后面的词**；多于词数 → **多余的忽略**；留空 → 默认 5。
  - 容错：分值标签里写成 `10,5`／`10 5`／`10、5` 会自动拆成两个分值，不必纠结写法。
  - 大小写不敏感子串匹配；同一个词在一条消息里出现多次**只计一次**；加分词与减分词可同时命中。
  - 结果受「评分上限」与 0 下限约束，与既有累计分同一池子。群聊/私聊**共用同一份**配置。
- **修正机器人自己发言的过滤**：原过滤**只对私聊生效**；群聊里 bot 自己的消息（适配器可能作为普通消息送达，如 NapCat 的 `reportSelfMessage`）会被当成用户消息——存在感占比、累计评分、额外信号、骚扰检测被自身发言污染，且与发送事件重复计数。现在**群聊/私聊一致丢弃**（z 版此前完全没有该过滤，一并补上）。bot 自身发言的正确统计口径是发送事件（`on.message_sent` 的 `bot_speech`），不受影响。
- 版本 v2.5.10 → v2.5.11

### v2.5.10
- **媒体识别修复（“VLM 跑了却看不见图”）**
- **修复识别结果被静默丢弃**（关键）：框架在批次处理时会先**压缩图片**（`compress_image_element` 置 `media.md5 = None`），渲染时又**重新 `hash_image()`**，导致元素 md5 与 stage1 记录的键不再一致。此前 stage2 从 `elem.md5` 反推查找键 → 查不到 → 识别结果被静默丢弃，LLM 只收到空的 `[Image , file_path: ...]`。现在 stage1 会把键钉在元素上（`_pir_short_id`），stage2 优先使用它；md5 / `noid_` 兜底保留兼容。
- **修复会话级能力判定分叉**：框架按「会话级生效能力」（`session_mgr.get_effective_capabilities`，会话覆盖优先于全局）解析 `image_recognition.mode` / `desc_prompt`，而此前本插件只读全局 `bot_config`。一旦某会话单独覆盖过配置就会出现：全局 native + 会话 vlm → 我们跳过、框架自己识图；全局 vlm + 会话 native → 我们照常识图而框架走原生直传、**本次 VLM 完全白跑**。现已严格对齐框架口径（VLM 描述词同样跟随会话级 `desc_prompt`）。
- **识图日志可观测**：VLM 调用此前完全静默，无法与框架自身的识图日志区分。现使用专用日志器 `MediaRecognize`（**紫色**，与框架 `llm` / 并行识图插件 `parallel_vlm` 同款配色），输出与官方同款文案 `Describing image using <model> (<provider>)`。
- 版本 v2.5.9 → v2.5.10

### v2.5.9
- **稳定性与接管完善**
- **自动接管默认聊天插件**：检测到框架内置 `default-chat` 已加载时，自动停用并**迁移其唤醒词**（仅迁移 `waking_words`，本插件已填写唤醒词则不迁移、不覆盖）。避免两者同时启用造成的双重防抖/buffer（顺延延迟翻倍、批次计数错乱）。独立防骚扰插件（`anti-harass`）同样自动停用。
- **修复 VLM 泄露**（评分门控降级）：消息被 `proactive_score_gate_deny` 判定为“不触发”而从唤醒降级为围观时，此前已按唤醒口径保留的待识别图片会被继续送 VLM/STT——消息最终不进入 LLM，识别成本全部白付。现在降级时会同步回补非唤醒口径的媒体标记，识别成本为 0。
- **通知合并任务自清理**：`_flush_later` 结束后主动释放自身引用，避免已完成 Task 对象按会话累积。
- 清理 `_debounce_loop` 中的重复 `break` 死代码。
- 版本 v2.5.7 → v2.5.9

### v2.5.7
- **媒体管线重构（与 **Plus-One 复读插件**完美兼容 + 官方格式对齐）**
- **Image/Sticker 元素不再替换删除**——表情包元素保留 → Plus-One 能正确复读表情包；图片元素保留 → 纯图片消息天然不参与复读。识别结果通过预置官方 `caption` 表达，渲染为官方 `[Image 描述, file_path: ...]` / `[Sticker 描述]`（表情包增强附带 file_path）。
- **仅唤醒识别/媒体预处理完整保留**：非唤醒媒体预置空 caption（官方空占位 `[Image , file_path: ...]` / `[Sticker ]`），零 VLM 调用、LLM 仍知道有媒体；唤醒消息才并行 VLM/STT。
- **自动互斥接管**（默认开）：检测到并行识图插件（PIR）自动禁用，图片识别完全由本插件接管；同样自动停用框架内置 `default-chat` 与独立 `anti-harass`（详见 v2.5.9 说明）。
- **原生多模态不截断**：`max_images_per_message` 在 native 模式下自动跳过（图片全直传，框架压缩控制 token），转发/语音策略照旧。
- **native 超限占位**：native 模式超限图片替换为 `[Image attached]` 占位拦直传（省 token，LLM 仍知道有图）；Sticker 永不占位（复读优先）。
- **唤醒消息图片上限**（`max_images_per_message_mentioned`，默认 0 = 不限制）：唤醒消息超限图片同样占位省 token。
- **native 仅唤醒识别生效**：仅唤醒开时非唤醒图片占位拦直传、唤醒图片保留直传（LLM 直接看图）。
- **native 表情包跟随仅唤醒**（`native_sticker_follow_mention`，默认开，受上级仅唤醒开关门控）：非唤醒表情包占位 `[Sticker attached]` 省 token；注意开启后 Plus-One 复读表情包会不正确（复读占位文本），酌情关闭以保复读。
- 群聊提示词默认文案同步迁移为官方占位符格式（仅替换两个字样，安全原子写回）。
- 版本 v2.5.6 → v2.5.7

### v2.5.6
- **-1 永久不再绕过钳制**：设置最大时长限制（max_duration/extra_max_duration>0）后，bot 输入 -1 按最大允许值执行（不再永久）；仅未启用上限时 -1 才真正永久；allow_bot_duration=False 时 -1 也强制默认时长。hint 已同步更新
- **白名单豁免**：`harass_whitelist_users` / `harass_whitelist_sessions` 中的用户/会话不受任何屏蔽影响（消息照常进入 LLM）——原先白名单仅挡检测不挡屏蔽
- **额外信号独立钳制配置**：user_msgs / bot_speech / session_msgs 不再兜落 poke 配置，新增 `extra_max_duration`（默认 300，0=不钳制）/ `extra_allow_bot_duration`（默认开）——bot 自设时长钳到上限，关闭则强制默认时长
- **通知动态教"允许最大值"**：额外信号通知里建议的 duration 动态取 `extra_max_duration`（未启钳制回落 `extra_default_duration`）；不再教 `-1`（永久仅在 hint 中说明，避免绕过钳制）
- **bot_speech 开关**：新增 `bot_speech_block_session`（默认开）——检测到 bot 发言过多时，通知教会话级拉黑标签 `<ignore>all|duration:N</ignore>`（输入 = 拉黑当前会话，所有消息停止进入 LLM，N 秒后自动恢复）；关闭则仅提醒（bot 自觉）
- **hint 补全**：`<ignore>` / `<poke_ignore>` 标签描述补"-1 表示永久"
- 版本 v2.5.5 → v2.5.6

### v2.5.5
- **过滤空通知事件（QQ 戳一戳别人等系统通知）**：框架会把所有 notice（poke 别人 / 运气王 / 头衔 / 荣誉 / 进退群 / 管理员等）以"message_id=None、零内容"的消息事件广播给插件（v2.34 在插件链之后才拦截空链，插件层副作用已先发生）——此前空事件会进入存在感评分（+3）、前文缓冲、主动概率判定（刷"评分补正"日志）、顺延重置（刷"顺延开始"日志），并在顺延到点时清空残留批次状态，导致后续真实消息丢失触发时机（"卡住"）
- 修复后：`is_notice` 且消息链完全为空 → 丢弃（群聊/私聊一致），不进评分/前文/判定/顺延/骚扰统计；**有内容的事件全部保留**——poke bot（框架构造 `[Poke …戳了戳你]` 文本）、bot 被禁言 / 新成员进群（qq-enhance 等插件先增强后 chain 非空）、`[System: …]` 系统提示、图片/语音/贴纸/文本消息
- **兼容性**：过滤只依赖 `event.is_notice` + `message.chain` 两个框架核心字段；`event.discard()` 不设 is_stopped，不中断其他插件处理链；任何"先增强"插件（priority > HIGH）自动兼容（增强后 chain 非空 → 保留）
- 版本 v2.5.2 → v2.5.5

### v2.5.2
- **停窗撤销在途批次**：`_stop_sustain_round`（空消息/停止词/达上限停窗）现在会清理 batch_started/batch_count——仍在顺延等待中的"持续命中批次"不再被 flush（保险丝拦截），消息留在前文缓冲等下次真唤醒/概率命中时随前文一起送出
- **背景**：持续命中 → 空消息停止 → 顺延到点的时序下，已命中的消息会被"补刀"再次送进 LLM（空回复循环的余波）；修复后停止语义彻底——bot 闭口后不再误触发
- **安全性**：只清插件侧批次状态，不碰框架 session buffer——消息不丢（前文保留）、不卡回复（保险丝轻量拦截）、不影响真唤醒/满即推/队列合并（drop_sustain_pending 管 QueueMerge 层，本修复管 _debounce_loop 层，双层互补）
- **验证**：命中→停止→顺延到点 flush=0（被拦）；真唤醒后前文（含被撤销消息）完整带出
- 版本 v2.5.1 → v2.5.2

### v2.5.1
- **修复空消息无限重开循环**：`sustain_retry_on_empty` / `dm_retry_on_empty` 开启时，AI 空消息**不再无条件重开窗口**——改为先检查评分（含 bot 本次回复扣分后），**评分不足阈值 → 停止窗口**，评分达标才重开等评分补上再触发
- **背景**：持续对话概率=1 时，空消息 + 评分不足（1:1 对话下评分恒低）会造成"空消息→重开窗口→下条必命中→又空消息"的无限循环（日志连续次数无限累加）
- **验证**：评分 12 → bot 空回复 -5 → 7≥5 重开 ✓；评分 6 → bot 空回复 -5 → 1<5 停止 ✓
- 窗口超时逻辑复核无影响（群聊 `_end_sustain_window` / 私聊 `_dm_sustain_loop` 正常）
- 版本 v2.5.0 → v2.5.1

### v2.5.0
- **顺延误触发修复**：非唤醒消息只在**批次已开启**（有真唤醒/持续命中）时才重置顺延计时器；无唤醒来历的非唤醒消息只作为前文缓冲，绝不启动顺延/flush
- **flush 保险丝**：`_debounce_loop` flush 前校验批次唤醒来历，无唤醒来历跳过 flush（前文保留等下次真唤醒）
- **评分/概率/k_prob 判定全链路日志**：`[Enhance] 评分补正(...)`（deny 抑制 / boost 补触发 / 清零，info 级）+ 持续判定/积极概率/私聊判定的"概率×k_prob→有效概率、随机值、评分门"（debug 级）
- **poke 屏蔽拦截**：被屏蔽用户的戳一戳事件不进 LLM（poke 单屏蔽只挡戳，不拉黑普通消息）
- **manage_ignore duration 动态提示**：工具参数描述中的默认时长取自当前配置真实值（不写死）
- 版本 v2.4.9 → v2.5.0

### v2.4.9
- **消息缓冲模型重构（前文+批次）**：与原版语义对齐并修复丢消息——
  - `max_unmentioned_messages`：唤醒消息**之前**的非唤醒前文上限（超限弹最老前文，唤醒出现后前文锁定不裁剪）
  - `max_buffer_messages`：**从首个唤醒消息起**（含它）进入 buffer 的消息数，达到即满即推；批次内唤醒/普通消息一视同仁（不重置）
  - 推送内容 = 前文 + 批次全部；未满即推则顺延到点推送
- **修复丢消息**：原版"总 buffer 满即推"只数唤醒前存量、且非唤醒到达先裁剪可能弹掉唤醒——现按"前文上限 + 批次计数"模型，唤醒消息随批次完整送出，不再被裁剪丢弃
- 版本 v2.4.8 → v2.4.9

### v2.4.8
- **修复非唤醒消息不重置顺延**：之前 merge_window_seconds 顺延只被唤醒消息重置，非唤醒消息（receive_unmentioned）到达后计时器不重置——导致顺延形同"首条唤醒消息后固定 N 秒"。现在非唤醒消息也会重置计时器，真正实现"最后一条消息到达后 N 秒无新消息才 flush"
- 版本 v2.4.7 → v2.4.8

### v2.4.7
- **消息合并顺延默认启用**：`merge_window_seconds` 默认 -1（自动取 WebUI 设置值），新装/升级后立刻体现合并顺延特性
- **顺延调试日志**：`section_basic.debug_log_enabled`（默认关），开启后打印顺延开始/重置/结束日志，便于排查合并时机
- **清理死代码**：`queue_merge.py` 中未使用的 `merge_window_seconds` 字段移除（积压队列合并仍由 `max_merge_seconds` 超时控制）
- 版本 v2.4.6 → v2.4.7

### v2.4.6
- **队列合并防抖修复**：`_push_pending` 防抖等待移到锁外（之前 await 在锁内阻塞队列处理 + 防抖重置不生效）
- 版本 v2.4.6 → v2.4.6

### v2.4.6
- **修复：消息合并间隔顺延移到 buffer 层**：之前被错误实现在队列合并层导致 pending 合并前傻等，现移至框架 debounce 层实现真正的防抖重置（新消息到达重置倒计时）
- 版本 v2.4.4 → v2.4.6

### v2.4.4
- **私聊独立存在感节流**：`dm_presence_enabled`（默认开），私聊有独立评分/k_prob 参数（窗口 10 条、目标占比 0.7、阈值 30、加分 2 扣分 3）
- **概率调节独立开关**：`proactive_k_prob_enabled`（默认开）、`sustain_k_prob_enabled`（默认关）、`dm_k_prob_enabled`（默认关）
- **评分补正细化**：`proactive_score_gate_deny/boost`（section_basic，默认开）+ `sustain_score_gate_deny/boost`（默认关）+ `dm_sustain_score_gate_deny/boost`（默认关）+ `mentioned_score_gate_deny/boost`（群聊提及，默认关）+ `mentioned_dm_score_gate_deny/boost`（私聊提及，默认关）
- **提及消息评分**：`mentioned_score_gate_deny/boost`（群聊）和 `mentioned_dm_score_gate_deny/boost`（私聊），默认全关
- 版本 v2.4.2 → v2.4.4

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
- 版本 v2.3.4 → v2.4.2

### v2.3.4
**修复热重载后子模块不更新（AttributeError）**
- 框架热重载只重新 import `main.py`，`sys.modules` 中缓存的同目录子模块（`queue_merge` / `media_recognize`）不会更新，导致新版 main.py 调用子模块新增方法（如 `drop_sustain_pending`）时热重载后报 `AttributeError`
- `main.py` 导入子模块前对已缓存模块执行 `importlib.reload`，热重载即可加载子模块最新代码；首次加载无行为变化，reload 失败静默忽略
- 注意：从 ≤v2.3.3 升级本次仍需**完整重启一次**（清掉旧模块缓存），之后热重载即可正常生效
- 版本 v2.3.3 → v2.3.4

### v2.3.3
**修复持续对话停止后被「兜底开窗」复活**
- **根因**：AI 输出空消息 / 命中停止词（或用户停止词、达上限）停窗时窗口与计数被清零，但两点让停止形同虚设：① 停窗前已命中（被标记为唤醒）的积压批次仍留在 QueueMerge pending 中，放行后照常触发一次 LLM 回复；② 该批次的 LLM 请求又触发兜底开窗（连续次数 0），开启全新一轮，连锁产生更多与停止意图相悖的回复
- **修复**：
  1. 新增「本轮已终止」标志：上述停止路径置位，LLM 请求兜底开窗检测到后不再开窗，直到下次真实唤醒（@/唤醒词/引用回复）解除
  2. 持续命中时记录 message_id，用于区分「持续命中触发」与「真实唤醒」；停窗时丢弃 pending 中「仅由持续命中消息触发」的积压批（含真实唤醒消息的批次保留）；停窗后姗姗来迟的纯持续命中批次（debounce 尚未 flush）在批次入口直接拦截
- 覆盖 both / either / llm_processing / after_reply 四种判定时机（after_reply 无兜底开窗问题，但共享积压批清理）；per_message / per_round 同时生效
- 被丢弃批次的消息仍保留在会话缓冲中作为上下文，仅少一次回复，不丢上下文
- 兼容说明：若某适配器消息无 `message_id` 字段，积压批清理自动退化为不生效（不报错、不影响其他逻辑）
- 版本 v2.3.2 → v2.3.3

### v2.3.2
**媒体识别填充 file_path，对齐原版图片路径**
- 并行媒体识别把 Image/Sticker 替换为标识符后，LLM 拿不到本地文件路径，图生图/上传等工具找不到文件。现在 stage1（缓存命中）/ stage2（识别完成）/ stage3（历史兜底）都会对媒体调用 `to_path()` 落盘并转 `data/` 相对路径，填充为 `[Image #id: 描述, file_path: data/temp/xxx.jpg]`，对齐原版 `message_format_to_text` 行为
- 路径获取失败时降级为旧格式（不带 file_path）；`#id` 前缀保留，stage3 兜底正则、队列合并重放 `_done` 跳过逻辑不受影响
- Record 原版也不带路径，行为不变；File/Video 未被替换，仍走框架原逻辑，不动
- native 多模态模式行为不变（原版 native 也是 `[Image attached]` 不带路径）
- 版本 v2.3.1 → v2.3.2

### v2.3.1
**修复媒体识别填充崩溃（bad escape）**
- `_fill_text` / `_fill_chain` 的 `re.sub` 改为 `str.replace`：`re.sub` 的 replacement 是模板字符串，VLM/STT 返回的描述含反斜杠序列（如 Windows 路径 `C:\Users\...` 的 `\U`、`\x`）时抛 `re.PatternError: bad escape`，stage2 整批媒体识别崩溃。占位符是精确字面量，`str.replace` 无转义问题
- 版本 v2.3.0 → v2.3.1

### v2.3.0
**群聊持续对话：作用域控制 + 判定时机可配置**
> ⚠️ **升级提醒**：默认判定时机为 `either`（LLM 处理期间 + 回复后都判，窗口重叠时一轮只判一次），老用户升级后，AI 处理消息期间群友的发言也会触发持续回复（此前只有回复后才判）。如果觉得 bot 变吵，可将 `sustain_judge_timing` 设为 `after_reply` 恢复旧行为。
- **群聊作用域白名单/黑名单**（`sustain_allowed_sessions` / `sustain_denied_sessions`）：白名单非空时仅白名单内群生效；白名单为空时排除黑名单。格式如 `qq:gm:123456`，与私聊黑白名单语义一致
- **判定时机**（`sustain_judge_timing`，默认 `either`）：`both` 两个时机都判定；`either` 窗口重叠时一轮只判一次；`llm_processing` 仅 LLM 处理期间判定（回复后立即关闭兜底窗）；`after_reply` 仅回复后判定。LLM 处理期间兜底开窗覆盖处理中到达的消息——此前这些消息只进缓冲，无新消息触发 flush 时永远不会被处理（AI 会“错过”群聊）。窗口已存在时完全不动（不刷新不关闭），由 AI 最终回复按 timing 策略续期
- **修复长 LLM 处理绕过 max_sustain_replies**：窗口超时清理保留连续计数（count 只在真实唤醒时清零），工具循环超过窗口时长不再导致计数归零
- 作用域检查同时应用于消息判定与 AI 回复开窗，黑白名单外的群完全不受持续对话影响；群被移出作用域时顺带清理残留的窗口/计数状态
- 新增插件图标（`icon.png`，manifest 增加 `icon` 字段，遵循 KiraAI 最新 manifest 图标规范）
> 💡 **注意**：`per_message` 模式 + 高回复概率 + 长工具循环的组合下，LLM 处理期间兜底开窗可能连续命中积压消息（每条命中都会触发一次回复），如不希望这样，建议降低 `sustain_reply_probability` 或改用 `llm_processing` / `after_reply` 时机。
- 版本 v2.2.5 → v2.3.0

### v2.2.5
**兼容 KiraAI v2.31.0 原生多模态（native 模式）**
- 运行时检测 `bot_config.capabilities.image_recognition.mode == "native"`：图片保留在消息链中，由框架原生多模态直传模型（官方图片压缩 + kira_image_ref 持久化引用），本插件只做音频 STT，不再走 VLM 描述
- stage1 / stage2 / stage3 三阶段均跳过图片处理；非唤醒消息图片仍按「非唤醒不识别」策略替换为占位（省 token 设计不变）
- 默认 `vlm_description` 模式行为完全不变
- 版本 v2.2.4 → v2.2.5

### v2.2.4
**最后一步带工具即时收尾 + provider 全挂不误开窗**
- **队列合并不再哑 3 分钟**：agent 在最大步数（`max_tool_loop`）仍返回工具调用时，该步工具执行完即结束、无最终文本收尾。此前 `_final_marked` 只认“无 tool_calls 的文本收尾”，此类批次只能等「批次卡死超时」兜底（默认 LLM 超时 + 工具超时 ≈ 180s）才推送 pending，期间新消息全部被拦截（bot 哑 3 分钟，不丢消息）。现在 `on_llm_response` 通过 `resp.agent_step_index >= max_tool_loop` 识别“最后一步仍带工具”，同样标记收尾，由 tick / ON_STEP_RESULT 立即推送
- **provider 全挂时不再误开持续窗口**：框架在所有模型失败时返回 `[ProviderError] ...` 错误文本（无 tool_calls、直接收尾）。此前持续对话判定会把它当成正常 AI 回复而重新开窗，在 provider 恢复前反复主动触发。现在识别该前缀后静默结束，不开窗
- 兼容性：旧框架响应缺 `agent_step_index` 字段时自动退回原行为（等卡死兜底），不出错
- 版本 v2.2.3 → v2.2.4

### v2.2.3
**队列合并自拦截死锁修复（与 ContextCondensation 等阻塞型插件共存时稳定复现）**
- **根因**：`BatchMergeScheduler._push_pending()` 调用的 `_decide_and_apply_locked()` 会**无条件清空 `_inflight[sid]`**（即使 pending 为空）；而 KiraAI `EventBus.publish()` 只是**异步入队**（`asyncio.Queue.put`，见 `core/event_bus.py`），发布后的合并批次要等事件循环调度才到达 `on_batch_message`。在这个异步窗口内，若同一会话再次触发 `_push_pending`（ON_STEP_RESULT 重复广播、插件 hook 重复注册、tick 竞争等——`core/message_manager.py::send_llm_text()` 在 Agent 每一步都会触发 ON_STEP_RESULT），会把刚发布的合并批次的 inflight 标记清掉，导致该批次到达 `on_batch_message` 时匹配不上 `_inflight`，被误判为外部批次 `event.stop()` 拦截进 pending，会话队列永久死锁
- **日志特征**：`进入最后一步（文本收尾）` 打印两次（同 event_id）；`发布批次 xxx` 后紧跟 `拦截批次 xxx 进 pending（pending=1）`；之后新消息全部 `拦截进 pending` 且数量只增不减
- **修复**：
  1. `_push_pending(sid, done_event_id)` 增加完成批次校验：锁内先确认 `_inflight[sid]` 仍是本次完成的 event_id 才执行推送决策，重复/并发事件直接跳过，不再误清 in-flight 状态
  2. `_build_merged_batch` 为合并批次打 `_qm_self` 自发布标记，`on_batch_message` 识别后无条件放行并恢复 inflight 跟踪——双保险，对一切竞态路径（含 tick、shutdown 重发）免疫自拦截
- 版本 v2.2.2 → v2.2.3

### v2.2.2
**队列稳定性修复 + 媒体并发控制增强**
- **修复热重载丢消息**：插件终止时积压批次（pending）改为按会话合并为**全新批次**重发（新 event_id、干净 stop 状态）。原实现直接重发原事件对象，而框架 `_is_stopped` 一旦置位无法复位，重进管线会被再次拦停，消息永久丢失
- **新增「批次卡死超时」兜底（`section_queue_merge.inflight_stall_timeout`）**：当前批次自最后一次 LLM 响应起超过阈值仍无动静（LLM 挂起 / 异常崩溃导致收尾事件缺失）时，强制推送积压批次，避免会话队列死锁。默认 `0`=自动跟随默认 LLM 模型超时 + 60s 余量；LLM 慢但每轮有响应不会被误判
- **媒体识别并发限流改为三级**（`section_media_recognition`）：批次级（`max_parallel_images` / `max_parallel_audios`，单批突发保护）+ 会话级（`vlm/stt_max_parallel_per_session`）+ 全局级（`vlm/stt_max_parallel_global`），固定获取顺序无死锁
- **媒体「最多识别一次」**：同一消息内的每个图片/语音成功或失败后标记已处理，队列合并重发时不再重复调用 VLM/STT（防限流/429 风暴）
- **并行识图插件兼容增强**：`compat_mode=auto` 改为运行时实时检测并行识图插件加载状态（热重载/启停即时生效）；媒体积压放行判定同时识别本插件与并行识图插件的暂存属性
- 跨会话状态修复：媒体暂存索引按会话分层，多会话并发处理不再串扰
- 版本 v2.2.1 → v2.2.2

### v2.2.1
**STT 兼容修复**
- 适配新版 KiraAI：`ctx.llm_api.speech_to_text` 已废弃，改为 `provider_mgr.get_default_stt()` + `core.utils.common_utils.speech_to_text`，修复语音识别失效问题
- 版本 v2.2.0 → v2.2.1

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
- 版本 v2.1.0 → v2.2.0

### v2.1.0
**语音时长限制修复**
- 修复 `voice_max_duration` 对机器人自己的语音消息无效的问题：当用户引用机器人发出的语音时，该语音缺少 `duration` 元数据，导致时长限制被绕过
- 新增 `_get_record_duration`：优先读取元数据 `duration`，缺失时自动从音频原始字节估算时长
- 新增音频时长估算能力：支持 WAV（通过 `wave` 模块解析帧头）和 MP3（通过首个有效帧头码率推算）两种格式
- 新增 `_record_bytes`：统一从 base64 / data_url / 本地路径三种来源提取音频原始字节
- 现在所有语音消息（含机器人自己的语音被引用）都受 `voice_max_duration` 约束，超长语音统一替换为 `[长语音 N秒]`
- 版本 v2.0.3 → v2.1.0

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
