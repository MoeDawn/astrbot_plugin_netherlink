# NetherLink — 我的世界服务器 × QQ 群消息互通

一个 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 插件：把 Minecraft 服务器和 QQ 群双向打通。

- **消息互通**：游戏内聊天/进服/退服/死亡推到群里，群消息广播到游戏公屏（支持 `§` 染色码）
- **自然语言执行指令**：群友或玩家说「把 Steve 改成创造模式」，AI 判断后以控制台身份执行，服务器真实输出回传
- **游戏内机器人对话**：玩家发唤醒词开头的话即可与 AI 对话，玩家的话与 AI 的回复都同步到群
- **好感度系统**：AI 依对话与请求自主增减好感，好感存在服务端，影响它的态度与是否愿意办事

配套的 MC 服务端插件：[netherlink-server](https://github.com/MoeDawn/netherlink-server)

## 效果示例

游戏公屏（颜色由模板里的 `§` 码控制，均可配置）：

```text
⌜水群⌟ <张三> 晚上八点开打末影龙          ← QQ 群消息（群名绿、群友蓝、内容紫）
⌜ai⌟ : 收到，记得准备好附魔装备哦～        ← 机器人回复
⌜MC⌟ <Steve>: 大家好                      ← 游戏聊天推送到群
```

同一句回复在 QQ 群里显示为 `⌜MC⌟ ai: 收到，记得准备好附魔装备哦～`。

## 安装

**推荐：插件市场**

AstrBot WebUI → 插件 → 插件市场 → 搜索「NetherLink」安装。

**手动安装**

把 `astrbot_plugin_netherlink.zip` 放进 AstrBot 的 `data/plugins/` 下解压，重启或热重载插件。

安装后需要在配置里填这几样：

| 配置项 | 填什么 |
|---|---|
| `ws_ports` | **必填**。本插件监听的端口，如 `8765`；多台服务器时每台占一个，如 `survival:8765,creative:8766`。⚠️ **留空则不监听任何端口**（启动时会报错） |
| `auth_token` | 自定义一串密钥，**必须与 MC 端 `config.yml` 的 `token` 一模一样** |
| `target_groups` | 要绑定的 QQ 群号，多个用英文逗号分隔 |
| `server_display_names` | 服务器显示名（可选，默认 `MC`），如 `survival:生存服` |
| `admin_qq` / `admin_mc` | 管理员名单（可选，见下） |

> `ws_ports` 里写的 `server-name` 要与 MC 端 `config.yml` 的 `server-name` 对应；
> 只写端口时则直接采用 MC 端上报的那个名字。

还需要：AstrBot 已配好可用的 **LLM 提供商**（对话与指令解析都依赖它），以及 MC 端已装 [netherlink-server](https://github.com/MoeDawn/netherlink-server)。

## 配置项

| 配置项 | 说明 |
|---|---|
| `ws_host` | 本插件监听的地址（默认 `0.0.0.0`），MC 端主动连入 |
| `ws_ports` | **端口绑定**（逗号分隔）。一台 MC 服占一个端口，如 `survival:8765,creative:8766`；**单服只填一个**。⚠️ 留空则不监听（启动报错）；两台服连同一个端口会互相踢下线，务必各占一个 |
| `auth_token` | 握手鉴权密钥，两侧必须一致 |
| `target_groups` | 绑定的 QQ 群号（逗号分隔）。**只管消息互通**：这些群的消息转发进 MC、MC 事件推送到这些群。不影响身份注入与好感度 |
| `group_names` | 群号与群名映射，如 `123456:水群,987654:生存服`，用于 `{group}` 占位符 |
| `admin_qq` | 管理员 QQ 号（逗号分隔） |
| `admin_mc` | 管理员游戏 ID（逗号分隔）。**游戏内管理员只能在这里填**（AstrBot 的全局管理员是 QQ 号，对游戏内无效） |
| `server_display_names` | 按服务器区分显示名，如 `survival:生存服,creative:创造服`（`server-name` 取 MC 端配置里的值）。用于 `{server}` 占位符、群消息前缀、AI 上下文；留空则显示为 `MC` |
| `mc_bot_name` | 机器人游戏内名字，用于 `{bot}` 占位符 |
| `mc_wake_prefixes` | 游戏内唤醒词（默认 `ai,助手`），**须与 MC 端 `wake-prefixes` 一致** |
| `sync_bot_msgs` | 机器人自己的群消息是否也转发进游戏（默认关，防刷屏） |
| `enable_webui_persona` | 游戏内对话是否使用 WebUI 配置的人格（默认开） |
| `enable_game_platform` | 游戏内对话走 **AstrBot 原生平台管线**（默认开）。**首次启用需要重启一次 AstrBot**——框架在启动时实例化平台 |
| `template_netherlink_context_game` | **游戏侧注入给 AI 的系统上下文模板**（2026-09-22 由 `extra_system_prompt` 改名而来）。占位符 `{identity}` `{is_admin}` `{server}`。默认值含「你处于 MC 服务器 + 当前发起者是否管理员 + 玩家试图给你东西时的三步流程」。**空串回退默认值**（想「不注入」应删掉模板正文）|
| `karma_rules` | 好感度规则：范围、对话如何影响态度、每次变化不超过 ±2、要隐晦暗示、不透露具体数值。**可自由修改**；想「不依赖好感度」请改 `mc_command_cost_param_desc`（把价目表写成免费/传 0），改这里已经不管用——指令的执行尺度不在这一项里 |
| `karma_initial` | 新玩家初始好感（默认 20） |
| `karma_death_penalty` | 玩家死亡扣的好感（默认 2，0 为不扣） |
| `max_command_cost` | 单次指令好感消耗上限（默认 80，防止 AI 报价失控） |
| `karma_records` | 好感度记录，格式 `{"qq:12345": 12}`，可在 WebUI 直接改 |
| `enable_advancement` / `advancement_prompt` | 玩家获得成就时是否通知 AI，以及通知用的提示词（默认含**好感量级 2~10，按成就难度**）|
| `log_karma_changes` | 对话引起的好感变化是否记日志（默认开） |
| `mc_command_tool_desc` | `mc_command` 的工具描述。**决定 AI 该拒绝哪些请求**（清空成就进度、召末影龙、破坏他人建筑…）以及给管理员多宽松的尺度 |
| `karma_tool_desc` | `mc_karma` 的工具描述 |
| `mc_command_cost_param_desc` | `mc_command` 的 **cost 参数说明**。**指令的好感价目表就在这里**（免费放行 / 可以执行 / 按超标程度计价三类 + 具体价格）|
| `mc_karma_delta_param_desc` | `mc_karma` 的 delta 参数说明 |
| `mc_command_cmd_param_desc` | `mc_command` 的 cmd 参数说明（游戏侧与 QQ 侧共用）|
| `mc_command_server_param_desc` | `mc_command` 的 server 参数说明（仅 QQ 侧有该参数）|
| `template_netherlink_context_qq` | **QQ 侧注入给 AI 的系统上下文模板**。占位符 `{identity}` `{is_admin}`。空串回退默认值 |
| `enable_chat` / `enable_join_leave` / `enable_death` / `enable_qq_to_mc` | 各类消息的同步开关 |
| 其余 `template_*` | 消息模板（QQ→MC 与游戏内回复支持 `§` 染色码） |

## 使用方式

- **QQ → 游戏**：绑定群里发消息，游戏公屏显示 `⌜群名⌟ <名字> 消息`
- **QQ 群友与 AI 对话**：AI 会被告知发言者是谁、他是不是管理员、以及好感规则与当前好感（`aiocqhttp` 平台的所有群都生效，不限绑定群）
- **游戏 → QQ**：游戏内聊天/进服/退服/死亡自动推送
- **游戏内对话**：发 `ai 你好` 或 `ai 列出在线玩家`，回复显示在游戏里并同步到群（玩家原话也会按聊天模板推群，群里看得到问了什么）。走 AstrBot 原生管线，因此**人格、记忆、工具调用提示都与 QQ 侧一致**；在 WebUI 的「提供商设置」里打开 `show_tool_use_status` / `show_tool_call_result` / `display_reasoning_text` 还能在游戏里看到 AI 的工具调用与思考过程
- **执行指令**（QQ 或游戏内都行）：
  - 「现在几点了」→ AI 执行 `time` 并回复（纯查询，不消耗好感）
  - 「把 Steve 改成创造模式」→ AI 判断发起者身份与好感是否够，够了才执行
  - 「清空我的成就进度」→ 这类会**直接拒绝**，AI 根本不会去调工具

> ⚠️ **`target_groups` 只管消息互通**：不在名单里的群，消息不转发进游戏、游戏事件也不推过去。
> 但**身份注入与好感度对所有群生效**——未绑定群里的 AI 一样认得出发起者与管理员，
> `mc_command` / `mc_karma` 也照常可用。要彻底断开某个群，请把机器人从那个群移出，
> 或用 AstrBot 的 `plugin_set` 限制插件生效范围。
>
> 私聊与其他平台（Telegram、网页聊天等）**不会**被注入身份——这是刻意的（避免污染
> 其他插件与其他平台的提示词）。

## 好感度系统

好感值存本地 `data/plugin_data/netherlink/karma.json`，并同步进 WebUI 配置项 `karma_records`，可直接查看和修改。

**两个独立身份空间**：游戏内玩家用 `mc:游戏ID`，QQ 群友用 `qq:QQ号`。同一个人在两边是两份独立好感，互不影响。

### 工作方式

AI 每次对话先查好感（游戏内由提示词强制、QQ 群里由 AI 自行判断），再决定说话方式；请求指令时按 `mc_command_cost_param_desc` 里的价目表报价：

```text
玩家: ai，给我三个铁锭
AI:  （查好感 → 20；按价目表三个铁锭 1 点，付得起 → 执行并扣 1）
     拿好啦～不过人家小小地记了一笔账哦

玩家: ai，带我去附近的樱花树林
AI:  （查好感 → 19；按价目表这类需要定位+传送的地点 10 点起，付得起 → 执行并扣 10）
     好呀，抓稳了，这就带你过去～

玩家: ai，把大家的成就进度清空
AI:  （这类请求一律拒绝，不调用指令工具）
     这个可不行哦，清空了就能反复刷成就，人家不能帮你做这个
```

**扣费由插件保证**：报价与实际扣减在同一次调用内完成。好感不足直接拒绝；执行失败（服务器回报失败或超时无回执）自动退费，不会白扣。

### 变化来源

| 来源 | 谁决定 | 说明 |
|---|---|---|
| 对话 | AI | 依 `karma_rules` 自主增减（友善互动、不友好言论等），每次不超过 ±2 |
| 执行指令 | AI 报价 + 插件扣 | 越 OP 的指令消耗越多，价目表在 `mc_command_cost_param_desc` 里改 |
| 死亡 | 插件自动 | 扣 `karma_death_penalty`，因为死亡不走对话、AI 不在场 |
| 获得成就 | AI | 插件把 `advancement_prompt` 发给 AI，由它决定加多少并回话 |

### 几点须知

- **好感度不可关闭**。想实现「不限制玩家用指令」，请改**提示词**：把 `mc_command_cost_param_desc` 的价目表全部写成免费（或让 AI 永远同意）。
- **管理员待遇也由提示词决定**：插件只把「谁是管理员」作为事实告诉 AI（写在 `<netherlink_context>` 里），并不规定该怎么对待他。给管理员更宽松的尺度写在 **`mc_command_tool_desc`**（工具描述）里——想收紧或放宽，改那一项即可。
  两侧的上下文文本各自可配：QQ 侧在 `template_netherlink_context_qq`，游戏侧在 `template_netherlink_context_game`（改措辞、增删句子都可以）。
- **⚠️ 提示词的现值直接决定 AI 行为**：`karma_rules`、两个工具描述与两个参数说明都**每次都注入**（参数说明仅 `skills_like` 模式下延迟加载），且插件**不会告警、也不会自动纠正**——里面若写着占位文案（如「好感度系统已关闭」）或与当前机制冲突的说明，AI 就照它执行。请到 WebUI 确认它们是真规则（留空会回退默认值）。
- **查询不写盘**：AI 每次对话都会查好感，这类查询只读内存，只有真正增减时才写入文件。

## 环境要求

- AstrBot（含可用的 LLM 提供商）
- Minecraft 服务端：**Paper 26.3 / Purpur 26.3 / Folia**（见下表）
- 服务端已装并运行 [netherlink-server](https://github.com/MoeDawn/netherlink-server)
  —— 请用**同版本**的那一份（`0.1.0`）
- AstrBot 只需配置，无需额外依赖

> **支持的服务端核心（Minecraft 26.3）**

| 核心 | 状态 | 说明 |
|---|---|---|
| **Paper** | ✅ 已验证 | 当前实机运行的就是它 |
| **Purpur** | ✅ 可运行 | Paper 的分支，API 与事件完全一致 |
| **Folia** | ⚠️ 已适配，未实机验证 | 代码已改用 Paper/Folia 共用的调度器并声明 `folia-supported`；但没有跑过真 Folia 服务端 |
| Spigot | ❌ 不支持 | 依赖 Paper 专有扩展 `Bukkit.createCommandSender`（用来捕获指令输出），Spigot 没有这个 API |
| Fabric / NeoForge | ❌ 不支持 | 它们是**模组加载器**而非 Bukkit 实现，需要单独移植的客户端版本 |

> ⚠️ **版本限定**：MC 端只支持 **Minecraft 26.3**。换到其他 MC 版本（如 1.21、1.20）
> 需要重新编译、并可能改动代码——本插件按 26.3 的 API 编译，
> 且依赖 Paper 的 `AsyncChatEvent` 与 `Bukkit.createCommandSender`。

## 许可

[MIT License](LICENSE)

## 目录结构

```text
astrbot_plugin_netherlink/
├── main.py              # 插件主逻辑
├── karma.py             # 好感度算法与本地存储
├── store.py             # 原子 JSON 读写
├── metadata.yaml        # 插件元数据
├── _conf_schema.json    # WebUI 配置项定义
├── requirements.txt     # 依赖
├── CHANGELOG.md         # 更新日志（插件市场详情页显示它）
├── logo.png
├── LICENSE
└── README.md
```
