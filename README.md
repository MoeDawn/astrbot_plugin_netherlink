# NetherLink — 我的世界服务器 × QQ 群消息互通

一个 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 插件：把 Minecraft 服务器和 QQ 群双向打通。

- **消息互通**：游戏内聊天/进服/退服/死亡推到群里，群消息广播到游戏公屏（支持 `§` 染色码）
- **自然语言执行指令**：群友或玩家说「把 Steve 改成创造模式」，AI 判断后以控制台身份执行，服务器真实输出回传
- **游戏内机器人对话**：玩家发唤醒词开头的话即可与 AI 对话，玩家的话与 AI 的回复都同步到群
- **好感度系统**：AI 依对话与请求自主增减好感，好感存在服务端，影响它的态度与是否愿意办事

配套的 MC 服务端插件：[ab-netherlink-paper](https://github.com/MoeDawn/ab-netherlink-paper)

## 效果示例

游戏公屏（颜色由模板里的 `§` 码控制，均可配置）：

```text
⌜水群⌟ <张三> 晚上八点开打末影龙          ← QQ 群消息（群名绿、群友蓝、内容紫）
⌜ai⌟ : 收到，记得准备好附魔装备哦～        ← 机器人回复
[MC] Steve: 大家好                        ← 游戏聊天推送到群
```

同一句回复在 QQ 群里显示为 `[MC] ai: 收到，记得准备好附魔装备哦～`。

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

还需要：AstrBot 已配好可用的 **LLM 提供商**（对话与指令解析都依赖它），以及 MC 端已装 [ab-netherlink-paper](https://github.com/MoeDawn/ab-netherlink-paper)。

## 配置项

| 配置项 | 说明 |
|---|---|
| `ws_host` | 本插件监听的地址（默认 `0.0.0.0`），MC 端主动连入 |
| `ws_ports` | **端口绑定**（逗号分隔）。一台 MC 服占一个端口，如 `survival:8765,creative:8766`；**单服只填一个**。⚠️ 留空则不监听（启动报错）；两台服连同一个端口会互相踢下线，务必各占一个 |
| `auth_token` | 握手鉴权密钥，两侧必须一致 |
| `target_groups` | 绑定的 QQ 群号（逗号分隔） |
| `group_names` | 群号与群名映射，如 `123456:水群,987654:生存服`，用于 `{group}` 占位符 |
| `admin_qq` | 管理员 QQ 号（逗号分隔） |
| `admin_mc` | 管理员游戏 ID（逗号分隔）。**游戏内管理员只能在这里填**（AstrBot 的全局管理员是 QQ 号，对游戏内无效） |
| `server_display_names` | 按服务器区分显示名，如 `survival:生存服,creative:创造服`（`server-name` 取 MC 端配置里的值）。用于 `{server}` 占位符、群消息前缀、AI 上下文；留空则显示为 `MC` |
| `mc_bot_name` | 机器人游戏内名字，用于 `{bot}` 占位符 |
| `mc_wake_prefixes` | 游戏内唤醒词（默认 `ai,助手`），**须与 MC 端 `wake-prefixes` 一致** |
| `sync_bot_msgs` | 机器人自己的群消息是否也转发进游戏（默认关，防刷屏） |
| `enable_webui_persona` | 游戏内对话是否使用 WebUI 配置的人格（默认开） |
| `extra_system_prompt` | 附加提示词，支持 `{server}` 占位符，**留空则不附加** |
| `karma_rules` | 好感度规则提示词（消耗表、初始值等）。**可自由修改**；想让指令不消耗好感，把消耗表全改成 0 |
| `karma_initial` | 新玩家初始好感（默认 10） |
| `karma_death_penalty` | 玩家死亡扣的好感（默认 2，0 为不扣） |
| `max_command_cost` | 单次指令好感消耗上限（默认 100，防止 AI 报价失控） |
| `karma_records` | 好感度记录，格式 `{"qq:12345": 12}`，可在 WebUI 直接改 |
| `enable_advancement` / `advancement_prompt` | 玩家获得成就时是否通知 AI，以及通知用的提示词 |
| `log_karma_changes` | 对话引起的好感变化是否记日志（默认开） |
| `mc_command_tool_desc` / `karma_tool_desc` | 两个工具的描述文本，决定 AI 如何判断 |
| `enable_chat` / `enable_join_leave` / `enable_death` / `enable_qq_to_mc` | 各类消息的同步开关 |
| 各 `template_*` | 消息模板（QQ→MC 与游戏内回复支持 `§` 染色码） |

## 使用方式

- **QQ → 游戏**：绑定群里发消息，游戏公屏显示 `⌜群名⌟ <名字> 消息`
- **QQ 群友与 AI 对话**：AI 会被告知发言者是谁、以及他是不是管理员（绑定群内生效）
- **游戏 → QQ**：游戏内聊天/进服/退服/死亡自动推送
- **游戏内对话**：发 `ai 你好` 或 `ai 列出在线玩家`，回复显示在游戏里并同步到群（玩家原话也会按聊天模板推群，群里看得到问了什么）
- **执行指令**（QQ 或游戏内都行）：
  - 「现在几点了」→ AI 执行 `time` 并回复（纯查询，不消耗好感）
  - 「把 Steve 改成创造模式」→ AI 判断发起者身份与好感是否够，够了才执行

## 好感度系统

好感值存本地 `data/plugin_data/netherlink/karma.json`，并同步进 WebUI 配置项 `karma_records`，可直接查看和修改。

**两个独立身份空间**：游戏内玩家用 `mc:游戏ID`，QQ 群友用 `qq:QQ号`。同一个人在两边是两份独立好感，互不影响。

### 工作方式

AI 每次对话先查好感，再决定说话方式；请求指令时按 `karma_rules` 决定消耗：

```text
玩家: ai，带我去附近的村庄
AI:  （查好感 → 10；按规则村庄传送要 20，付不起 → 拒绝，不执行）
     呜…带着你跑那么远要消耗 20 点好感，你现在只有 10 点，人家心有余而力不足啦

玩家: ai，给我三个铁锭
AI:  （查好感 → 10；按规则报价 3，付得起 → 执行并扣 3）
     拿好啦～不过人家小小地记了一笔账哦
```

**扣费由插件保证**：报价与实际扣减在同一次调用内完成。好感不足直接拒绝；执行失败（服务器回报失败或超时无回执）自动退费，不会白扣。

### 变化来源

| 来源 | 谁决定 | 说明 |
|---|---|---|
| 对话 | AI | 依 `karma_rules` 自主增减（友善互动、不友好言论等），每次不超过 ±2 |
| 执行指令 | AI 报价 + 插件扣 | 越 OP 的指令消耗越多，在 `karma_rules` 里改消耗表 |
| 死亡 | 插件自动 | 扣 `karma_death_penalty`，因为死亡不走对话、AI 不在场 |
| 获得成就 | AI | 插件把 `advancement_prompt` 发给 AI，由它决定加多少并回话 |

### 四点须知

- **好感度不可关闭**。想实现「不限制玩家用指令」，请改**提示词**：把 `karma_rules` 的消耗表全部写成 0。
- **管理员待遇也由提示词决定**：插件只把「谁是管理员」作为事实告诉 AI（写在 `<netherlink_context>` 里），并不规定该怎么对待他。给管理员更宽松的尺度是在 `karma_rules` 里写的——想收紧或放宽，改那段提示词即可。
- **⚠️ `karma_rules` 的现值直接决定 AI 行为**：它**每次都注入** system_prompt，且插件**不会告警、也不会自动纠正**——里面若写着占位文案（如「好感度系统已关闭」）或与当前机制冲突的说明，AI 就照它执行。请到 WebUI 确认它是真正的规则（留空会回退默认值）。
- **查询不写盘**：AI 每次对话都会查好感，这类查询只读内存，只有真正增减时才写入文件。

## 环境要求

- AstrBot（含可用的 LLM 提供商）
- Minecraft 服务端：**目前仅支持 Paper 26.3**（其他版本或服务端核心未适配）
- 服务端已装并运行 [ab-netherlink-paper](https://github.com/MoeDawn/ab-netherlink-paper)
  —— 请用**同版本**的那一份（`0.0.2`）
- AstrBot 只需配置，无需额外依赖

> ⚠️ **版本限定**：本插件目前**只支持 Minecraft 26.3 的 Paper 端**。
> 换到其他 MC 版本（如 1.21、1.20）或其他服务端核心（Spigot / Fabric / Forge 等）
> **不能保证可用**——MC 端插件是按 Paper 26.3 的 API 编译的，且依赖一些 Paper
> 特有的接口。适配其他版本需要单独改造 MC 端。

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
