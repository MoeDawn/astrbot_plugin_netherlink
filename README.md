# NetherLink — 我的世界服务器 × QQ 群消息互通

一个 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 插件：把 Minecraft 服务器和 QQ 群双向打通。

- **消息互通**：游戏内聊天/进服/退服/死亡推到群里，群消息广播到游戏公屏（支持 `§` 染色码）
- **自然语言执行指令**：群友或玩家说「把 Steve 改成创造模式」，AI 判断后以控制台身份执行，服务器真实输出回传
- **游戏内机器人对话**：玩家发唤醒词开头的话即可与 AI 对话，回复显示在游戏里并同步到群
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

安装后需要在配置里填三样东西：

| 配置项 | 填什么 |
|---|---|
| `auth_token` | 自定义一串密钥，**必须与 MC 端 `config.yml` 的 `token` 一模一样** |
| `target_groups` | 要绑定的 QQ 群号，多个用英文逗号分隔 |
| `admin_qq` / `admin_mc` | 管理员名单（可选，见下） |

还需要：AstrBot 已配好可用的 **LLM 提供商**（对话与指令解析都依赖它），以及 MC 端已装 [ab-netherlink-paper](https://github.com/MoeDawn/ab-netherlink-paper)。

## 配置项

| 配置项 | 说明 |
|---|---|
| `ws_host` / `ws_port` | 本插件监听的地址/端口（默认 `0.0.0.0:8765`），MC 端主动连入 |
| `auth_token` | 握手鉴权密钥，两侧必须一致 |
| `target_groups` | 绑定的 QQ 群号（逗号分隔） |
| `group_names` | 群号与群名映射，如 `123456:水群,987654:生存服`，用于 `{group}` 占位符 |
| `admin_qq` | 管理员 QQ 号（逗号分隔） |
| `admin_mc` | 管理员游戏 ID（逗号分隔）。**游戏内管理员只能在这里填**（AstrBot 的全局管理员是 QQ 号，对游戏内无效） |
| `mc_server_name` | 服务器**显示名**，用于 `{server}` 占位符、群消息前缀、AI 上下文 |
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
- **游戏 → QQ**：游戏内聊天/进服/退服/死亡自动推送
- **游戏内对话**：发 `ai 你好` 或 `ai 列出在线玩家`，回复显示在游戏里并同步到群
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

**扣费由插件保证**：报价与实际扣减在同一次调用内完成。好感不足直接拒绝；执行失败（服务器无回执）自动退费，不会白扣。

### 变化来源

| 来源 | 谁决定 | 说明 |
|---|---|---|
| 对话 | AI | 依 `karma_rules` 自主增减（友善互动、不友好言论等），每次不超过 ±2 |
| 执行指令 | AI 报价 + 插件扣 | 越 OP 的指令消耗越多，在 `karma_rules` 里改消耗表 |
| 死亡 | 插件自动 | 扣 `karma_death_penalty`，因为死亡不走对话、AI 不在场 |
| 获得成就 | AI | 插件把 `advancement_prompt` 发给 AI，由它决定加多少并回话 |

### 两点须知

- **好感度不可关闭**。想实现「不限制玩家用指令」，请改**提示词**：把 `karma_rules` 的消耗表全部写成 0。
- **查询不写盘**：AI 每次对话都会查好感，这类查询只读内存，只有真正增减时才写入文件。

## 升级须知（从旧版升级）

- **群消息曾静默丢失（已修复，无需改配置）**：旧版把平台标识写死成 `aiocqhttp`，只有把机器人命名为 `aiocqhttp` 的部署能推送成功，其余部署下游戏内事件在群里**完全看不到**（AstrBot 日志里会有一条 `cannot find platform for session ...`）。升级即可修复。
- **`karma_rules` 会自动迁移**：旧版默认规则与新的扣费机制冲突，插件启动时检测到会自动换成新规则并在日志里告警。**如果你曾自定义过它，请到 WebUI 重新确认**。
- **好感度不会自动迁移**：旧版把好感存在 MC 计分板上，新版改存本地文件。旧数值需要你手动搬。
- **⚠️ 检查 `karma_rules` 与 `karma_tool_desc` 的现值**：旧版关掉好感度时这两个字段不生效，里面可能还留着当时的占位文案（如「好感度系统已关闭」）。**这些文本现在会立即生效**并与扣费机制冲突，请改成真正的规则。
- **`enable_webui_persona` 开始真正生效**：旧版因一处 bug 一直空转，若你配置过 WebUI 人格，游戏内说话风格会变化。
- **`enable_karma` 开关已删除**：好感度改为强制机制，该键被无视（残留在配置里无害）。
- **`bindings.json` 已废弃**：插件不再读取。
- **已删除的配置项**：`normal_commands`、`strict_command_mode`、`bindings`、`template_cmd_done_game`、`enable_karma`，残留在配置里无副作用。

## 环境要求

- AstrBot（含可用的 LLM 提供商）
- 已安装并运行 [ab-netherlink-paper](https://github.com/MoeDawn/ab-netherlink-paper) 的 Minecraft 服务端
- AstrBot 只需配置，无需额外依赖

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
├── logo.png
├── LICENSE
└── README.md
```
