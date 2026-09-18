# NetherLink — Minecraft(Paper) × QQ 群双向消息互通

Minecraft 服务器与 QQ 群双向消息互通、双侧自然语言执行 MC 指令、游戏内机器人对话。

两个成品：
1. **astrbot_plugin_netherlink**：AstrBot 插件（Python）
2. **paper-plugin**：MC 端 Paper 26.3 插件（Java）

## 架构

```
QQ群 ←→ NapCat/Lagrange ←→ AstrBot(插件, WS服务端) ←WebSocket/JSON行→ Paper服务器(插件, WS客户端)
```

MC 端作为客户端主动连入 AstrBot（AstrBot 换机器不用改 MC 配置），token 握手鉴权，
断线自动指数退避重连，15 秒心跳。

## 功能总览

| 功能 | 说明 |
|---|---|
| MC → QQ | 聊天/进服/退服/死亡按模板推送（`{server}` 占位符 = 服务器名） |
| QQ → MC | 绑定群消息按模板渲染（**支持 § 染色码**）广播到公屏，`{group}` = 群名；`sync_bot_msgs` 开启后机器人自己的回复也同步进游戏 |
| 游戏内对话 | 玩家发唤醒词开头消息（默认 `ai`、`助手`）→ 触发 LLM 对话，回复广播在游戏内 + 同步到 QQ 群 |
| QQ 指令 | 群里对机器人说自然语言 → LLM 调用 `mc_command` 工具以控制台身份执行，真实输出回传 |
| 游戏内指令 | 游戏里同样可以对机器人说指令，判断规则与 QQ 侧一致——同样由 AI 依提示词决定 |
| 指令执行 | `mc_command` 工具以控制台身份执行，带必填 `cost` 参数；插件在同一次调用内原子扣好感（不足则拒绝、执行失败则回滚） |
| AI 判断 | **插件不做权限判断、不持有任何指令名单**——高危识别、管理员判定、好感消耗全部由 AI 依提示词决定 |
| 好感度系统 | 存本地 `data/plugin_data/netherlink/karma.json`，双身份空间（`mc:游戏ID` / `qq:QQ号`），并同步写回 `karma_records` 配置供 WebUI 查看 |
| 死亡惩罚 | 玩家死亡由插件自动扣 `karma_death_penalty`（默认 2），不经 AI；`enable_death` 关闭则**同时**停用死亡播报与死亡惩罚（它是唯一门槛） |
| 成就加成 | 玩家获得成就（不含配方解锁/根成就）→ 配置的提示词发给 AI，由它更新好感并回话；回复广播回游戏并同步 QQ。`enable_advancement` 开关 |

### 消息效果示例

游戏公屏（颜色由模板中的 § 码控制，均可配置）：

```
⌜水群⌟ <张三> 晚上八点开打末影龙          ← QQ 群消息（群名绿色、群友蓝色、内容紫色）
⌜妹⌟ : 收到，记得准备好附魔装备哦～        ← 机器人回复（名字紫色、内容绿色）
[MC] Steve: 大家好                         ← 游戏聊天推送到 QQ 群
```

同一台机器人的回复在 QQ 群里则显示为 `[ai] 收到，记得准备好附魔装备哦～`（纯文本，无染色码）。
QQ 侧发起的指令**没有单独的游戏内简报**：内层 agent 的文本是返回给主 agent 的**工具结果**，
主 agent 据此组织的回复发在 QQ 群；开启 `sync_bot_msgs` 时这条群回复才会按 QQ→MC 模板
同步进游戏公屏。

### 服务器离线时的行为

**好感度是本地功能，服务器没连上也能用。** MC 未连接时：

- `mc_command` **不会**提前拒绝——仍然起内层 agent、照常报价与决策；只有「执行」
  这步失败，插件会如实告知并**退还**已扣的好感
- `mc_karma` 可正常查询与增减（它本来就只读写本地 `karma.json`，不依赖 MC 连接）
- QQ → MC 的**普通消息转发不受影响**：仍然尝试发送，失败时记一条
  `MC 服务器未连接，消息丢弃` 日志（不缓存、不重放）

> 早先的实现是「MC 未连接就直接返回、连 LLM 都不起」，那会把整个对话连同好感度
> 一起挡掉。2026-09-18 已改为：离线只让「执行」失败，决策与好感度照常。

### 权限模型

**本插件不做任何权限判断，也不持有任何指令名单。** 是否执行一条指令，完全由 AI
读取提示词后自行决定。

- 管理员名单作为**参考信息**注入提示词（`<netherlink_context>`），供 AI 判断：
  - QQ 侧：AstrBot 全局管理员（`event.is_admin()`），或 `admin_qq` 配置里的 QQ 号
  - 游戏侧：**仅** `admin_mc` 配置里的游戏 ID（AstrBot 的 `admins_id` 是 QQ 号，对游戏内无效）
- 只注入**适用的那一份名单**：QQ 侧给 QQ 名单，游戏侧给游戏 ID 名单
- ⚠️ **QQ 外层 agent 拿不到 `<netherlink_context>`**：那份名单只注入游戏侧与 QQ
  **内层** agent 的 system_prompt；而外层 agent（`@filter.llm_tool` 注册的
  `mc_command`，也是唯一读得到工具描述的那一面）的 system_prompt 由 AstrBot 内建，
  插件注入不进去。所以默认工具描述写成条件式（「如需判断发起者身份，以对话中提供的
  `<netherlink_context>` 为准（若未提供，则按普通玩家对待）」）。**自定义这段描述时
  不要写「管理员名单见 netherlink_context」**——外层 agent 找不到名单，会把管理员的
  高危请求当普通玩家直接拒掉，连手握名单的内层 agent 都不会被启动（fail-closed，
  但管理员机制被整个架空）
- `mc_command` 与 `mc_karma` 的**工具描述可在 WebUI 配置**（`mc_command_tool_desc`
  / `karma_tool_desc`），AI 的判断依据完全由你掌控。两者都**始终生效**——好感度是
  强制机制，没有开关能关掉它们（决策十删除了 `enable_karma`）
- 高危指令的识别依赖 AI 自身的 MC 知识。第三方插件指令 AI 可能无法识别风险

> ⚠️ 这是**提示词层防线，不是技术层防线**。原设计有一层代码硬拦截（白名单外仅管理员可执行）
> 与 QQ↔游戏 ID 绑定表，本次重构按需求把它们整体移除了。AI 若被说服就会真的执行。

## AstrBot 插件安装

将本目录（main.py 等）放入 AstrBot `data/plugins/astrbot_plugin_netherlink/`，
在 WebUI 插件配置中填写：

| 配置项 | 说明 |
|---|---|
| `ws_host` / `ws_port` | 本插件监听的 WebSocket 地址/端口（默认 0.0.0.0:8765） |
| `auth_token` | 鉴权 token，两侧必须一致 |
| `target_groups` | 绑定的 QQ 群号（逗号分隔） |
| `group_names` | 群号与群名映射（`123456:水群,987654:生存服`），用于 `{group}` 占位符 |
| `admin_qq` | 管理员 QQ 号（逗号分隔，仅作参考信息注入提示词） |
| `admin_mc` | 管理员游戏 ID（逗号分隔，游戏侧唯一来源） |
| `mc_server_name` | 服务器名，用于 `{server}` 占位符 |
| `mc_bot_name` | 机器人游戏内名字，用于 `{bot}` 占位符 |
| `mc_wake_prefixes` | 游戏内唤醒词（逗号分隔，默认 `ai,助手`；**须与 Paper 端 `wake-prefixes` 一致**） |
| `sync_bot_msgs` | 机器人 QQ 消息也同步进游戏（默认关，防刷屏；开启后 QQ→MC 转发不再忽略机器人消息） |
| `enable_webui_persona` | 游戏内对话是否使用 AstrBot WebUI 配置的人格（默认开） |
| `extra_system_prompt` | 附加提示词，支持 `{server}`；**留空则不附加** |
| `karma_rules` | 好感规则提示词全文（消耗表/初始值等；**始终注入、无开关**，可自行修改，**留空回退默认值**）。想让指令不消耗好感，就把消耗表全部改成 0 |
| `karma_initial` | 新玩家初始好感（默认 10） |
| `max_command_cost` | 单次指令好感消耗上限（默认 100，AI 报价超出会被裁剪；负值夹到 0 = 指令全免费） |
| `karma_death_penalty` | 玩家死亡扣减（默认 2，0 为不扣，负值夹到 0；只需 `enable_death` 开启） |
| `karma_records` | 好感度记录 JSON，格式 `{"qq:12345": 12}`（只有键与好感值，**不带时间**）。**手改优先级最高** |
| `enable_advancement` | 玩家获得成就时通知 AI（默认开） |
| `advancement_prompt` | 成就提示词，占位符 `{player}`/`{server}`/`{advancement}` |
| `log_karma_changes` | 对话触发的好感变化是否记日志（默认开）。AI 因对话自主增减好感时记一条 info，标明来源侧（`QQ 对话` / `游戏内对话`）与变化量 |
| `mc_command_tool_desc` | `mc_command` 工具描述（默认不含好感度内容；**留空回退默认值**） |
| `karma_tool_desc` | `mc_karma` 工具描述（**始终生效、无开关**；**留空回退默认值**） |
| `enable_death` | 死亡播报开关，**同时**门控死亡扣好感——它是死亡惩罚**唯一**的门槛 |
| `enable_chat` / `enable_join_leave` / `enable_qq_to_mc` | 事件同步开关 |
| 各 `template_*` | 消息模板（QQ→MC 与游戏内回复支持 § 染色码） |

需要 AstrBot 已配置可用的 LLM 提供商（游戏内对话与指令解析都会用到）。

## Paper 插件构建与安装

构建（本机已验证）：
```
cd paper-plugin
build.cmd          # 需要 C:\jdk25\jdk-25.0.4.1+1 与 C:\gradle\gradle-9.1.0
```
产物：`paper-plugin/build/libs/netherlink-paper-1.0.0.jar`

安装：jar 放入服务器 `plugins/`，首次启动后编辑 `plugins/NetherLink/config.yml`：
```yaml
host: "AstrBot机器IP"
port: 8765
token: "与AstrBot侧一致"
server-name: "mc"
wake-prefixes: "ai,助手"   # 游戏内唤醒词，与 AstrBot 侧 mc_wake_prefixes 保持一致
```

## 升级须知（从 v2.x 升级，重要）

本次重构移除了代码级权限名单与绑定表，并**把好感度从 MC 计分板搬到本地文件**。

- **`karma_rules` 会自动迁移**：旧版默认规则（计分板时代）要求 AI「先用 mc_karma 判断
  并扣除好感度」，与新的 `cost` 机制冲突。插件启动时检测到该旧文案会**自动覆盖为当前
  默认规则**并在日志中告警。**如果你曾自定义过 `karma_rules`，请到 WebUI 重新确认**。
  原理：AstrBot 会把 schema 的 default 落盘进 `cmd_config.json`，已保存过配置的部署会
  一直返回落盘的那份，改 schema 追不回来。
- **好感度不会自动从计分板迁移**：本地 `karma.json` 从零开始（新玩家按 `karma_initial`
  建档）。旧计分板上的数值需要你自行搬运。
- **`bindings.json` 已废弃**：插件不再读取它。启动日志会提示，可自行删除。
- **`extra_system_prompt` 的默认值变了**（`""` → `你当前处于一个我的世界服务器内,
  服务器名称为{server}`），但**存量部署不会被自动迁移**——因为落盘的空串是你明确选择
  「关闭」的合法值，插件覆盖它比不覆盖更危险。所以：新装机器自动获得这句；**从 v2.x
  升级且从未设置过它的机器保持关闭**，想要就手动填。
- **`enable_webui_persona` 从本次起才真正生效**（此前因漏 `await` 一直空转）。若你配置过
  WebUI 人格，游戏内说话风格会发生变化。
- **`enable_karma` 开关已删除，好感度改为强制机制**：曾把它落盘为 `false` 的部署
  **失去了关闭好感度的能力**——该键现在被彻底无视（残留在 `cmd_config.json` 里无害，
  插件不读）。替代做法是**改提示词**：想让指令不消耗好感，把 `karma_rules` 的消耗表
  全部写成 0；想弱化工具文案，改 `karma_tool_desc`。同时，QQ 侧指令现在**永远**走
  内层 agent（不再有"关掉好感度就直接执行"的短路分支）。
- **⚠️ 升级前请到 WebUI 检查 `karma_rules` 与 `karma_tool_desc` 的现值**：这两个字段
  此前在 `enable_karma=false` 时不生效，所以关着好感度的部署没有理由维护它们——里面
  很可能还是当初为"关闭态"写的**占位文案**（如「好感度系统已关闭，不要考虑好感相关
  内容」）。这段文本**现在会立即生效**并被当作定价规则注入每一次 system_prompt，
  于是与必填的 `cost` 参数直接矛盾（这正是本次改造要消灭的自相矛盾），而且**不会**
  有任何告警。若发现占位文案，请把它改成真正的规则——想不限制消耗就写消耗表全 0。
- **`enable_death` 成为死亡惩罚唯一的门槛**（原先还需 `enable_karma`）。
- **已删除的配置项**（`normal_commands`、`strict_command_mode`、`bindings`、
  `template_cmd_done_game`、`enable_karma`）残留在 `cmd_config.json` 里无副作用，插件不读。

## 使用方式

- **QQ → 游戏**：绑定群里发普通消息，游戏公屏显示 `⌜群名⌟ <名字> 消息`
- **游戏 → QQ**：游戏内聊天/进服/退服/死亡自动推送
- **游戏内对话**：发 `ai 你好` 或 `ai 列出在线玩家`，机器人回复全网屏可见并同步到群
- **执行指令**（QQ 或游戏内均可）：
  - "现在几点了" → 机器人执行 `time` 并回复（纯查询，AI 报价 `cost=0`）
  - "把 Steve 改成创造模式" → AI 判断发起者是不是管理员，是则执行
    `gamemode creative Steve`，并按自己的报价扣除好感
  - 反馈去向：**游戏内发起** → 游戏里只看到机器人回复（`⌜ai⌟ : …` 格式），
    LLM 的回复本身就是最终反馈；**QQ 侧发起** → 由内层 agent 处理，其回复发在群里，
    QQ 群收到 LLM 组织的回复。v3.0 起**不再有单独的游戏内指令简报模板**

### 好感度系统

好感值存本地文件 `data/plugin_data/netherlink/karma.json`，并同步写回 WebUI 配置项
`karma_records`，管理员可直接在 WebUI 查看与修改。

**两个独立身份空间**：游戏内玩家用 `mc:游戏ID`，QQ 群友用 `qq:QQ号`。同一个人在两边
是**两份独立好感**，互相不影响。玩家**无法**在游戏里用 `/scoreboard` 查询——好感不在
计分板上。

工作方式：AI 每次对话先用 `mc_karma(delta=0)` 查好感，再据此决定说话方式；请求指令时
按 `karma_rules` 提示词决定消耗，通过 `mc_command` 的 `cost` 参数提交，插件原子扣减：

```
玩家: 妹，带我去附近的村庄
AI:  （mc_karma(delta=0) → 好感 10）
     （村庄传送按规则要 20 > 剩余 10 → 拒绝，不调 mc_command）
     呜…带着你跑那么远要消耗 20 点好感，你现在只有 10 点，人家心有余而力不足啦
玩家: 妹，给我三个铁锭
AI:  （mc_karma(delta=0) → 10；按规则报价 cost=3，付得起 →
      mc_command(cmd="give Steve iron_ingot 3", cost=3) → 插件原子扣 3）
     拿好啦～不过人家小小地记了一笔账哦
```

**扣费由插件保证**：`cost` 与实际扣减在同一次调用内完成。好感不足直接拒绝执行；
执行失败（服务器无回执）自动回滚，不会白扣（万一回滚本身也失败，会明确告知
「已扣的 N 点未退回」，不会静默吞掉）。**插件被卸载/重载导致这次执行被取消时同样退费**
（成功退费会在日志里留一条 warning），不会出现「扣了钱却查不到任何记录」的情形。

QQ 侧的实际消耗**永远**由**内层 agent** 决定：外层 agent 看不到好感度规则，它的
`cost` 参数只作为「初步报价」被带进内层提示词，内层可以采纳、调整或推翻——用户被告知
的价格与插件实际扣的价格因此出自同一次决策，不会各算各的。

**成就加成**：玩家获得成就时（不含配方解锁与根成就），插件把 `advancement_prompt`
（[玩家 id]、[服务器名]、[成就名] 已替换成实际值）发给 AI，由它自行决定加减多少好感
并回话，回复广播到游戏公屏并同步 QQ 群。这与死亡惩罚相反：死亡是代码扣减（AI 不在场），
成就是 AI 在场自行判断。可用 `enable_advancement` 关闭。

**死亡惩罚**：玩家死亡时插件主动扣 `karma_death_penalty`（默认 2），因为死亡不走对话，
AI 无法参与。它的门槛**只有一道**：`enable_death`。关掉它等于同时关闭死亡播报与死亡
扣减（刻意的耦合：不想看到死亡播报的人多半也不想要静默扣好感）。

**好感度是强制机制，没有开关**：`mc_karma` 工具、好感规则提示词、QQ 侧内层决策
**永远在场**，没有任何配置能关掉它们。想实现「不依赖好感度」或「不限制玩家触发指令」，
请改**提示词**——把 `karma_rules` 的消耗表全部写成 0（或让 AI 永远同意），必要时再
把 `karma_tool_desc` 改成中性文本。（起因：`mc_command` 的 `cost` 是必填参数，一旦
关掉好感度，AI 就无从得知该报多少价——工具接口在关闭态下不成立。）

**查询不写盘**：`mc_karma(delta=0)` 只读内存。只有真正增减时才写入文件与配置。

规则细节（各指令消耗多少、初始值 10、范围 -50~100、每次对话增减 ±2 等）都在
`karma_rules` 配置里，WebUI 直接改，不用动代码。

## 通信协议（JSON 行，每行一个对象）

```jsonc
// MC → AstrBot（上行）
{"type": "hello", "token": "...", "server_name": "survival"}
{"type": "chat",  "player": "Steve", "text": "大家好"}
{"type": "join",  "player": "Steve"}
{"type": "leave", "player": "Steve"}
{"type": "death", "player": "Steve", "message": "Steve 掉出了世界"}
{"type": "bot_chat", "player": "Steve", "text": "妹 你好"}   // 唤醒词开头，走 LLM
{"type": "heartbeat"}
{"type": "command_result", "id": "uuid", "ok": true, "output": "..."}

// AstrBot → MC（下行，line 为渲染完成的整行文本，可含 § 染色码）
{"type": "chat",      "line": "⌜§a水群§f⌟ <§b张三§f> §5你好§f"}
{"type": "bot_reply", "line": "⌜§d妹§f⌟ : §a你好呀§f"}
{"type": "command",   "id": "uuid", "cmd": "gamemode creative Steve"}
```

## 防循环设计

机器人自身消息只通过**账号识别**（OneBot `self_id == sender.user_id`）过滤，
不做任何文本匹配——群友复读机器人的消息不会被误伤。

## 开发

- 构建/环境约束/协议细节/插件开发要点见仓库根目录 `claude.md`
- AstrBot 插件开发文档: https://docs.astrbot.app/dev/star/plugin-new.html
