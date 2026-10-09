# NetherLink — 我的世界服务器 × QQ 群消息互通

**把 Minecraft 服务器和 QQ 群消息连接起来的 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 插件**

| 能力 | 说明 |
|---|---|
| 消息互通 | 游戏内聊天 / 进服 / 退服 / 死亡推到群里；群消息广播到游戏公屏，支持 `§` 染色码 |
| 自然语言执行指令 | 说「把 Steve 改成创造模式」，AI 判断身份与好感后以控制台身份执行，真实输出回传 |
| 游戏内机器人对话 | 玩家发唤醒词开头的话即可与 AI 对话，玩家原话与 AI 回复都同步到群 |
| 好感度系统 | AI 依对话与请求自主增减好感，好感影响它的态度与指令执行 |
| QQ ↔ 游戏账号绑定 | 未绑定玩家进服被拦下并收到验证码，在群里发码即完成绑定；绑定后两边共享一份好感 |
| 管理面板 | AstrBot WebUI 里的插件页面：在线服务器 / 好感度 / 绑定管理 / 配置诊断 / 在线玩家 / 指令审计 / 快捷指令 |

---

## 支持的版本

| 项 | 要求 |
|---|---|
| AstrBot | `>= 4.16, < 5` |
| Minecraft | **26.3** |
| MC 服务端 | 三选一：[netherlink-plugin](https://github.com/MoeDawn/netherlink-plugin)、[netherlink-fabric](https://github.com/MoeDawn/netherlink-fabric)、[netherlink-neoforge](https://github.com/MoeDawn/netherlink-neoforge) |

三个 MC 端协议完全相同，接同一个 AstrBot 插件，服务端不用改配置。MC 端按 Minecraft 26.3 的 API 编译，其他 MC 版本未适配，Spigot 不支持。

管理面板依赖 AstrBot 的插件页面能力。装了不带该能力的旧版 AstrBot 时面板会安静禁用并记一条 warning，其余功能不受影响。

---

## 效果示例

游戏公屏，模板可配，颜色由模板里的 `§` 码控制：

```text
⌜水群⌟ <张三> 大家好        ← 来自 QQ 群的消息
⌜ai⌟ : 嘻嘻嘻              ← 游戏内可见的机器人消息
```

QQ 群聊：

```text
⌜MC⌟ <steve>: 大家好       ← 来自 MC 游戏内的玩家消息
⌜MC⌟ ai: 嘻嘻嘻            ← 来自 MC 游戏内机器人的消息
```

指令执行，QQ 群里或游戏内都能用：

```text
群友：ai，把 Steve 改成创造模式
AI：  搞定，Steve 已经是创造模式了

群友：ai，清空我的成就进度
AI：  这个不行，清空了就能反复刷成就
```

---

## 安装

### 1. 在 AstrBot 上装插件

插件市场安装：AstrBot WebUI → 插件 → 插件市场 → 搜索「NetherLink」。

手动安装：把 `astrbot_plugin_netherlink.zip` 放进 AstrBot 的 `data/plugins/` 下解压，重启或热重载插件。

### 2. 在 MC 上装插件

按服务端类型选一个，安装步骤见各自仓库的 README：

- Paper / Purpur / Folia → [netherlink-plugin](https://github.com/MoeDawn/netherlink-plugin)
- Fabric → [netherlink-fabric](https://github.com/MoeDawn/netherlink-fabric)
- NeoForge → [netherlink-neoforge](https://github.com/MoeDawn/netherlink-neoforge)

### 3. 两边填配置

AstrBot 端与 MC 端都要配**端口**、**密钥**、**唤醒词**：

| 本插件 | MC 端 | 要求 |
|---|---|---|
| `ws_ports` | `port` | 端口必须一致 |
| `auth_token` | `token` | 字符串必须一模一样 |
| `mc_wake_prefixes` | `wake-prefixes` | 唤醒词列表必须一致 |
| `ws_ports` 里冒号前那段 | 仅供显示 | 显示名不参与身份，身份用 MC 端上报的 `server-name` |

⚠️ MC 端的配置文件要启动一次服务器后才会创建。

### 4. 要用 AI 对话功能还需要

- AstrBot 配好可用的 **LLM 提供商**，对话与指令解析都依赖它
- 想让游戏内对话用上人格，在 AstrBot 的「人格管理」里配好默认人格

---

## 使用方式

### QQ → 游戏

在绑定群里发消息，游戏公屏显示 `⌜群名⌟ <名字> 消息`。群友发**图片 / 表情 / 语音**这类非文字内容时，游戏里显示 `[图片]`、`[表情]` 这类占位符。

### 游戏 → QQ

游戏内的聊天 / 进服 / 退服 / 死亡自动推送，每类都有独立开关。

### 游戏内与 AI 对话

以唤醒词开头，如 `ai 你好`、`ai 列出在线玩家`，回复显示在游戏里并同步到群。

每台服务器 = 一个模拟群，会话按服务器建。在 AstrBot 的 `对话` 里能看到对应的模拟群聊，群名就是该服的显示名。

### 请 AI 执行指令

QQ 群或游戏内用自然语言描述需求即可，AI 自行判断三件事：**该不该执行**、**发起者是不是管理员**、**要多少好感**。高危请求默认拒绝，管理员尺度与价目表都写在配置项的提示词里。

AI 可以执行各种有趣或危险的指令，全权交由提示词判断。想限制它就改人格提示词或各处的系统上下文配置项。

### 不在绑定群名单里的群

消息不转发进游戏，游戏事件也不推过去。但身份注入与好感度对**所有 aiocqhttp 群**生效，未绑定群里的 AI 一样认得出发起者与管理员，`指令工具` / `好感度工具` 照常可用。要彻底断开某个群，用 AstrBot 限制插件生效范围，或把机器人移出那个群。

私聊与其他平台都不会被注入身份，例如 Telegram、网页聊天。

---

## 账号绑定

把游戏玩家与 QQ 群友认成同一个人，两边的好感合成一份。

### 怎么绑

1. 玩家进服。未绑定的被踢出，踢出界面上写着绑定群名与验证码；同一条提示还会私聊发给他
2. 群友在绑定群里单独把码发出来即完成绑定，成功播报到游戏公屏与 QQ 群

- 码默认 6 位、5 分钟内有效，过期重新进服拿新码，同一个玩家只保留最新那个
- 大小写、全角半角都认
- 只有正好那么多位的字母数字发言才当码，普通聊天照常转发

### 绑定后的变化

| | 未绑定 | 已绑定 |
|---|---|---|
| 好感键 | `mc:游戏ID` | `qq:QQ号` |
| 与这个 QQ 的好感 | 各自独立 | 同一份 |
| AI 看到的身份 | 当前发起者 | 多一句已绑定 QQ |

绑定不会把游戏内已攒的好感并过去。一次性迁移在管理面板。

### 开关

- `QQ 绑定功能总开关` 关掉则整块停用，不发码、不提示、不拦截
- `强制绑定` 默认开。关掉后未绑定玩家照常收提示与验证码、照常进服，只是不被踢
- `接受验证码的 QQ 群号` 留空则所有消息互通的群都能发码
- `忽略名单` 与 `门禁豁免的游戏 ID` 里的名字永远不拦

### 已知限制

| 场景 | 行为 |
|---|---|
| 玩家改名 | 视作新人，需重新绑定 |
| 同一个玩家绑到另一个 QQ | 好感留在旧 QQ，新 QQ 从初始值开始 |
| QQ 昵称变更 | 表里仍是绑定当时的昵称 |
| 抢绑 | 码只私聊发给本人。能看服务端日志的人仍可看到 |
| 一个 QQ 绑多个游戏 ID | 允许，AI 上下文会全部列出 |

---

## 好感度系统

好感值以 WebUI 配置项 `好感度记录(JSON)` 为准，可以直接查看和修改。`data/plugin_data/netherlink/karma.json` 是同步写出的镜像，插件启动时不读它。

⚠️ 清空那个配置项 = 记录归零。改动要**重启或热重载**后才生效，否则内存里仍是旧记录，下一次好感变化会把整个快照写回去。

### 身份空间

游戏内玩家用 `mc:游戏ID`，QQ 群友用 `qq:QQ号`。已绑定的玩家与其 QQ 号共享同一份好感；未绑定的玩家仍是一份独立好感。

### 谁在什么时候增减

| 来源 | 谁决定 | 说明 |
|---|---|---|
| 对话 | AI | 依 `karma_rules` 自主增减 |
| 执行指令 | AI 报价 + 插件扣 | 越 OP 的指令消耗越多，价目表在 `mc_command 的 cost 参数说明` |
| 死亡 | 插件自动 | 可在配置项改成 0 |
| 获得成就 | AI | 插件把 `成就提示词` 发给 AI，由它决定加多少并回话 |

报价与实际扣减在同一次调用内原子完成。好感不足直接拒绝，执行失败自动退费。

### 几点须知

- **好感度不可关闭**。想让指令不消耗好感，改提示词——把 `mc_command 的 cost 参数说明` 的价目表全写成免费或传 0。
- 管理员待遇由提示词决定。插件只把「谁是管理员」作为事实告诉 AI，尺度写在 `mc_command_tool_desc` 里。
- **提示词的现值直接决定 AI 行为**，插件不告警也不自动纠正。里面若写着占位文案或与当前机制冲突的说明，AI 就照它执行。留空会回退默认值。

---

## 管理面板

插件自带网页面板，在 AstrBot 的 WebUI 里打开本插件的插件页面。

### 能干什么

| 分区 | 说明 |
|---|---|
| 在线服务器 | 当前连进来的 MC 服务器与端口 |
| 好感度 | 记录列表，可搜索、改、删 |
| 绑定管理 | 改绑、解绑，以及一次性好感迁移 |
| 配置诊断 | 管理员名单 / 绑定群 / 端口绑定的解析结果 |
| 在线玩家 | 对每台在线服务器下发一次 `/list` |
| 指令审计 | 最近 1000 条，可按发起者 / 指令 / 结局筛选 |
| 快捷指令 | 由配置项驱动的按钮，点击下发一条 MC 指令 |

### 安全边界

- **面板**：改好感、解绑、下指令，点击立即生效
- **快捷指令**：不经 AI 直接向服务器后台发送指令
- 面板只暴露本地已有的数据：好感记录 / 绑定表 / 审计 / 快捷指令 / 配置解析结果

---

## 配置项

> 配置界面里已按模块分组，下面是同样的顺序。你的实际配置存在 `data/config/astrbot_plugin_netherlink_config.json`，插件目录里的 `_conf_schema.json` 只是默认值模板。提示词类配置项**留空都会回退默认值**——想「不注入」请删掉内容，别清空。

### 连接与服务器

| 配置项 | 说明 |
|---|---|
| `ws_host` | 本插件监听的地址，默认 `0.0.0.0`，MC 端主动连入 |
| `ws_ports` | **必填**。格式 `显示名:端口`，如 `生存服:8765`；也可只填端口号，那样显示为 `MC`。一台 MC 服占一个端口，单服只填一个。⚠️ 留空则不监听并报错；两台服的 `server-name` 必须不同，它默认是 `mc`，同名会互相顶掉 |
| `auth_token` | 握手鉴权密钥，两侧必须一致 |
| `server_display_names` | 已删除，显示名改到 `ws_ports` 里配 |
| `mc_bot_name` | 机器人游戏内名字，用于 `{bot}` 占位符，默认 `ai` |

### QQ 群互通

| 配置项 | 说明 |
|---|---|
| `qq_platform_id` | 用哪个机器人发群消息。留空自动选择；填了就必须是运行中且名称精确匹配的机器人，否则不推送并报错，报错里会列出可用的 |
| `group_names` | **绑定的 QQ 群**。**键**是绑定群号，决定消息互通范围；**值**是群名，用于 `{group}` 占位符。如 `123456:水群`；只填群号 `123456` 时群名回退成群号 |
| `enable_chat` | 同步游戏聊天 MC→QQ，默认开 |
| `enable_join_leave` | 同步进服 / 退服提示，默认开 |
| `enable_death` | 同步死亡消息，默认开。⚠️ 它同时门控死亡扣好感，是死亡惩罚唯一的门槛 |
| `enable_qq_to_mc` | QQ 群消息转发进游戏公屏，默认开 |
| `sync_bot_msgs` | 机器人自己的群消息是否也转发进游戏，默认关，防刷屏 |
| `mc_ignored_players` | **忽略名单**：这些玩家名产生的 MC 事件一律不处理、也不推群，用于排除机器人账号与小号。⚠️ 按名字**精确**匹配；装了假人 / AI 实体时必须填，否则它会自己和自己对话、白烧 token |

### 游戏内对话

| 配置项 | 说明 |
|---|---|
| `enable_game_platform` | 游戏内对话走 AstrBot **原生平台管线**，默认开。⚠️ **首次启用需要重启一次 AstrBot**，框架在启动时实例化平台 |
| `enable_webui_persona` | 游戏内对话是否使用 WebUI 配置的人格，默认开 |
| `mc_wake_prefixes` | 游戏内唤醒词，默认 `ai` `助手`。**须与 MC 端的 `wake-prefixes` 一致** |

### 账号绑定

| 配置项 | 说明 |
|---|---|
| `enable_binding` | QQ 绑定功能总开关，默认开 |
| `binding_group` | 接受验证码的 QQ 群号。留空则所有消息互通的群都能发码 |
| `binding_join_gate` | 强制绑定，默认开：未绑定玩家是否被踢出 |
| `binding_code_length` | 验证码位数，默认 6 |
| `binding_code_ttl` | 验证码有效期，秒，默认 300 |
| `binding_exempt_players` | 门禁豁免的游戏 ID，不区分大小写 |

### 管理员

| 配置项 | 说明 |
|---|---|
| `admin_qq` | 管理员 QQ 号，用于 QQ 侧的身份判定 |
| `admin_mc` | 管理员游戏 ID。**游戏内管理员只能在这里填**，AstrBot 的全局管理员是 QQ 号，对游戏内无效 |

> 名单只作为参考信息注入提示词，插件不做任何拦截，是否放行由 AI 决定；给管理员的宽松尺度写在 `mc_command_tool_desc` 里。

### 管理面板

| 配置项 | 说明 |
|---|---|
| `quick_commands` | 面板上的快捷指令按钮。每项写 `名称\|指令\|服务器`，第三段可省略。竖线是分隔符，指令里不能含它；一条列表项就是一条完整指令。这些指令不经 AI 判断、也不消耗好感度 |

### 好感度

| 配置项 | 说明 |
|---|---|
| `karma_rules` | 好感度规则：范围、对话如何影响态度、每次不超过 ±2、要隐晦暗示、不透露具体数值。默认值里的数字用 `{karma_min}` / `{karma_max}` / `{karma_initial}` 占位符，会自动跟随下面几项 |
| `karma_min` / `karma_max` | 好感值下限 / 上限，默认 -50 / 100。⚠️ 上下限写反了会回退默认并记 warning |
| `karma_initial` | 新玩家初始好感，默认 20，会被夹进上面的范围 |
| `max_command_cost` | 单次指令好感消耗上限，默认 80 |
| `karma_death_penalty` | 玩家死亡扣的好感，默认 2，0 为不扣 |
| `log_karma_changes` | 对话引起的好感变化是否记日志，默认开 |
| `log_command_audit` | 指令审计，默认开。每次 AI 执行指令都会留下结构化记录：来源侧、发起者、目标服务器、指令原文、报价与实际消耗、结局。关掉则日志与记录都不写 |
| `enable_advancement` | 玩家获得成就时是否通知 AI，默认开 |
| `advancement_prompt` | 成就通知用的提示词，默认含好感量级 2~10，按成就难度 |
| `karma_records` | 好感度记录，格式 `{"qq:12345": 12}`，可直接在 WebUI 改 |

### 工具提示词

| 配置项 | 说明 |
|---|---|
| `mc_command_tool_desc` | `mc_command` 的工具描述。**决定 AI 该拒绝哪些请求**，例如清空成就进度、召末影龙、破坏他人建筑，以及给管理员多宽松的尺度 |
| `mc_command_cost_param_desc` | `mc_command` 的 **cost 参数说明**。**指令的好感价目表就在这里**：免费放行 / 趣味指令 / 按超标程度计价三类加具体价格 |
| `mc_command_cmd_param_desc` | `mc_command` 的 cmd 参数说明，游戏侧与 QQ 侧共用 |
| `mc_command_server_param_desc` | `mc_command` 的 server 参数说明，仅 QQ 侧有该参数 |
| `karma_tool_desc` | `mc_karma` 的工具描述 |
| `mc_karma_delta_param_desc` | `mc_karma` 的 delta 参数说明 |

### AI 上下文

| 配置项 | 说明 |
|---|---|
| `template_netherlink_context_qq` | **QQ 侧**注入给 AI 的身份模板，随本轮消息下发。占位符 `{identity}` `{is_admin}` `{binding}` |
| `template_netherlink_context_game` | **游戏侧**注入给 AI 的身份模板，随本轮消息下发。占位符多一个 `{server}`，默认值含「物品换好感的三步流程」 |

> `{binding}` 在已绑定时追加一句「已绑定…」，QQ 侧会把该 QQ 绑的全部游戏 ID 用顿号列出；未绑定则是空串。它**不是**必需占位符，缺了不回退默认值。

> 两者**空串都回退默认值**——想「不注入」请删掉模板正文，而不是清空配置项。

### 消息模板

| 配置项 | 默认值 | 场景 |
|---|---|---|
| `template_chat` | `⌜{server}⌟ <{player}>: {text}` | MC 聊天 → QQ 群 |
| `template_join` / `template_leave` | `[{server}] {player} 进入了/离开了服务器` | 进服 / 退服 → QQ 群 |
| `template_death` | `⌜{server}⌟ {message}` | 死亡 → QQ 群 |
| `template_qq_to_mc` | `§a⌜{group}⌟§f <§b{sender}§f> §d{text}§f` | QQ → 游戏公屏，支持 `§` 码 |
| `template_bot_reply_game` | `§c⌜{bot}⌟§f : §d{text}§f` | 机器人游戏内回复格式 |
| `template_bind_hint` | `§e请到 QQ 群 [{group}] 发送验证码 §b{code}§e 完成绑定({ttl}内有效)` | 进服提示，**也是踢出原因**；占位符 `{group}` `{code}` `{player}` `{server}` `{ttl}`，有效期默认渲染成 `5 分钟` |
| `template_bind_success_game` | `玩家 {player} 已完成 QQ 绑定` | 绑定成功 → 游戏内广播 |
| `template_bind_success_qq` | `{qq_name} ({qq}) 已绑定游戏账号 {player}` | 绑定成功 → 群里回话 |

---

## 目录结构

```text
astrbot_plugin_netherlink/
├── main.py              # 插件主逻辑
├── karma.py             # 好感度算法与本地存储
├── bindings.py          # QQ ↔ 游戏账号绑定表（读写与原子落盘）
├── binding_flow.py      # 绑定流程的纯逻辑（门禁判据 / 验证码 / 迁移规划）
├── audit.py             # 指令审计结构化落盘（JSONL）
├── panel.py             # 管理面板的数据整形层（脱离 AstrBot 可单测）
├── store.py             # 原子 JSON 读写
├── mc_platform.py       # 注册 MC 模拟平台（游戏内对话走原生管线）
├── pages/               # 管理面板前端（AstrBot Plugin Pages）
│   └── admin/           #   页面名 = 目录名；入口 index.html
│       ├── index.html
│       ├── app.js
│       └── style.css
├── metadata.yaml        # 插件元数据
├── _conf_schema.json    # WebUI 配置项定义
├── requirements.txt     # 依赖
├── CHANGELOG.md         # 更新日志（插件市场详情页显示它）
├── logo.png
├── LICENSE
└── README.md
```

---

## 许可

[MIT License](LICENSE)
