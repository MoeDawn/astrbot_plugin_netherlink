# NetherLink — 我的世界服务器 × QQ 群消息互通

一个 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 插件，把 Minecraft 服务器和 QQ 群双向打通。

| 能力 | 说明 |
|---|---|
| **消息互通** | 游戏内聊天 / 进服 / 退服 / 死亡推到群里；群消息广播到游戏公屏（支持 `§` 染色码） |
| **自然语言执行指令** | 说「把 Steve 改成创造模式」，AI 判断身份与好感后以控制台身份执行，服务器真实输出回传 |
| **游戏内机器人对话** | 玩家发唤醒词开头的话即可与 AI 对话，玩家原话与 AI 回复都同步到群 |
| **好感度系统** | AI 依对话与请求自主增减好感；好感影响它的态度与是否愿意办事 |

---

## 支持的版本

| 项 | 要求 |
|---|---|
| **AstrBot** | `>= 4.16, < 5` |
| **Minecraft** | **26.3**（换版本需重新编译 MC 端） |
| **MC 端** | 二选一：[netherlink-plugin-server](https://github.com/MoeDawn/netherlink-plugin-server)（Paper / Purpur / Folia）<br>或 [netherlink-fabric](https://github.com/MoeDawn/netherlink-fabric)（Fabric 模组） |

> 两个 MC 端**协议完全相同**，接同一个 AstrBot 插件，服务端不用改配置。

**MC 端支持的服务端核心：**

| 核心 | 状态 | 说明 |
|---|---|---|
| **Paper 26.3** | ✅ 已验证 | 当前实机运行的就是它 |
| **Purpur 26.3** | ✅ 可运行 | Paper 的分支，API 与事件完全一致 |
| **Fabric 26.3** | ✅ 可用 | 见 [netherlink-fabric](https://github.com/MoeDawn/netherlink-fabric)（独立模组） |
| **Folia** | ⚠️ 已适配，未实机验证 | 已改用 Paper/Folia 共用的调度器并声明 `folia-supported`，但没跑过真 Folia |
| Spigot | ❌ 不支持 | 缺 `Bukkit.createCommandSender`（Paper 专有扩展，用于捕获指令输出） |
| NeoForge | ❌ 不支持 | 尚未移植 |

---

## 效果示例

游戏公屏（颜色由模板里的 `§` 码控制，均可配置）：

```text
⌜水群⌟ <张三> 晚上八点开打末影龙          ← QQ 群消息（群名绿、群友蓝、内容紫）
⌜ai⌟ : 收到，记得准备好附魔装备哦～        ← 机器人回复
⌜MC⌟ <Steve>: 大家好                      ← 游戏聊天推送到群
```

同一句机器人的回复在 QQ 群里显示为 `⌜MC⌟ ai: 收到，记得准备好附魔装备哦～`。

**指令执行**（QQ 群里或游戏内都可以说）：

```text
群友: ai，现在几点了
AI:   （执行 time，纯查询不消耗好感）现在是游戏时间第 3 天 14:20

群友: ai，把 Steve 改成创造模式
AI:   （查好感 → 判断付得起 → 执行并扣好感）搞定，Steve 已经是创造模式了

群友: ai，清空我的成就进度
AI:   这个不行哦，清空了就能反复刷成就，人家不能帮你做这个
```

---

## 安装

### 1. 装 AstrBot 插件

**推荐：插件市场** —— AstrBot WebUI → 插件 → 插件市场 → 搜索「NetherLink」安装。

**手动安装** —— 把 `astrbot_plugin_netherlink.zip` 放进 AstrBot 的 `data/plugins/` 下解压，重启或热重载插件。

### 2. 装 MC 端（二选一）

- **Paper / Purpur / Folia** → [netherlink-plugin-server](https://github.com/MoeDawn/netherlink-plugin-server)
- **Fabric** → [netherlink-fabric](https://github.com/MoeDawn/netherlink-fabric)

各自的安装步骤见对方仓库的 README。

### 3. 两边填对上

装完必须让两侧的**端口**、**密钥**、**唤醒词**对上，否则连不上：

| 本插件（AstrBot 侧） | MC 端 | 要求 |
|---|---|---|
| `ws_ports` | `port` | 端口必须一致 |
| `auth_token` | `token` | 字符串必须一模一样 |
| `mc_wake_prefixes` | `wake-prefixes` | 唤醒词列表必须一致 |
| `server_display_names` 里的 key | `server-name` | 名称要对得上（也可以只填端口，让 MC 上报什么就用什么）|

> ⚠️ **`ws_ports` 留空 = 不监听任何端口**（启动时会明确报错）。这是刻意的失败方式——
> 宁可响亮地不启动，也不要静默用一个陈旧的默认端口。

### 4. 还需要

- AstrBot 已配好可用的 **LLM 提供商**（对话与指令解析都依赖它）
- 想让游戏内对话用上人格，在 AstrBot 的「人格管理」里配好默认人格

---

## 使用方式

### QQ → 游戏

在**绑定群**里发消息，游戏公屏会显示 `⌜群名⌟ <名字> 消息`。

### 游戏 → QQ

游戏内的聊天 / 进服 / 退服 / 死亡自动推送（每类都有独立开关）。

### 游戏内与 AI 对话

发 `ai 你好` 或 `ai 列出在线玩家`：

- 回复显示在游戏里，同时同步到群
- **玩家原话也会按聊天模板推群**，所以群里看得到你问了什么
- 走 AstrBot **原生平台管线**，因此人格、记忆、工具调用提示都与 QQ 侧一致

> 💡 想**在游戏里看到 AI 调用工具与思考过程**？到 AstrBot 的「提供商设置」里打开
> `show_tool_use_status` / `show_tool_call_result` / `display_reasoning_text`
> ——这三个默认都是关的，不开的话游戏里只看得到最终回复。

### 请 AI 执行指令

QQ 群里或游戏内都可以，说人话就行。AI 会自己判断：**该不该执行**（高危请求直接拒绝）、
**要多少好感**（按价目表报价）、**发起者是不是管理员**（管理员尺度更宽松）。

### ⚠️ `target_groups` 只管消息互通

不在名单里的群：**消息不转发进游戏，游戏事件也不推过去**。

但**身份注入与好感度对所有 aiocqhttp 群生效**——未绑定群里的 AI 一样认得出发起者与管理员，
`mc_command` / `mc_karma` 也照常可用。要彻底断开某个群，请把机器人移出那个群，
或用 AstrBot 的 `plugin_set` 限制插件生效范围。

**私聊与其他平台**（Telegram、网页聊天等）**不会被注入身份**——这是刻意的，避免污染
其他插件与其他平台的提示词。

---

## 好感度系统

好感值存在本地 `data/plugin_data/netherlink/karma.json`，并同步进 WebUI 配置项
`karma_records`，可以直接查看和修改。

### 两个独立身份空间

游戏内玩家用 `mc:游戏ID`，QQ 群友用 `qq:QQ号`。**同一个人在两边是两份独立好感**，互不影响。

### 谁在什么时候增减

| 来源 | 谁决定 | 说明 |
|---|---|---|
| 对话 | AI | 依 `karma_rules` 自主增减，每次不超过 ±2 |
| 执行指令 | AI 报价 + 插件扣 | 越 OP 的指令消耗越多，价目表在 `mc_command_cost_param_desc` |
| 死亡 | 插件自动 | 扣 `karma_death_penalty`（死亡不走对话，AI 不在场） |
| 获得成就 | AI | 插件把 `advancement_prompt` 发给 AI，由它决定加多少并回话 |

**扣费由插件保证**：报价与实际扣减在同一次调用内原子完成。好感不足直接拒绝；
执行失败（服务器回报失败或超时无回执）自动退费，不会白扣。

### 几点须知

- **好感度不可关闭**。想让指令不消耗好感，请改**提示词**——把
  `mc_command_cost_param_desc` 的价目表全部写成免费/传 0。改 `karma_rules` 已经不管用，
  指令尺度不在那一项里。
- **管理员待遇也由提示词决定**：插件只把「谁是管理员」作为事实告诉 AI，并不规定该怎么对待他。
  给管理员更宽松的尺度写在 **`mc_command_tool_desc`**（工具描述）里，想收紧或放宽改那一项即可。
- ⚠️ **提示词的现值直接决定 AI 行为**：几个提示词配置项**每次都注入**，
  且插件**不会告警、也不会自动纠正**——里面若写着占位文案（如「好感度系统已关闭」）
  或与当前机制冲突的说明，AI 就照它执行。请到 WebUI 确认它们是真规则（**留空会回退默认值**）。
- **查询不写盘**：AI 每次对话都会查好感，这类查询只读内存，只有真正增减时才写入文件。

---

## 配置项

> 配置界面里已按模块分组，下面是同样的顺序。
> 提示词类配置项**留空都会回退默认值**——想「不注入」请删掉内容，别清空。

### 连接与服务器

| 配置项 | 说明 |
|---|---|
| `ws_host` | 本插件监听的地址（默认 `0.0.0.0`），MC 端主动连入 |
| `ws_ports` | **必填**。一台 MC 服占一个端口，如 `survival:8765`；单服只填一个端口即可。⚠️ 留空则不监听（启动报错）；两台服连同一端口会互相踢下线 |
| `auth_token` | 握手鉴权密钥，两侧必须一致 |
| `server_display_names` | 按服务器区分显示名，如 `survival:生存服`。用于 `{server}` 占位符、群消息前缀、AI 上下文；留空则显示为 `MC` |
| `mc_bot_name` | 机器人游戏内名字，用于 `{bot}` 占位符（默认 `ai`） |

### QQ 群互通

| 配置项 | 说明 |
|---|---|
| `target_groups` | 绑定的 QQ 群号。**只管消息互通**，不影响身份注入与好感度（见上文） |
| `group_names` | 群号与群名映射，如 `123456:水群`，用于 `{group}` 占位符；没配的群用群号代替 |
| `enable_chat` | 同步游戏聊天 MC→QQ（默认开） |
| `enable_join_leave` | 同步进服 / 退服提示（默认开） |
| `enable_death` | 同步死亡消息（默认开）。⚠️ 它**同时**门控死亡扣好感——是死亡惩罚**唯一**的门槛 |
| `enable_qq_to_mc` | QQ 群消息转发进游戏公屏（默认开） |
| `sync_bot_msgs` | 机器人自己的群消息是否也转发进游戏（默认关，防刷屏） |

### 游戏内对话

| 配置项 | 说明 |
|---|---|
| `enable_game_platform` | 游戏内对话走 AstrBot **原生平台管线**（默认开）。⚠️ **首次启用需要重启一次 AstrBot**——框架在启动时实例化平台 |
| `enable_webui_persona` | 游戏内对话是否使用 WebUI 配置的人格（默认开） |
| `mc_wake_prefixes` | 游戏内唤醒词（默认 `ai` `助手`）。**须与 MC 端的 `wake-prefixes` 一致** |

### 管理员

| 配置项 | 说明 |
|---|---|
| `admin_qq` | 管理员 QQ 号。用于 QQ 侧的身份判定 |
| `admin_mc` | 管理员游戏 ID。**游戏内管理员只能在这里填**——AstrBot 的全局管理员是 QQ 号，对游戏内无效 |

> 名单只作为**参考信息**注入提示词，插件不做任何拦截——是否放行由 AI 决定。
> 而且插件只告诉 AI「谁是管理员」，**从不说该怎么对待他**；给管理员的宽松尺度
> 写在 `mc_command_tool_desc` 里。

### 好感度

| 配置项 | 说明 |
|---|---|
| `karma_rules` | 好感度规则：范围、对话如何影响态度、每次不超过 ±2、要隐晦暗示、不透露具体数值。默认值里的数字用 `{karma_min}` / `{karma_max}` / `{karma_initial}` 占位符，**会自动跟随下面几项的配置** |
| `karma_min` / `karma_max` | 好感值下限 / 上限（默认 -50 / 100）。⚠️ 上下限写反了会回退默认并记 warning |
| `karma_initial` | 新玩家初始好感（默认 20），会被夹进上面的范围 |
| `max_command_cost` | 单次指令好感消耗上限（默认 80，防止 AI 报价失控） |
| `karma_death_penalty` | 玩家死亡扣的好感（默认 2，0 为不扣） |
| `log_karma_changes` | 对话引起的好感变化是否记日志（默认开） |
| `enable_advancement` | 玩家获得成就时是否通知 AI（默认开） |
| `advancement_prompt` | 成就通知用的提示词（默认含好感量级 2~10，按成就难度） |
| `karma_records` | 好感度记录，格式 `{"qq:12345": 12}`，可直接在 WebUI 改 |

### 工具提示词

| 配置项 | 说明 |
|---|---|
| `mc_command_tool_desc` | `mc_command` 的工具描述。**决定 AI 该拒绝哪些请求**（清空成就进度、召末影龙、破坏他人建筑…），以及给管理员多宽松的尺度 |
| `mc_command_cost_param_desc` | `mc_command` 的 **cost 参数说明**。**指令的好感价目表就在这里**（免费放行 / 可以执行 / 按超标程度计价三类 + 具体价格） |
| `mc_command_cmd_param_desc` | `mc_command` 的 cmd 参数说明（游戏侧与 QQ 侧共用） |
| `mc_command_server_param_desc` | `mc_command` 的 server 参数说明（仅 QQ 侧有该参数） |
| `karma_tool_desc` | `mc_karma` 的工具描述 |
| `mc_karma_delta_param_desc` | `mc_karma` 的 delta 参数说明 |

### AI 上下文

| 配置项 | 说明 |
|---|---|
| `template_netherlink_context_qq` | **QQ 侧**注入给 AI 的系统上下文模板。占位符 `{identity}` `{is_admin}` |
| `template_netherlink_context_game` | **游戏侧**注入给 AI 的系统上下文模板。占位符多一个 `{server}`；默认值含「物品换好感的三步流程」 |

> 两者**空串都回退默认值**——想「不注入」请删掉模板正文，而不是清空配置项。

### 消息模板

| 配置项 | 默认值 | 场景 |
|---|---|---|
| `template_chat` | `⌜{server}⌟ <{player}>: {text}` | MC 聊天 → QQ 群 |
| `template_join` / `template_leave` | `[{server}] {player} 进入了/离开了服务器` | 进服 / 退服 → QQ 群 |
| `template_death` | `⌜{server}⌟ {message}` | 死亡 → QQ 群 |
| `template_qq_to_mc` | `§a⌜{group}⌟§f <§b{sender}§f> §d{text}§f` | QQ → 游戏公屏（支持 `§` 码） |
| `template_bot_reply_game` | `§c⌜{bot}⌟§f : §d{text}§f` | 机器人游戏内回复格式 |

---

## 许可

[MIT License](LICENSE)

---

## 目录结构

```text
astrbot_plugin_netherlink/
├── main.py              # 插件主逻辑
├── karma.py             # 好感度算法与本地存储
├── store.py             # 原子 JSON 读写
├── mc_platform.py       # 注册 MC 模拟平台（游戏内对话走原生管线）
├── metadata.yaml        # 插件元数据
├── _conf_schema.json    # WebUI 配置项定义
├── requirements.txt     # 依赖
├── CHANGELOG.md         # 更新日志（插件市场详情页显示它）
├── logo.png
├── LICENSE
└── README.md
```
