# MaiBot IIROSE 适配器

让 [麦麦（MaiBot）](https://github.com/Mai-with-u/MaiBot) 接入 **[IIROSE 蔷薇花园](https://iirose.com/)** 的适配器插件。

以插件形式运行在 MaiBot 内，通过 WebSocket 直连 IIROSE，把房间公屏消息、私聊消息、成员上下线事件交给麦麦，并把麦麦的回复（文本、@、引用、表情包）转成 IIROSE 原生报文发出去。

---

## 功能

| 分类 | 能力 |
|---|---|
| **入站** | 公屏消息、私聊消息、房间成员上线 / 下线 / 重连 / 换房 |
| **出站** | 文本、`@用户`、引用回复、图片、表情包（经图床转链） |
| **消息格式** | 自动转换 IIROSE 原生语法：`[*用户名*]` 提及、`(_hr) 发送者_时间戳 (hr_)` 引用、`[url#e]` 图片、点歌 / 点播卡片 |
| **图片 / 表情** | 对接自建图床：base64 → 上传换 URL → 以 `[url#e]` 发送；按内容指纹复用链接，链接失效自动重传 |
| **名单过滤** | 黑白名单（按 IIROSE 唯一标识 UID 或用户名）+ 永久屏蔽名单 |
| **连接稳定性** | 应用层保活、停滞检测、指数退避重连、永不放弃 |
| **可观测性** | 原始报文开关、名单拦截日志、图床 / 引用 / 卡片识别日志 |

---

## 环境要求

- MaiBot Host `1.2.0` ~ `1.2.99`
- `maibot-plugin-sdk` `2.5.0` ~ `2.99.99`
- Python 3.10+
- 插件运行环境（MaiBot Runner 子进程）需要能 `import websockets`

---

## 安装

```bash
cd /path/to/MaiBot/plugins
git clone <本仓库地址> MaiBot-IIROSE-Adapter
```

也可以在 WebUI 的「插件管理」里从插件市场安装，或直接把本目录整个复制到 `plugins/` 下。

> 目录名可以自定义，但 `plugin.py` / `_manifest.json` / `__init__.py` 必须同级放在一起。
> 安装完成后插件仍是**禁用**状态，需要在 WebUI 里手动启用。

---

## 配置

在 WebUI 的插件设置里填写，或直接编辑 `data/MaiBot/plugins/<插件目录>/config.toml`。

### `[plugin]` 插件设置

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 是否启用并连接 IIROSE |
| `config_version` | `"0.1.0"` | 配置结构版本号，**不要改** |

### `[account]` IIROSE 账号

| 字段 | 默认 | 说明 |
|---|---|---|
| `username` | `""` | 机器人用户名，不带 `[* *]` |
| `uid` | `""` | **13 位唯一标识**，强烈建议填写（见下方说明） |
| `password` | `""` | 账号密码（仅本地用于计算 MD5） |
| `room_id` | `""` | 初始房间 ID，例如 `16a7c38409e902` |
| `room_password` | `""` | 房间密码，一般留空 |

**UID 怎么拿**：登录 IIROSE 客户端 → 左侧菜单 → 个人资料 → 复制「唯一标识」。

填了 UID 之后三件事才稳：① 过滤掉机器人自己发的消息（否则会自问自答）；② 判断「有人在 @ 我」；③ 出站路由时 Host 能把回复派回本适配器。

### `[bot]` 机器人外观

| 字段 | 默认 | 说明 |
|---|---|---|
| `status` | `"n"` | 平台状态码：`n` 无状态 / `0` 会话中 / `1` 忙碌中 / `8` 睡觉中 … |
| `signature` | `"Bot of MaiBot"` | 个性签名 |
| `only_hang_up` | `false` | 静默模式：只接收不发送 |
| `report_member_events` | `true` | 把成员上线 / 下线 / 重连 / 换房作为「通知」上报给麦麦（日志始终记录） |
| `hello_on_login` | `false` | 登录成功后向房间发一条自测消息，**用来单独验证出站通道** |
| `hello_text` | `"【IIROSE 适配器】已上线~"` | 自测消息内容 |

### `[connection]` 连接设置

| 字段 | 默认 | 说明 |
|---|---|---|
| `keepalive` | `true` | 启用心跳保活 |
| `keepalive_interval_seconds` | `30.0` | 保活包间隔（秒）。官方 adapter 同款做法：定时发一个空串 |
| `stall_timeout_seconds` | `120.0` | 多久没收到任何数据就判定连接假死并重连（`0` = 关闭） |
| `timeout_ms` | `5000` | 连接 / 登录超时（毫秒） |
| `max_retries` | `5` | 连续失败达到该次数后**转为长间隔重试**（不会停止重连） |
| `reconnect_base_seconds` | `2.0` | 重连退避基准秒数 |
| `max_reconnect_seconds` | `300.0` | 重连退避上限 |
| `mc` | `"66ccff"` | 气泡颜色（6 位十六进制，透明度无效） |
| `debug_raw` | `false` | 打印原始报文，用于协议校准 / 排查 |

### `[chat_list]` 黑白名单

判定顺序：**永久屏蔽名单 → 房间名单 → 用户名单**（语义对齐官方 NapCat 适配器的 `filters.py`）。

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 关闭时所有人都能触发麦麦回复 |
| `user_list_type` | `"whitelist"` | `whitelist` 只回名单内的人 / `blacklist` 不回名单内的人 |
| `user_list` | `[]` | 用户名单，填 UID；也支持直接填用户名（按昵称兜底匹配） |
| `ban_user_list` | `[]` | 永久屏蔽：这些人无论什么模式都不回复，**优先级最高** |
| `room_list_type` | `"blacklist"` | 房间名单模式，一般不用（机器人只在自己所在的房间） |
| `room_list` | `[]` | 房间 ID 名单 |
| `log_dropped` | `true` | 记录被名单拦下的消息，方便排查「为什么不回复」 |

```toml
[chat_list]
enabled = true
user_list_type = "whitelist"
user_list = ["16a7c38409e902", "某个用户名"]   # UID 或用户名
ban_user_list = ["广告号"]
```

> ⚠️ 白名单模式下**名单为空 = 谁都不回**（与 NapCat 行为一致）。被拦下的消息**不会进入麦麦的上下文**，也不会计入频率统计。

### `[image_host]` 图床（发送图片 / 表情用，**必须自己配置**）

IIROSE 只能显示 URL 图片，而麦麦发来的图片 / 表情是 base64，因此需要一个图床来中转：先把图片 POST 上去，拿到返回的 URL 再以 `[url#e]` 发送。

**本插件不内置任何图床地址和 token**，需要自己准备一个支持以下接口的图床（例如 EasyImage 系）：

```
POST {base_url}{upload_path}
Content-Type: multipart/form-data
  {field_name}=<图片文件>   token=<上传 token>

成功响应：{"result":"success","code":200,"url":"https://.../xxx.webp"}
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 关闭后图片 / 表情会退化成 `[图片]` / `[表情]` 占位文本 |
| `base_url` | `""` | **图床地址，必填**，例如 `https://img.example.com` |
| `upload_path` | `"/api/index.php"` | 上传接口路径 |
| `token` | `""` | **上传 token，必填**（图床的 `tokenList` 里查） |
| `field_name` | `"image"` | multipart 表单里的文件字段名 |
| `reuse_uploaded` | `true` | 按内容指纹复用已上传链接，**重启后依然有效**，避免重复上传 |
| `verify_upload` | `true` | 校验链接真的能取到图片，取不到就重传 |
| `verify_interval_hours` | `24.0` | 复用时多久重新校验一次链接（`0` = 每次复用都校验） |
| `verify_timeout_ms` | `5000` | 链接校验超时 |
| `timeout_ms` | `15000` | 上传超时 |
| `max_bytes` | `8388608` | 单张图片大小上限（8 MB） |

```toml
[image_host]
enabled = true
base_url = "https://your-image-host.example.com"
token = "your-token"
```

> `base_url` 留空时不会发无效请求，只提示一次「图床未配置」，图文消息照常发出（图片位置显示为 `[图片]`）。

上传结果缓存在 `data/plugins/<插件id>/image_host_cache.json`：

```json
{
  "<图片字节的 sha256>": { "url": "https://.../a.webp", "checked": 1789216385.47 }
}
```

---

## 快速验证

### 1. 启动

```bash
docker restart maim-bot-core
docker logs maim-bot-core -f | grep -iE "iirose"
```

期望看到：

```
IIROSE 适配器已启动连接任务（保活=30s 停滞重连=120s 退避上限=300s 聊天名单：未启用（所有人都能触发回复））
IIROSE 选定服务器 wss://m8.iirose.com:8778 (xxx ms)
IIROSE 已连接 wss://m8.iirose.com:8778
IIROSE 登录报文已发送，等待服务端数据 …
IIROSE 登录成功（服务端首个报文类型=init）
IIROSE 网关状态上报: ready=True platform=iirose account_id=... result=True
```

### 2. 验证入站

让另一个号在房间里 `@机器人名 你好`，期望：

```
IIROSE 入站消息已交给 Host（raw_message 形态 [...]）: id=... user=... text=...
所见 [会话id]某人:@机器人名  你好
```

### 3. 验证出站（推荐先做这一步）

出站依赖「入站正文解析正确」和「麦麦决定回复」两个前提，容易互相甩锅。先用自测消息把出站单独验证掉：

```toml
[bot]
hello_on_login = true
```

重启后房间里出现「【IIROSE 适配器】已上线~」，就说明连接和发送通道都是好的。验证完记得关掉。

---

## 消息格式转换

适配器负责在 MaiBot 的组件格式和 IIROSE 原生语法之间双向转换：

| IIROSE 语法 | 含义 | 入站（交给麦麦） | 出站（由麦麦生成） |
|---|---|---|---|
| ` [*用户名*] ` | @ 用户 | `at` 组件（指向该用户 UID） | ` [*用户名*] ` |
| ` [_房间id_] ` | @ 房间 | 保留为普通文本（不当作 @ 人） | — |
| `旧内容 (_hr) 发送者_时间戳 (hr_) 新内容` | 引用回复 | `[引用 发送者：旧内容] 新内容` | 还原成原生引用语法 |
| `[url#e]` | 图片 | 还原成裸 URL | 图片 / 表情经图床转链后包成 `[url#e]` |
| `m__4<平台>><标题>><作者>><封面>><颜色>><码率>` | 点歌 / 点播卡片 | `[点歌·网易云] 《歌名》 - 歌手` | — |
| `'1` / `'3` / `'2<房间>` | 成员加入 / 离开 / 换房 | 作为「通知」上报（`is_notify`） | — |

细节见 [`docs/iirose-protocol-notes.md`](docs/iirose-protocol-notes.md)。

---

## 常见问题

| 现象 | 排查方向 |
|---|---|
| **日志有入站，但麦麦不回复** | ① 检查 `[chat_list]` 是否把该用户拦了（开 `log_dropped` 看日志）；② 检查麦麦自己的聊天名单 / 回复频率 / 是否被 `@`；③ 看有没有 `IIROSE 已发送出站消息` |
| **出站报「无法解析该出站消息」** | 日志会把 Host 下发的 `raw_message` 原文打出来，据此对齐字段 |
| **发出去但房间里没显示** | 打开 `[bot] hello_on_login` 单独验证发送通道；若自测消息也不出现，就是报文被服务端拒了 |
| **表情包显示成 `[表情]`** | 图床没配好。日志会写具体原因：`图床上传被拒绝: ... message=Token Error` 就是 token 不对，`未配置图床地址` 就是 `base_url` 没填 |
| **频繁掉线** | 先看是不是有 `IIROSE 连接断开(...)`。默认已开启 30 秒保活 + 120 秒停滞检测；网络差可调大 `stall_timeout_seconds` |
| **成员事件和聊天像是两个会话** | 说明配置的 `room_id` 与服务端下发的房间号不一致，日志里有 `IIROSE 成员事件里的房间=... 按配置房间 ... 上报` |
| **机器人自问自答** | `[account] uid` 没填，填上即可 |
| **一直连不上** | 检查容器到 `*.iirose.com:8778` 的网络；日志会打 `所有 IIROSE 服务器均不可达` |

---

## 目录结构

```
├── plugin.py                    # 插件主实现（单文件）
├── _manifest.json               # 插件清单
├── __init__.py                  # 导出 create_plugin
├── test_outbound_decode.py      # 回归测试（64 项，无需 pytest）
└── docs/
    └── iirose-protocol-notes.md # 协议要点与代码落点对照
```

---

## 开发与测试

```bash
python test_outbound_decode.py
```

测试用桩替换了 `maibot_sdk` 与网络层，**不联网**，一秒内跑完，覆盖：

- 出站组件解码（文本 / @ / 引用 / 图片 / 点歌卡片）、目标解析
- 入站报文解析（公屏、私聊、成员事件、引用链、点歌卡片）
- 图床上传、复用、链接校验与重传（含模拟插件重启）
- 黑白名单判定与拦截时机
- 连接保活、停滞检测、退避与「永不放弃重连」

---

## 已知限制

- 图片只能以 URL 形式发送，需要一个图床；本地 base64 无法直传 IIROSE。
- 语音 / 视频暂不支持（IIROSE 要求语音 URL 以 `.weba` 结尾）。
- 消息正文里出现 `>` 时，若服务端未转义，字段切分靠尾部字段形状还原；**歌曲标题本身含 `>`** 时无法与分隔符区分（官方解码器也有此限制）。
- 房间消息报文不带房间号，由登录态决定，因此配置的 `room_id` 必须与实际所在房间一致。
- 进房初始化大包里的**当前在线名单**尚未解析（只处理实时上下线事件）。

---

## 注意

- 不要把 `config.toml`、`data/` 或任何含账号密码、图床 token 的文件提交进仓库（`.gitignore` 已做基础排除）。
- 图床 token 属于个人凭据，请只写在本地配置里。

---

## 参考

- [IIROSE 逆向文档](https://lezhengan.github.io/iirose-re-docs/#/) — 协议字段、卡片格式、传输层
- [koishi adapter-iirose](https://github.com/iirose-plugins/adapter-iirose) — 官方 Koishi 适配器，报文与心跳实现
- [IIROSE 插件文档](https://iirose-plugins.github.io/iirose-plugins-docs/) — 平台与适配器使用文档
- [MaiBot](https://github.com/Mai-with-u/MaiBot) / [MaiBot-Napcat-Adapter](https://github.com/Mai-with-u/MaiBot-Napcat-Adapter) — 插件接口与消息网关实现参考

## License

[MIT](LICENSE)

