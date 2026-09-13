"""IIROSE（蔷薇花园）MaiBot 适配器插件 —— 单文件版。

特性：
1. WebSocket 直连 IIROSE（多端点选服 + gzip 帧 + 心跳 + 断线重连）；
2. 入站：房间/私聊消息 → Host 标准结构（ctx.gateway.route_message）；
3. 出站：Host MessageDict → IIROSE 报文（@MessageGateway duplex）；
4. 网关状态上报（ctx.gateway.update_state），登录成功由“收到服务端首个报文”判定。
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import logging
import random
import re
import time
import urllib.error
import urllib.request
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Dict, List, Mapping, Optional

from maibot_sdk import Field, MaiBotPlugin, MessageGateway, PluginConfigBase

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

GATEWAY_NAME = "iirose_gateway"
PLATFORM = "iirose"
LOGIN_WARN_SECONDS = 60.0
GZIP_FLAG = b"\x01"
GZIP_THRESHOLD = 256
WS_PORT = 8778
DEFAULT_MC = "66ccff"          # 官方 adapter 默认气泡色（6 位 hex）
RECENT_MESSAGES = 200          # 近期消息缓存条数，用于回复时还原引用
UPLOAD_CACHE_SIZE = 2000       # 图床结果缓存条数（按内容指纹去重，重启后仍有效）
UPLOAD_CACHE_FILE = "image_host_cache.json"
STABLE_CONNECTION_SECONDS = 30.0   # 连接稳定超过这么久才重置重连退避
MOVE_ROOM_SETTLE_SECONDS = 1.5     # 切房指令发出后等服务端处理的间隔


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """指数退避 + 抖动：base、2×base、4×base… 直到 cap。

    指数必须先钳住再算：长时间断网时 attempt 会涨到几百，
    直接 `base * 2 ** (attempt-1)` 会抛 OverflowError 把重连循环打挂。
    """
    base = max(0.05, float(base))
    cap = max(base, float(cap))
    step = max(1, int(attempt))

    if base >= cap:
        delay = cap
    else:
        # 翻倍到上限需要几步（循环次数有上限，不会退化）
        max_shift = 0
        while max_shift < 64 and base * (2.0 ** max_shift) < cap:
            max_shift += 1
        delay = min(base * (2.0 ** min(step - 1, max_shift)), cap)

    return min(delay + random.uniform(0.0, min(2.0, delay * 0.1)), cap + 2.0)
DEFAULT_IMAGE_HOST = ""            # 留空：必须在自己配置里填 image_host.base_url
DEFAULT_IMAGE_HOST_PATH = "/api/index.php"
# 图床上传 token 不内置任何私有值，请在插件配置里填 image_host.token。
DEFAULT_IMAGE_HOST_TOKEN = ""
SERVER_URLS = tuple(f"wss://{p}.iirose.com:{WS_PORT}" for p in ("m1", "m2", "m8", "m9", "m"))

LOGIN_ERRORS = {
    "0": "名字被占用（游客登录）",
    "1": "用户名不存在",
    "2": "密码错误",
    "4": "今日登录次数过多（IP 限制）",
    "5": "房间密码错误",
    "x": "账号已被封禁",
    "n0": "房间无法进入（房间已满/仅白名单）",
    "6": "房间不存在或无法进入",
}


# ---------------- 协议层：帧 / 登录 / 报文 ----------------

def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def encode_frame(text: str) -> bytes:
    raw = text.encode("utf-8")
    return GZIP_FLAG + gzip.compress(raw) if len(raw) > GZIP_THRESHOLD else raw


def decode_frame(data: bytes | str) -> str:
    if isinstance(data, str):
        return data
    if data.startswith(GZIP_FLAG):
        return gzip.decompress(data[1:]).decode("utf-8")
    return data.decode("utf-8")


def build_login(room_id: str, username: str, password: str,
                room_password: str | None = None, status: str = "n",
                signature: str = "", last_room_id: str = "") -> str:
    payload: Dict[str, str] = {
        "r": room_id,
        "n": username,
        "p": md5(password),
        "st": status,
        "mo": signature,
        "mb": "",
        "mu": "01",
        "vc": "1142",
        "fp": "@" + md5(username),
    }
    if room_password:
        payload["rp"] = room_password
    if last_room_id:
        # 切房后重连认证要带上原房间 id（协议里叫 lr）
        payload["lr"] = last_room_id
    return "*" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def next_client_message_id() -> str:
    """消息 id。

    协议要求 12 位随机数字（官方 adapter 用 `Math.random().toString().substring(2, 14)`），
    服务端会把它作为消息 id 回显（公屏字段 10），用于引用 / 撤回。
    消息 id 只用于「回复时引用」和「撤回」，见 `_split_quote`。
    """
    return f"{random.randrange(10 ** 12):012d}"


def encode_room_message(text: str, mc: str = DEFAULT_MC) -> str:
    return json.dumps({"m": text, "mc": mc, "i": next_client_message_id()},
                      ensure_ascii=False, separators=(",", ":"))


def encode_private_message(target_uid: str, text: str, mc: str = DEFAULT_MC) -> str:
    return json.dumps({"g": target_uid, "m": text, "mc": mc, "i": next_client_message_id()},
                      ensure_ascii=False, separators=(",", ":"))


# ---------------- 消息内容语法（逆向文档「正文里的特殊语法」） ----------------
#
#   @用户     ` [*用户名*] `（**两侧空格是语法的一部分**，官方正则 `(\s+)(\[\*…\*\])(\s)`）
#   @频道     ` [_{频道id}_] `
#   引用      `旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`（标记两侧同样带空格）
#   图片      `[url#e]`；文件 `[url]`；语音 URL 需以 .weba 结尾；链接 `\url`
#
# 引用标记里是**被引用消息的时间戳（秒）**，不是消息 id。客户端拿到这个数字会
# 按日期渲染成「xxxx年x月x日」，喂 12 位随机消息 id 就会显示成 6088 年这种鬼日期。
# 官方 `PublicMessage.ts` 把它存进名为 `time` 的字段，third-party.md 也写作
# 「发送者_时间戳秒」。（`messages.md` 散文里那句「发送者_消息id」是文档误记。）
# `[@uid@]` 是官方的 at-by-id 写法，客户端会按 UID 反查名字。

MENTION_RE = re.compile(r"\[\*([\s\S]+?)\*\]|\[_([A-Za-z0-9_]+)_\]|\[@([A-Za-z0-9_]+)@\]")
QUOTE_RE = re.compile(r"^(?P<quoted>.*?)\s*\(_hr\)\s*(?P<who>.*?)\s*\(hr_\)\s*(?P<tail>.*)$", re.S)
QUOTE_AUTHOR_RE = re.compile(r"^(?P<who>.*)_(?P<ref>\d+)$")
# 引用时间戳的合理区间（2000-01-01 ~ 2100-01-01），越界说明拿到的根本不是时间戳
QUOTE_TS_MIN = 946_684_800
QUOTE_TS_MAX = 4_102_444_800

IMAGE_CONTENT_RE = re.compile(r"^\[?((?:https?://)[^\s\]]+?)(?:#e)?\]?$")
# 收发两端都用的图片标记：`[url#e]` 是图片，`[url]` 是文件
IMAGE_MARK_RE = re.compile(r"\[(https?://[^\s\]]+?)#e\]")
# 房间号：13 位 hex（双房用 `_` 连接两个，例如 5b792cb650749_6a4a56d0b4ca7）
ROOM_ID_RE = re.compile(r"^(?=.*[a-f])[a-f0-9]{10,}(?:_[a-f0-9]{10,})*$")


def strip_wrapper(value: Any, mark: str) -> str:
    """剥离 IIROSE 界面上的包裹语法。

    `mark` 是括号里的符号：`*` → `[*名字*]`、`@` → `[@uid@]`、`_` → `[_房间id_]`。
    官方 adapter 的配置说明就是这么处理的（「可填写 `[*用户名*]`，适配器会自动剥离」），
    用户从 IIROSE 界面复制粘贴时很容易带着这层括号，不剥离就会导致
    「机器人认不出自己被 @」这类问题。
    """
    text = str(value or "").strip()
    left, right = f"[{mark}", f"{mark}]"
    if len(text) > len(left) + len(right) and text.startswith(left) and text.endswith(right):
        text = text[len(left):-len(right)].strip()
    return text


def normalize_username(value: Any) -> str:
    return strip_wrapper(value, "*")


def normalize_uid(value: Any) -> str:
    # 文档：UID 可填 `[@uid@]` 或大写形式，适配器会转小写
    return strip_wrapper(value, "@").lower()


def normalize_room_id(value: Any) -> str:
    return strip_wrapper(value, "_")

# ---------------- 点歌 / 点播卡片 ----------------
#
# 房间里点歌后，聊天记录里会出现一张「卡片」，正文格式（官方 encoder/messages/media_card.ts）：
#
#   m__4<平台标识>><标题>><歌手或作者>><封面URL>><颜色>><码率>
#   视频卡还会多带时长：...>><码率>>11451<时长文本>
#
# 标题/歌手用 HTML 实体转义（utils/entities.ts），所以 `>` 在内容里一定是 `&gt;`，
# 按 `>` 拆分是安全的。官方 adapter 只认 `m__4@...`（音乐），视频卡（`m__4*...`）不解析；
# 这里两类都认，并把卡片转成一句人能读的话交给麦麦。

MEDIA_CARD_PREFIX = "m__4"
# 卡片子类型（commands.md 的「点播卡片消息 m__4」表）：
#   @ 音乐卡 / = 音乐卡带封面 / % 音乐卡带封面(变体)
#   # 视频卡 / * 视频卡带封面 / ! 视频卡带封面(变体)
# 后面跟平台数字 0~8
MEDIA_CARD_ORIGIN_RE = re.compile(r"^[=@*%#!][0-8]?$")
MEDIA_CARD_ORIGINS = {
    "=0": "音乐", "=1": "视频",
    "@0": "网易云", "@1": "虾米", "@2": "QQ音乐", "@3": "千千", "@4": "酷狗",
    "@5": "喜马拉雅", "@6": "荔枝", "@7": "回声", "@8": "5sing",
    "*0": "爱奇艺", "*1": "腾讯视频", "*2": "YouTube", "*3": "B站", "*4": "芒果TV",
    "*5": "抖音", "*6": "快手", "*7": "163MV", "*8": "B站直播",
}
# 带封面变体（% / # / !）沿用同数字的平台表
_MEDIA_MUSIC_PLATFORMS = ("网易云", "虾米", "QQ音乐", "千千", "酷狗",
                          "喜马拉雅", "荔枝", "回声", "5sing")
_MEDIA_VIDEO_PLATFORMS = ("爱奇艺", "腾讯视频", "YouTube", "B站", "芒果TV",
                          "抖音", "快手", "163MV", "B站直播")
_MEDIA_MUSIC_MARKS = ("@", "%")
_MEDIA_VIDEO_MARKS = ("*", "#", "!")
DURATION_MARKER = "11451"


def describe_media_origin(origin: str) -> tuple[str, bool]:
    """平台标识 → (平台名, 是否视频卡)。

    `=0`/`=1` 是通用的音乐/视频卡；`@`/`%` + 数字是音乐平台；
    `*`/`#`/`!` + 数字是视频平台（三组是带不带封面的变体，数字含义相同）。
    """
    mark = str(origin or "")[:1]
    digit = str(origin or "")[1:]
    if mark == "=":
        return ("视频" if digit == "1" else "音乐", digit == "1")
    if mark in _MEDIA_VIDEO_MARKS:
        return (_MEDIA_VIDEO_PLATFORMS[int(digit)] if digit.isdigit() and int(digit) < 9
                else "视频", True)
    if mark in _MEDIA_MUSIC_MARKS:
        return (_MEDIA_MUSIC_PLATFORMS[int(digit)] if digit.isdigit() and int(digit) < 9
                else "音乐", False)
    return ("视频" if mark in _MEDIA_VIDEO_MARKS else "音乐", mark in _MEDIA_VIDEO_MARKS)

_ENTITY_MAP = {"&amp;": "&", "&lt;": "<", "&gt;": ">",
               "&quot;": '"', "&#39;": "'", "&#x2F;": "/"}
_ENTITY_RE = re.compile("|".join(re.escape(key) for key in _ENTITY_MAP))


def decode_entities(text: str) -> str:
    """反转义官方 `utils/entities.ts` 的 HTML 实体；循环解码以处理多重转义。"""
    previous = str(text or "")
    while True:
        current = _ENTITY_RE.sub(lambda match: _ENTITY_MAP[match.group(0)], previous)
        if current == previous:
            return current
        previous = current


def parse_room_directory(payload: str) -> Dict[str, str]:
    """从 `%` 初始化 / 刷新大包里提取「房间号 → 房间名」。

    报文结构（逆向文档 6.1）：`%` 之后按 `"` 分成三段，第一段是「用户+房间」列表；
    段内按 `<` 分记录、记录内按 `>` 分字段。房间记录的判据是首字段长得像房间号
    （`/^(?=.*[a-f])([a-f0-9]{10,}_?)+$/`），第二个字段就是房间名。

    实测样例：
        `'5b792cb650749_6a4a56d0b4ca7>存在放映社 | MeowTV>4,88,58>2003>>://r.iirose.com/...`
    双房用 `_` 连接两个房间号，名字用 ` | ` 连接。
    """
    names: Dict[str, str] = {}
    for record in str(payload or "").split('"')[0].split("<"):
        if not record:
            continue
        fields = record.split(">")
        if len(fields) < 2:
            continue
        room_id = fields[0].lstrip("'").strip()
        name = fields[1].strip()
        if not name or not ROOM_ID_RE.match(room_id):
            continue

        names[room_id] = name
        # 双房：把两个房间号分别登记（名字也按 ` | ` 拆开对应）
        sub_ids = room_id.split("_")
        sub_names = [item.strip() for item in name.split("|")]
        if len(sub_ids) == len(sub_names):
            for sub_id, sub_name in zip(sub_ids, sub_names):
                names.setdefault(sub_id, sub_name)
        else:
            for sub_id in sub_ids:
                names.setdefault(sub_id, name)
    return names


def parse_media_card(text: str) -> Optional[Dict[str, Any]]:
    """解析点歌 / 点播卡片；不是卡片时返回 None。"""
    raw = str(text or "").strip()
    if not raw.startswith(MEDIA_CARD_PREFIX):
        return None
    parts = raw.split(">")
    if len(parts) < 5:
        return None

    origin = parts[0][len(MEDIA_CARD_PREFIX):].strip()
    if not MEDIA_CARD_ORIGIN_RE.match(origin):
        return None

    extra = [item.strip() for item in parts[5:]]
    duration = ""
    for item in extra:
        if item.startswith(DURATION_MARKER):
            duration = item[len(DURATION_MARKER):].strip()
    # 视频带码率的形态是 `...>>码率>><时长>`，时长前面没有 11451 标记
    if not duration and len(extra) >= 4 and extra[3]:
        duration = extra[3]
    bitrate = next((item for item in extra
                    if item and not item.startswith(DURATION_MARKER)), "")

    platform, is_video = describe_media_origin(origin)
    return {
        "origin": origin,
        "platform": platform,
        "title": decode_entities(parts[1]).strip(),
        "author": decode_entities(parts[2]).strip(),
        "cover": parts[3].strip(),
        "color": parts[4].strip(),
        "bitrate": bitrate,
        "duration": duration,
        "is_video": is_video,
    }


def describe_media_card(card: Mapping[str, Any]) -> str:
    """把卡片整理成一句话，让麦麦能看懂「谁点了什么」。"""
    action = "点播" if card.get("is_video") else "点歌"
    title = str(card.get("title") or "").strip()
    author = str(card.get("author") or "").strip()
    text = f"[{action}·{card.get('platform') or '媒体'}] 《{title or '未知'}》"
    if author:
        text += f" - {author}"
    if card.get("duration"):
        text += f"（时长 {card['duration']}）"
    return text


def split_quotes(text: str) -> tuple[list[Dict[str, str]], str]:
    """按官方算法拆引用链：返回 (引用列表, 新正文)。

    官方 `decoder/messages/chat/PublicMessage.ts` 的做法：先按 ` (hr_) ` 切开，
    最后一段是新消息，前面每段再按 ` (_hr) ` 切成「被引用内容」和「发送者_时间戳」。
    这样一条消息里叠加多层引用也能全部还原。
    """
    raw = str(text or "")
    if " (_hr) " not in raw:
        return [], raw

    segments = raw.split(" (hr_) ")
    tail = segments.pop() if segments else raw
    replies: list[Dict[str, str]] = []
    for segment in segments:
        pieces = segment.split(" (_hr) ")
        if len(pieces) != 2:
            continue
        message, author = pieces
        match = QUOTE_AUTHOR_RE.match(author.strip())
        if match:
            replies.append({
                "message": decode_entities(message.strip()),
                "who": decode_entities(match.group("who")).strip(),
                "ref": match.group("ref"),
            })
    return replies, tail.strip()


def split_quote(text: str) -> tuple[str, str, str, str]:
    """拆出第一层引用，返回 (被引用的正文, 被引用者, 时间戳, 新正文)。

    未命中时返回 ("", "", "", 原文)。多层引用见 `split_quotes`。
    """
    replies, tail = split_quotes(text)
    if not replies:
        return "", "", "", text
    first = replies[0]
    return first["message"], first["who"], first["ref"], tail


def strip_mentions(text: str) -> str:
    """把 @ 语法降级成纯文本。

    引用块里如果留着 `[*机器人名*]`，Host 会误判成「我被 @ 了」并强制触发回复，
    所以引用正文在拼进可见文本前必须先剥掉提及语法。
    """
    return MENTION_RE.sub(lambda m: (m.group(1) or m.group(2) or m.group(3) or "").strip(), text)


def matches_identity(configured: Any, user_id: str, user_name: str) -> bool:
    """名单项与用户是否匹配：优先按 IIROSE 唯一标识（UID），其次按用户名兜底。

    名单里可以混填 UID（13 位）和用户名，方便手写；UID 匹配大小写不敏感。
    """
    target = str(configured or "").strip()
    if not target:
        return False
    if user_id and target.lower() == user_id.strip().lower():
        return True
    if user_name and target == user_name.strip():
        return True
    return False


def is_id_allowed_by_policy(target_id: str, list_type: str, configured: Any) -> bool:
    """白名单：只放行名单内；黑名单：只拦截名单内（对齐 NapCat `_is_id_allowed_by_list_policy`）。"""
    entries = configured if isinstance(configured, (list, tuple, set)) else []
    hit = any(str(item).strip() == target_id for item in entries if str(item).strip())
    if str(list_type or "").strip().lower() == "whitelist":
        return hit
    return not hit


def format_quote(quoted: str, who: str, timestamp: str, text: str) -> str:
    """按官方语法拼一条引用消息：`旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`。

    两处空格同样是语法的一部分：官方解析器是**按 ` (_hr) ` 与 ` (hr_) ` 拆的**
    （PublicMessage.ts 的 `msg.split(' (hr_) ')`），少了空格就拆不出来。
    没有旧内容时也保留标记前的空格，这样对方的解析器仍能识别成引用。
    """
    marker = f"(_hr) {who}_{timestamp} (hr_)"
    body = text.lstrip()      # 标记后面的空格已经提供了分隔，避免叠成两个空格
    if quoted:
        return f"{quoted} {marker} {body}"
    return f" {marker} {body}"


def normalize_inbound_text(text: str) -> str:
    """把入站正文里的 `[url#e]` 图片标记还原成裸 URL 再交给麦麦。

    实测别人发图时公屏字段 3 就是 `[http://r.iirose.com/....gif#e]`，
    原样透传会让模型看到一堆方括号和 `#e` 后缀。
    """
    return IMAGE_MARK_RE.sub(r"\1", text)


def format_image(url: str) -> str:
    """IIROSE 侧图片必须是 `[url#e]` 才会渲染成图，裸 URL 只会当链接文本。"""
    return f"[{url}#e]"


# ---------------- 房间成员事件 ----------------
#
# 和聊天消息共用 `"` 前缀，靠字段 3 区分（对照官方 adapter
# src/decoder/messages/room/JoinRoom.ts、LeaveRoom.ts、SwitchRoom.ts、MemberUpdate.ts）：
#
#   '1       加入。最后一个字段形如 `15fdcb9b634621'n'''`（n=新加入 / d=重连），
#            房间 id 是这一段里第一个单引号之前的部分。
#   '3       离开。要求 `末二字段 == ''` 且 `末字段 == '2'`（刷新也会先发一条离开）。
#   '2<房间id> 移动到别的房间。末字段为 `3<房间id>`，两处房间 id 必须一致。
#
# 这三类记录都是 12 个字段（聊天是 11 个），末字段不是消息 id，
# 所以不能按聊天记录解析，否则「离开/移动」会被当成聊天正文 `'3` / `'2xxx` 灌给麦麦。

MEMBER_JOIN_RE = re.compile(r"^[^']*'([nd])")
MEMBER_ROOM_RE = re.compile(r"^[a-f0-9]{6,}_?$")


def classify_member_event(parts: list[str]) -> Optional[Dict[str, Any]]:
    """识别房间成员事件；不是成员事件时返回 None（交回聊天解析）。"""
    if len(parts) < 10:
        return None
    marker = _pick(parts, 3)
    if not marker.startswith("'"):
        return None

    last = parts[-1]
    second_last = parts[-2] if len(parts) >= 2 else ""
    record: Dict[str, Any] = {
        "timestamp": int(parts[0]) if parts[0].isdigit() else 0,
        "user_id": _pick(parts, 8),
        "user_name": _pick(parts, 2).strip(),
        "avatar": _pick(parts, 1),
        "room_id": _pick(parts, 10),
        "raw": parts,
    }

    if marker == "'1":
        # 新版本会在末尾附带头像 URL，所以状态字符不能只从字符串尾部找
        status = MEMBER_JOIN_RE.match(last)
        if status is None:
            return None
        record["event"] = "join"
        record["join_type"] = "new" if status.group(1) == "n" else "reconnect"
        record["room_id"] = last.split("'")[0].strip() or record["room_id"]
        return record

    if marker == "'3" and second_last == "" and last == "2":
        record["event"] = "leave"
        record["is_move"] = False
        return record

    if marker.startswith("'2"):
        target = marker[2:]
        # 末字段 `3<目标房间id>` 必须与字段 3 里的房间 id 一致，否则不是移动事件
        if target and last.startswith("3") and last[1:] == target:
            record["event"] = "leave"
            record["is_move"] = True
            record["target_room_id"] = target
            return record

    return None


def describe_member_event(event: Dict[str, Any], room_label: str = "") -> str:
    """把成员事件整理成一句给麦麦/日志看的中文。

    `room_label` 是「房间名 (房间号)」这样的可读标签，由调用方从房间目录里查好后传进来
    —— 模块级函数拿不到插件实例，所以不在这里查表。换房 / 进入房间时带上，
    离开就不用带了（本来就是当前房间）。
    """
    name = str(event.get("user_name") or event.get("user_id") or "有人")
    if event.get("event") == "join":
        action = "重新连接进入房间" if event.get("join_type") == "reconnect" else "进入了房间"
    elif event.get("is_move"):
        return f"{name} 去了别的房间 → {room_label or event.get('target_room_id') or '未知房间'}"
    else:
        return f"{name} 离开了房间"
    return f"{name} {action}" + (f" → {room_label}" if room_label else "")


def _split(payload: str) -> list[str]:
    return payload.split(">")


def _pick(parts: list[str], index: int) -> str:
    return parts[index] if len(parts) > index else ""


HEX6_RE = re.compile(r"^[0-9a-fA-F]{6}$")
UID_FIELD_RE = re.compile(r"^[a-z0-9]{10,16}$")
GENDER_FIELDS = ("1", "2", "4")


def split_room_record(parts: list[str]) -> tuple[str, Dict[str, str]]:
    """把公屏记录拆成 (正文, 尾部字段)。

    正常记录是 11 项，正文就是第 3 项。但正文里可能带 `>`（例如点歌卡片
    `m__4@0>歌名>歌手>封面>颜色>码率`），此时项数会变多，直接取第 3 项只能拿到
    `m__4@0`。这里靠尾部字段的形状（mc 六位 hex → nc 六位 hex → 性别 → UID）
    定位尾部块的起点，把中间剩下的部分拼回正文。

    另外服务端会把正文里的 `>` 转义成 `&gt;`（官方 decoder 也是先 `decode()` 再按
    `>` 拆卡片），两种形态都要能处理。
    """
    start = 4
    for index in range(4, max(4, len(parts) - 6)):
        if (HEX6_RE.match(_pick(parts, index))
                and HEX6_RE.match(_pick(parts, index + 1))
                and _pick(parts, index + 2) in GENDER_FIELDS
                and UID_FIELD_RE.match(_pick(parts, index + 4))):
            start = index
            break

    content = ">".join(parts[3:start]) if start > 3 else _pick(parts, 3)
    return content, {
        "mc": _pick(parts, start),
        "nc": _pick(parts, start + 1),
        "gender": _pick(parts, start + 2),
        "deco": _pick(parts, start + 3),
        "user_id": _pick(parts, start + 4),
        "level": _pick(parts, start + 5),
        "message_id": _pick(parts, start + 6) or _pick(parts, -1),
    }


def parse_frame(text: str) -> tuple[str, Any]:
    """返回 (kind, value)：login_error / init / heartbeat / room / private / unknown"""
    if text.startswith("%*"):
        body = text[2:]
        if body.startswith('"'):
            body = body[1:]
        if body.startswith("n0"):
            return "login_error", "n0"
        if body and body[:1] in "012456x":
            return "login_error", body[:1]
        return "init", body

    # 服务端 `c` 是应用层心跳启动包，收到后需要回发 `c`（否则会被判定掉线）
    if text[:1] == "c" and len(text) <= 16:
        return "heartbeat", text

    # 切房确认（`m`）与失败错误码（`m!5` 未提供密码）
    if text[:1] == "m" and len(text) <= 32:
        return "room_move", text

    # 密码房校验结果（`` `~1 `` 正确 / `` `~0 `` 错误）
    if text.startswith("`~"):
        return "room_password", text

    # fetchMsg 按双引号拆分：段1 = 公屏，段2 = 私聊；`""` 开头表示公屏段为空
    if text.startswith('""'):
        parts = _split(text[2:])
        return "private", {
            "message_id": _pick(parts, -1),
            "user_id": _pick(parts, 1),
            "user_name": decode_entities(_pick(parts, 2)),
            "text": decode_entities(_pick(parts, 4)),
            "room_id": "",
            "avatar": _pick(parts, 3),
            "timestamp": int(parts[0]) if parts and parts[0].isdigit() else 0,
            "anonymous": _pick(parts, 6) == "@",
            "raw": parts,
        }
    if text.startswith('"'):
        parts = _split(text[1:])
        member = classify_member_event(parts)
        if member is not None:
            return "member", member
        content, fields = split_room_record(parts)
        return "room", {
            "message_id": fields.get("message_id") or _pick(parts, -1),
            "user_id": fields.get("user_id") or _pick(parts, 8),
            "user_name": decode_entities(_pick(parts, 2)),
            "text": decode_entities(content),
            "room_id": "",   # 房间 ID 不在报文中（[4]/[5] 是气泡/昵称颜色），用配置的初始房间
            "avatar": _pick(parts, 1),
            "timestamp": int(parts[0]) if parts and parts[0].isdigit() else 0,
            "anonymous": False,
            "raw": parts,
        }
    return "unknown", text


# ---------------- 图床（图片 / 表情上传） ----------------
#
# IIROSE 只能显示 URL 图片（`[url#e]`），而 Host 下发的图片/表情是 base64
# （官方 NapCat 适配器 codecs/outbound/segment_encoder.py：
#   `{"type":"image","data":{"file":"base64://<b64>","sub_type":0/1}}`，sub_type=1 是表情）。
# 所以必须先传到图床换回 URL 再发。
#
# 上传走 stdlib（urllib + 手写 multipart），不引入新依赖；阻塞调用丢到线程里跑。

MEDIA_IMAGE_TYPES = ("image", "imageurl", "emoji", "face")


def build_multipart(fields: Mapping[str, str], files: Mapping[str, tuple[str, bytes, str]],
                    boundary: str) -> bytes:
    """拼 multipart/form-data 请求体（files: 字段名 → (文件名, 内容, MIME)）。"""
    body = bytearray()
    for name, value in fields.items():
        body += f"--{boundary}\r\n".encode("utf-8")
        body += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8")
        body += str(value).encode("utf-8") + b"\r\n"
    for name, (filename, content, content_type) in files.items():
        body += f"--{boundary}\r\n".encode("utf-8")
        body += (f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                 ).encode("utf-8")
        body += f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
        body += content + b"\r\n"
    body += f"--{boundary}--\r\n".encode("utf-8")
    return bytes(body)


def parse_upload_response(text: str, base_url: str = "") -> str:
    """从图床返回的 JSON 里取出图片链接；失败返回空串。

    典型响应：`{"result":"success","code":200,"url":"https://.../a.webp", ...}`
    """
    try:
        payload = json.loads(text)
    except Exception:
        return ""
    if not isinstance(payload, Mapping):
        return ""

    ok = str(payload.get("result") or "").strip().lower() == "success"
    try:
        ok = ok or int(payload.get("code")) == 200
    except (TypeError, ValueError):
        pass
    if not ok:
        return ""

    url = str(payload.get("url") or "").strip()
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return base_url.rstrip("/") + url
    if not url.startswith(("http://", "https://")):
        return ""
    return url


def describe_upload_response(text: str) -> str:
    """把图床响应压成一句人能看的失败原因（token 错 / 体积超限 / 格式不对）。"""
    try:
        payload = json.loads(text)
    except Exception:
        return f"非 JSON 响应: {text[:120]!r}"
    if not isinstance(payload, Mapping):
        return str(payload)[:120]

    parts = [f"{key}={payload[key]}" for key in ("result", "code", "message", "msg", "error")
             if payload.get(key) not in (None, "")]
    url = payload.get("url")
    if url:
        parts.append(f"url={str(url)[:80]}")
    return " ".join(parts) or str(payload)[:120]


def sniff_image_type(payload: bytes) -> str:
    """按magic bytes 猜图片 MIME；Host 给的 base64 常常没有 MIME 信息。"""
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if payload.startswith(b"GIF8"):
        return "image/gif"
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp"
    if payload.startswith(b"BM"):
        return "image/bmp"
    return ""


def guess_extension(content_type: str, hint: str = "") -> str:
    """从 MIME 或文件名提示里推个扩展名，用于给上传文件起名。"""
    table = {"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
             "image/gif": "gif", "image/webp": "webp", "image/bmp": "bmp"}
    if content_type in table:
        return table[content_type]
    hint = hint.strip().lstrip(".").lower()
    if hint in tuple(table.values()):
        return hint
    return "png"


# ---------------- 传输层 ----------------

class IIRoseClient:
    """IIROSE 长连接。

    稳定性主要靠三件事（对照官方 adapter src/utils/ws/*）：

    1. **应用层保活**：每 30 秒发一个空串。官方 adapter 就是这么做的
       （`heartbeat.ts` 里 `IIROSE_WSsend(bot, '')`），只靠 WebSocket 协议层的
       ping 是不够的——服务端掉线判定看的是业务通道是否活跃。
    2. **不用 WS ping 超时判死**：`ping_timeout` 保持 None，否则服务端 pong 稍慢
       就会被 websockets 判成断线（这正是之前频繁掉线的主因之一）。
       改由「多久没收到任何数据」的停滞检测来判断假死。
    3. **永不停止重连**：连续失败达到阈值只降级为长间隔重试，不再彻底放弃。
    """

    def __init__(self, on_frame: Callable[[str], Awaitable[None]], *,
                 logger: logging.Logger, max_retries: int = 5, keepalive: bool = True,
                 timeout_ms: int = 5000, reconnect_base_seconds: float = 2.0,
                 keepalive_interval: float = 30.0, stall_timeout: float = 120.0,
                 max_reconnect_seconds: float = 300.0) -> None:
        self._on_frame = on_frame
        self._logger = logger
        self._max_retries = max(1, max_retries)
        self._keepalive = keepalive
        self._timeout = max(1.0, timeout_ms / 1000)
        self._reconnect_base = max(1.0, reconnect_base_seconds)
        self._keepalive_interval = max(5.0, float(keepalive_interval or 30.0))
        self._stall_timeout = max(0.0, float(stall_timeout or 0.0))
        self._max_reconnect = max(self._reconnect_base, float(max_reconnect_seconds or 300.0))
        self._ws: Any = None
        # 只由 close() 清空的引用：run() 收尾时会把 _ws 置空，
        # 如果 close() 只看 _ws，插件停用时就会「找不到 socket」而根本没关连接，
        # 服务端会一直以为机器人还在线。
        self._socket_to_close: Any = None
        self._stop = False
        self._last_frame_at = time.monotonic()
        self._connected_at: Optional[float] = None
        self._reconnects = 0
        # 便于测试替换（也可用于接入自定义调度）
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    @staticmethod
    def is_available() -> bool:
        return websockets is not None

    @property
    def connected(self) -> bool:
        return self._ws is not None

    @property
    def reconnects(self) -> int:
        return self._reconnects

    def _next_delay(self, attempt: int) -> float:
        """指数退避 + 抖动，上限 max_reconnect_seconds。"""
        return backoff_delay(attempt, self._reconnect_base, self._max_reconnect)

    async def _pick_best_server(self) -> str:
        async def probe(url: str):
            started = time.monotonic()
            try:
                async with websockets.connect(url, open_timeout=self._timeout) as ws:
                    await ws.ping()
                return url, time.monotonic() - started
            except Exception:
                return None

        results = await asyncio.gather(*(probe(url) for url in SERVER_URLS))
        scored = [item for item in results if item is not None]
        if not scored:
            raise ConnectionError("所有 IIROSE 服务器均不可达")
        scored.sort(key=lambda item: item[1])
        url, latency = scored[0]
        self._logger.info("IIROSE 选定服务器 %s (%.0f ms)", url, latency * 1000)
        return url

    async def connect(self) -> None:
        url = await self._pick_best_server()
        self._ws = await websockets.connect(
            url,
            open_timeout=self._timeout,
            # 协议层 ping 只用于探测，不设超时（不拿它判死连接）
            ping_interval=self._keepalive_interval if self._keepalive else None,
            ping_timeout=None,
            close_timeout=self._timeout,
            max_size=None,
        )
        self._socket_to_close = self._ws
        self._last_frame_at = time.monotonic()
        self._connected_at = self._last_frame_at
        self._logger.info("IIROSE 已连接 %s", url)

    async def send(self, text: str) -> None:
        if self._ws is None:
            raise RuntimeError("IIROSE 连接尚未建立")
        await self._ws.send(encode_frame(text))

    async def _watchdog_loop(self) -> None:
        """保活 + 停滞检测。发现异常就主动关掉连接，让 run() 去重连。"""
        while not self._stop:
            await asyncio.sleep(self._keepalive_interval)
            ws = self._ws
            if ws is None:
                return

            idle = time.monotonic() - self._last_frame_at
            if self._stall_timeout and idle > self._stall_timeout:
                self._logger.warning(
                    "IIROSE 已 %.0fs 没收到任何数据（疑似连接假死），主动重连", idle)
                await self._abort(ws)
                return

            if not self._keepalive:
                continue
            try:
                # 官方 adapter 的保活方式：空串（长度 0 不压缩，直接发空帧）
                await self.send("")
            except Exception as exc:
                self._logger.warning("IIROSE 保活包发送失败，主动重连: %s", exc)
                await self._abort(ws)
                return

    async def _abort(self, ws: Any) -> None:
        try:
            await ws.close()
        except Exception:
            self._logger.debug("关闭连接时出错", exc_info=True)

    async def run(self, on_connected=None, on_disconnected=None) -> None:
        attempt = 0
        while not self._stop:
            failure: Optional[BaseException] = None
            watchdog: Optional[asyncio.Task[None]] = None
            connected_at: Optional[float] = None

            try:
                await self.connect()
                connected_at = self._connected_at
                if on_connected is not None:
                    await on_connected()
                watchdog = asyncio.create_task(self._watchdog_loop(), name="iirose-keepalive")
                async for raw in self._ws:
                    self._last_frame_at = time.monotonic()
                    try:
                        text = decode_frame(raw)
                    except Exception:
                        self._logger.exception("IIROSE 帧解码失败")
                        continue
                    try:
                        await self._on_frame(text)
                    except Exception:
                        self._logger.warning("处理 IIROSE 报文异常，已忽略", exc_info=True)
                failure = ConnectionError("连接已被服务端关闭")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failure = exc
            finally:
                if watchdog is not None:
                    watchdog.cancel()
                    try:
                        await watchdog
                    except (asyncio.CancelledError, Exception):
                        pass
                # 先清理再退避：否则退避期间 connected 仍是 True，
                # 出站消息会往一条死连接上发。
                self._ws = None

            if on_disconnected is not None:
                try:
                    await on_disconnected()
                except Exception:
                    self._logger.debug("断线回调异常", exc_info=True)

            if self._stop:
                break

            # 只有「稳了一会儿又断」才重置退避，避免快速反复抖动时空转
            if connected_at is not None \
                    and (time.monotonic() - connected_at) >= STABLE_CONNECTION_SECONDS:
                attempt = 0
            attempt += 1
            self._reconnects += 1
            delay = self._next_delay(attempt)

            if failure is not None and attempt == self._max_retries:
                # 「转入长间隔重试」只提示一次，之后每 10 次汇总一条，
                # 避免长时间断网时把日志刷屏
                self._logger.error(
                    "IIROSE 已连续 %d 次连接失败（最后一次：%s），转入每 %.0fs 重试，不会停止",
                    attempt, failure, delay)
            elif failure is not None and attempt > self._max_retries:
                if attempt % 10 == 0:
                    self._logger.warning("IIROSE 仍无法连接（已连续 %d 次）：%s", attempt, failure)
            elif failure is not None:
                self._logger.warning(
                    "IIROSE 连接断开(%s)，%.0fs 后重连（连续第 %d 次）", failure, delay, attempt)
            else:
                self._logger.warning("IIROSE 连接结束，%.0fs 后重连", delay)

            await self._sleep(delay)

    async def close(self) -> None:
        """停止并**真正关闭** WebSocket。

        IIROSE 没有「下线报文」，服务端就是靠 WS 断开把用户标记为离开
        （官方 adapter 的 `WsClient.stop()` 也是 `socket.close(1000, 'Plugin disposing')`）。
        所以这里必须确保 socket 被关掉：用 `_socket_to_close` 而不是 `_ws`，
        因为 run() 收尾时会把 `_ws` 置空。
        """
        self._stop = True
        ws, self._socket_to_close = self._socket_to_close, None
        self._ws = None
        if ws is None:
            return
        try:
            await ws.close(1000, "plugin disposing")
            self._logger.info("IIROSE 连接已关闭（已下线）")
        except Exception:
            self._logger.debug("关闭 IIROSE 连接时出错", exc_info=True)


# ---------------- 配置模型 ----------------
#
# 每个字段都带 `json_schema_extra={"label": ...}`，WebUI 配置面板会优先显示它，
# 否则面板上只会出现 `user_list_enabled` 这种英文键名，分不清哪个是哪个。
# `hint` 是字段下方的补充说明；`placeholder` 是输入框灰字示例。

class PluginSection(PluginConfigBase):
    __ui_label__ = "① 插件设置"
    enabled: bool = Field(default=False, description="是否启用并连接 IIROSE",
                          json_schema_extra={"label": "启用适配器"})
    config_version: str = Field(default="0.1.0", description="配置版本号（勿改）",
                                json_schema_extra={"label": "配置版本（勿改）"})


class AccountSection(PluginConfigBase):
    __ui_label__ = "② IIROSE 账号"
    username: str = Field(default="", description="机器人用户名（不带 [* *]）",
                          json_schema_extra={"label": "机器人用户名", "placeholder": "例如 03酱"})
    uid: str = Field(default="", description="13 位唯一标识，用于过滤机器人自己的消息",
                     json_schema_extra={"label": "机器人 UID（唯一标识）",
                                        "hint": "个人资料页的「唯一标识」，强烈建议填写："
                                                "否则会自问自答、也无法判断有谁 @ 了机器人",
                                        "placeholder": "13 位小写字母数字，例如 62b98115cefd2"})
    password: str = Field(default="", description="账号密码，仅本地用于计算 MD5",
                          json_schema_extra={"label": "账号密码",
                                             "hint": "仅本地用于计算 MD5，不会明文外发",
                                             "x-widget": "password"})
    room_id: str = Field(default="", description="初始房间 ID，例如 64d5f8e17b2ad",
                         json_schema_extra={"label": "房间 ID",
                                            "hint": "改这里会实时切房：先发移动指令，再下线重连进新房间",
                                            "placeholder": "13 位房间号，例如 16a7c38409e902"})
    room_password: str = Field(default="", description="加密房间密码，一般留空",
                               json_schema_extra={"label": "房间密码", "hint": "密码房才需要填"})


class BotSection(PluginConfigBase):
    __ui_label__ = "③ 机器人外观"
    status: str = Field(default="n", description="平台状态码：n / 0 / d / 8 …",
                        json_schema_extra={"label": "在线状态",
                                           "hint": "n 无状态 / 0 会话中 / 1 忙碌中 / 2 离开中 / "
                                                   "8 睡觉中 / f 请撩我",
                                           "x-widget": "select",
                                           "choices": ["n", "0", "1", "2", "3", "4", "5", "6",
                                                       "7", "8", "9", "a", "b", "c", "d", "e", "f"]})
    signature: str = Field(default="Bot of MaiBot", description="个性签名",
                           json_schema_extra={"label": "个性签名"})
    only_hang_up: bool = Field(default=False, description="静默模式：只接收不发送",
                               json_schema_extra={"label": "静默模式（只收不发）",
                                                  "hint": "打开后机器人只读不回，用来排查问题"})
    report_member_events: bool = Field(
        default=True, description="把房间成员上线/下线/重连/换房作为通知上报给 MaiBot（日志始终记录）",
        json_schema_extra={"label": "上报成员上下线",
                           "hint": "关掉后只在日志里记录，不交给麦麦"})
    hello_on_login: bool = Field(
        default=False, description="登录成功后向房间发一条自测消息（单独验证出站通道）",
        json_schema_extra={"label": "上线自测消息",
                           "hint": "用来单独验证「机器人能不能发出消息」，验证完记得关掉"})
    hello_text: str = Field(
        default="【IIROSE 适配器】已上线~", description="自测消息内容",
        json_schema_extra={"label": "自测消息内容"})


class ConnectionSection(PluginConfigBase):
    __ui_label__ = "④ 连接设置"
    keepalive: bool = Field(default=True, description="启用心跳保活",
                            json_schema_extra={"label": "心跳保活"})
    keepalive_interval_seconds: float = Field(
        default=30.0, description="保活包发送间隔（秒）；官方 adapter 为 30 秒",
        json_schema_extra={"label": "保活间隔（秒）",
                           "hint": "官方 adapter 用 30 秒发一个空串保活，一般不用改"})
    stall_timeout_seconds: float = Field(
        default=120.0, description="多久没收到任何数据就判定连接假死并重连（秒，0=关闭）",
        json_schema_extra={"label": "假死判定（秒）",
                           "hint": "网络差导致误判断线时可以调大，0 = 关闭这项检测"})
    timeout_ms: int = Field(default=5000, description="连接/登录超时（毫秒）",
                            json_schema_extra={"label": "连接超时（毫秒）"})
    max_retries: int = Field(
        default=5, description="连续失败达到该次数后转为长间隔重试（不会停止重连）",
        json_schema_extra={"label": "转长间隔重试的阈值",
                           "hint": "达到这个次数后只是把重试间隔拉长，永远不会停止重连"})
    reconnect_base_seconds: float = Field(default=2.0, description="重连退避基准秒数",
                                          json_schema_extra={"label": "重连退避基准（秒）"})
    max_reconnect_seconds: float = Field(default=300.0, description="重连退避上限（秒）",
                                         json_schema_extra={"label": "重连退避上限（秒）"})
    mc: str = Field(default=DEFAULT_MC, description="气泡颜色（6 位十六进制），一般无需修改",
                    json_schema_extra={"label": "气泡颜色", "placeholder": "66ccff"})
    debug_raw: bool = Field(default=False, description="打印原始报文，用于协议校准",
                            json_schema_extra={"label": "打印原始报文",
                                               "hint": "排查协议问题时才开，平时开着日志会很吵"})


class ChatListSection(PluginConfigBase):
    __ui_label__ = "⑤ 黑白名单"
    enabled: bool = Field(
        default=False, description="总开关：关闭时下面三项名单全部不生效（安全起见默认关闭）",
        json_schema_extra={"label": "名单总开关",
                           "hint": "关闭时下面三个名单都不生效，所有人都能触发回复"})
    user_list_enabled: bool = Field(
        default=False, description="启用用户名单（独立开关，只影响用户维度）",
        json_schema_extra={"label": "启用用户名单",
                           "hint": "独立开关：只影响「谁能触发回复」，不影响房间名单"})
    user_list_type: str = Field(
        default="whitelist", description="用户名单模式：whitelist 只回名单内的人 / blacklist 不回名单内的人",
        json_schema_extra={"label": "用户名单模式", "x-widget": "select",
                           "choices": ["whitelist", "blacklist"]})
    user_list: List[str] = Field(
        default_factory=list,
        description="用户名单：填 IIROSE 唯一标识（13 位 UID，个人资料页「唯一标识」）；"
                    "也支持直接填用户名，按昵称兜底匹配",
        json_schema_extra={"label": "用户名单",
                           "hint": "一行一个：优先填 13 位 UID，也可以直接填用户名；"
                                   "名单为空时本项不生效（不会拦人）"})
    ban_user_enabled: bool = Field(
        default=False, description="启用永久屏蔽名单（独立开关）",
        json_schema_extra={"label": "启用永久屏蔽名单", "hint": "独立开关，优先级最高"})
    ban_user_list: List[str] = Field(
        default_factory=list, description="永久屏蔽名单：这些人无论名单模式如何都不回复（UID 或用户名）",
        json_schema_extra={"label": "永久屏蔽名单", "hint": "一行一个，UID 或用户名"})
    room_list_enabled: bool = Field(
        default=False, description="启用房间名单（独立开关，一般不用；机器人只在自己所在的房间）",
        json_schema_extra={"label": "启用房间名单",
                           "hint": "一般用不到：机器人只会在自己登录的那个房间"})
    room_list_type: str = Field(
        default="blacklist", description="房间名单模式",
        json_schema_extra={"label": "房间名单模式", "x-widget": "select",
                           "choices": ["whitelist", "blacklist"]})
    room_list: List[str] = Field(default_factory=list, description="房间 ID 名单",
                                 json_schema_extra={"label": "房间名单", "hint": "一行一个房间 ID"})
    log_dropped: bool = Field(default=True, description="记录被名单拦下的消息（方便排查为什么不回复）",
                              json_schema_extra={"label": "记录被拦下的消息",
                                                 "hint": "排查「为什么没回复」时很有用"})


class ImageHostSection(PluginConfigBase):
    __ui_label__ = "⑥ 图床（图片 / 表情上传）"
    enabled: bool = Field(
        default=True, description="把麦麦发来的图片/表情先传到图床，再用返回的链接发送（IIROSE 只认 URL）",
        json_schema_extra={"label": "启用图床",
                           "hint": "关闭后图片/表情会退化成 [图片] [表情] 占位文字"})
    base_url: str = Field(default=DEFAULT_IMAGE_HOST, description="图床地址",
                          json_schema_extra={"label": "图床地址",
                                             "hint": "必填：自己的图床首页地址，不带结尾斜杠",
                                             "placeholder": "https://your-image-host.example.com"})
    upload_path: str = Field(default=DEFAULT_IMAGE_HOST_PATH, description="上传接口路径",
                             json_schema_extra={"label": "上传接口路径",
                                                "placeholder": "/api/index.php"})
    token: str = Field(default=DEFAULT_IMAGE_HOST_TOKEN,
                       description="图床上传 token（在图床的 tokenList 里查）",
                       json_schema_extra={"label": "图床上传 Token", "hint": "必填，个人凭据，别外传",
                                          "x-widget": "password"})
    field_name: str = Field(default="image", description="上传表单里的文件字段名",
                            json_schema_extra={"label": "上传字段名", "hint": "一般不用改",
                                               "placeholder": "image"})
    reuse_uploaded: bool = Field(
        default=True, description="复用已上传过的图片：按内容指纹缓存链接，重启后依然生效，避免重复上传",
        json_schema_extra={"label": "复用已上传图片",
                           "hint": "同一张表情只传一次，重启后依然有效"})
    verify_upload: bool = Field(
        default=True, description="校验链接真的能取到图片；取不到就重新上传（避免发出打不开的图）",
        json_schema_extra={"label": "校验图片链接",
                           "hint": "上传后/复用前检查链接能否取到图片，取不到就重传"})
    verify_interval_hours: float = Field(
        default=24.0, description="复用时多久重新校验一次链接（小时）；0 = 每次复用都校验",
        json_schema_extra={"label": "链接校验周期（小时）",
                           "hint": "0 = 每次复用都校验（最保险但多一次请求）"})
    verify_timeout_ms: int = Field(default=5000, description="链接校验超时（毫秒）",
                                   json_schema_extra={"label": "链接校验超时（毫秒）"})
    timeout_ms: int = Field(default=15000, description="上传超时（毫秒）",
                            json_schema_extra={"label": "上传超时（毫秒）"})
    max_bytes: int = Field(default=8 * 1024 * 1024, description="单张图片最大字节数，超出则退化为占位文本",
                           json_schema_extra={"label": "单张图片大小上限（字节）",
                                              "hint": "默认 8 MB"})


class IIRosePluginConfig(PluginConfigBase):
    plugin: PluginSection = Field(default_factory=PluginSection)
    account: AccountSection = Field(default_factory=AccountSection)
    bot: BotSection = Field(default_factory=BotSection)
    connection: ConnectionSection = Field(default_factory=ConnectionSection)
    image_host: ImageHostSection = Field(default_factory=ImageHostSection)
    chat_list: ChatListSection = Field(default_factory=ChatListSection)


# ---------------- 插件主体 ----------------

class IIRoseAdapterPlugin(MaiBotPlugin):
    config_model: ClassVar[type[PluginConfigBase] | None] = IIPluginConfig if False else IIRosePluginConfig

    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[IIRoseClient] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._awaiting_login = False
        self._login_ok = False
        self._slow_login_warned = False
        # 消息 id → 发送者/时间戳/正文，用于把 Host 的 reply 组件还原成 IIROSE 引用语法
        self._recent: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        # 用户名 ↔ UID 双向表：IIROSE 的 @ 只认用户名，而 Host 的 at 组件常只给 UID
        self._uid_names: Dict[str, str] = {}
        self._name_uids: Dict[str, str] = {}
        # 已经提示过的通知投递失败原因，避免成员频繁上下线时刷屏
        self._notice_warned: set[str] = set()
        self._image_host_warned = False
        # 名单开着但为空时只提示一次
        self._chat_list_warned: set[str] = set()
        # 当前实际登录的房间（用于配置改房间号时执行切房流程）
        self._active_room_id = ""
        # 切房重连时登录包要带的原房间 id（协议字段 lr）
        self._last_room_id = ""
        # 房间号 → 房间名（从 `%` 大包里解析，用于后台会话名 / 成员事件文案）
        self._room_names: Dict[str, str] = {}
        # 图床结果缓存：sha256(图片字节) → URL，同一张表情不重复上传
        self._upload_cache: OrderedDict[str, str] = OrderedDict()
        self._upload_cache_loaded = False
        self._upload_hits = 0

    # ---- 近期消息 / 用户目录 ----

    def _remember(self, message_id: str, *, user_id: str, user_name: str,
                 timestamp: int, text: str) -> None:
        if user_id and user_name:
            self._uid_names[user_id] = user_name
            self._name_uids[user_name] = user_id
        if not message_id:
            return
        self._recent[message_id] = {
            "user_id": user_id, "user_name": user_name,
            "timestamp": timestamp, "text": text,
        }
        self._recent.move_to_end(message_id)
        while len(self._recent) > RECENT_MESSAGES:
            self._recent.popitem(last=False)

    def _name_for_uid(self, uid: str) -> str:
        return self._uid_names.get(uid.strip(), "")

    def _uid_for_name(self, name: str) -> str:
        return self._name_uids.get(name.strip(), "")

    # ---- 生命周期 ----

    async def on_load(self) -> None:
        await self._restart_connection_if_needed()

    async def on_unload(self) -> None:
        await self._stop_connection()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        if scope != "self":
            return
        setter = getattr(self, "set_plugin_config", None)
        if callable(setter):
            try:
                setter(config_data)
            except Exception:
                self.ctx.logger.debug("set_plugin_config 失败", exc_info=True)
        if version:
            self.ctx.logger.debug("IIROSE 适配器收到配置更新: %s", version)
        await self._restart_connection_if_needed()

    @property
    def _settings(self) -> IIRosePluginConfig:
        try:
            return self.config  # type: ignore[return-value]
        except Exception:
            return IIRosePluginConfig()

    def _account_id(self) -> str:
        """Host 侧路由键用的账号 ID。

        必须与 `_report_state` 上报的 account_id、以及入站 route_metadata 的 self_id
        保持完全一致，否则 Platform IO 的出站投递找不到本网关。
        """
        return (self._self_uid or self._username or "iirose").strip()

    # ---- 账号字段：剥离用户可能从 IIROSE 界面粘进来的包裹语法 ----

    @property
    def _username(self) -> str:
        return normalize_username(self._settings.account.username)

    @property
    def _self_uid(self) -> str:
        return normalize_uid(self._settings.account.uid)

    @property
    def _room_id(self) -> str:
        return normalize_room_id(self._settings.account.room_id)

    def _room_name(self, room_id: str) -> str:
        """房间号 → 房间名；没抓到就用房间号本身兜底。"""
        key = str(room_id or "").strip()
        if not key:
            return ""
        return self._room_names.get(key, "")

    def _describe_room(self, room_id: str) -> str:
        """房间号 → `房间名 (房间号)`；没抓到名字就只给房间号。"""
        key = str(room_id or "").strip()
        if not key:
            return ""
        name = self._room_name(key)
        if name and name != key:
            return f"{name} ({key})"
        return key

    # ---- 连接管理 ----

    async def _restart_connection_if_needed(self) -> None:
        settings = self._settings
        target_room = str(self._room_id or "").strip()

        # 配置里换了房间号：先按协议发切房指令，断线后带 `lr` 重连进新房间
        if (self._client is not None and self._client.connected
                and self._active_room_id and target_room
                and target_room != self._active_room_id):
            await self._move_room(self._active_room_id, target_room, settings)

        await self._stop_connection()

        if not settings.plugin.enabled:
            self.ctx.logger.info("IIROSE 适配器未启用，保持空闲")
            return
        if not (self._username and settings.account.password and self._room_id):
            self.ctx.logger.error("IIROSE 配置不完整：username / password / room_id 均为必填")
            return
        if not IIRoseClient.is_available():
            self.ctx.logger.error("IIROSE 适配器依赖 websockets，但当前环境未安装")
            return

        self._client = IIRoseClient(
            self._handle_frame,
            logger=self.ctx.logger,
            max_retries=settings.connection.max_retries,
            keepalive=settings.connection.keepalive,
            timeout_ms=settings.connection.timeout_ms,
            reconnect_base_seconds=settings.connection.reconnect_base_seconds,
            keepalive_interval=settings.connection.keepalive_interval_seconds,
            stall_timeout=settings.connection.stall_timeout_seconds,
            max_reconnect_seconds=settings.connection.max_reconnect_seconds,
        )
        self._task = asyncio.create_task(
            self._client.run(on_connected=self._handle_connected,
                             on_disconnected=self._handle_disconnected),
            name="iirose-gateway",
        )
        if getattr(settings.image_host, "enabled", False) \
                and not str(settings.image_host.base_url or "").strip():
            self.ctx.logger.warning(
                "IIROSE 图床未配置（image_host.base_url 为空），图片 / 表情将退化为占位文本；"
                "如需发送图片请填写图床地址与 token")
        self.ctx.logger.info(
            "IIROSE 适配器已启动连接任务（账号=%s 房间=%s 保活=%.0fs 停滞重连=%.0fs 退避上限=%.0fs 聊天名单：%s）",
            self._username or "-",
            self._room_id or "-",
            settings.connection.keepalive_interval_seconds,
            settings.connection.stall_timeout_seconds,
            settings.connection.max_reconnect_seconds,
            self._describe_chat_list(),
        )

    async def _move_room(self, old_room: str, new_room: str, settings: Any) -> None:
        """按 IIROSE 协议换房间：密码房先验密码，再发移动包，随后断线重连。

        协议里移动成功后必须断开 WS，重新发登录包把 `r` 改成目标房间，
        并带上 `lr`（原房间 id）与 `rp`（目标房间密码）。
        """
        client = self._client
        if client is None:
            return
        room_password = str(getattr(settings.account, "room_password", "") or "").strip()
        self.ctx.logger.info("IIROSE 房间配置变更：%s → %s，执行切房", old_room, new_room)
        try:
            if room_password:
                # 密码房先发询问包验密码
                await client.send(f"=^~{new_room}>{room_password}")
                await asyncio.sleep(MOVE_ROOM_SETTLE_SECONDS)
            await client.send(f"m{new_room}")
            self.ctx.logger.info("IIROSE 切房指令已发送（m%s），%.1fs 后断开并按新房间重连",
                                 new_room, MOVE_ROOM_SETTLE_SECONDS)
            await asyncio.sleep(MOVE_ROOM_SETTLE_SECONDS)
        except Exception:
            self.ctx.logger.warning("IIROSE 切房指令发送失败，直接按新房间重连", exc_info=True)
        # 让登录包带上原房间，服务端才知道是从哪个房间切过来的
        self._last_room_id = old_room

    async def _stop_connection(self) -> None:
        task, self._task = self._task, None
        client, self._client = self._client, None

        # 顺序很重要：**先关 socket，再收尾任务**。
        # 反过来的话 run() 的 finally 会先把 self._ws 置空，
        # 之后 close() 就找不到 socket 了 —— 等于没关连接，
        # 服务端会一直以为机器人还在线（插件停用/重载后不下线就是这么来的）。
        if client is not None:
            await client.close()
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self.ctx.logger.debug("IIROSE 连接任务退出异常", exc_info=True)

        self._login_ok = False
        self._awaiting_login = False
        self._active_room_id = ""
        await self._report_state(False, message="stopped")

    async def _report_state(self, ready: bool, *, message: str = "") -> None:
        settings = self._settings
        account_id = self._account_id()
        metadata: Dict[str, Any] = {"protocol": "iirose", "room_id": self._room_id}
        if message:
            metadata["message"] = message
        try:
            result = await self.ctx.gateway.update_state(
                gateway_name=GATEWAY_NAME,
                ready=ready,
                platform=PLATFORM,
                account_id=account_id,
                scope="primary",
                metadata=metadata,
            )
            self.ctx.logger.info("IIROSE 网关状态上报: ready=%s platform=%s account_id=%s result=%s",
                                 ready, PLATFORM, account_id, result)
        except Exception:
            self.ctx.logger.warning("上报 IIROSE 网关状态失败: ready=%s", ready, exc_info=True)

    async def _handle_connected(self) -> None:
        client, settings = self._client, self._settings
        if client is None:
            return
        self._awaiting_login = True
        self._login_ok = False
        self._slow_login_warned = False
        await client.send(build_login(
            room_id=self._room_id,
            username=self._username,
            password=settings.account.password,
            room_password=settings.account.room_password or None,
            status=settings.bot.status,
            signature=settings.bot.signature,
            last_room_id=self._last_room_id,
        ))
        if self._last_room_id:
            self.ctx.logger.info("IIROSE 登录报文已发送（切房重连：%s → %s），等待服务端数据 …",
                                 self._last_room_id, self._room_id)
        else:
            self.ctx.logger.info("IIROSE 登录报文已发送，等待服务端数据 …")
        asyncio.create_task(self._warn_slow_login(), name="iirose-login-watch")

    async def _warn_slow_login(self) -> None:
        await asyncio.sleep(LOGIN_WARN_SECONDS)
        if self._awaiting_login and not self._slow_login_warned:
            self._slow_login_warned = True
            self.ctx.logger.warning(
                "IIROSE 连接已建立但 %.0fs 内没收到任何服务端数据，请检查账号/房间/网络",
                LOGIN_WARN_SECONDS,
            )

    async def _mark_ready(self, reason: str) -> None:
        if self._login_ok:
            return
        self._login_ok = True
        self._awaiting_login = False
        settings = self._settings
        # 记下实际进到的房间，配置改房间号时据此判断要不要走切房流程
        self._active_room_id = str(self._room_id or "")
        if self._last_room_id:
            self.ctx.logger.info("IIROSE 已切换到新房间 %s（原房间 %s）",
                                 self._active_room_id or "-", self._last_room_id)
        self._last_room_id = ""
        self.ctx.logger.info("IIROSE 登录成功（%s），当前房间=%s", reason, self._active_room_id or "-")
        await self._report_state(True)

        # 出站自测：不经过 MaiBot 的回复流程，直接往房间发一条，用来单独验证发送通道。
        if not getattr(settings.bot, "hello_on_login", False):
            return
        if self._client is None or not self._client.connected:
            self.ctx.logger.warning("IIROSE 上线测试消息未发送：连接已断开")
            return
        try:
            await self._client.send(encode_room_message(
                str(getattr(settings.bot, "hello_text", "") or "【IIROSE 适配器】已上线~"),
                settings.connection.mc))
            self.ctx.logger.info("IIROSE 上线测试消息已发送到房间 target=%s",
                                 self._room_id)
        except Exception:
            self.ctx.logger.warning("IIROSE 上线测试消息发送失败", exc_info=True)

    async def _handle_disconnected(self) -> None:
        self._login_ok = False
        self._awaiting_login = False
        await self._report_state(False, message="connection closed")

    # ---- 入站 ----

    def _chat_list_check(self, kind: str, user_id: str, user_name: str,
                         room_id: str) -> tuple[bool, str]:
        """黑白名单判定，返回 (是否放行, 原因)。

        三个名单各自有独立开关，互不影响：
          `ban_user_enabled` → 永久屏蔽名单
          `room_list_enabled` → 房间名单
          `user_list_enabled` → 用户名单
        总开关 `enabled` 关闭时三者都不生效。

        白名单为空时**不再拦截所有人**——那只会让机器人莫名其妙装死，
        这里当作「还没配」跳过并提示，行为与黑名单空名单一致。
        """
        settings = self._settings
        chat = getattr(settings, "chat_list", None)
        if chat is None or not getattr(chat, "enabled", False):
            return True, ""

        if getattr(chat, "ban_user_enabled", False):
            for entry in list(getattr(chat, "ban_user_list", []) or []):
                if matches_identity(entry, user_id, user_name):
                    return False, f"命中永久屏蔽名单（{entry}）"

        if getattr(chat, "room_list_enabled", False):
            entries = [str(item).strip() for item in (getattr(chat, "room_list", []) or [])
                       if str(item).strip()]
            if entries:
                mode = str(getattr(chat, "room_list_type", "blacklist")).strip().lower()
                hit = room_id in entries
                if (mode == "whitelist") != hit:
                    return False, (f"房间 {room_id} 不在房间白名单内" if mode == "whitelist"
                                   else f"房间 {room_id} 命中房间黑名单")
            else:
                self._warn_empty_list("room_list", "房间名单")
        if getattr(chat, "user_list_enabled", False):
            entries = [str(item).strip() for item in (getattr(chat, "user_list", []) or [])
                       if str(item).strip()]
            if not entries:
                self._warn_empty_list("user_list", "用户名单")
                return True, ""
            mode = str(getattr(chat, "user_list_type", "whitelist")).strip().lower()
            hit = any(matches_identity(entry, user_id, user_name) for entry in entries)
            if mode == "whitelist" and not hit:
                return False, "不在用户白名单内"
            if mode == "blacklist" and hit:
                return False, "命中用户黑名单"

        return True, ""

    def _warn_empty_list(self, key: str, label: str) -> None:
        """名单开着但是空的：只提示一次，然后按「未配置」处理，不拦任何人。"""
        if key in self._chat_list_warned:
            return
        self._chat_list_warned.add(key)
        self.ctx.logger.warning(
            "IIROSE %s 已启用但名单为空，本次按「未配置」处理（不拦截任何人）；"
            "要生效请填入 UID 或用户名，或把 chat_list 里对应的开关关掉", label)

    def _describe_chat_list(self) -> str:
        chat = getattr(self._settings, "chat_list", None)
        if chat is None or not getattr(chat, "enabled", False):
            return "总开关=关（所有人都能触发回复）"

        def state(enabled_key: str, list_key: str, label: str) -> str:
            if not getattr(chat, enabled_key, False):
                return f"{label}=关"
            mode = str(getattr(chat, f"{list_key.split('_list')[0]}_list_type", "")).lower()
            mode_text = "白" if mode == "whitelist" else "黑"
            count = len([x for x in (getattr(chat, list_key, []) or []) if str(x).strip()])
            return f"{label}={mode_text}名单({count} 条)"

        return "总开关=开 | " + " | ".join((
            state("ban_user_enabled", "ban_user_list", "永久屏蔽"),
            state("user_list_enabled", "user_list", "用户"),
            state("room_list_enabled", "room_list", "房间"),
        ))

    def _log_dropped_chat(self, user_id: str, user_name: str, reason: str) -> None:
        chat = getattr(self._settings, "chat_list", None)
        if chat is not None and not getattr(chat, "log_dropped", True):
            return
        self.ctx.logger.info("IIROSE 名单过滤：忽略 %s(%s) 的消息 —— %s",
                             user_name or "未知", user_id or "-", reason)

    async def _handle_frame(self, text: str) -> None:
        settings = self._settings
        kind, value = parse_frame(text)

        if settings.connection.debug_raw:
            preview = str(value)[:400] if kind != "unknown" else text[:400]
            self.ctx.logger.info("[IIROSE RAW][%s] %s", kind, preview)

        if kind == "login_error":
            self._login_ok = False
            self._awaiting_login = False
            self.ctx.logger.error("IIROSE 登录失败：%s（错误码 %s）",
                                  LOGIN_ERRORS.get(value, "未知错误"), value)
            return

        if self._awaiting_login:
            await self._mark_ready(f"服务端首个报文类型={kind}")

        if kind == "init":
            names = parse_room_directory(value)
            if names:
                self._room_names.update(names)
                # 避免无限增长：只保留最近的 5000 条
                while len(self._room_names) > 5000:
                    self._room_names.pop(next(iter(self._room_names)))
            self.ctx.logger.info(
                "IIROSE 服务端数据包：%d 字符（解析出 %d 个房间名，当前房间=%s）",
                len(value), len(names), self._describe_room(self._room_id) or "-")
            return

        if kind == "heartbeat":
            # 应用层保活：服务端发 `c`，客户端必须回 `c`
            try:
                if self._client is not None:
                    await self._client.send("c")
            except Exception:
                self.ctx.logger.debug("IIROSE 心跳回包失败", exc_info=True)
            return

        if kind == "room_move":
            if value.startswith("m!"):
                code = value[2:]
                self.ctx.logger.warning(
                    "IIROSE 切房被拒绝（错误码 %s%s），将按新房间直接重连",
                    code, "：未提供房间密码" if code == "5" else "")
            else:
                self.ctx.logger.info("IIROSE 服务端已允许移动房间")
            return

        if kind == "room_password":
            self.ctx.logger.info("IIROSE 房间密码校验结果: %s（%s）", value[2:],
                                 "通过" if value[2:3] == "1" else "未通过")
            return

        if kind == "member":
            try:
                await self._handle_member_event(value)
            except Exception:
                self.ctx.logger.warning("处理 IIROSE 成员事件异常，已忽略", exc_info=True)
            return

        if kind in ("room", "private"):
            try:
                await self._dispatch_inbound(kind, value)
            except Exception:
                self.ctx.logger.warning("处理 IIROSE 入站报文异常，已忽略", exc_info=True)
            return

        self.ctx.logger.debug("IIROSE 未识别报文：%s", text[:200])

    def _build_components(self, text: str, self_uid: str,
                          self_name: str) -> list[Dict[str, Any]]:
        """把 IIROSE 的提及语法拆成 Host 标准组件。

        对照官方 `decoder/core/clearMsg.ts` 的三条规则：

        | IIROSE 写法 | 官方解析 | 这里交给 Host 的形式 |
        |---|---|---|
        | ` [*用户名*] ` | at（按用户名查用户） | `at` 组件，target_user_id 用查到的 UID |
        | ` [@uid@] ` | at by id（按 UID 查用户） | `at` 组件，target_user_id 直接用该 UID |
        | ` [_房间id_] ` | sharp（提及频道/房间） | 文本组件，渲染成**房间名**（Host 没有 sharp 概念） |

        提及机器人自己时 target_user_id 用机器人身份 ID，Host 才能判定「我被 @ 了」；
        提及别人时优先查本地用户目录把用户名换成真实 UID，查不到才退回用户名。
        """
        components: list[Dict[str, Any]] = []
        position = 0

        for match in MENTION_RE.finditer(text):
            head = text[position:match.start()]
            if head.strip():
                components.append({"type": "text", "data": head})

            name, room_id, uid = (match.group(1), match.group(2), match.group(3))
            if name is not None:
                name = decode_entities(name).strip()
                if self_name and name == self_name:
                    target_id = self_uid or name          # 让 Host 认出「被点名的是我」
                else:
                    target_id = self._uid_for_name(name) or name
                components.append({"type": "at", "data": {
                    "target_user_id": target_id,
                    "target_user_nickname": name,
                }})
            elif uid is not None:
                uid = uid.strip()
                components.append({"type": "at", "data": {
                    "target_user_id": uid,
                    # 官方是 at-by-id：名字由客户端按 UID 反查，这里能查到就顺带给上
                    "target_user_nickname": self._name_for_uid(uid),
                }})
            else:
                # @ 房间（官方叫 sharp）：不能当成「有人 @ 我」，渲染成房间名更好读
                room_key = (room_id or "").strip()
                label = self._room_name(room_key) or room_key
                components.append({"type": "text", "data": f"[{label}]"})
            position = match.end()

        tail = text[position:]
        if tail.strip():
            components.append({"type": "text", "data": tail})

        if not components:
            components.append({"type": "text", "data": text})
        return components

    # ---- 成员事件 ----

    async def _handle_member_event(self, event: Mapping[str, Any]) -> None:
        """成员上线 / 下线 / 重连 / 换房。

        日志永远记录；是否上报给 MaiBot 由 `bot.report_member_events` 控制。
        上报走 Host 的「通知消息」形态（对照官方 NapCat 适配器 codecs/notice：
        is_notify=True + display_message），不是伪造一条用户聊天。
        """
        settings = self._settings
        uid = str(event.get("user_id") or "")
        name = str(event.get("user_name") or "")
        self._remember("", user_id=uid, user_name=name,
                       timestamp=int(event.get("timestamp") or 0), text="")

        # 事件文案带上房间名：换房时「去了别的房间 → 放映社 (5b7ab80a2017d)」
        label_room = (str(event.get("target_room_id") or "") if event.get("is_move")
                      else (str(event.get("room_id") or "") or self._room_id))
        description = describe_member_event(event, self._describe_room(label_room))
        self.ctx.logger.info(
            "IIROSE 成员事件: %s（event=%s join_type=%s uid=%s room=%s target=%s）",
            description, event.get("event"), event.get("join_type") or "-",
            uid or "-", self._describe_room(str(event.get("room_id") or "")) or "-",
            self._describe_room(str(event.get("target_room_id") or "")) or "-")

        if not getattr(settings.bot, "report_member_events", True):
            return
        # 机器人自己的上下线没必要回灌给 Host，避免刷屏
        if uid and uid == self._self_uid:
            return

        # group_id 必须与聊天消息用同一个（配置里的房间），否则 Host 会把成员事件
        # 当成另一个会话，麦麦的记忆/上下文就被劈成两半。
        record_room = str(event.get("room_id") or "")
        room_id = self._room_id or record_room
        if record_room and self._room_id and record_room != self._room_id:
            key = f"member-room-mismatch:{record_room}"
            if key not in self._notice_warned:
                self._notice_warned.add(key)
                self.ctx.logger.info(
                    "IIROSE 成员事件里的房间=%s，按配置房间 %s 上报（保持与聊天同一会话）",
                    record_room, self._room_id)

        event_key = (f"{event.get('event')}:{event.get('join_type') or ''}:"
                     f"{uid}:{event.get('timestamp') or 0}")
        payload: Dict[str, Any] = {
            "message_id": f"iirose-member-{event_key}",
            "timestamp": str(float(event.get("timestamp") or time.time())),
            "platform": PLATFORM,
            "message_info": {
                "user_info": {"user_id": uid, "user_nickname": name},
                "additional_config": {
                    "self_id": self._account_id(),
                    "iirose_member_event": dict(event),
                },
            },
            "raw_message": [{"type": "text", "data": description}],
            "is_mentioned": False,
            "is_at": False,
            "is_emoji": False,
            "is_picture": False,
            "is_command": False,
            "is_notify": True,
            "session_id": "",
            "processed_plain_text": description,
            "display_message": description,
        }
        if room_id:
            payload["message_info"]["group_info"] = {
                "group_id": room_id,
                # 会话名用真实房间名，后台聊天列表才不会只显示一串房间号
                "group_name": self._room_name(room_id) or room_id,
            }
            payload["message_info"]["additional_config"]["platform_io_target_group_id"] = room_id

        route_metadata = {
            "self_id": self._account_id(),
            "connection_id": "primary",
            "room_id": room_id,
            "chat_type": "group",
        }
        try:
            accepted = await self._route_inbound_message(
                payload, route_metadata, payload["message_id"], payload["message_id"])
            if accepted:
                self.ctx.logger.info("IIROSE 成员事件已作为通知交给 Host: %s", description)
        except Exception as exc:
            self._warn_notice_failure(str(exc))

    def _warn_image_host_unset(self) -> None:
        """图床没配就只用占位文本，并且只提示一次。"""
        if self._image_host_warned:
            return
        self._image_host_warned = True
        self.ctx.logger.warning(
            "IIROSE 未配置图床地址（image_host.base_url），"
            "图片 / 表情会退化为 [图片] / [表情] 占位文本")

    def _warn_notice_failure(self, reason: str) -> None:
        """通知被 Host 拒绝时只提示一次，避免上线下线刷屏。"""
        if reason in self._notice_warned:
            self.ctx.logger.debug("IIROSE 通知再次被 Host 拒绝: %s", reason)
            return
        self._notice_warned.add(reason)
        self.ctx.logger.warning(
            "IIROSE 成员事件通知被 Host 拒绝（后续同类错误不再重复提示）: %s。"
            "如需完全关闭，把插件配置里的 bot.report_member_events 设为 false。", reason)

    async def _dispatch_inbound(self, kind: str, msg: Mapping[str, Any]) -> None:
        settings = self._settings
        user_id = str(msg.get("user_id") or "")
        user_name = str(msg.get("user_name") or "")
        text = str(msg.get("text") or "")
        message_id = str(msg.get("message_id") or "")
        timestamp = int(msg.get("timestamp") or 0)

        if self._self_uid and user_id == self._self_uid:
            return  # 机器人自己发的消息
        if not text.strip():
            return
        # 消息 id 有两种形态：服务器生成的纯数字，以及客户端带 id 时的
        # `<客户端id><服务器时间戳`（实测 `411952792881<1789271137`，自己的消息回显也是这种）。
        # 之前用 isalnum() 判断，把后者整条丢掉了 —— 别人 @ 别人时麦麦会完全看不到。
        if not any(ch.isdigit() for ch in message_id):
            self.ctx.logger.debug("IIROSE 跳过没有消息 id 的记录: %s", str(msg.get("raw"))[:200])
            return

        if not self._self_uid and self._username \
                and user_name == self._username:
            return  # uid 未配置时的兜底：按昵称过滤机器人自己发的消息

        room_id = self._room_id or str(msg.get("room_id") or "")

        # 黑白名单：在投递给麦麦之前就拦掉，麦麦根本看不到这些消息
        allowed, reason = self._chat_list_check(kind, user_id, user_name, room_id)
        if not allowed:
            self._log_dropped_chat(user_id, user_name, reason)
            return

        # 先登记，再判断卡片/引用：这样回复时能还原 `发送者_时间戳秒` 和旧正文
        replies, tail = split_quotes(text)
        card = parse_media_card(tail)
        self._remember(message_id, user_id=user_id, user_name=user_name,
                       timestamp=timestamp, text=normalize_inbound_text(tail))

        message_info: Dict[str, Any] = {
            "user_info": {"user_id": user_id, "user_nickname": user_name},
            "platform": PLATFORM,
            "message_id": message_id,
            "additional_config": {},
        }
        if kind == "room":
            # group_name 用真实房间名：后台聊天列表/会话名读的就是它，
            # 之前两个字段都填房间号，所以界面上群聊显示不出名字。
            message_info["group_info"] = {
                "group_id": room_id,
                "group_name": self._room_name(room_id) or room_id,
            }

        route_metadata = {
            "self_id": self._account_id(),
            "connection_id": "primary",
            "room_id": room_id,
            "chat_type": "group" if kind == "room" else "private",
        }

        # 引用块里的 @ 语法必须降级成纯文本：否则 `[*机器人名*]` 会让 Host 以为被点名，
        # 从而对一条「引用别人旧消息」的内容强制触发回复。
        # 注意只处理引用部分：新正文里的 @ 是真的在叫机器人，要原样交给 _build_components。
        visible = text
        if card is not None:
            visible = describe_media_card(card)
            message_info["additional_config"]["iirose_media_card"] = dict(card)
            self.ctx.logger.info("IIROSE 识别到%s卡片: %s（原始 %s）",
                                 "点播" if card["is_video"] else "点歌", visible, tail[:80])
        elif replies:
            quoted = " ／ ".join(
                f"{item['who']}：{strip_mentions(item['message'])}" for item in replies)
            visible = f"[引用 {quoted}] {tail}".strip()
            self.ctx.logger.info("IIROSE 入站引用（%s）已展开为可见文本: %s",
                                 ", ".join(f"{item['who']}_{item['ref']}" for item in replies),
                                 visible[:80])
        visible = normalize_inbound_text(visible)

        # 组件规范（对照官方 NapCat 适配器）：text → {"type":"text","data":str}
        #                                      at   → {"type":"at","data":{...}}
        canonical = self._build_components(visible,
                                           self_uid=self._self_uid,
                                           self_name=self._username)
        plain = [{"type": "text", "data": visible}]
        component_variants: list[list[Dict[str, Any]]] = [canonical]
        if canonical != plain:
            component_variants.append(plain)   # 万一 at 组件被宿主拒，退回纯文本


        last_error: Optional[BaseException] = None
        for components in component_variants:
            payload: Dict[str, Any] = {
                "message_id": message_id,
                "platform": PLATFORM,
                "message_info": message_info,
                "raw_message": components,
            }
            try:
                accepted = await self._route_inbound_message(
                    payload, route_metadata, message_id, message_id)
            except Exception as exc:
                last_error = exc
                self.ctx.logger.warning(
                    "IIROSE 入站投递失败（raw_message 形态 %s）: %s",
                    json.dumps(components, ensure_ascii=False)[:60], exc)
                continue

            if accepted:
                self.ctx.logger.info(
                    "IIROSE 入站消息已交给 Host（raw_message 形态 %s）: id=%s user=%s text=%s",
                    json.dumps(components, ensure_ascii=False)[:60], message_id, user_id, text[:50])
                return
            self.ctx.logger.warning("Host 未接收入站消息: %s", message_id)
            return

        self.ctx.logger.error("IIROSE 入站消息全部形态均被拒绝，最后一个错误: %s", last_error)

    async def _route_inbound_message(self, payload: Dict[str, Any],
                                     route_metadata: Dict[str, Any],
                                     external_message_id: str,
                                     dedupe_key: str) -> bool:
        """调用 ctx.gateway.route_message；宿主侧校验错误会抛出，由调用方决定是否换 payload 形态。"""
        route_fn = self.ctx.gateway.route_message

        try:
            return bool(await route_fn(
                gateway_name=GATEWAY_NAME,
                message=payload,
                route_metadata=route_metadata,
                external_message_id=external_message_id,
                dedupe_key=dedupe_key,
            ))
        except TypeError as exc:
            if "keyword" not in str(exc) and "argument" not in str(exc):
                raise
            self.ctx.logger.debug("首选 route_message 形态不匹配: %s", exc)

        fallbacks: list[Callable[[], Awaitable[Any]]] = [
            lambda: route_fn(gateway_name=GATEWAY_NAME, payload=payload,
                             route_metadata=route_metadata,
                             external_message_id=external_message_id,
                             dedupe_key=dedupe_key),
            lambda: route_fn(payload, gateway_name=GATEWAY_NAME,
                             route_metadata=route_metadata,
                             external_message_id=external_message_id,
                             dedupe_key=dedupe_key),
            lambda: route_fn(GATEWAY_NAME, payload),
        ]
        for index, call in enumerate(fallbacks):
            try:
                return bool(await call())
            except TypeError as exc:
                if "keyword" not in str(exc) and "argument" not in str(exc):
                    raise
                self.ctx.logger.debug("route_message 兜底形态 %d 不匹配: %s", index, exc)
        self.ctx.logger.error("无法调用 ctx.gateway.route_message，请核对 SDK 版本")
        return False

    # ---- 出站 ----

    @MessageGateway(
        name=GATEWAY_NAME,
        route_type="duplex",
        platform=PLATFORM,
        protocol="iirose",
        description="IIROSE 蔷薇花园 WebSocket 双工消息网关",
    )
    async def handle_iirose_gateway(self, message: Dict[str, Any],
                                    route: Optional[Dict[str, Any]] = None,
                                    metadata: Optional[Dict[str, Any]] = None,
                                    **kwargs: Any) -> Dict[str, Any]:
        del metadata, kwargs
        settings, client = self._settings, self._client

        raw_dump = json.dumps(message.get("raw_message"), ensure_ascii=False)[:400]

        if client is None or not client.connected:
            self.ctx.logger.warning("IIROSE 出站失败：连接未就绪（raw_message=%s）", raw_dump)
            return {"success": False, "error": "IIROSE 连接未就绪"}
        if settings.bot.only_hang_up:
            self.ctx.logger.warning("IIROSE 出站被拦截：静默模式（only_hang_up）已开启")
            return {"success": False, "error": "静默模式已开启，未发送"}

        text = await self._extract_text(message)
        if not text:
            self.ctx.logger.warning(
                "IIROSE 出站失败：无法从消息组件中解析出可发送内容（raw_message=%s）", raw_dump)
            return {"success": False, "error": "IIROSE 适配器无法解析该出站消息（仅支持文本 / 图片 URL / @）"}

        kind, target_id = self._resolve_target(message, route or {})
        if not target_id:
            self.ctx.logger.warning("IIROSE 出站失败：无法确定目标（raw_message=%s）", raw_dump)
            return {"success": False, "error": "IIROSE 无法确定发送目标（房间 ID / 用户 UID 均为空）"}

        try:
            raw = (encode_private_message(target_id, text, settings.connection.mc)
                   if kind == "private" else encode_room_message(text, settings.connection.mc))
            await client.send(raw)
            self.ctx.logger.info(
                "IIROSE 已发送出站消息: type=%s target=%s len=%d（配置房间=%s）",
                kind, target_id, len(text), self._room_id or "-")
        except Exception as exc:
            self.ctx.logger.warning("IIROSE 出站发送异常: %s", exc, exc_info=True)
            return {"success": False, "error": str(exc)}

        internal_id = str(message.get("message_id") or "").strip()
        return {
            "success": True,
            "external_message_id": internal_id or None,
            "metadata": {"protocol": "iirose", "target": {"type": kind, "id": target_id}},
        }

    # ---- 出站消息解码 ----
    #
    # 对照官方 NapCat 适配器 codecs/outbound/segment_encoder.py，Host 下发的
    # raw_message 是「组件列表」，且文本正文放在 data 里（data 是字符串，不是对象）：
    #   text   → {"type": "text",  "data": "正文"}
    #   at     → {"type": "at",    "data": {"target_user_id": ..., "target_user_nickname": ...}}
    #   reply  → {"type": "reply", "data": {"target_message_id": ...}}
    #   image  → {"type": "image", "data": {"file": "base64://..."}} / binary_data_base64
    #   imageurl → {"type": "imageurl", "data": {"file": "https://..."}}
    #
    # 转成 IIROSE 原生语法（见 iirose-re-docs 5.1）：
    #   @用户 → ` [*用户名*] `；引用 → `旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`；
    #   图片 → `[url#e]`。

    _URL_TYPES = ("image", "imageurl", "emoji", "face", "voice", "voiceurl",
                  "record", "video", "videourl", "music")

    # ---- 图床上传 ----

    @staticmethod
    def _post_multipart(url: str, fields: Mapping[str, str],
                        files: Mapping[str, tuple[str, bytes, str]], timeout: float) -> str:
        """同步 POST multipart；由 `_upload_image` 丢到线程里执行。"""
        boundary = f"----MaiBotIIROSE{uuid.uuid4().hex}"
        body = build_multipart(fields, files, boundary)
        request = urllib.request.Request(
            url, data=body, method="POST",
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(len(body)),
                "User-Agent": "MaiBot-IIROSE-Adapter",
                "Accept": "application/json",
            })
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace")

    async def _upload_image(self, b64: str, hint: str = "") -> str:
        """把 base64 图片传到图床换回 URL；任何一步失败都返回空串（调用方退回占位文本）。

        上传前先按「图片内容指纹（sha256）」查缓存：命中就直接复用之前拿到的链接，
        不再重复上传。缓存会落盘到插件数据目录，插件重启 / 热重载后依然有效。
        """
        settings = self._settings
        host = getattr(settings, "image_host", None)
        if host is None or not getattr(host, "enabled", False):
            return ""

        payload, content_type = self._decode_base64_image(b64, hint)
        if not payload:
            return ""

        digest = hashlib.sha256(payload).hexdigest()
        reuse = bool(getattr(host, "reuse_uploaded", True))
        verify = bool(getattr(host, "verify_upload", True))
        try:
            ttl_hours = float(getattr(host, "verify_interval_hours", 24.0) or 0.0)
        except (TypeError, ValueError):
            ttl_hours = 24.0
        ttl = max(0.0, ttl_hours) * 3600

        # ---- 1) 命中缓存：先确认链接还能取到图片，取不到就重传 ----
        if reuse:
            await self._ensure_upload_cache_loaded()
            # 内存里存的一律是 (url, checked_at) 元组
            entry = self._upload_cache.get(digest)
            if entry is not None:
                self._upload_cache.move_to_end(digest)
                cached_url, checked_at = entry
                fresh = bool(ttl) and (time.time() - checked_at) < ttl
                if not verify or fresh:
                    self._upload_hits += 1
                    self.ctx.logger.info(
                        "IIROSE 复用已上传图片，跳过上传: %s（累计复用 %d 次）",
                        cached_url, self._upload_hits)
                    return cached_url

                usable, detail, definitive = await self._verify_image_url(cached_url)
                if usable or not definitive:
                    # 探测本身失败时不能断定图片坏了，继续复用，避免无谓重传
                    if usable:
                        self.ctx.logger.info("IIROSE 复用已上传图片（链接校验通过：%s）: %s"
                                             "（累计复用 %d 次）", detail, cached_url, self._upload_hits + 1)
                    else:
                        self.ctx.logger.warning(
                            "IIROSE 无法确认缓存链接是否有效（%s），继续复用: %s", detail, cached_url)
                    self._store_entry(digest, cached_url, time.time())
                    await asyncio.to_thread(self._write_upload_cache)
                    self._upload_hits += 1
                    return cached_url

                self.ctx.logger.warning(
                    "IIROSE 缓存的图床链接已失效（%s），丢弃并重新上传: %s", detail, cached_url)
                self._upload_cache.pop(digest, None)
                await asyncio.to_thread(self._write_upload_cache)

        max_bytes = int(getattr(host, "max_bytes", 0) or 0)
        if max_bytes and len(payload) > max_bytes:
            self.ctx.logger.warning(
                "IIROSE 图片过大（%d 字节 > %d），跳过上传", len(payload), max_bytes)
            return ""

        base_url = str(host.base_url or DEFAULT_IMAGE_HOST).strip().rstrip("/")
        if not base_url:
            self._warn_image_host_unset()
            return ""
        endpoint = base_url + "/" + str(host.upload_path or DEFAULT_IMAGE_HOST_PATH).lstrip("/")
        fields: Dict[str, str] = {}
        token = str(getattr(host, "token", "") or "").strip()
        if token:
            fields["token"] = token
        file_field = str(getattr(host, "field_name", "image") or "image")
        timeout = max(1.0, int(getattr(host, "timeout_ms", 15000) or 15000) / 1000)
        extension = guess_extension(content_type, hint)

        # ---- 2) 上传；上传后校验链接，取不到图片就重传一次 ----
        attempts = 2 if verify else 1
        for attempt in range(1, attempts + 1):
            filename = f"maibot-{uuid.uuid4().hex[:12]}.{extension}"
            files = {file_field: (filename, payload, content_type or "image/png")}
            try:
                text = await asyncio.to_thread(
                    self._post_multipart, endpoint, fields, files, timeout)
            except urllib.error.HTTPError as exc:
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", "replace")[:200]
                except Exception:
                    pass
                self.ctx.logger.warning("IIROSE 图床上传失败 HTTP %s: %s", exc.code, detail)
                return ""
            except Exception as exc:
                self.ctx.logger.warning("IIROSE 图床上传失败: %s", exc)
                return ""

            url = parse_upload_response(text, base_url)
            if not url:
                reason = describe_upload_response(text)
                self.ctx.logger.warning("IIROSE 图床上传被拒绝: %s", reason)
                if "token" in reason.lower():
                    self.ctx.logger.warning(
                        "IIROSE 图床 token 不正确或未配置：请在插件配置的 image_host.token 里填入图床 token"
                        "（图床的 tokenList 文件里查）")
                return ""

            if verify:
                usable, detail, definitive = await self._verify_image_url(url)
                if usable:
                    self.ctx.logger.info("IIROSE 上传后的链接校验通过: %s（%s）", url, detail)
                elif not definitive:
                    # 探测失败但不算「确认坏」：信任上传接口的成功结果，照常发送
                    self.ctx.logger.warning(
                        "IIROSE 无法确认刚上传的链接（%s），按上传成功结果继续发送: %s", detail, url)
                else:
                    self.ctx.logger.warning(
                        "IIROSE 第 %d/%d 次上传的链接取不到图片（%s）: %s",
                        attempt, attempts, detail, url)
                    if attempt < attempts:
                        continue
                    return ""

            if reuse:
                self._store_entry(digest, url, time.time())
                await asyncio.to_thread(self._write_upload_cache)
            self.ctx.logger.info("IIROSE 图片已上传图床: %s（%d 字节）", url, len(payload))
            return url

        return ""

    # ---- 图床缓存的落盘与载入 ----

    @staticmethod
    def _parse_cache_entry(value: Any) -> Optional[tuple[str, float]]:
        """解析缓存文件里的值：旧版是裸 URL 字符串，新版是 {"url","checked"}。"""
        if isinstance(value, str):
            return (value, 0.0) if value.startswith("http") else None
        if isinstance(value, Mapping):
            url = str(value.get("url") or "").strip()
            if not url.startswith("http"):
                return None
            checked = value.get("checked")
            if isinstance(checked, bool) or not isinstance(checked, (int, float)):
                checked = 0.0
            return url, float(checked)
        return None

    def _store_entry(self, digest: str, url: str, checked_at: float) -> None:
        self._upload_cache[digest] = (url, checked_at)
        self._upload_cache.move_to_end(digest)
        while len(self._upload_cache) > UPLOAD_CACHE_SIZE:
            self._upload_cache.popitem(last=False)

    @staticmethod
    def _probe_image_url(url: str, timeout: float) -> tuple[bool, str, bool]:
        """同步探测链接是不是真的能取到图片。

        返回 `(可用, 说明, 是否确定)`。`确定=False` 表示探测本身失败（超时 / DNS / 连不上），
        这种情况不能据此判定图片坏了，调用方会从宽处理。
        """
        request = urllib.request.Request(
            url, method="GET",
            headers={
                "Range": "bytes=0-2047",
                "User-Agent": "MaiBot-IIROSE-Adapter",
                "Accept": "image/*,*/*;q=0.8",
            })
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = getattr(response, "status", 200) or 200
                content_type = str(response.headers.get("Content-Type") or "").lower()
                head = response.read(2048)
        except urllib.error.HTTPError as exc:
            # 服务端明确回答了 → 这条链接确实取不到图
            return False, f"HTTP {exc.code}", True
        except Exception as exc:
            return False, str(exc), False

        if status not in (200, 206):
            return False, f"HTTP {status}", True
        if content_type.startswith("image/"):
            return True, content_type, True
        sniffed = sniff_image_type(head or b"")
        if sniffed:
            return True, f"{content_type or '无 Content-Type'}（文件头是 {sniffed}）", True
        return False, f"Content-Type={content_type or '-'} 且文件头不是图片", True

    async def _verify_image_url(self, url: str) -> tuple[bool, str, bool]:
        """校验图床链接；探测本身异常时返回「不确定」，由调用方从宽处理。"""
        settings = self._settings
        host = getattr(settings, "image_host", None)
        timeout_ms = int(getattr(host, "verify_timeout_ms", 5000) or 5000)
        try:
            return await asyncio.to_thread(self._probe_image_url, url, max(1.0, timeout_ms / 1000))
        except Exception as exc:
            return False, str(exc), False

    def _cache_path(self) -> Optional[Path]:
        """插件数据目录（ctx.paths.data_dir）下的缓存文件路径；拿不到就退化成纯内存。"""
        try:
            data_dir = getattr(getattr(self.ctx, "paths", None), "data_dir", None)
        except Exception:
            return None
        if not data_dir:
            return None
        try:
            return Path(data_dir) / UPLOAD_CACHE_FILE
        except Exception:
            return None

    async def _ensure_upload_cache_loaded(self) -> None:
        if self._upload_cache_loaded:
            return
        self._upload_cache_loaded = True
        path = self._cache_path()
        if path is None:
            return
        try:
            raw = await asyncio.to_thread(path.read_text, "utf-8")
        except FileNotFoundError:
            return
        except Exception:
            self.ctx.logger.debug("图床缓存读取失败，按空缓存继续", exc_info=True)
            return

        try:
            payload = json.loads(raw)
        except Exception:
            self.ctx.logger.warning("图床缓存文件损坏，已按空缓存继续: %s", path)
            return
        if not isinstance(payload, Mapping):
            return

        for digest, value in payload.items():
            if not isinstance(digest, str):
                continue
            entry = self._parse_cache_entry(value)
            if entry is not None:
                self._upload_cache[digest] = entry
        while len(self._upload_cache) > UPLOAD_CACHE_SIZE:
            self._upload_cache.popitem(last=False)
        self.ctx.logger.info("图床缓存已载入 %d 条（%s），相同图片不会再重复上传",
                             len(self._upload_cache), path)

    def _write_upload_cache(self) -> None:
        """原子写：先写临时文件再替换，避免写一半把缓存写坏。"""
        path = self._cache_path()
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {digest: {"url": url, "checked": checked}
                       for digest, (url, checked) in self._upload_cache.items()}
            temp = path.with_name(path.name + ".tmp")
            temp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            temp.replace(path)
        except Exception:
            self.ctx.logger.debug("图床缓存写入失败（不影响发送）", exc_info=True)

    @staticmethod
    def _decode_base64_image(value: str, hint: str = "") -> tuple[bytes, str]:
        """解析 `data:` URI / `base64://` / 裸 base64，返回 (字节, MIME)。"""
        raw = (value or "").strip()
        if not raw:
            return b"", ""
        content_type = ""
        if raw.startswith("data:"):
            header, _, payload = raw.partition(",")
            if ";" in header:
                content_type = header[5:].split(";")[0].strip()
            raw = payload
        elif raw.startswith("base64://"):
            raw = raw[len("base64://"):]

        try:
            payload = base64.b64decode(raw, validate=False)
        except Exception:
            return b"", ""
        if not payload:
            return b"", ""
        # MIME 优先级：data URI 里写的 > magic bytes > 扩展名提示 > png
        if not content_type:
            content_type = sniff_image_type(payload)
        if not content_type:
            content_type = "image/" + guess_extension("", hint)
        return payload, content_type

    @staticmethod
    def _component_base64(data: Any, component: Mapping[str, Any]) -> tuple[str, str]:
        """从组件里取出 base64 图片源，返回 (base64 字符串, 扩展名提示)。"""
        candidates: list[tuple[str, str]] = []
        if isinstance(data, Mapping):
            for key in ("file", "base64", "data", "content", "src"):
                item = data.get(key)
                if isinstance(item, str):
                    candidates.append((item, str(data.get("filename") or "")))
        binary = component.get("binary_data_base64")
        if isinstance(binary, str):
            candidates.append((binary, ""))
        if isinstance(data, str):
            candidates.append((data, ""))

        for value, hint in candidates:
            text = value.strip()
            if text.startswith("base64://") or text.startswith("data:"):
                return text, hint
        # 单独的 binary_data_base64 字段是裸 base64
        if isinstance(binary, str) and binary.strip():
            return binary.strip(), ""
        return "", ""

    _MEDIA_PLACEHOLDERS = {
        "emoji": "[表情]", "face": "[表情]",
        "image": "[图片]", "imageurl": "[图片]",
        "voice": "[语音]", "voiceurl": "[语音]", "record": "[语音]",
        "video": "[视频]", "videourl": "[视频]",
        "music": "[音乐]",
    }

    def _media_placeholder(self, ctype: str) -> str:
        return self._MEDIA_PLACEHOLDERS.get(ctype, "[媒体]")

    @staticmethod
    def _component_text(value: Any) -> str:
        """从组件的 data / text / content 字段里取出纯文本，兼容字符串与对象两种形态。"""
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            for key in ("text", "content", "value", "plain_text"):
                item = value.get(key)
                if isinstance(item, str) and item:
                    return item
        return ""

    @staticmethod
    def _component_url(value: Any, component: Mapping[str, Any]) -> str:
        """从组件里提取可用 URL；base64 之类的本地数据直接丢弃（IIROSE 只吃 URL）。"""
        candidates: list[Any] = [value, component.get("data"), component.get("url"),
                                 component.get("file"), component.get("binary_data_base64")]
        for candidate in candidates:
            if isinstance(candidate, Mapping):
                for key in ("url", "file", "src", "href"):
                    item = candidate.get(key)
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
            elif isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                return candidate
        return ""

    def _resolve_reply(self, target_id: str, item_data: Mapping[str, Any]) -> tuple[str, str, str]:
        """把 Host 的引用组件还原成 (旧正文, 发送者, **被引用消息的时间戳秒**)。

        标记里那个数字是**时间戳（秒）**，不是消息 id —— IIROSE 客户端会把它按日期渲染：
        - 传 12 位随机消息 id（如 `828102986562`）→ 客户端显示成 **6088 年**这种离谱日期；
        - 传 10 位秒级时间戳（如 `1789272020`）→ 正常显示。
        官方 `PublicMessage.ts` 解析出来也是存进名为 `time` 的字段，
        third-party.md 同样写作「发送者_时间戳秒」。
        （`messages.md` 散文里那句「发送者_消息id」是文档误记，别照抄。）

        时间戳只能从本地近期消息缓存里拿（Host 的 reply 组件只给消息 id）。
        拿不到就**不发引用**，退化成普通文本——宁可少一个引用框，也不发一个错误日期。
        """
        cached = self._recent.get(target_id) if target_id else None
        if cached:
            who = str(cached.get("user_name") or "").strip()
            timestamp = str(cached.get("timestamp") or "").strip()
            quoted = str(cached.get("text") or "").strip()
            if who and timestamp.isdigit() and QUOTE_TS_MIN <= int(timestamp) <= QUOTE_TS_MAX:
                return quoted, who, timestamp
            if who:
                self.ctx.logger.info(
                    "IIROSE 引用的时间戳不合理（%s），按普通文本发送: %s",
                    timestamp or "-", target_id or "-")

        # 缓存里没有（或时间戳异常）：组件自带的昵称也救不了时间戳，直接不引用
        self.ctx.logger.info(
            "IIROSE 出站引用无法还原时间戳，按普通文本发送: target_message_id=%r 组件=%s 近期缓存=%d 条",
            target_id or "-", json.dumps(dict(item_data), ensure_ascii=False)[:200],
            len(self._recent))
        return "", "", ""

    async def _extract_text(self, message: Mapping[str, Any]) -> str:
        raw = message.get("raw_message")
        if isinstance(raw, str):
            return raw.strip()

        if isinstance(raw, Mapping):
            comps = raw.get("components")
            components: list[Any] = comps if isinstance(comps, list) else []
        elif isinstance(raw, list):
            components = raw
        else:
            components = []

        chunks: list[str] = []
        quote: tuple[str, str, str] = ("", "", "")
        # 只要输出里出现过 @ / 房间提及标记，整体就不能再做 strip（见下方说明）
        has_mention = False
        # (chunks 下标, 扩展名提示, base64) —— 上传成功后原地替换占位文本
        pending: list[tuple[int, str, str]] = []

        for comp in components:
            if not isinstance(comp, Mapping):
                if isinstance(comp, str):
                    chunks.append(comp)
                continue

            ctype = str(comp.get("type") or "").strip().lower()
            data = comp.get("data")

            if ctype in ("", "text", "plain"):
                chunk = (self._component_text(data)
                         or self._component_text(comp.get("text"))
                         or self._component_text(comp.get("content")))
                if chunk:
                    chunks.append(chunk)
                continue

            if ctype == "at":
                item_data = data if isinstance(data, Mapping) else {}
                uid = str(item_data.get("target_user_id") or "").strip()
                # IIROSE 的 `[*名字*]` 只认「用户名」，必须和对方登录名完全一致才生效。
                # Host 给的 cardname 是群名片、nickname 可能是备注，用它们发出去
                # 会变成一段普通文字而不是真的 @。所以优先用本地目录里
                # 从房间报文拿到的用户名（记录字段 2 就是用户名）。
                name = self._name_for_uid(uid) if uid else ""
                if not name:
                    name = str(item_data.get("target_user_nickname")
                               or self._component_text(data) or "").strip()
                if name:
                    chunks.append(f" [*{name}*] ")       # 官方 @ 用户写法（两侧空格）
                    has_mention = True
                elif uid:
                    # 查不到名字就退化成官方支持的 at-by-id（客户端按 UID 反查）
                    chunks.append(f" [@{uid}@] ")
                    has_mention = True
                else:
                    chunks.append(" " + str(
                        item_data.get("target_user_cardname") or "").strip() + " ")
                    self.ctx.logger.info(
                        "IIROSE 出站 at 组件既没有 UID 也没有用户名，已降级为纯文本")
                continue

            if ctype == "reply":
                item_data = data if isinstance(data, Mapping) else {}
                target_id = str(item_data.get("target_message_id")
                                or self._component_text(data) or "").strip()
                quoted, who, timestamp = self._resolve_reply(target_id, item_data)
                if who and timestamp:
                    quote = (quoted, who, timestamp)
                continue

            if ctype in self._URL_TYPES:
                url = self._component_url(data, comp)
                if url:
                    chunks.append(f" {format_image(url)} ")
                    continue
                # 没有现成 URL：base64 图片 / 表情先传图床换链接，再按图片语法发
                encoded, hint = self._component_base64(data, comp)
                if encoded and ctype in MEDIA_IMAGE_TYPES:
                    pending.append((len(chunks), hint, encoded))
                    chunks.append(f" {self._media_placeholder(ctype)} ")
                    continue
                # 对照官方 NapCat 适配器的降级策略：转换不了的资源用占位文本，
                # 避免一条「只有图/表情」的回复整条发不出去。
                chunks.append(f" {self._media_placeholder(ctype)} ")
                self.ctx.logger.info(
                    "IIROSE 出站把 %s 组件降级为占位文本（无法取得 URL）", ctype)
                continue

            fallback = self._component_text(data) or self._component_text(comp.get("content"))
            if fallback:
                chunks.append(fallback)

        for index, hint, encoded in pending:
            url = await self._upload_image(encoded, hint)
            chunks[index] = f" {format_image(url)} " if url else chunks[index]

        if not chunks and not quote[1]:
            # 兜底：个别宿主版本不把正文放进 raw_message 组件，而是放在顶层字段
            for key in ("plain_text", "content", "text"):
                value = message.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        body = "".join(chunks)
        if not has_mention:
            # 只有纯文本时才去掉首尾空白。
            # **一旦有 @ / 引用标记就不能 strip**：协议要求 `@用户` 写成 ` [*用户名*] `
            # （两侧空格是语法的一部分），官方解析正则也是 `(\s+)(\[\*…\*\])(\s)`。
            # 把前面的空格 strip 掉，接收端就认不出这是一个 @，只会显示成一段普通文字。
            body = body.strip()
        if quote[1]:
            # 引用消息：`旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`
            return format_quote(strip_mentions(quote[0]), quote[1], quote[2], body)
        return body

    def _resolve_target(self, message: Mapping[str, Any], route: Mapping[str, Any]) -> tuple[str, str]:
        """确定发送目标：群/房间 → room，私聊 → private。

        目标 ID 的优先级对齐官方 NapCat 适配器：group_info.group_id →
        additional_config.platform_io_target_group_id / 用户 ID → route.target_user_id
        → user_info.user_id（回退）。

        注意：房间消息的报文里**不带房间 id**（`{"m","mc","i"}`，房间由登录态决定），
        而且 Host 回传的 `group_info.group_id` 是它自己的会话 id（实测是 `6a7c38409e902`
        这种），不是 IIROSE 房间 id。所以 room 分支一律用配置里的房间，避免日志误导。
        """
        settings = self._settings
        group_id = ""
        user_id = ""
        route_target_user = str(route.get("target_user_id") or "").strip()

        info = message.get("message_info")
        if isinstance(info, Mapping):
            group = info.get("group_info")
            if isinstance(group, Mapping):
                group_id = str(group.get("group_id") or "").strip()
            additional = info.get("additional_config")
            if isinstance(additional, Mapping):
                group_id = group_id or str(additional.get("platform_io_target_group_id") or "").strip()
                user_id = str(additional.get("platform_io_target_user_id")
                              or additional.get("target_user_id") or "").strip()
            user = info.get("user_info")
            if isinstance(user, Mapping):
                sender_id = str(user.get("user_id") or "").strip()
                # user_info 里是发送者；只有私聊场景它才是接收方
                user_id = user_id or ("" if group_id else sender_id)

        chat_type = str(route.get("chat_type") or "").lower()
        if group_id or chat_type == "group":
            if group_id and self._room_id and group_id != self._room_id:
                # Host 回传的是它自己的会话 id；房间消息报文不带房间号，
                # 所以一律按配置房间发送（只提示一次，避免刷屏）。
                key = f"group-mismatch:{group_id}"
                if key not in self._notice_warned:
                    self._notice_warned.add(key)
                    self.ctx.logger.info(
                        "IIROSE 出站目标：Host 的 group_id=%s，实际按配置房间 %s 发送"
                        "（房间消息报文本身不含房间号）", group_id, self._room_id)
            return "room", self._room_id or group_id
        if user_id or route_target_user:
            return "private", user_id or route_target_user
        return "room", self._room_id


def create_plugin() -> IIRoseAdapterPlugin:
    return IIRoseAdapterPlugin()
