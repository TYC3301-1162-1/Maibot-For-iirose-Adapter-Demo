"""出站解码回归测试（不依赖 MaiBot / maibot_sdk）。

用法：python test_outbound_decode.py

覆盖 Host 实际下发的 raw_message 形态 —— 来自官方 NapCat 适配器
codecs/outbound/segment_encoder.py 的构造结果：
    text  → {"type": "text",  "data": "正文"}        ← data 是字符串，不是对象
    at    → {"type": "at",    "data": {"target_user_id": ..., "target_user_nickname": ...}}
    image → {"type": "image", "data": {"file": "base64://..."}}
    imageurl → {"type": "imageurl", "data": {"file": "https://..."}}
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
import types
import uuid


# ---------------- 最小 maibot_sdk 桩 ----------------

def _make_stub() -> None:
    if "maibot_sdk" in sys.modules:
        return
    module = types.ModuleType("maibot_sdk")

    def Field(default=None, *, default_factory=None, description="", **kwargs):  # noqa: N802
        del description, kwargs
        return default_factory() if default_factory is not None else default

    class PluginConfigBase:
        pass

    class MaiBotPlugin:
        config_model = None
        config = None

    def MessageGateway(**kwargs):
        del kwargs

        def decorate(func):
            return func

        return decorate

    module.Field = Field
    module.PluginConfigBase = PluginConfigBase
    module.MaiBotPlugin = MaiBotPlugin
    module.MessageGateway = MessageGateway
    sys.modules["maibot_sdk"] = module


_make_stub()

import plugin as adapter  # noqa: E402

# 测试里会故意制造失败路径，日志只保留严重级别，避免刷屏
logging.getLogger("iirose-test").setLevel(logging.CRITICAL)


class _FakeGateway:
    def __init__(self) -> None:
        self.payloads: list[dict] = []
        self.states: list[dict] = []

    async def route_message(self, gateway_name, message, route_metadata=None,
                            external_message_id="", dedupe_key=""):
        del gateway_name, route_metadata, external_message_id, dedupe_key
        self.payloads.append(message)
        return True

    async def update_state(self, **kwargs):
        self.states.append(kwargs)
        return True


def _plugin(data_dir=None) -> adapter.IIRoseAdapterPlugin:
    if adapter.IIRoseAdapterPlugin.config is None:
        adapter.IIRoseAdapterPlugin.config = adapter.IIRosePluginConfig()
    config = adapter.IIRoseAdapterPlugin.config
    config.plugin.enabled = True
    config.account.username = "03酱"
    config.account.uid = "62b98115cefd2"
    config.account.room_id = "64d5f8e17b2ad"
    config.image_host.enabled = True
    config.image_host.base_url = "https://img.example.com"
    config.image_host.upload_path = "/api/index.php"
    config.image_host.token = "test-token"
    config.image_host.reuse_uploaded = True
    config.chat_list.enabled = False
    config.chat_list.user_list_enabled = False
    config.chat_list.user_list_type = "whitelist"
    config.chat_list.user_list = []
    config.chat_list.ban_user_enabled = False
    config.chat_list.ban_user_list = []
    config.chat_list.room_list_enabled = False
    config.chat_list.room_list_type = "blacklist"
    config.chat_list.room_list = []
    config.chat_list.log_dropped = False

    instance = adapter.IIRoseAdapterPlugin()
    instance.ctx = types.SimpleNamespace(  # type: ignore[attr-defined]
        logger=logging.getLogger("iirose-test"), gateway=_FakeGateway(),
        paths=types.SimpleNamespace(data_dir=data_dir))
    # 预置本地目录 / 近期消息，模拟入站跑过一轮之后的状态
    instance._remember("760771550794", user_id="61f6cdbcdca5f", user_name="Es.",
                       timestamp=1789213830, text="图床修好啦")
    instance._remember("770000000001", user_id="65abb7a99dc60", user_name="柒洛",
                       timestamp=1789213900, text="图片")
    return instance


def _message(raw_message, *, group_id="", user_id="u9", additional=None):
    info = {
        "user_info": {"user_id": user_id, "user_nickname": "Es."},
        "additional_config": additional or {},
    }
    if group_id:
        info["group_info"] = {"group_id": group_id, "group_name": group_id}
    return {"message_id": "m1", "platform": "iirose", "message_info": info,
            "raw_message": raw_message}


CASES = [
    # (说明, message, 期望文本, 期望 kind, 期望 target)
    (
        "Host 文本组件（data 为字符串）—— 本次修复的主目标",
        _message([{"type": "text", "data": "你好呀"}]),
        "你好呀", "private", "u9",
    ),
    (
        "多段文本拼接",
        _message([{"type": "text", "data": "前半"},
                  {"type": "text", "data": "后半"}]),
        "前半后半", "private", "u9",
    ),
    (
        "data 为对象的旧形态仍兼容",
        _message([{"type": "text", "data": {"text": "对象里的正文"}}]),
        "对象里的正文", "private", "u9",
    ),
    (
        "纯字符串 raw_message",
        _message("直接是字符串"),
        "直接是字符串", "private", "u9",
    ),
    (
        "群消息 + at 组件",
        _message([{"type": "at", "data": {"target_user_id": "61f6cdbcdca5f",
                                         "target_user_nickname": "Es."}},
                  {"type": "text", "data": " 在吗"}],
                 group_id="64d5f8e17b2ad"),
        " [*Es.*]  在吗", "room", "64d5f8e17b2ad",
    ),
    (
        "at 只有 uid 且目录里查不到 → 兜底保留 uid",
        _message([{"type": "at", "data": {"target_user_id": "0000000000000"}}],
                 group_id="64d5f8e17b2ad"),
        " [@0000000000000@] ", "room", "64d5f8e17b2ad",
    ),
    (
        "图片 URL 转成 IIROSE 图片语法 [url#e]",
        _message([{"type": "imageurl", "data": {"file": "https://r.iirose.com/a.jpg"}}],
                 group_id="64d5f8e17b2ad"),
        "[https://r.iirose.com/a.jpg#e]", "room", "64d5f8e17b2ad",
    ),
    (
        "base64 图片在未接图床时降级为占位文本，不阻断整条消息",
        _message([{"type": "image", "data": {"file": "base64://AAAA"}},
                  {"type": "text", "data": "配图"}],
                 group_id="64d5f8e17b2ad"),
        "[图片] 配图", "room", "64d5f8e17b2ad",
    ),
    (
        "空组件列表 → 无内容（调用方将拒绝发送）",
        _message([]),
        "", "private", "u9",
    ),
    (
        "兜底：正文在顶层 plain_text 字段",
        {"message_id": "m1", "platform": "iirose",
         "message_info": {"user_info": {"user_id": "u9", "user_nickname": "Es."},
                          "additional_config": {}},
         "plain_text": "顶层正文", "raw_message": []},
        "顶层正文", "private", "u9",
    ),
    (
        "引用组件：从近期缓存还原成 `旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`",
        _message([{"type": "reply", "data": {"target_message_id": "760771550794"}},
                  {"type": "text", "data": "我记得的"}],
                 group_id="64d5f8e17b2ad"),
        "图床修好啦 (_hr) Es._1789213830 (hr_) 我记得的", "room", "64d5f8e17b2ad",
    ),
    (
        "引用目标不在缓存：拿不到时间戳就不发引用（不拿消息 id 冒充时间戳）",
        _message([{"type": "reply", "data": {"target_message_id": "999",
                                             "target_user_nickname": "柒洛"}},
                  {"type": "text", "data": "嗯嗯"}],
                 group_id="64d5f8e17b2ad"),
        "嗯嗯", "room", "64d5f8e17b2ad",
    ),
    (
        "引用目标完全无法还原：降级为普通文本，不吐畸形的引用标记",
        _message([{"type": "reply", "data": {"target_message_id": "888"}},
                  {"type": "text", "data": "只有正文"}],
                 group_id="64d5f8e17b2ad"),
        "只有正文", "room", "64d5f8e17b2ad",
    ),
    (
        "at 只有 UID 时查本地用户目录换成用户名（IIROSE 只支持按名字 @）",
        _message([{"type": "at", "data": {"target_user_id": "65abb7a99dc60"}},
                  {"type": "text", "data": " 在吗"}],
                 group_id="64d5f8e17b2ad"),
        " [*柒洛*]  在吗", "room", "64d5f8e17b2ad",
    ),
    (
        "at 同时带群名片/备注时，仍用本地用户名（群名片不是登录名，@ 会失效）",
        _message([{"type": "at", "data": {"target_user_id": "65abb7a99dc60",
                                          "target_user_cardname": "群里的外号",
                                          "target_user_nickname": "群里的小名"}},
                  {"type": "text", "data": " 在吗"}],
                 group_id="64d5f8e17b2ad"),
        " [*柒洛*]  在吗", "room", "64d5f8e17b2ad",
    ),
    (
        "私聊目标来自 additional_config.platform_io_target_user_id",
        _message([{"type": "text", "data": "私聊"}], user_id="",
                 additional={"platform_io_target_user_id": "61f6cdbcdca5f"}),
        "私聊", "private", "61f6cdbcdca5f",
    ),
    (
        "房间目标用配置里的房间 id（Host 回传的是它自己的会话 id，实测 6a7c38409e902）",
        _message([{"type": "text", "data": "房间"}], user_id="u9",
                 additional={"platform_io_target_group_id": "6a7c38409e902"}),
        "房间", "room", "64d5f8e17b2ad",
    ),
]


def _legacy_extract_text(message) -> str:
    """修复前的实现，用来证明这次修的就是它。"""
    raw = message.get("raw_message")
    components = []
    if isinstance(raw, dict):
        comps = raw.get("components")
        if isinstance(comps, list):
            components = comps
    elif isinstance(raw, list):
        components = raw
    elif isinstance(raw, str):
        return raw.strip()

    chunks = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        ctype = str(comp.get("type") or "").lower()
        data = comp.get("data") if isinstance(comp.get("data"), dict) else {}
        if ctype == "text":
            chunks.append(str(comp.get("text") or data.get("text") or comp.get("content") or ""))
    return "".join(chunks).strip()


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    @property
    def connected(self) -> bool:
        return True

    async def send(self, text: str) -> None:
        self.sent.append(text)

    async def close(self) -> None:
        self.closed = True


async def _check_gateway(start_index: int) -> int:
    """端到端验证出站处理器：Host 消息 → IIROSE 报文。"""
    instance = _plugin()
    client = _FakeClient()
    instance._client = client  # type: ignore[attr-defined]

    failures = 0

    ok_message = _message([{"type": "text", "data": "麦麦的回复"}],
                          group_id="64d5f8e17b2ad")
    result = await instance.handle_iirose_gateway(ok_message, {"chat_type": "group"}, {})
    payload = json.loads(client.sent[-1]) if client.sent else {}
    problems = []
    if result.get("success") is not True:
        problems.append(f"success={result!r}")
    if payload.get("m") != "麦麦的回复":
        problems.append(f"报文 m={payload.get('m')!r}")
    if "i" not in payload or "mc" not in payload:
        problems.append(f"报文缺少 i/mc: {payload!r}")
    if not (isinstance(payload.get("i"), str) and payload["i"].isdigit()
            and len(payload["i"]) == 12):
        problems.append(f"消息 id 应为 12 位数字: {payload.get('i')!r}")
    print(f"[{'PASS' if not problems else 'FAIL'}] {start_index}. "
          f"handle_iirose_gateway 群消息出站 → {payload}")
    if problems:
        failures += 1
        for problem in problems:
            print(f"        {problem}")

    # 空消息必须被明确拒绝，而不是静默“成功”
    client.sent.clear()
    result = await instance.handle_iirose_gateway(_message([]), {}, {})
    problems = []
    if result.get("success") is not False:
        problems.append(f"空消息应被拒绝，实际 {result!r}")
    if client.sent:
        problems.append("空消息不应发出任何报文")
    print(f"[{'PASS' if not problems else 'FAIL'}] {start_index + 1}. 空内容出站被拒绝 → {result}")
    if problems:
        failures += 1
        for problem in problems:
            print(f"        {problem}")

    # 连接未就绪时必须失败，且不能抛异常
    instance._client = None  # type: ignore[attr-defined]
    result = await instance.handle_iirose_gateway(ok_message, {}, {})
    problems = [] if result.get("success") is False else [f"应返回失败，实际 {result!r}"]
    print(f"[{'PASS' if not problems else 'FAIL'}] {start_index + 2}. 断线时出站返回失败 → {result}")
    if problems:
        failures += 1
        for problem in problems:
            print(f"        {problem}")

    return failures


# 入站回归夹具：直接取自实测日志里的原始报文（raw 各字段用 > 拼回）
ROOM_FRAME = (
    '"1789213830>https://img.example.com/i/2026/09/07/zan4so.jpg>Es.> [*03酱*]  你好'
    '>bd8b96>bd8b96>2>>61f6cdbcdca5f>\'\'4140\'1422\'37,76.4,0.75>460015977161'
)
PRIVATE_FRAME = (
    '""1789213869>61f6cdbcdca5f>Es.>https://img.example.com/i/2026/09/07/zan4so.jpg'
    '>你好你好~>bd8b96>>bd8b96>2>https://img.scdn.io/i/6a3a5b3561afe_1782209333.webp#e'
    '>842418842168'
)


def _quote_frame(content: str, message_id: str = "460015977162") -> str:
    return (f'""1789213869>61f6cdbcdca5f>Es.>http://r.iirose.com/a.jpg'
            f'>{content}>bd8b96>>bd8b96>2>http://r.iirose.com/b.png#e>{message_id}')


def _room_frame(content: str, message_id: str = "700000000001",
                user_id: str = "6088e40d12bd1", user_name: str = "雾雨吟",
                timestamp: int = 1789216551) -> str:
    """公屏记录：0 时间戳 / 1 头像 / 2 用户名 / 3 正文 / 4 mc / 5 nc / 6 性别 / 7 装饰
    / 8 UID / 9 等级 / 10 消息 id。正文里若含 `>` 会自然撑出更多字段。"""
    parts = [str(timestamp), "http://r.iirose.com/i/a.jpg", user_name, content,
             "311fba", "9ab8cd", "2", "", user_id, "''60'95'1,100,0.5", message_id]
    return '"' + ">".join(parts)


def _member_frame(parts: list[str]) -> str:
    return '"' + ">".join(parts)


# 成员事件记录（字段表对照官方 adapter JoinRoom/LeaveRoom/SwitchRoom/MemberUpdate）
_AVATAR = "http://r.iirose.com/i/26/4/30/7/4020-80.jpg"
_UID = "65abb7a99dc60"
MEMBER_JOIN_FRAME = _member_frame(
    ["1789213830", _AVATAR, "柒洛", "'1", "bd8b96", "bd8b96", "2", "", _UID,
     "'108", "", "64d5f8e17b2ad'n'''"])
MEMBER_RECONNECT_FRAME = _member_frame(
    ["1789213831", _AVATAR, "柒洛", "'1", "bd8b96", "bd8b96", "2", "", _UID,
     "'108", "", "64d5f8e17b2ad'd'''"])
MEMBER_LEAVE_FRAME = _member_frame(
    ["1789213835", _AVATAR, "柒洛", "'3", "bd8b96", "bd8b96", "2", "", _UID,
     "'108", "", "2"])
MEMBER_MOVE_FRAME = _member_frame(
    ["1789213840", _AVATAR, "柒洛", "'25b7ab80a2017d", "bd8b96", "bd8b96", "2", "", _UID,
     "'108", "64d5f8e17b2ad", "35b7ab80a2017d"])
# 正文恰好以单引号开头的聊天消息，不能被误判成成员事件
CHAT_WITH_QUOTE_FRAME = (
    f'"1789213860>{_AVATAR}>Es.>\'1 你好>bd8b96>bd8b96>2>>61f6cdbcdca5f>\'\'4132>460015977199'
)


async def _check_member_events(start_index: int) -> int:
    """房间成员上线 / 重连 / 下线 / 换房的识别与上报。"""
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 四类事件都要能被识别，且带上正确的用户/事件类型
    expected = [
        (MEMBER_JOIN_FRAME, "member", "join", "new", "柒洛"),
        (MEMBER_RECONNECT_FRAME, "member", "join", "reconnect", "柒洛"),
        (MEMBER_LEAVE_FRAME, "member", "leave", None, "柒洛"),
        (MEMBER_MOVE_FRAME, "member", "leave", None, "柒洛"),
    ]
    problems = []
    details = []
    for frame, want_kind, want_event, want_join_type, want_name in expected:
        kind, event = adapter.parse_frame(frame)
        details.append(f"{want_event}/{want_join_type or '-'}")
        if kind != want_kind:
            problems.append(f"{frame[:24]}… kind={kind!r} 期望 {want_kind!r}")
            continue
        if event.get("event") != want_event:
            problems.append(f"event={event.get('event')!r} 期望 {want_event!r}")
        if want_join_type and event.get("join_type") != want_join_type:
            problems.append(f"join_type={event.get('join_type')!r} 期望 {want_join_type!r}")
        if event.get("user_name") != want_name or event.get("user_id") != _UID:
            problems.append(f"用户字段={event.get('user_name')!r}/{event.get('user_id')!r}")
    report(start_index, "成员事件识别：加入 / 重连 / 离开 / 换房", problems, ", ".join(details))

    # 2) 移动事件要带上目标房间；加入事件要从末字段取出房间 id
    problems = []
    _, join_event = adapter.parse_frame(MEMBER_JOIN_FRAME)
    if join_event.get("room_id") != "64d5f8e17b2ad":
        problems.append(f"加入事件 room_id={join_event.get('room_id')!r}")
    _, move_event = adapter.parse_frame(MEMBER_MOVE_FRAME)
    if move_event.get("target_room_id") != "5b7ab80a2017d" or move_event.get("is_move") is not True:
        problems.append(f"移动事件 target={move_event.get('target_room_id')!r} is_move={move_event.get('is_move')!r}")
    report(start_index + 1, "加入事件取房间 id；移动事件带目标房间 id", problems,
           f"join.room={join_event.get('room_id')} move.target={move_event.get('target_room_id')}")

    # 3) 正文以单引号开头的聊天消息不能被误判成成员事件
    kind, msg = adapter.parse_frame(CHAT_WITH_QUOTE_FRAME)
    problems = []
    if kind != "room":
        problems.append(f"kind={kind!r} 期望 room")
    elif msg.get("text") != "'1 你好":
        problems.append(f"text={msg.get('text')!r}")
    report(start_index + 2, "以单引号开头的正文不算成员事件", problems,
           f"kind={kind} text={msg.get('text')!r}")

    # 4) 成员事件绝不能作为聊天消息注入给麦麦（只能是 is_notify 的通知）
    instance = _plugin()
    await instance._handle_frame(MEMBER_LEAVE_FRAME)
    payloads = instance.ctx.gateway.payloads  # type: ignore[attr-defined]
    problems = []
    for payload in payloads:
        if payload.get("is_notify") is not True:
            problems.append(f"出现了非通知形态的注入：{payload!r}")
        text = json.dumps(payload.get("raw_message"), ensure_ascii=False)
        if "'3" in text or "'2" in text or "'1" in text:
            problems.append(f"成员标记泄漏进了正文：{text}")
    if not payloads:
        problems.append("成员事件没有被识别出来（既没注入也没通知）")
    report(start_index + 3, "离开事件不再被当成正文 `'3` 灌给麦麦", problems,
           f"{len(payloads)} 条通知，正文={payloads[0].get('display_message')!r}" if payloads else "")

    # 5) 上报给 Host 的是「通知」形态，不是伪造的用户聊天
    instance = _plugin()
    await instance._handle_frame(MEMBER_RECONNECT_FRAME)
    payloads = instance.ctx.gateway.payloads  # type: ignore[attr-defined]
    problems = []
    if len(payloads) != 1:
        problems.append(f"通知条数={len(payloads)}")
    else:
        payload = payloads[0]
        if payload.get("is_notify") is not True:
            problems.append(f"is_notify={payload.get('is_notify')!r}")
        if payload.get("display_message") != "柒洛 重新连接进入房间 → 64d5f8e17b2ad":
            problems.append(f"display_message={payload.get('display_message')!r}")
        if payload.get("raw_message") != [
                {"type": "text", "data": "柒洛 重新连接进入房间 → 64d5f8e17b2ad"}]:
            problems.append(f"raw_message={payload.get('raw_message')!r}")
        group = payload.get("message_info", {}).get("group_info", {})
        if group.get("group_id") != "64d5f8e17b2ad":
            problems.append(f"group_info={group!r}")
    report(start_index + 4, "重连事件作为通知上报（is_notify / display_message / group_info）", problems,
           json.dumps(payloads[0].get("raw_message") if payloads else None, ensure_ascii=False))

    # 6) 关掉开关就不上报（日志仍会记录）
    instance = _plugin()
    adapter.IIRoseAdapterPlugin.config.bot.report_member_events = False
    await instance._handle_frame(MEMBER_JOIN_FRAME)
    problems = []
    if instance.ctx.gateway.payloads:  # type: ignore[attr-defined]
        problems.append(f"开关关闭后仍上报了：{instance.ctx.gateway.payloads!r}")  # type: ignore[attr-defined]
    report(start_index + 5, "report_member_events=false 时只记日志不上报", problems)
    adapter.IIRoseAdapterPlugin.config.bot.report_member_events = True

    # 7) 成员事件的 group_id 必须与聊天一致（配置房间），否则 Host 会劈成两个会话
    instance = _plugin()
    await instance._handle_frame(MEMBER_JOIN_FRAME)
    payloads = instance.ctx.gateway.payloads  # type: ignore[attr-defined]
    problems = []
    group = payloads[0].get("message_info", {}).get("group_info", {}) if payloads else {}
    if group.get("group_id") != "64d5f8e17b2ad":
        problems.append(f"group_id={group.get('group_id')!r}，应为配置房间 64d5f8e17b2ad")
    record = payloads[0]["message_info"]["additional_config"].get("iirose_member_event", {}) if payloads else {}
    if record.get("room_id") != "64d5f8e17b2ad":
        problems.append(f"事件原始 room_id 未保留: {record.get('room_id')!r}")
    report(start_index + 6, "成员事件与聊天用同一个 group_id（不劈成两个会话）", problems,
           f"group_id={group.get('group_id')}")

    return failures


async def _check_inbound(start_index: int) -> int:
    """入站：报文解析 → Host 组件（@ 与引用）。"""
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 公屏报文按逆向文档的字段表解析
    kind, msg = adapter.parse_frame(ROOM_FRAME)
    problems = []
    if kind != "room":
        problems.append(f"kind={kind!r}")
    for field, want in (("user_id", "61f6cdbcdca5f"), ("user_name", "Es."),
                        ("text", " [*03酱*]  你好"), ("message_id", "460015977161")):
        if msg.get(field) != want:
            problems.append(f"{field}={msg.get(field)!r} 期望 {want!r}")
    if msg.get("timestamp") != 1789213830:
        problems.append(f"timestamp={msg.get('timestamp')!r} 期望 1789213830")
    report(start_index, "公屏报文按文档字段表解析（uid=[8] 内容=[3] id=[10] 时间=[0]）",
           problems, f"user={msg.get('user_name')} text={msg.get('text')!r}")

    # 2) 私聊报文
    kind, msg = adapter.parse_frame(PRIVATE_FRAME)
    problems = []
    if kind != "private":
        problems.append(f"kind={kind!r}")
    for field, want in (("user_id", "61f6cdbcdca5f"), ("text", "你好你好~"),
                        ("message_id", "842418842168")):
        if msg.get(field) != want:
            problems.append(f"{field}={msg.get(field)!r} 期望 {want!r}")
    report(start_index + 1, "私聊报文按文档字段表解析（uid=[1] 内容=[4] id=[10]）",
           problems, f"user={msg.get('user_name')} text={msg.get('text')!r}")

    # 3) 应用层心跳
    kind, value = adapter.parse_frame("c")
    report(start_index + 2, "服务端 `c` 心跳识别", [] if kind == "heartbeat" else [f"kind={kind!r}"],
           repr(kind))

    # 4) `[*03酱*]` 必须变成指向机器人自己的 at 组件（Host 靠它判定被点名）
    instance = _plugin()
    _, room_msg = adapter.parse_frame(ROOM_FRAME)
    await instance._dispatch_inbound("room", room_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    ats = [c for c in components if c.get("type") == "at"]
    problems = []
    if len(ats) != 1:
        problems.append(f"at 组件数={len(ats)}：{components!r}")
    else:
        if ats[0]["data"].get("target_user_id") != "62b98115cefd2":
            problems.append(f"target_user_id={ats[0]['data'].get('target_user_id')!r} 应为机器人 uid")
        if ats[0]["data"].get("target_user_nickname") != "03酱":
            problems.append(f"nickname={ats[0]['data'].get('target_user_nickname')!r}")
    report(start_index + 3, "` [*03酱*] ` → at 组件（用户名为机器人自己）", problems,
           json.dumps(components, ensure_ascii=False))

    # 5) 引用：`旧内容 (_hr) 发送者_时间戳秒 (hr_) 新内容`
    instance = _plugin()
    kind, quote_msg = adapter.parse_frame(
        _quote_frame("图床修好啦 (_hr) Es._1789213830 (hr_) 那太好了"))
    await instance._dispatch_inbound(kind, quote_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    problems = []
    if visible != "[引用 Es.：图床修好啦] 那太好了":
        problems.append(f"可见文本={visible!r}")
    if any(c.get("type") == "at" for c in components):
        problems.append(f"引用不该产生 at 组件：{components!r}")
    report(start_index + 4, "入站引用展开成可见文本（引用标记不出现在正文里）", problems,
           repr(visible))

    # 6) 引用块里的 @ 不能触发 Host 的“被点名”判定
    instance = _plugin()
    kind, quote_msg = adapter.parse_frame(
        _quote_frame("[*03酱*] 你好 (_hr) Es._1789213830 (hr_) 嗯嗯"))
    await instance._dispatch_inbound(kind, quote_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    problems = []
    if any(c.get("type") == "at" for c in components):
        problems.append(f"引用块内的提及被误判成 at：{components!r}")
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    if visible != "[引用 Es.：03酱 你好] 嗯嗯":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 5, "引用块内的 `[*名字*]` 降级为纯文本，不误触发回复", problems,
           repr(visible))

    # 7) 引用块里的 @ 被剥掉，但新正文里的 @ 必须保留成 at（真的在叫机器人）
    instance = _plugin()
    kind, quote_msg = adapter.parse_frame(
        _quote_frame("[*柒洛*] 旧消息 (_hr) Es._1789213830 (hr_) [*03酱*] 你说呢"))
    await instance._dispatch_inbound(kind, quote_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    ats = [c for c in components if c.get("type") == "at"]
    problems = []
    if len(ats) != 1 or ats[0]["data"].get("target_user_id") != "62b98115cefd2":
        problems.append(f"新正文里的提及应保留为指向机器人的 at：{components!r}")
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    if "[引用 Es.：柒洛 旧消息]" not in visible:
        problems.append(f"引用部分的提及未降级：{visible!r}")
    report(start_index + 6, "引用部分去 @、新正文保留 @（只剥引用块）", problems,
           json.dumps(components, ensure_ascii=False))

    # 7b) 消息 id 带 `<` 的消息不能被丢掉
    #     实测：`411952792881<1789271137`（客户端 id + 服务器时间戳）被原来的 isalnum() 检查误杀
    instance = _plugin()
    kind, weird = adapter.parse_frame(_room_frame(
        " [*03酱*] 你好", message_id="411952792881<1789271137"))
    await instance._dispatch_inbound(kind, weird)
    problems = []
    payloads = instance.ctx.gateway.payloads  # type: ignore[attr-defined]
    if len(payloads) != 1:
        problems.append(f"带 `<` 的消息 id 被丢掉了（投递 {len(payloads)} 条）")
    elif payloads[0].get("message_id") != "411952792881<1789271137":
        problems.append(f"message_id={payloads[0].get('message_id')!r}")
    report(start_index + 7, "消息 id 带 `<`（客户端id<服务器时间戳）不再被丢弃", problems,
           f"投递 {len(payloads)} 条")

    # 8) 图片内容 `[url#e]` 交给麦麦前要还原成裸 URL（实测别人发图就是这个形态）
    instance = _plugin()
    kind, img_msg = adapter.parse_frame(
        _quote_frame("[http://r.iirose.com/i/21/4/10/22/4409-EB.gif#e]", message_id="909313188403"))
    await instance._dispatch_inbound(kind, img_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    problems = []
    if visible != "http://r.iirose.com/i/21/4/10/22/4409-EB.gif":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 7, "入站图片 `[url#e]` 还原成裸 URL", problems, repr(visible))

    # 9) @房间 `[_房间id_]` 不是 @ 人
    instance = _plugin()
    kind, room_msg = adapter.parse_frame(
        _quote_frame("[_64d5f8e17b2ad_] 大家看这里", message_id="460015977163"))
    await instance._dispatch_inbound(kind, room_msg)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    problems = []
    if any(c.get("type") == "at" for c in components):
        problems.append(f"@房间 不该产生 at 组件：{components!r}")
    report(start_index + 8, "@房间 `[_房间id_]` 保留为文本，不当作 @ 人", problems,
           json.dumps(components, ensure_ascii=False))

    # 10) 多层引用（引用里再套引用）要全部还原
    instance = _plugin()
    kind, nested = adapter.parse_frame(_quote_frame(
        "最初的话 (_hr) 柒洛_1789213800 (hr_) 中间的话 (_hr) Es._1789213830 (hr_) 我来说",
        message_id="460015977164"))
    await instance._dispatch_inbound(kind, nested)
    components = instance.ctx.gateway.payloads[-1]["raw_message"]  # type: ignore[attr-defined]
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    problems = []
    if visible != "[引用 柒洛：最初的话 ／ Es.：中间的话] 我来说":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 9, "多层引用链全部还原（不只第一层）", problems, repr(visible))

    # 11) 消息 id 必须符合协议的 12 位数字
    ids = {adapter.next_client_message_id() for _ in range(50)}
    problems = []
    if len(ids) != 50:
        problems.append("消息 id 出现重复")
    if any(len(i) != 12 or not i.isdigit() for i in ids):
        problems.append(f"非 12 位数字：{sorted(ids)[:3]}")
    report(start_index + 10, "出站消息 id 为 12 位数字且不重复", problems, sorted(ids)[0])

    return failures


# 点歌 / 点播卡片（格式见官方 encoder/messages/media_card.ts）
# 卡片的 `>` 是字段分隔符，服务端会转义成 `&gt;`（官方 decoder 也是先 decode 再拆），
# 两种形态都要能认：WIRE_* 是线上形态，RAW_* 是没转义时的形态。
CARD_RAW = ("m__4@0>起风了>买辣椒也用券"
            ">http://r.iirose.com/i/24/6/13/22/0556-TI.jpg>66ccff>320")
CARD_WIRE = CARD_RAW.replace(">", "&gt;")
CARD_MUSIC_NO_BITRATE = "m__4@2&gt;晴天&gt;周杰伦&gt;http://r.iirose.com/i/a.jpg&gt;ff0000&gt;&gt;114511分钟30秒"
CARD_VIDEO = "m__4*3&gt;【明日方舟】主线PV&gt;某某UP主&gt;http://r.iirose.com/i/b.jpg&gt;ff0000&gt;&gt;320&gt;&gt;1分钟30秒"
# 标题/歌手里的 `&` 会被发送方 encode，再叠上服务端的转义。
# 注意：标题里如果含 `>`，转义后与字段分隔符无法区分——官方 decoder 也有这个限制。
CARD_ESCAPED = ("m__4@0&gt;Tom &amp; Jerry&gt;A&amp;B&gt;"
                "http://r.iirose.com/i/c.jpg&gt;66ccff")


async def _check_media_card(start_index: int) -> int:
    """点歌 / 点播卡片识别。"""
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    async def render(content: str, message_id: str) -> tuple[str, dict, dict]:
        instance = _plugin()
        kind, msg = adapter.parse_frame(_room_frame(content, message_id=message_id))
        await instance._dispatch_inbound(kind, msg)
        payload = instance.ctx.gateway.payloads[-1]  # type: ignore[attr-defined]
        components = payload["raw_message"]
        visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
        return visible, payload, msg

    # 1) 音乐卡（网易云）—— 转义形态（线上真实形态）
    visible, payload, _ = await render(CARD_WIRE, "700000000001")
    problems = []
    if visible != "[点歌·网易云] 《起风了》 - 买辣椒也用券":
        problems.append(f"可见文本={visible!r}")
    card = payload["message_info"]["additional_config"].get("iirose_media_card", {})
    if card.get("cover") != "http://r.iirose.com/i/24/6/13/22/0556-TI.jpg":
        problems.append(f"封面未透传: {card.get('cover')!r}")
    report(start_index, "音乐卡（`&gt;` 转义形态）→ 可读文本", problems, repr(visible))

    # 2) 同一张卡但分隔符没转义：正文会撑出 11 项以上，靠尾部字段形状还原
    visible, _, msg = await render(CARD_RAW, "700000000002")
    problems = []
    if visible != "[点歌·网易云] 《起风了》 - 买辣椒也用券":
        problems.append(f"可见文本={visible!r}")
    if msg.get("message_id") != "700000000002":
        problems.append(f"消息 id 解析错位: {msg.get('message_id')!r}")
    if msg.get("user_id") != "6088e40d12bd1":
        problems.append(f"UID 解析错位: {msg.get('user_id')!r}")
    report(start_index + 1, "未转义卡片（字段数 > 11）也能还原正文与尾部字段", problems, repr(visible))

    # 3) 视频卡（B站）—— 官方 adapter 不解析这类
    visible, _, _ = await render(CARD_VIDEO, "700000000003")
    problems = []
    if visible != "[点播·B站] 《【明日方舟】主线PV》 - 某某UP主（时长 1分钟30秒）":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 2, "视频卡 `m__4*3>...` → 点播文本（含时长）", problems, repr(visible))

    # 4) 无码率形态：时长藏在 `11451` 标记后面
    visible, _, _ = await render(CARD_MUSIC_NO_BITRATE, "700000000004")
    problems = []
    if visible != "[点歌·QQ音乐] 《晴天》 - 周杰伦（时长 1分钟30秒）":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 3, "无码率卡片的时长解析（`11451` 标记）", problems, repr(visible))

    # 5) 标题/歌手里的 HTML 实体要反转义
    visible, _, _ = await render(CARD_ESCAPED, "700000000005")
    problems = []
    if visible != "[点歌·网易云] 《Tom & Jerry》 - A&B":
        problems.append(f"可见文本={visible!r}")
    report(start_index + 4, "卡片里的 HTML 实体反转义（&amp; 等）", problems, repr(visible))

    # 6) 普通聊天里的残缺 / 非法前缀不能被误判成卡片
    problems = []
    for bad, why in (("m__4@0>只有标题", "字段不足"),
                     ("m__4zz>a>b>c>d", "平台标识非法"),
                     ("m__5@0>a>b>c>d", "前缀不对")):
        parsed = adapter.parse_media_card(bad)
        if parsed is not None:
            problems.append(f"{why} 被误判为卡片: {bad!r} → {parsed!r}")
    report(start_index + 5, "残缺 / 非法前缀的文本不误判为卡片", problems, "3 种反例")

    # 7) 带封面变体前缀（% / # / !）也要能识别
    problems = []
    variants = [
        ("m__4%2>晴天>周杰伦>http://r.iirose.com/i/d.jpg>66ccff>128", "QQ音乐", False),
        ("m__4#3>B站视频>UP主>http://r.iirose.com/i/e.jpg>66ccff>128", "B站", True),
        ("m__4!3>B站视频>UP主>http://r.iirose.com/i/f.jpg>66ccff>128", "B站", True),
    ]
    details = []
    for raw_card, want_platform, want_video in variants:
        card = adapter.parse_media_card(raw_card)
        details.append(f"{raw_card[4:6]}→{card.get('platform') if card else None}")
        if card is None:
            problems.append(f"{raw_card[:8]} 没被识别成卡片")
            continue
        if card.get("platform") != want_platform or card.get("is_video") is not want_video:
            problems.append(f"{raw_card[:8]}: platform={card.get('platform')!r} "
                            f"is_video={card.get('is_video')!r}")
    report(start_index + 6, "卡片变体前缀（`%` 音乐带封面 / `#`、`!` 视频带封面）", problems,
           " ".join(details))

    return failures


async def _check_image_host(start_index: int) -> int:
    """图床：base64 图片/表情 → 上传换 URL → 校验链接 → `[url#e]`。"""
    failures = 0
    calls: list[dict] = []
    post_state = {"urls": [], "default": "https://img.example.com/i/2026/09/12/abc123.webp"}
    probe_state = {"dead": set(), "inconclusive": set(), "calls": []}

    def fake_post(url, fields, files, timeout):
        calls.append({"url": url, "fields": dict(fields), "files": dict(files), "timeout": timeout})
        if "boom" in fields.get("token", ""):
            raise OSError("connection refused")
        next_url = post_state["urls"].pop(0) if post_state["urls"] else post_state["default"]
        return json.dumps({"result": "success", "code": 200, "url": next_url,
                           "srcName": "195124"})

    def fake_probe(url, timeout):
        del timeout
        probe_state["calls"].append(url)
        if url in probe_state["dead"]:
            return False, "HTTP 404", True          # 服务端明确回答：确实取不到
        if url in probe_state["inconclusive"]:
            return False, "timed out", False        # 探测本身失败：不能断定图片坏了
        return True, "image/webp", True

    original_post = adapter.IIRoseAdapterPlugin._post_multipart
    original_probe = adapter.IIRoseAdapterPlugin._probe_image_url
    adapter.IIRoseAdapterPlugin._post_multipart = staticmethod(fake_post)
    adapter.IIRoseAdapterPlugin._probe_image_url = staticmethod(fake_probe)

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    try:
        png = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"fake-image-bytes").decode()
        emoji = base64.b64encode(b"GIF89a" + b"fake-emoji-bytes").decode()

        # 1) base64 图片组件 → 上传 → [url#e]
        instance = _plugin()
        calls.clear()
        text = await instance._extract_text(_message(
            [{"type": "image", "data": {"file": f"base64://{png}", "sub_type": 0}}],
            group_id="64d5f8e17b2ad"))
        problems = []
        if text != "[https://img.example.com/i/2026/09/12/abc123.webp#e]":
            problems.append(f"text={text!r}")
        if len(calls) != 1:
            problems.append(f"上传次数={len(calls)}")
        else:
            call = calls[0]
            if call["url"] != "https://img.example.com/api/index.php":
                problems.append(f"上传地址={call['url']!r}")
            if "image" not in call["files"]:
                problems.append(f"文件字段={list(call['files'])!r}")
            else:
                filename, content, mime = call["files"]["image"]
                if not filename.endswith(".png") or mime != "image/png":
                    problems.append(f"文件名/MIME={filename!r}/{mime!r}")
        report(start_index, "base64 图片 → 上传图床 → `[url#e]`", problems, repr(text))

        # 2) 表情（sub_type=1，也是 base64://）走同一条路
        instance = _plugin()
        calls.clear()
        text = await instance._extract_text(_message(
            [{"type": "image", "data": {"file": f"base64://{emoji}", "sub_type": 1,
                                        "summary": "[动画表情]"}}],
            group_id="64d5f8e17b2ad"))
        problems = []
        if text != "[https://img.example.com/i/2026/09/12/abc123.webp#e]":
            problems.append(f"text={text!r}")
        if len(calls) != 1:
            problems.append(f"上传次数={len(calls)}")
        report(start_index + 1, "表情（sub_type=1 的 base64 组件）同样上传后发送", problems, repr(text))

        # 3) 图文混排：文本与图片链接按顺序拼好
        instance = _plugin()
        calls.clear()
        text = await instance._extract_text(_message(
            [{"type": "text", "data": "看图"},
             {"type": "image", "data": {"file": f"base64://{png}"}},
             {"type": "text", "data": "可爱吧"}],
            group_id="64d5f8e17b2ad"))
        problems = []
        want = "看图 [https://img.example.com/i/2026/09/12/abc123.webp#e] 可爱吧"
        if text != want:
            problems.append(f"text={text!r} 期望 {want!r}")
        report(start_index + 2, "图文混排顺序正确", problems, repr(text))

        # 4) 同一张图只上传一次（缓存）
        instance = _plugin()
        calls.clear()
        component = [{"type": "image", "data": {"file": f"base64://{png}"}}]
        await instance._extract_text(_message(component, group_id="64d5f8e17b2ad"))
        await instance._extract_text(_message(component, group_id="64d5f8e17b2ad"))
        problems = [] if len(calls) == 1 else [f"上传次数={len(calls)}，应命中缓存只传 1 次"]
        report(start_index + 3, "同一张图命中缓存，不重复上传", problems, f"上传 {len(calls)} 次")

        # 5) 上传失败 → 退回占位文本，消息照样能发出去
        instance = _plugin()
        adapter.IIRoseAdapterPlugin.config.image_host.token = "boom"
        text = await instance._extract_text(_message(
            [{"type": "text", "data": "配图"},
             {"type": "image", "data": {"file": f"base64://{png}"}}],
            group_id="64d5f8e17b2ad"))
        problems = [] if text == "配图 [图片]" else [f"text={text!r}"]
        report(start_index + 4, "上传失败时退回占位文本（不阻断整条消息）", problems, repr(text))
        adapter.IIRoseAdapterPlugin.config.image_host.token = "test-token"

        # 6) 配置里的 token 必须带上
        instance = _plugin()
        calls.clear()
        await instance._extract_text(_message(
            [{"type": "image", "data": {"file": f"data:image/gif;base64,{emoji}"}}],
            group_id="64d5f8e17b2ad"))
        problems = []
        if not calls or calls[0]["fields"].get("token") != "test-token":
            problems.append(f"表单字段={calls[0]['fields'] if calls else None!r}")
        if calls and not calls[0]["files"]["image"][0].endswith(".gif"):
            problems.append(f"data URI 的 MIME 未用于扩展名: {calls[0]['files']['image'][0]!r}")
        report(start_index + 5, "带上 token；data URI 的 MIME 决定扩展名", problems,
               f"fields={calls[0]['fields'] if calls else None}")

        # 7) 响应解析的各种形态
        cases = [
            ('{"result":"success","code":200,"url":"https://a.b/c.webp"}', "https://a.b/c.webp"),
            ('{"result":"failed","code":400,"url":""}', ""),
            ('{"result":"success","code":200,"url":"/i/x.png"}',
             "https://img.example.com/i/x.png"),
            ('{"result":"success","code":200,"url":"//cdn.x/y.png"}', "https://cdn.x/y.png"),
            ("not json", ""),
        ]
        problems = []
        for payload, want_url in cases:
            got = adapter.parse_upload_response(payload, "https://img.example.com")
            if got != want_url:
                problems.append(f"{payload[:32]!r} → {got!r} 期望 {want_url!r}")
        report(start_index + 6, "图床响应解析（成功 / 失败 / 相对路径 / 协议相对 / 非 JSON）",
               problems, f"{len(cases)} 种形态")

        # 8) multipart 请求体结构（PHP 端要能正常解析）
        boundary = "----TESTBOUND"
        body = adapter.build_multipart(
            {"token": "tk"}, {"image": ("a.gif", b"GIF89a\x00\x01\x02", "image/gif")}, boundary)
        problems = []
        if not body.startswith(f"--{boundary}\r\n".encode()):
            problems.append("缺少起始 boundary")
        if b'Content-Disposition: form-data; name="token"\r\n\r\ntk\r\n' not in body:
            problems.append("token 字段格式不对")
        if b'name="image"; filename="a.gif"' not in body or b"Content-Type: image/gif" not in body:
            problems.append("文件字段格式不对")
        if b"GIF89a\x00\x01\x02" not in body:
            problems.append("二进制内容被破坏")
        if not body.endswith(f"--{boundary}--\r\n".encode()):
            problems.append("缺少结束 boundary")
        report(start_index + 7, "multipart/form-data 请求体结构正确", problems,
               f"{len(body)} 字节")

        # 9) 图床报错要能说人话（实测遇到过 Token Error）
        problems = []
        got = adapter.describe_upload_response(
            '{"result":"failed","code":202,"message":"Token Error"}')
        if "Token Error" not in got or "202" not in got:
            problems.append(f"describe={got!r}")
        if "非 JSON" not in adapter.describe_upload_response("<html>502</html>"):
            problems.append("非 JSON 响应未识别")
        if not isinstance(adapter.ImageHostSection().token, str):
            problems.append("ImageHostSection().token 字段类型异常")
        report(start_index + 8, "图床返回失败时给出可读原因（token / code / message）", problems,
               repr(got))

        # 10) 缓存落盘：插件重启（新实例）后同一张图不再重复上传
        #     注意：这里用工作区内的目录，不用系统 temp（某些环境对系统 temp 只读）
        cache_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".test-cache-tmp")
        workdir = os.path.join(cache_root, uuid.uuid4().hex[:8])
        os.makedirs(workdir, exist_ok=True)
        try:
            png_component = [{"type": "image", "data": {"file": f"base64://{png}"}}]

            first = _plugin(data_dir=workdir)
            calls.clear()
            probe_state["calls"].clear()
            await first._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            uploads_first = len(calls)
            probes_first = len(probe_state["calls"])
            cache_file = os.path.join(workdir, adapter.UPLOAD_CACHE_FILE)
            wrote = os.path.exists(cache_file)

            # 模拟插件重启：全新实例、同一个数据目录、内存缓存是空的
            second = _plugin(data_dir=workdir)
            calls.clear()
            probe_state["calls"].clear()
            text = await second._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))

            problems = []
            if uploads_first != 1:
                problems.append(f"首次上传次数={uploads_first}")
            if probes_first != 1:
                problems.append(f"上传后校验次数={probes_first}，应为 1")
            if not wrote:
                problems.append(f"缓存文件未落盘: {cache_file}")
            if len(calls) != 0:
                problems.append(f"重启后仍重复上传了 {len(calls)} 次")
            if text != "[https://img.example.com/i/2026/09/12/abc123.webp#e]":
                problems.append(f"重启后 text={text!r}")
            report(start_index + 9, "图床缓存落盘：插件重启后复用链接、不再上传", problems,
                   f"首次上传 {uploads_first} 次 / 校验 {probes_first} 次，重启后上传 {len(calls)} 次")

            # 11) reuse_uploaded=false 时每次都重新上传
            third = _plugin(data_dir=workdir)
            adapter.IIRoseAdapterPlugin.config.image_host.reuse_uploaded = False
            calls.clear()
            await third._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            await third._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            problems = [] if len(calls) == 2 else [f"上传次数={len(calls)}，关掉复用后应为 2"]
            report(start_index + 10, "reuse_uploaded=false 时关闭复用", problems,
                   f"上传 {len(calls)} 次")
            adapter.IIRoseAdapterPlugin.config.image_host.reuse_uploaded = True

            # 12) 缓存里的链接已经失效 → 丢弃缓存并重新上传
            instance = _plugin(data_dir=workdir)
            await instance._ensure_upload_cache_loaded()
            digest = hashlib.sha256(base64.b64decode(png)).hexdigest()
            instance._store_entry(digest, "https://img.example.com/i/dead.webp", 0.0)
            probe_state["dead"] = {"https://img.example.com/i/dead.webp"}
            calls.clear()
            post_state["urls"] = ["https://img.example.com/i/2026/09/12/fresh.webp"]
            text = await instance._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            problems = []
            if len(calls) != 1:
                problems.append(f"失效链接后应重新上传，实际上传 {len(calls)} 次")
            if text != "[https://img.example.com/i/2026/09/12/fresh.webp#e]":
                problems.append(f"text={text!r}")
            stored = instance._upload_cache.get(digest)
            if not stored or stored[0] != "https://img.example.com/i/2026/09/12/fresh.webp":
                problems.append(f"缓存未更新为新链接: {stored!r}")
            report(start_index + 11, "缓存链接失效 → 自动重新上传并更新缓存", problems, repr(text))
            probe_state["dead"] = set()

            # 13) 刚上传的链接取不到图 → 重传一次，第二次成功
            instance = _plugin()
            calls.clear()
            post_state["urls"] = ["https://img.example.com/i/broken.webp",
                                  "https://img.example.com/i/2026/09/12/ok.webp"]
            probe_state["dead"] = {"https://img.example.com/i/broken.webp"}
            text = await instance._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            problems = []
            if len(calls) != 2:
                problems.append(f"应上传 2 次（首次链接不可用），实际 {len(calls)} 次")
            if text != "[https://img.example.com/i/2026/09/12/ok.webp#e]":
                problems.append(f"text={text!r}")
            report(start_index + 12, "上传后链接校验失败 → 重传一次后用新链接发送", problems, repr(text))

            # 14) 两次都取不到图 → 退回占位文本
            instance = _plugin()
            calls.clear()
            post_state["urls"] = ["https://img.example.com/i/bad1.webp",
                                  "https://img.example.com/i/bad2.webp"]
            probe_state["dead"] = {"https://img.example.com/i/bad1.webp",
                                   "https://img.example.com/i/bad2.webp"}
            text = await instance._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            problems = []
            if len(calls) != 2:
                problems.append(f"应尝试 2 次，实际 {len(calls)} 次")
            if text != "[图片]":
                problems.append(f"text={text!r}，两次都失败应退回占位")
            report(start_index + 13, "两次链接都不可用 → 退回占位文本（不发送坏链接）", problems,
                   repr(text))
            probe_state["dead"] = set()

            # 15) 探测本身失败（超时）不算「图片坏了」：照常发送，不无谓重传
            instance = _plugin()
            calls.clear()
            post_state["urls"] = ["https://img.example.com/i/2026/09/12/hiccup.webp"]
            probe_state["inconclusive"] = {"https://img.example.com/i/2026/09/12/hiccup.webp"}
            text = await instance._extract_text(_message(png_component, group_id="64d5f8e17b2ad"))
            problems = []
            if len(calls) != 1:
                problems.append(f"探测失败不应重传，实际上传 {len(calls)} 次")
            if text != "[https://img.example.com/i/2026/09/12/hiccup.webp#e]":
                problems.append(f"text={text!r}")
            report(start_index + 14, "链接探测超时 → 从宽处理，照常发送（避免误判丢图）", problems,
                   repr(text))
            probe_state["inconclusive"] = set()
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
    finally:
        adapter.IIRoseAdapterPlugin._post_multipart = original_post
        adapter.IIRoseAdapterPlugin._probe_image_url = original_probe

    return failures


def _set_chat_list(**kwargs) -> None:
    """显式设置名单状态（配置对象是共享的，每次都要重置干净）。"""
    chat = adapter.IIRoseAdapterPlugin.config.chat_list
    chat.enabled = kwargs.get("enabled", False)
    chat.user_list_enabled = kwargs.get("user_list_enabled", False)
    chat.user_list_type = kwargs.get("user_list_type", "whitelist")
    chat.user_list = list(kwargs.get("user_list", []))
    chat.ban_user_enabled = kwargs.get("ban_user_enabled", False)
    chat.ban_user_list = list(kwargs.get("ban_user_list", []))
    chat.room_list_enabled = kwargs.get("room_list_enabled", False)
    chat.room_list_type = kwargs.get("room_list_type", "blacklist")
    chat.room_list = list(kwargs.get("room_list", []))
    chat.log_dropped = False


async def _check_chat_list(start_index: int) -> int:
    """黑白名单过滤：三个名单独立开关，互不影响。"""
    failures = 0
    UID = "6649f1377e8c7"
    NAME = "柒月依"
    ROOM = "64d5f8e17b2ad"
    OTHER_UID = "0000000000000"

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 注意：_plugin() 会把配置重置为默认值，所以先建实例、再逐个改配置
    instance = _plugin()

    def decide(user_id: str = UID, user_name: str = NAME, room: str = ROOM) -> tuple[bool, str]:
        return instance._chat_list_check("room", user_id, user_name, room)

    cases = [
        # (说明, 配置, 期望放行)
        ("总开关关闭：三个名单都不生效（即使子开关全开）",
         dict(enabled=False, user_list_enabled=True, user_list=["别人"],
              ban_user_enabled=True, ban_user_list=[UID],
              room_list_enabled=True, room_list_type="whitelist", room_list=[]), True),
        ("总开关开 + 用户名单开关关 → 不看用户名单",
         dict(enabled=True, user_list_enabled=False, user_list=["别人"]), True),
        ("用户白名单命中 UID", dict(enabled=True, user_list_enabled=True, user_list=[UID]), True),
        ("用户白名单命中用户名（兜底）", dict(enabled=True, user_list_enabled=True, user_list=[NAME]), True),
        ("用户白名单未命中 → 拦截",
         dict(enabled=True, user_list_enabled=True, user_list=[OTHER_UID]), False),
        ("用户白名单为空 → 放行（不再全员拦截）",
         dict(enabled=True, user_list_enabled=True, user_list=[]), True),
        ("用户黑名单命中 → 拦截",
         dict(enabled=True, user_list_enabled=True, user_list_type="blacklist",
              user_list=[UID]), False),
        ("用户黑名单未命中 → 放行",
         dict(enabled=True, user_list_enabled=True, user_list_type="blacklist",
              user_list=[OTHER_UID]), True),
        ("永久屏蔽开关关 → 名单里有也不拦",
         dict(enabled=True, ban_user_enabled=False, ban_user_list=[UID]), True),
        ("永久屏蔽开关开 + 命中 → 拦截",
         dict(enabled=True, ban_user_enabled=True, ban_user_list=[UID]), False),
        ("永久屏蔽优先于用户白名单",
         dict(enabled=True, ban_user_enabled=True, ban_user_list=[NAME],
              user_list_enabled=True, user_list=[UID]), False),
        ("房间名单开关开但名单为空 → 放行",
         dict(enabled=True, room_list_enabled=True, room_list_type="whitelist", room_list=[]),
         True),
        ("房间白名单命中 → 放行",
         dict(enabled=True, room_list_enabled=True, room_list_type="whitelist", room_list=[ROOM]),
         True),
        ("房间白名单未命中 → 拦截",
         dict(enabled=True, room_list_enabled=True, room_list_type="whitelist",
              room_list=["5b7ab80a2017d"]), False),
        ("房间黑名单命中 → 拦截",
         dict(enabled=True, room_list_enabled=True, room_list_type="blacklist",
              room_list=[ROOM]), False),
        ("只开房间名单不影响用户维度（黑名单里的人在别的房间照常放行）",
         dict(enabled=True, room_list_enabled=True, room_list_type="blacklist", room_list=[ROOM],
              user_list_enabled=False, user_list_type="blacklist", user_list=[UID]), False),
    ]
    problems = []
    for why, config, want in cases:
        _set_chat_list(**config)
        got, reason = decide()
        if got is not want:
            problems.append(f"{why}：期望 {'放行' if want else '拦截'}，实际 {got}（{reason}）")
    report(start_index, "三个名单独立开关 + 空名单不再误拦", problems, f"{len(cases)} 种情形")

    # 被拦截的消息绝不能投递给麦麦；独立开关要真的生效
    # （实例先建好，_set_chat_list 只改共享配置，不重新构造实例）
    problems = []
    kind, msg = adapter.parse_frame(
        _room_frame("你好呀", message_id="800000000001", user_id=UID, user_name=NAME))

    _set_chat_list(enabled=True, user_list_enabled=True, user_list_type="blacklist", user_list=[UID])
    instance.ctx.gateway.payloads.clear()  # type: ignore[attr-defined]
    await instance._dispatch_inbound(kind, msg)
    blocked_count = len(instance.ctx.gateway.payloads)  # type: ignore[attr-defined]
    if blocked_count:
        problems.append(f"用户黑名单里的消息被投递了（{blocked_count} 条）")

    _set_chat_list(enabled=True, user_list_enabled=True, user_list_type="whitelist", user_list=[UID])
    instance.ctx.gateway.payloads.clear()  # type: ignore[attr-defined]
    await instance._dispatch_inbound(kind, msg)
    allowed_count = len(instance.ctx.gateway.payloads)  # type: ignore[attr-defined]
    if allowed_count != 1:
        problems.append(f"用户白名单内的消息没有投递（{allowed_count} 条）")

    # 名单开关关掉后，同一个人立刻恢复
    _set_chat_list(enabled=True, user_list_enabled=False, user_list_type="whitelist",
                   user_list=[UID])
    instance.ctx.gateway.payloads.clear()  # type: ignore[attr-defined]
    await instance._dispatch_inbound(kind, msg)
    reopened_count = len(instance.ctx.gateway.payloads)  # type: ignore[attr-defined]
    if reopened_count != 1:
        problems.append(f"关掉用户名单开关后消息仍未投递（{reopened_count} 条）")

    report(start_index + 1, "拦截发生在投递给麦麦之前，且开关实时生效", problems,
           f"黑名单 {blocked_count} 条 / 白名单 {allowed_count} 条 / 关开关后 {reopened_count} 条")

    _set_chat_list()
    return failures


async def _check_room_change(start_index: int) -> int:
    """配置改房间号 → 实时切房（发 m 指令 + 带 lr 重连进新房间）。"""
    failures = 0
    OLD_ROOM = "64d5f8e17b2ad"
    NEW_ROOM = "5b7ab80a2017d"

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if problems else 'FAIL'}] {index}. {name}" if problems
              else f"[PASS] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 切房相关回包要能被识别
    cases = [("m6547d48b60b2b", "room_move"), ("m!5", "room_move"),
             ("`~1", "room_password"), ("`~0", "room_password")]
    problems = []
    details = []
    for frame, want in cases:
        kind, _ = adapter.parse_frame(frame)
        details.append(f"{frame}={kind}")
        if kind != want:
            problems.append(f"{frame!r} → {kind!r} 期望 {want!r}")
    report(start_index, "识别服务端切房确认（`m` / `m!5`）与密码校验（`` `~ ``）", problems,
           " ".join(details))

    # 2) 切房后的登录包必须带 lr（原房间 id）
    problems = []
    payload = json.loads(adapter.build_login(
        NEW_ROOM, "03酱", "pw", last_room_id=OLD_ROOM)[1:])
    if payload.get("r") != NEW_ROOM:
        problems.append(f"r={payload.get('r')!r}")
    if payload.get("lr") != OLD_ROOM:
        problems.append(f"lr={payload.get('lr')!r} 期望 {OLD_ROOM!r}")
    plain = json.loads(adapter.build_login(NEW_ROOM, "03酱", "pw")[1:])
    if "lr" in plain:
        problems.append("没有切房时不该带 lr")
    report(start_index + 1, "切房重连的登录包带 lr（原房间 id）", problems,
           f"r={payload.get('r')} lr={payload.get('lr')}")

    # 3) 改房间号 → 发切房指令，并记下原房间用于重连
    original_settle = adapter.MOVE_ROOM_SETTLE_SECONDS
    adapter.MOVE_ROOM_SETTLE_SECONDS = 0.01
    try:
        instance = _plugin()
        adapter.IIRoseAdapterPlugin.config.plugin.enabled = False   # 只验证切房，不真的建连接
        fake = _FakeClient()                                        # connected 固定返回 True
        instance._client = fake                                      # type: ignore[attr-defined]
        instance._active_room_id = OLD_ROOM                          # type: ignore[attr-defined]
        adapter.IIRoseAdapterPlugin.config.account.room_id = NEW_ROOM
        await instance._restart_connection_if_needed()
        problems = []
        if fake.sent != [f"m{NEW_ROOM}"]:
            problems.append(f"切房指令={fake.sent!r} 期望 ['m{NEW_ROOM}']")
        if instance._last_room_id != OLD_ROOM:                       # type: ignore[attr-defined]
            problems.append(f"未记录原房间: {instance._last_room_id!r}")  # type: ignore[attr-defined]
        report(start_index + 2, "改房间号 → 立即发 `m<新房间>` 并准备带 lr 重连", problems,
               f"sent={fake.sent!r} last_room={instance._last_room_id!r}")  # type: ignore[attr-defined]

        # 4) 房间号没变 → 不发切房指令
        instance = _plugin()
        adapter.IIRoseAdapterPlugin.config.plugin.enabled = False
        fake = _FakeClient()                                        # connected 固定返回 True
        instance._client = fake                                      # type: ignore[attr-defined]
        instance._active_room_id = OLD_ROOM                          # type: ignore[attr-defined]
        adapter.IIRoseAdapterPlugin.config.account.room_id = OLD_ROOM
        await instance._restart_connection_if_needed()
        problems = [] if not fake.sent else [f"房间没变却发了切房指令: {fake.sent!r}"]
        report(start_index + 3, "房间号未变化时不发切房指令", problems)
    finally:
        adapter.MOVE_ROOM_SETTLE_SECONDS = original_settle
        adapter.IIRoseAdapterPlugin.config.plugin.enabled = True
        adapter.IIRoseAdapterPlugin.config.account.room_id = "64d5f8e17b2ad"

    return failures


class _FakeWS:
    def __init__(self) -> None:
        self.closed = False
        self.sent: list[bytes] = []
        self.close_args: tuple = ()

    async def close(self, *args: object) -> None:
        self.closed = True
        self.close_args = args

    async def send(self, data: bytes) -> None:
        self.sent.append(data)


async def _check_shutdown(start_index: int) -> int:
    """插件停用 / 重载时必须真正关掉 WebSocket（IIROSE 靠断线判定下线）。"""
    failures = 0
    logger = logging.getLogger("iirose-test")

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) run() 收尾会把 _ws 置空；close() 仍然必须关掉真正的 socket
    problems = []
    client = adapter.IIRoseClient(lambda t: asyncio.sleep(0), logger=logger)
    fake = _FakeWS()
    client._socket_to_close = fake
    client._ws = None                      # 模拟 run() 的 finally 已经执行过
    await client.close()
    if not fake.closed:
        problems.append("close() 没有关闭 socket（插件停用后服务端会以为还在线）")
    if fake.close_args[:1] != (1000,):
        problems.append(f"关闭码不是 1000（优雅关闭）: {fake.close_args!r}")
    report(start_index, "close() 即使 _ws 已清空也真正关闭连接（关闭码 1000）", problems,
           f"closed={fake.closed} args={fake.close_args}")

    # 2) _stop_connection：先关 socket 再收尾任务，顺序不能反
    problems = []
    instance = _plugin()
    order: list[str] = []

    class _OrderedClient(_FakeClient):
        async def close(self) -> None:
            order.append("close")
            self.closed = True

    fake_client = _OrderedClient()

    async def long_running() -> None:
        try:
            await asyncio.sleep(3600)
        finally:
            # 模拟 IIRoseClient.run() 的 finally：收尾时才清空 socket 引用
            order.append("task-finalized")

    instance._client = fake_client                                  # type: ignore[attr-defined]
    instance._task = asyncio.create_task(long_running())            # type: ignore[attr-defined]
    await asyncio.sleep(0)      # 让任务真正开始执行，否则取消时它的 finally 不会跑
    await instance._stop_connection()
    if "close" not in order:
        problems.append("_stop_connection 没有调用 client.close()")
    elif "task-finalized" not in order:
        problems.append(f"任务收尾没执行: {order}")
    elif order.index("close") > order.index("task-finalized"):
        problems.append(f"关闭发生得太晚（顺序反了）: {order}")
    if instance._task is not None:                                  # type: ignore[attr-defined]
        problems.append("连接任务没有被清理")
    report(start_index + 1, "_stop_connection 先关 socket、再收尾任务（顺序正确）", problems,
           " → ".join(order) or "无")

    return failures


async def _check_connection(start_index: int) -> int:
    """连接稳定性：保活、停滞检测、永不放弃重连。"""
    logger = logging.getLogger("iirose-test")
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 退避：单调递增、有上限、带抖动，且极大 attempt 不溢出
    problems = []
    delays = [adapter.backoff_delay(n, 2.0, 300.0) for n in range(1, 12)]
    capped = [d for d in delays if d <= 302.0]
    if len(capped) != len(delays):
        problems.append(f"退避没有封顶: {delays}")
    if any(x > y + 3 for x, y in zip(delays, delays[1:])):
        problems.append(f"退避不是单调不降: {delays}")
    if delays[0] > 3.0 or delays[-1] < 200.0:
        problems.append(f"退避区间异常: {distinct(delays)}")
    # 长时间断网时 attempt 会涨到很大，不能因为 2**N 溢出把重连循环打挂
    for extreme in (10 ** 4, 10 ** 9, 10 ** 18):
        try:
            value = adapter.backoff_delay(extreme, 2.0, 300.0)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"attempt={extreme} 抛异常: {exc!r}")
            continue
        if not (0 < value <= 302.0):
            problems.append(f"attempt={extreme} 退避越界: {value}")
    report(start_index, "重连退避：指数增长 + 封顶 + 抖动 + 不溢出", problems,
           f"第1次={delays[0]:.1f}s 第11次={delays[-1]:.1f}s 第10^18次="
           f"{adapter.backoff_delay(10 ** 18, 2.0, 300.0):.1f}s")

    # 2) 保活：正常连接时每轮发一个空帧
    problems = []
    client = adapter.IIRoseClient(lambda t: asyncio.sleep(0), logger=logger,
                                  keepalive=True, keepalive_interval=30.0, stall_timeout=120.0)
    client._keepalive_interval = 0.01
    fake = _FakeWS()
    client._ws = fake
    client._last_frame_at = time.monotonic()
    watchdog = asyncio.create_task(client._watchdog_loop())
    await asyncio.sleep(0.05)
    client._stop = True
    watchdog.cancel()
    try:
        await watchdog
    except asyncio.CancelledError:
        pass
    if not fake.sent or any(frame != b"" for frame in fake.sent):
        problems.append(f"保活帧不是空串: {fake.sent[:3]!r}")
    if fake.closed:
        problems.append("正常连接被误判为异常")
    report(start_index + 1, "应用层保活：定时发空串（官方 adapter 同款）", problems,
           f"{len(fake.sent)} 个空帧")

    # 3) 停滞检测：长时间没数据 → 主动断开触发重连
    problems = []
    client = adapter.IIRoseClient(lambda t: asyncio.sleep(0), logger=logger,
                                  keepalive=True, keepalive_interval=30.0, stall_timeout=120.0)
    client._keepalive_interval = 0.01
    fake = _FakeWS()
    client._ws = fake
    client._last_frame_at = time.monotonic() - 300.0     # 假装 5 分钟没收到任何数据
    await asyncio.wait_for(client._watchdog_loop(), timeout=2.0)
    problems = [] if fake.closed else ["假死连接没有被主动关闭"]
    report(start_index + 2, "停滞检测：长时间无数据判为假死并重连", problems)

    # 4) 连续失败超过 max_retries 后仍然继续重连（不会永久离线）
    problems = []
    client = adapter.IIRoseClient(lambda t: asyncio.sleep(0), logger=logger,
                                  max_retries=3, reconnect_base_seconds=1.0,
                                  max_reconnect_seconds=8.0)
    calls = {"n": 0}

    async def failing_connect() -> None:
        calls["n"] += 1
        raise ConnectionError("模拟连接失败")

    async def fast_sleep(_delay: float) -> None:
        await asyncio.sleep(0)

    client.connect = failing_connect          # type: ignore[assignment]
    client._sleep = fast_sleep                # type: ignore[assignment]
    task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    still_running = not task.done()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    if not still_running:
        problems.append("连续失败后 run() 退出了（会永久离线）")
    if calls["n"] <= 3:
        problems.append(f"只尝试了 {calls['n']} 次，没有超过 max_retries=3")
    report(start_index + 3, "连续失败超过阈值仍然继续重连（永不放弃）", problems,
           f"max_retries=3，实际尝试 {calls['n']} 次后仍在运行")

    return failures


def distinct(values: list) -> str:
    return " → ".join(f"{v:.0f}" for v in values[:3] + values[-2:])


async def _check_room_names(start_index: int) -> int:
    """房间名：从 `%` 大包解析房间目录，并用于会话名 / 成员事件 / @房间。"""
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 真实 `%` 大包片段：含用户记录（[0] 是头像路径）和房间记录（[0] 是房间号）
    init_payload = (
        "scenery/500584>0>基站>000000>5b792977089e7>n>>>53f9aabd9987c>>>a>4>32187>0>8,45.8,0.5>"
        "<'5b792cb650749_6a4a56d0b4ca7>存在放映社 | MeowTV>4,88,58>2003>>://r.iirose.com/i/x.jpg "
        "20：00 《饮食男女》&&1&&"
        "<64d5f8e17b2ad>云端小屋>3,12,4>37>>://r.iirose.com/i/y.jpg 欢迎光临&&2&&"
        "\"第二个段：当前房间在线用户"
    )

    # 1) 房间目录解析
    directory = adapter.parse_room_directory(init_payload)
    problems = []
    for room_id, name in ({"64d5f8e17b2ad": "云端小屋",
                           "5b792cb650749": "存在放映社",
                           "6a4a56d0b4ca7": "MeowTV"}).items():
        if directory.get(room_id) != name:
            problems.append(f"{room_id} → {directory.get(room_id)!r} 期望 {name!r}")
    if "scenery/500584" in directory:
        problems.append("用户记录（[0] 是头像路径）被误判成房间")
    report(start_index, "从 `%` 大包解析房间目录（双房 `a_b` 也拆开）", problems,
           f"{len(directory)} 个房间")

    # 2) `_room_name` / `_describe_room`
    instance = _plugin()
    instance._room_names.update(directory)
    problems = []
    if instance._room_name("64d5f8e17b2ad") != "云端小屋":
        problems.append(f"_room_name 查不到: {instance._room_name('64d5f8e17b2ad')!r}")
    if instance._describe_room("64d5f8e17b2ad") != "云端小屋 (64d5f8e17b2ad)":
        problems.append(f"_describe_room={instance._describe_room('64d5f8e17b2ad')!r}")
    if instance._describe_room("fffffffffffff") != "fffffffffffff":
        problems.append("未知房间应回退成房间号")
    report(start_index + 1, "房间号 → 房间名的查表与展示格式", problems,
           instance._describe_room("64d5f8e17b2ad"))

    # 3) 会话信息里的 group_name 必须是房间名（后台聊天列表读的就是它）
    instance = _plugin()
    instance._room_names.update(directory)
    kind, msg = adapter.parse_frame(_room_frame("你好呀", message_id="900000000001"))
    instance.ctx.gateway.payloads.clear()  # type: ignore[attr-defined]
    await instance._dispatch_inbound(kind, msg)
    payload = instance.ctx.gateway.payloads[-1]  # type: ignore[attr-defined]
    group = payload["message_info"].get("group_info", {})
    problems = []
    if group.get("group_name") != "云端小屋":
        problems.append(f"group_name={group.get('group_name')!r}，应为房间名「云端小屋」")
    if group.get("group_id") != "64d5f8e17b2ad":
        problems.append(f"group_id={group.get('group_id')!r}")
    report(start_index + 2, "群聊会话名用真实房间名（不再只有房间号）", problems,
           f"group_info={group}")

    # 4) 成员换房事件要带上目标房间名
    instance = _plugin()
    instance._room_names.update(directory)
    kind, event = adapter.parse_frame(
        _member_frame(["1789213840", _AVATAR, "柒洛", "'25b792cb650749", "bd8b96", "bd8b96",
                       "2", "", _UID, "'108", "64d5f8e17b2ad", "35b792cb650749"]))
    instance.ctx.gateway.payloads.clear()  # type: ignore[attr-defined]
    await instance._handle_member_event(event)
    payload = instance.ctx.gateway.payloads[-1]  # type: ignore[attr-defined]
    text = payload.get("display_message", "")
    problems = []
    if "存在放映社" not in text or "5b792cb650749" not in text:
        problems.append(f"换房文案缺少目标房间名/房间号: {text!r}")
    notice_group = payload["message_info"].get("group_info", {})
    if notice_group.get("group_name") != "云端小屋":
        problems.append(f"通知的 group_name={notice_group.get('group_name')!r}")
    report(start_index + 3, "成员换房文案带目标房间名与房间号", problems, repr(text))

    return failures


async def _check_at(start_index: int) -> int:
    """@ 相关：配置包裹语法剥离、@房间渲染成房间名。"""
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 配置里的包裹语法必须被剥离（官方 adapter 的配置说明就是这么写的）
    cases = [
        ("[*03酱*]", adapter.normalize_username, "03酱"),
        ("  03酱  ", adapter.normalize_username, "03酱"),
        ("03酱", adapter.normalize_username, "03酱"),
        ("[@62B98115CEFD2@]", adapter.normalize_uid, "62b98115cefd2"),
        ("62B98115CEFD2", adapter.normalize_uid, "62b98115cefd2"),
        ("[_64D5F8E17B2AD_]", adapter.normalize_room_id, "64D5F8E17B2AD"),
        ("64d5f8e17b2ad", adapter.normalize_room_id, "64d5f8e17b2ad"),
    ]
    problems = []
    for raw, func, want in cases:
        got = func(raw)
        if got != want:
            problems.append(f"{func.__name__}({raw!r}) = {got!r} 期望 {want!r}")
    report(start_index, "剥离 `[*名字*]` / `[@uid@]` / `[_房间id_]` 包裹语法", problems,
           f"{len(cases)} 种写法")

    # 2) 填了带括号的用户名时，机器人仍然要认出「被 @ 的是自己」
    problems = []
    instance = _plugin()
    adapter.IIRoseAdapterPlugin.config.account.username = "[*03酱*]"
    adapter.IIRoseAdapterPlugin.config.account.uid = "[@62b98115cefd2@]"
    components = instance._build_components(
        " [*03酱*]  你好", self_uid=instance._self_uid, self_name=instance._username)
    ats = [c for c in components if c.get("type") == "at"]
    if len(ats) != 1:
        problems.append(f"at 组件数={len(ats)}：{components!r}")
    else:
        if ats[0]["data"].get("target_user_id") != "62b98115cefd2":
            problems.append(f"target_user_id={ats[0]['data'].get('target_user_id')!r}"
                            "（应归一化为 uid）")
        if ats[0]["data"].get("target_user_nickname") != "03酱":
            problems.append(f"nickname={ats[0]['data'].get('target_user_nickname')!r}")
    report(start_index + 1, "配置填 `[*名字*]` / `[@uid@]` 也能认出「我被 @ 了」", problems,
           json.dumps(components, ensure_ascii=False))
    adapter.IIRoseAdapterPlugin.config.account.username = "03酱"
    adapter.IIRoseAdapterPlugin.config.account.uid = "62b98115cefd2"

    # 3) `[_房间id_]` 渲染成房间名（官方把它当 sharp，Host 没这个概念）
    problems = []
    instance = _plugin()
    instance._room_names["5b792cb650749"] = "存在放映社"
    components = instance._build_components(
        "[_5b792cb650749_] 大家来这边", self_uid="62b98115cefd2", self_name="03酱")
    visible = "".join(c.get("data", "") for c in components if c.get("type") == "text")
    if "存在放映社" not in visible:
        problems.append(f"@房间没渲染成房间名: {visible!r}")
    if any(c.get("type") == "at" for c in components):
        problems.append("`[_房间id_]` 不该产生 at 组件")
    report(start_index + 2, "`[_房间id_]`（官方 sharp）渲染成房间名，不当作 @ 人", problems,
           repr(visible))

    # 4) 用户名里带空格 / 特殊字符也要能解析（官方用 `[\s\S]+?`，不限制字符集）
    problems = []
    instance = _plugin()
    instance._remember("", user_id="aaaa1111bbbb2", user_name="比企谷 八幡", timestamp=0, text="")
    components = instance._build_components(
        " [*比企谷 八幡*]  早", self_uid="62b98115cefd2", self_name="03酱")
    ats = [c for c in components if c.get("type") == "at"]
    if len(ats) != 1 or ats[0]["data"].get("target_user_id") != "aaaa1111bbbb2":
        problems.append(f"带空格的用户名解析失败：{components!r}")
    report(start_index + 3, "用户名含空格等字符也能解析（对齐官方正则）", problems,
           json.dumps(components, ensure_ascii=False))

    # 5) 出站 @ 的两侧空格是语法的一部分
    #    官方解析正则：`/(\s+)((?:\[\*[\s\S]+?\*\])+)(\s)/g`
    #    拼完消息后顺手 strip 一下，前导空格就没了 → 接收端认不出这是 @，只显示成普通文字
    at_pattern = re.compile(r"(\s+)(?:\[\*[\s\S]+?\*\])(\s)")
    at_id_pattern = re.compile(r"(\s+)(?:\[@[\s\S]+?@\])(\s)")
    quote_pattern = re.compile(r" \(_hr\) .+? \(hr_\) ")
    problems = []
    details = []
    for label, components in (
            ("at 在开头", [{"type": "at", "data": {"target_user_id": "65abb7a99dc60"}},
                           {"type": "text", "data": "你好"}]),
            ("at 在末尾", [{"type": "text", "data": "你好"},
                           {"type": "at", "data": {"target_user_id": "65abb7a99dc60"}}]),
            ("只有 at", [{"type": "at", "data": {"target_user_id": "65abb7a99dc60"}}]),
            ("at 在中间", [{"type": "text", "data": "你好"},
                           {"type": "at", "data": {"target_user_id": "65abb7a99dc60"}},
                           {"type": "text", "data": "在吗"}]),
    ):
        text = await instance._extract_text(_message(components, group_id="64d5f8e17b2ad"))
        details.append(f"{label}={text!r}")
        if not at_pattern.search(text):
            problems.append(f"{label}: {text!r} 不符合官方 @ 语法（两侧必须有空格）")
    uid_only = await instance._extract_text(
        _message([{"type": "at", "data": {"target_user_id": "fffffffffffff"}}],
                 group_id="64d5f8e17b2ad"))
    details.append(f"按 uid={uid_only!r}")
    if not at_id_pattern.search(uid_only):
        problems.append(f"按 uid 提及不符合官方语法: {uid_only!r}")

    quote_text = await instance._extract_text({
        "message_id": "m1", "platform": "iirose",
        "message_info": {"user_info": {"user_id": "u1", "user_nickname": "Es."},
                         "additional_config": {},
                         "group_info": {"group_id": "64d5f8e17b2ad"}},
        "raw_message": [{"type": "reply", "data": {"target_message_id": "760771550794"}},
                        {"type": "text", "data": "嗯嗯"}]})
    details.append(f"引用={quote_text!r}")
    if not quote_pattern.search(quote_text):
        problems.append(f"引用标记的空格不对（官方按 ` (_hr) ` / ` (hr_) ` 拆）: {quote_text!r}")
    report(start_index + 4, "出站 @ / 引用的两侧空格符合官方解析正则（不能被 strip 掉）", problems,
           " | ".join(details))

    return failures


def _check_config_labels(start_index: int) -> int:
    """静态检查：每个配置字段都要有中文 label，否则 WebUI 面板上只显示英文键名。"""
    import ast

    source_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugin.py")
    tree = ast.parse(open(source_path, encoding="utf-8").read())

    missing: list[str] = []
    total = 0
    sections: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or not node.name.endswith("Section"):
            continue
        sections.append(node.name)
        has_ui_label = any(
            isinstance(item, ast.Assign)
            and any(getattr(t, "id", "") == "__ui_label__" for t in item.targets)
            for item in node.body
        )
        if not has_ui_label:
            missing.append(f"{node.name}: 缺少 __ui_label__（配置节标题）")

        for item in node.body:
            if not isinstance(item, ast.AnnAssign) or not isinstance(item.value, ast.Call):
                continue
            call = item.value
            if getattr(call.func, "id", "") != "Field":
                continue
            field = getattr(item.target, "id", "?")
            total += 1
            extra = next((kw.value for kw in call.keywords if kw.arg == "json_schema_extra"), None)
            if extra is None:
                missing.append(f"{node.name}.{field}: 没有 json_schema_extra")
                continue
            label = None
            if isinstance(extra, ast.Dict):
                for key, value in zip(extra.keys, extra.values):
                    if isinstance(key, ast.Constant) and key.value == "label":
                        label = value
            if label is None or not isinstance(label, ast.Constant) or not isinstance(label.value, str):
                missing.append(f"{node.name}.{field}: 缺少 label")
            elif not any("\u4e00" <= ch <= "\u9fff" for ch in label.value):
                missing.append(f"{node.name}.{field}: label 不是中文（{label.value!r}）")

    problems = list(missing)
    if problems:
        failures = 1
    else:
        failures = 0
    print(f"[{'FAIL' if problems else 'PASS'}] {start_index}. "
          f"配置字段全部带中文 label（{total} 个字段 / {len(sections)} 个配置节）")
    for problem in problems:
        print(f"        {problem}")
    return failures


async def _check_quote_ref(start_index: int) -> int:
    """引用标记里的数字必须是**被引用消息的时间戳（秒）**，不是消息 id。

    真实事故：把 12 位随机消息 id（`828102986562`）当成时间戳发出去，
    IIROSE 客户端按日期渲染，房间里显示成 **6088 年 9 月 19 日**。
    所以这里锁死两条：ref 必须是秒级时间戳；拿不到时间戳就不发引用。
    """
    failures = 0

    def report(index: int, name: str, problems: list[str], detail: str = "") -> None:
        nonlocal failures
        if problems:
            failures += 1
        print(f"[{'PASS' if not problems else 'FAIL'}] {index}. {name}{(' → ' + detail) if detail else ''}")
        for problem in problems:
            print(f"        {problem}")

    # 1) 正常引用：ref = 秒级时间戳，绝不能是消息 id
    instance = _plugin()
    adapter.IIRoseAdapterPlugin.config.image_host.enabled = False
    instance._remember("828102986562", user_id="6088e40d12bd1", user_name="天忧",
                       timestamp=1789271137, text="要正面")
    text = await instance._extract_text(
        _message([{"type": "reply", "data": {"target_message_id": "828102986562"}},
                  {"type": "text", "data": "反面、正面、正面"}], group_id="6a7c38409e902"))
    problems = []
    if "_hr) 天忧_1789271137 (hr_" not in text:
        problems.append(f"ref 不是秒级时间戳：{text!r}")
    if "828102986562" in text:
        problems.append(f"消息 id 混进了引用标记（客户端会渲染成 6088 年）：{text!r}")
    report(start_index, "引用的 ref 是时间戳秒、不是消息 id（不再渲染出 6088 年）",
           problems, repr(text))

    # 2) 缓存里的时间戳本身就不合理（历史 bug 形态：直接把消息 id 存成时间戳）
    instance = _plugin()
    instance._remember("828102986562", user_id="6088e40d12bd1", user_name="天忧",
                       timestamp=828102986562, text="要正面")
    text = await instance._extract_text(
        _message([{"type": "reply", "data": {"target_message_id": "828102986562"}},
                  {"type": "text", "data": "反面、正面、正面"}], group_id="6a7c38409e902"))
    report(start_index + 1, "缓存里的时间戳越界（12 位消息 id 冒充）→ 拒绝发引用",
           [] if text == "反面、正面、正面" else [f"text={text!r}"], repr(text))

    # 3) 入站 → 出站往返：ref 等于入站报文里的时间戳，而不是报文里的消息 id
    instance = _plugin()
    kind, inbound = adapter.parse_frame(
        _room_frame("要正面", message_id="828102986562", user_name="天忧",
                    timestamp=1789271137))
    await instance._dispatch_inbound(kind, inbound)
    text = await instance._extract_text(
        _message([{"type": "reply", "data": {"target_message_id": "828102986562"}},
                  {"type": "text", "data": "反面、正面、正面"}], group_id="6a7c38409e902"))
    problems = []
    if "_hr) 天忧_1789271137 (hr_" not in text:
        problems.append(f"往返后的 ref 不对：{text!r}")
    if "要正面 (_hr)" not in text:
        problems.append(f"被引用正文丢失：{text!r}")
    report(start_index + 2, "入站 → 出站往返：ref 用报文时间戳（字段 0），不用消息 id（字段 10）",
           problems, repr(text))

    # 4) 时间戳缺失（0）→ 不发引用，退化成普通文本
    instance = _plugin()
    instance._remember("123456789012", user_id="u1", user_name="某人",
                       timestamp=0, text="啥也没说")
    text = await instance._extract_text(
        _message([{"type": "reply", "data": {"target_message_id": "123456789012"}},
                  {"type": "text", "data": "回一句"}], group_id="6a7c38409e902"))
    report(start_index + 3, "时间戳缺失（0）→ 不发引用，退化成普通文本",
           [] if text == "回一句" else [f"text={text!r}"], repr(text))

    return failures


def main() -> int:
    return asyncio.run(_main())


async def _main() -> int:
    instance = _plugin()
    # CASES 只验证解码本身：关掉图床，避免测试里去连真实图床
    adapter.IIRoseAdapterPlugin.config.image_host.enabled = False
    failures = 0
    for index, (name, message, want_text, want_kind, want_target) in enumerate(CASES, 1):
        got_text = await instance._extract_text(message)
        got_kind, got_target = instance._resolve_target(message, {})

        problems = []
        if got_text != want_text:
            problems.append(f"text={got_text!r} 期望 {want_text!r}")
        if (got_kind, got_target) != (want_kind, want_target):
            problems.append(f"target=({got_kind},{got_target}) 期望 ({want_kind},{want_target})")

        status = "PASS" if not problems else "FAIL"
        if problems:
            failures += 1
        print(f"[{status}] {index:02d}. {name}")
        for problem in problems:
            print(f"        {problem}")

    legacy = _legacy_extract_text(CASES[0][1])
    print(f"\n[对照] 修复前对第 01 条的解码结果: {legacy!r} → "
          f"{'复现出站失败' if not legacy else '未复现'}\n")

    outbound_checks = 3
    inbound_checks = 12
    member_checks = 7
    media_checks = 8
    image_host_checks = 15
    chat_list_checks = 2
    room_change_checks = 4
    connection_checks = 4
    config_label_checks = 1
    shutdown_checks = 2
    room_name_checks = 4
    at_checks = 5
    quote_ref_checks = 4
    failures += await _check_gateway(len(CASES) + 1)
    failures += await _check_inbound(len(CASES) + outbound_checks + 1)
    failures += await _check_member_events(len(CASES) + outbound_checks + inbound_checks + 1)
    failures += await _check_media_card(
        len(CASES) + outbound_checks + inbound_checks + member_checks + 1)
    failures += await _check_image_host(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks + 1)
    failures += await _check_chat_list(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + 1)
    failures += await _check_room_change(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + 1)
    failures += await _check_connection(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + 1)

    failures += _check_config_labels(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + connection_checks + 1)
    failures += await _check_shutdown(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + connection_checks
        + config_label_checks + 1)
    failures += await _check_room_names(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + connection_checks
        + config_label_checks + shutdown_checks + 1)
    failures += await _check_at(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + connection_checks
        + config_label_checks + shutdown_checks + room_name_checks + 1)
    failures += await _check_quote_ref(
        len(CASES) + outbound_checks + inbound_checks + member_checks + media_checks
        + image_host_checks + chat_list_checks + room_change_checks + connection_checks
        + config_label_checks + shutdown_checks + room_name_checks + at_checks + 1)

    total = (len(CASES) + outbound_checks + inbound_checks
             + member_checks + media_checks + image_host_checks
             + chat_list_checks + room_change_checks + connection_checks
             + config_label_checks + shutdown_checks + room_name_checks + at_checks
             + quote_ref_checks)
    print(f"\n{total - failures}/{total} 通过")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
