from __future__ import annotations

import asyncio
import html
import json
import re
import time
from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any, ClassVar, Literal, NamedTuple, TypedDict, cast

import aiohttp
import httpx2 as httpx

from app import logging
from app.constants import API_REQUEST_HEADERS, HTTPX_CLIENT


# Relays niconico's live comment service to the browsers.
# Browsers cannot read niconico's comment stream themselves because its HTTP server sends no CORS
# permission, so the server receives it as an anonymous guest, once per niconico channel, and pushes it
# to every viewer of that channel. The comment stream is protobuf; the few messages needed here are
# decoded by hand instead of pulling in a protobuf runtime and generated code for them.


DETAIL_PAGE_UNREACHABLE = 'ニコニコ生放送に接続できませんでした。ニコニコで障害が発生している可能性があります。'
DETAIL_PROGRAM_NOT_FOUND = '現在放送中のニコニコ実況番組が見つかりませんでした。'
DETAIL_DISCONNECTED = 'ニコニコ実況との接続が切断されました。'
DETAIL_COMMENT_SERVER_UNREACHABLE = 'ニコニコ実況のコメントサーバーに接続できませんでした。'
DETAIL_UNREADABLE = 'ニコニコ実況のコメントサーバーから読み取れないデータを受信しました。'
DETAIL_UNEXPECTED = 'ニコニコ実況との通信中に予期しないエラーが発生しました。'

# The run loop waits this long before the next session. The wait doubles after each session that
# failed before or soon after connecting, and starts over after one that stayed connected for
# STABLE_SESSION_SECONDS. Counting a session as working the moment it connects would retry every few
# seconds for ever against a service that accepts connections and then drops them, and each of those
# rounds reaches the viewers as a new outage.
RETRY_WAIT_FIRST = 3.0
RETRY_WAIT_MAX = 60.0
STABLE_SESSION_SECONDS = 60.0

# Browsers' EventSource reconnects are routine, so a stream outlives its last subscriber for a while
# instead of tearing down and rebuilding the niconico session on every reconnect.
LINGER_SECONDS = 15.0

# A subscriber that falls this far behind is dropped; its browser reconnects and gets a fresh
# initial event, which costs less than buffering without bound for a stalled client.
SUBSCRIBER_QUEUE_SIZE = 256

RECENT_COMMENTS_SIZE = 100

# Comment numbers are external input, so duplicates are found with a bounded set of the most recent
# numbers instead of one entry per comment for the whole day.
SEEN_NUMBERS_SIZE = 16384

# A comment posted at most this long ago by niconico's clock is drawn over the picture.
LIVE_THRESHOLD_SECONDS = 5.0

# A segment opened before its start is silent until it starts, about 23 s at most.
STREAM_TIMEOUT = httpx.Timeout(10.0, read=40.0)

# Covers only the WebSocket handshake; a dead connection afterwards is noticed by the heartbeat.
WEBSOCKET_HANDSHAKE_TIMEOUT = aiohttp.ClientTimeout(total=10.0)
WEBSOCKET_HEARTBEAT_SECONDS = 30.0

# Enum tables of dwango.nicolive.chat.data.atoms.Chat.Modifier, indexed by enum value.
POSITIONS: tuple[Literal['top', 'right', 'bottom'], ...] = ('right', 'bottom', 'top')  # naka, shita, ue
SIZES: tuple[Literal['big', 'medium', 'small'], ...] = ('medium', 'small', 'big')

# Colour names of niconico comments and their codes, in the order of the Chat.Modifier.ColorName enum.
NAMED_COLOR_CODES: dict[str, str] = {
    'white': '#FFEAEA',
    'red': '#F02840',
    'pink': '#FD7E80',
    'orange': '#FDA708',
    'yellow': '#FFE133',
    'green': '#64DD17',
    'cyan': '#00D4F5',
    'blue': '#4763FF',
    'purple': '#D500F9',
    'black': '#1E1310',
    'white2': '#CCCC99',
    'red2': '#CC0033',
    'pink2': '#FF33CC',
    'orange2': '#FF6600',
    'yellow2': '#999900',
    'green2': '#00CC66',
    'cyan2': '#00CCCC',
    'blue2': '#3399FF',
    'purple2': '#6633CC',
    'black2': '#666666',
}
NAMED_COLORS: tuple[str, ...] = tuple(NAMED_COLOR_CODES.values())

# Watch session messages the relay acts on. Every other kind is skipped unread, so a message niconico
# adds or changes later cannot end the session.
WATCH_MESSAGE_KINDS = frozenset({'ping', 'serverTime', 'seat', 'messageServer', 'disconnect', 'reconnect', 'error'})

EMBEDDED_DATA_PATTERN = re.compile(r'<script id="embedded-data" data-props="([^"]*)"')


class NicoliveUnavailable(Exception):
    """ niconico cannot be reached, answered with an error, or sent data that cannot be read. """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        # Japanese sentence shown to the viewer
        self.detail: str = detail


class _UnreadableData(NicoliveUnavailable):
    """ Data from niconico that does not decode; the reason goes to the log, not to the viewer. """

    def __init__(self, reason: str) -> None:
        super().__init__(DETAIL_UNREADABLE)
        self.reason: str = reason

    def __str__(self) -> str:
        return f'{self.detail} ({self.reason})'


class _SessionEnded(Exception):
    """ The session ended in a way niconico announced (programme change, reconnect request). """


class NicoliveProgram(NamedTuple):
    program_id: str
    websocket_url: str


class JikkyoLiveComment(TypedDict):
    no: int
    text: str
    at: float
    user_id: str
    color: str
    type: Literal['top', 'right', 'bottom']
    size: Literal['big', 'medium', 'small']


class StreamEvent(NamedTuple):
    event: Literal['initial', 'comments', 'status']
    data: dict[str, Any]


async def FetchNicoliveProgram(nicochannel_id: str) -> NicoliveProgram:
    """
    Find the programme a niconico channel broadcasts now and a guest watch session URL for it.

    Raises:
        NicoliveUnavailable: the watch page cannot be fetched or read, or the programme is not on air
    """

    try:
        async with HTTPX_CLIENT() as client:
            response = await client.get(f'https://live.nicovideo.jp/watch/{nicochannel_id}')
    except httpx.RequestError as ex:
        raise NicoliveUnavailable(DETAIL_PAGE_UNREACHABLE) from ex
    if response.status_code != 200:
        raise NicoliveUnavailable(f'現在、ニコニコ生放送でエラーが発生しています。(HTTP Error {response.status_code})')

    match = EMBEDDED_DATA_PATTERN.search(response.text)
    if match is None:
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] The watch page has no embedded data.')
        raise NicoliveUnavailable(DETAIL_PROGRAM_NOT_FOUND)
    try:
        props = json.loads(html.unescape(match.group(1)))
        program_status = props['program']['status']
        if program_status != 'ON_AIR':
            raise NicoliveUnavailable(DETAIL_PROGRAM_NOT_FOUND)
        program_id = props['program']['nicoliveProgramId']
        websocket_url = props['site']['relive']['webSocketUrl']
    except (ValueError, KeyError, TypeError) as ex:
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] The watch page data cannot be read. ({ex!r})')
        raise NicoliveUnavailable(DETAIL_PROGRAM_NOT_FOUND) from ex
    if not isinstance(program_id, str) or not isinstance(websocket_url, str) or not program_id or not websocket_url:
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] The watch page has no programme id or watch session URL.')
        raise NicoliveUnavailable(DETAIL_PROGRAM_NOT_FOUND)

    return NicoliveProgram(program_id, websocket_url)


# ***** protobuf wire format *****

# Field number -> values in wire order. Varint and fixed-width values are ints, length-delimited
# values are bytes, so reading a field with the wrong wire type is detected.
type _Fields = dict[int, list[int | bytes]]


def _ReadVarint(data: bytes | bytearray, position: int) -> tuple[int, int] | None:
    """ Returns (value, next position), or None when the data ends inside the varint. """

    result = 0
    shift = 0
    while position < len(data):
        byte = data[position]
        position += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, position
        shift += 7
        if shift >= 70:
            raise _UnreadableData('varint longer than 10 bytes')
    return None


def _ParseMessage(data: bytes) -> _Fields:
    fields: _Fields = {}
    position = 0
    while position < len(data):
        key = _ReadVarint(data, position)
        if key is None:
            raise _UnreadableData('message ends inside a field key')
        number, position = key[0] >> 3, key[1]
        wire_type = key[0] & 7
        value: int | bytes
        if wire_type == 0:
            varint = _ReadVarint(data, position)
            if varint is None:
                raise _UnreadableData('message ends inside a varint')
            value, position = varint
        elif wire_type == 2:
            length = _ReadVarint(data, position)
            if length is None or length[1] + length[0] > len(data):
                raise _UnreadableData('message ends inside a length-delimited field')
            value = data[length[1]:length[1] + length[0]]
            position = length[1] + length[0]
        elif wire_type in (1, 5):
            width = 8 if wire_type == 1 else 4
            if position + width > len(data):
                raise _UnreadableData('message ends inside a fixed-width field')
            value = int.from_bytes(data[position:position + width], 'little')
            position += width
        else:
            raise _UnreadableData(f'unsupported wire type {wire_type}')
        if number == 0:
            raise _UnreadableData('field number 0')
        fields.setdefault(number, []).append(value)
    return fields


def _GetInt(fields: _Fields, number: int) -> int | None:
    values = fields.get(number)
    if values is None:
        return None
    value = values[-1]
    if not isinstance(value, int):
        raise _UnreadableData(f'field {number} is not a varint')
    return value


def _GetSigned(fields: _Fields, number: int) -> int:
    """ int32 and int64 fields: negative values are sent as 64-bit two's complement. """

    value = _GetInt(fields, number) or 0
    return value - (1 << 64) if value >= (1 << 63) else value


def _GetBytes(fields: _Fields, number: int) -> bytes | None:
    values = fields.get(number)
    if values is None:
        return None
    value = values[-1]
    if not isinstance(value, bytes):
        raise _UnreadableData(f'field {number} is not length-delimited')
    return value


def _GetString(fields: _Fields, number: int) -> str | None:
    value = _GetBytes(fields, number)
    if value is None:
        return None
    try:
        return value.decode('utf-8')
    except UnicodeDecodeError as ex:
        raise _UnreadableData(f'field {number} is not UTF-8') from ex


def _GetMessage(fields: _Fields, number: int) -> _Fields | None:
    value = _GetBytes(fields, number)
    return None if value is None else _ParseMessage(value)


def _GetRepeated(fields: _Fields, number: int) -> list[bytes]:
    values = fields.get(number, [])
    if not all(isinstance(value, bytes) for value in values):
        raise _UnreadableData(f'field {number} is not length-delimited')
    return cast(list[bytes], values)


def _GetTimestamp(fields: _Fields, number: int) -> float | None:
    """ google.protobuf.Timestamp as unix seconds. """

    timestamp = _GetMessage(fields, number)
    if timestamp is None:
        return None
    return _GetSigned(timestamp, 1) + _GetSigned(timestamp, 2) / 1_000_000_000


class _DelimitedReader:
    """ Splits a stream of varint-length-prefixed messages arriving in arbitrary chunks. """

    def __init__(self) -> None:
        self.__buffer = bytearray()

    def feed(self, chunk: bytes) -> list[bytes]:
        """ Returns every message completed by this chunk. """

        buffer = self.__buffer
        buffer += chunk
        messages: list[bytes] = []
        position = 0
        while position < len(buffer):
            length = _ReadVarint(buffer, position)
            if length is None or length[1] + length[0] > len(buffer):
                break
            messages.append(bytes(buffer[length[1]:length[1] + length[0]]))
            position = length[1] + length[0]
        del buffer[:position]
        return messages

    def finish(self) -> None:
        if self.__buffer:
            raise _UnreadableData('stream ends inside a message')


def _ParseChat(nicochannel_id: str, chunked_message: bytes) -> JikkyoLiveComment | None:
    """
    Converts a ChunkedMessage to a comment. Returns None when it is not a chat, or when it is one
    without a time or with a modifier outside the known tables; that one comment is skipped and logged,
    because guessing a default would show it wrongly and failing the session would lose every other comment.
    """

    chunked = _ParseMessage(chunked_message)
    payload = _GetMessage(chunked, 2)
    if payload is None:
        return None
    chat = _GetMessage(payload, 1)
    if chat is None:
        return None

    meta = _GetMessage(chunked, 1)
    at = _GetTimestamp(meta, 2) if meta is not None else None
    if at is None:
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] Skipped a comment without a time.')
        return None

    hashed_user_id = _GetString(chat, 6)
    raw_user_id = _GetInt(chat, 5)
    if hashed_user_id:
        user_id = hashed_user_id
    elif raw_user_id is not None:
        user_id = str(_GetSigned(chat, 5))
    else:
        user_id = ''

    modifier = _GetMessage(chat, 7) or {}
    position = _GetInt(modifier, 1) or 0
    size = _GetInt(modifier, 2) or 0
    full_color = _GetMessage(modifier, 4)
    named_color = _GetInt(modifier, 3) or 0
    if position >= len(POSITIONS) or size >= len(SIZES):
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] Skipped a comment with unknown position {position} or size {size}.')
        return None
    if full_color is not None:
        rgb = (_GetSigned(full_color, 1), _GetSigned(full_color, 2), _GetSigned(full_color, 3))
        if not all(0 <= component <= 255 for component in rgb):
            logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] Skipped a comment with colour {rgb}.')
            return None
        color = '#{:02X}{:02X}{:02X}'.format(*rgb)
    elif named_color < len(NAMED_COLORS):
        color = NAMED_COLORS[named_color]
    else:
        logging.warning(f'[NicoliveCommentStream][{nicochannel_id}] Skipped a comment with unknown named colour {named_color}.')
        return None

    return JikkyoLiveComment(
        no = _GetSigned(chat, 8),
        text = _GetString(chat, 1) or '',
        at = at,
        user_id = user_id,
        color = color,
        type = POSITIONS[position],
        size = SIZES[size],
    )


# ***** the relay *****

def _JsonObject(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _UnreadableData(f'expected a JSON object, got {value!r:.100}')
    return cast(dict[str, Any], value)


class NicoliveCommentStream:
    """
    One niconico channel's comments, shared by all its subscribers.

    A stream starts with its first subscriber and stops LINGER_SECONDS after its last one leaves. While it
    has no subscribers and after it stops, nothing talks to niconico: a viewer turning comments off must
    end every connection and every retry.
    """

    __streams: ClassVar[dict[str, NicoliveCommentStream]] = {}

    # Strong references to every run task, including stopped ones still closing their connections,
    # so they are not collected mid-cleanup and closeAll() can wait for them.
    __tasks: ClassVar[set[asyncio.Task[None]]] = set()


    def __init__(self, nicochannel_id: str) -> None:
        self.__nicochannel_id = nicochannel_id
        self.__subscribers: set[asyncio.Queue[StreamEvent]] = set()
        self.__status: dict[str, Any] = {'state': 'connecting', 'detail': ''}
        self.__recent_comments: deque[JikkyoLiveComment] = deque(maxlen=RECENT_COMMENTS_SIZE)
        self.__seen_numbers: set[int] = set()
        self.__seen_numbers_order: deque[int] = deque()
        self.__program_id: str | None = None
        # niconico's clock minus time.monotonic(), from the watch session's serverTime
        self.__clock_offset: float | None = None
        # time.monotonic() when the current session connected, None until it does
        self.__connected_at: float | None = None
        self.__stop_timer: asyncio.TimerHandle | None = None
        self.__stopped = False
        self.__run_task = asyncio.create_task(self.__run(), name=f'NicoliveCommentStream[{nicochannel_id}]')
        NicoliveCommentStream.__tasks.add(self.__run_task)
        self.__run_task.add_done_callback(NicoliveCommentStream.__tasks.discard)


    @classmethod
    async def subscribe(cls, nicochannel_id: str) -> AsyncGenerator[StreamEvent]:
        """
        Yields an initial event with the recent comments, a status event with the current state, then
        every comments and status event. Ends when the subscriber falls too far behind or the server
        shuts down. Close the generator (aclose) when leaving so the stream learns of it at once.
        """

        stream = cls.__streams.get(nicochannel_id)
        if stream is None:
            stream = cls(nicochannel_id)
            cls.__streams[nicochannel_id] = stream
        queue: asyncio.Queue[StreamEvent] = asyncio.Queue(SUBSCRIBER_QUEUE_SIZE)
        stream.__addSubscriber(queue)
        try:
            while True:
                try:
                    event = await queue.get()
                except asyncio.QueueShutDown:
                    return
                yield event
        finally:
            stream.__removeSubscriber(queue)


    @classmethod
    async def closeAll(cls) -> None:
        """ Server shutdown: stop every stream and wait until its connections are closed. """

        for stream in list(cls.__streams.values()):
            stream.__stop()
        tasks = list(cls.__tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


    def __addSubscriber(self, queue: asyncio.Queue[StreamEvent]) -> None:
        if self.__stop_timer is not None:
            self.__stop_timer.cancel()
            self.__stop_timer = None
        # The queue is new and empty, so these two cannot overflow it.
        queue.put_nowait(StreamEvent('initial', {'comments': list(self.__recent_comments)}))
        queue.put_nowait(StreamEvent('status', dict(self.__status)))
        self.__subscribers.add(queue)


    def __removeSubscriber(self, queue: asyncio.Queue[StreamEvent]) -> None:
        self.__subscribers.discard(queue)
        if not self.__subscribers and not self.__stopped and self.__stop_timer is None:
            self.__stop_timer = asyncio.get_running_loop().call_later(LINGER_SECONDS, self.__stop)


    def __stop(self) -> None:
        """ Takes the stream out of the registry and cancels its run task; its subscribers' generators end. """

        self.__stopped = True
        if self.__stop_timer is not None:
            self.__stop_timer.cancel()
            self.__stop_timer = None
        if NicoliveCommentStream.__streams.get(self.__nicochannel_id) is self:
            del NicoliveCommentStream.__streams[self.__nicochannel_id]
        self.__run_task.cancel()
        for queue in self.__subscribers:
            queue.shutdown(immediate=True)
        self.__subscribers.clear()


    def __broadcast(self, event: StreamEvent) -> None:
        for queue in list(self.__subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logging.warning(f'[NicoliveCommentStream][{self.__nicochannel_id}] Dropped a subscriber that fell behind.')
                queue.shutdown(immediate=True)
                self.__removeSubscriber(queue)


    def __setStatus(self, state: Literal['connecting', 'connected', 'unavailable'], detail: str = '') -> None:
        status = {'state': state, 'detail': detail}
        if status == self.__status:
            return
        self.__status = status
        logging.info(f'[NicoliveCommentStream][{self.__nicochannel_id}] {state}{f" ({detail})" if detail else ""}')
        self.__broadcast(StreamEvent('status', dict(status)))


    def __publish(self, comments: list[JikkyoLiveComment]) -> None:
        """ Drops comments already published for this programme and sends the rest as one batch. """

        fresh: list[JikkyoLiveComment] = []
        for comment in comments:
            number = comment['no']
            if number != 0:
                if number in self.__seen_numbers:
                    continue
                if len(self.__seen_numbers_order) == SEEN_NUMBERS_SIZE:
                    self.__seen_numbers.discard(self.__seen_numbers_order.popleft())
                self.__seen_numbers.add(number)
                self.__seen_numbers_order.append(number)
            fresh.append(comment)
        if not fresh:
            return

        # Without niconico's clock nothing can be shown to be live, so it all goes to the list only.
        if self.__clock_offset is None:
            older, live = fresh, []
        else:
            now = time.monotonic() + self.__clock_offset
            older = [comment for comment in fresh if now - comment['at'] > LIVE_THRESHOLD_SECONDS]
            live = [comment for comment in fresh if now - comment['at'] <= LIVE_THRESHOLD_SECONDS]
        # Same order as the events below, so a subscriber's initial list matches what earlier ones received.
        self.__recent_comments.extend(older)
        self.__recent_comments.extend(live)
        if older:
            self.__broadcast(StreamEvent('comments', {'live': False, 'comments': older}))
        if live:
            self.__broadcast(StreamEvent('comments', {'live': True, 'comments': live}))


    def __publishMessages(self, messages: list[bytes]) -> None:
        comments: list[JikkyoLiveComment] = []
        for message in messages:
            comment = _ParseChat(self.__nicochannel_id, message)
            if comment is not None:
                comments.append(comment)
        self.__publish(comments)


    async def __run(self) -> None:
        wait = RETRY_WAIT_FIRST
        while True:
            self.__connected_at = None
            try:
                await self.__runSession()
                self.__setStatus('connecting')
            except NicoliveUnavailable as ex:
                logging.warning(f'[NicoliveCommentStream][{self.__nicochannel_id}] Session failed: {ex}')
                self.__setStatus('unavailable', ex.detail)
            except Exception as ex:
                logging.error(f'[NicoliveCommentStream][{self.__nicochannel_id}] Session failed unexpectedly.', exc_info=ex)
                self.__setStatus('unavailable', DETAIL_UNEXPECTED)
            stable = self.__connected_at is not None and time.monotonic() - self.__connected_at >= STABLE_SESSION_SECONDS
            if stable:
                wait = RETRY_WAIT_FIRST
            await asyncio.sleep(wait)
            if not stable:
                wait = min(wait * 2, RETRY_WAIT_MAX)


    async def __runSession(self) -> None:
        program = await FetchNicoliveProgram(self.__nicochannel_id)
        if program.program_id != self.__program_id:
            # Comment numbers restart with each programme.
            self.__program_id = program.program_id
            self.__recent_comments.clear()
            self.__seen_numbers.clear()
            self.__seen_numbers_order.clear()

        try:
            async with (
                httpx.AsyncClient(headers=API_REQUEST_HEADERS, follow_redirects=True, timeout=STREAM_TIMEOUT) as client,
                aiohttp.ClientSession(headers=API_REQUEST_HEADERS, timeout=WEBSOCKET_HANDSHAKE_TIMEOUT) as http,
            ):
                try:
                    websocket = await http.ws_connect(program.websocket_url, heartbeat=WEBSOCKET_HEARTBEAT_SECONDS)
                except (aiohttp.ClientError, TimeoutError) as ex:
                    raise NicoliveUnavailable(DETAIL_DISCONNECTED) from ex
                async with websocket, asyncio.TaskGroup() as tasks:
                    await self.__watch(websocket, tasks, client)
        # The task group wraps whatever ended the session; unwrap the one that matters to the run loop.
        except* _SessionEnded:
            pass
        except* NicoliveUnavailable as group:
            raise group.exceptions[0] from None


    async def __watch(self, websocket: aiohttp.ClientWebSocketResponse, tasks: asyncio.TaskGroup, client: httpx.AsyncClient) -> None:
        """ Runs the watch session; starts the keep-seat sender and the comment stream reader in tasks. """

        keep_seat: asyncio.Task[None] | None = None
        comment_stream_started = False
        try:
            await websocket.send_json({'type': 'startWatching', 'data': {'reconnect': False}})
            async for message in websocket:
                if message.type is aiohttp.WSMsgType.ERROR:
                    raise NicoliveUnavailable(DETAIL_DISCONNECTED) from websocket.exception()
                if message.type is not aiohttp.WSMsgType.TEXT:
                    raise _UnreadableData(f'watch session sent a {message.type.name} frame')
                try:
                    received = _JsonObject(json.loads(cast(str, message.data)))
                except ValueError as ex:
                    raise _UnreadableData('watch session sent invalid JSON') from ex
                kind = received.get('type')
                if kind not in WATCH_MESSAGE_KINDS:
                    continue
                data = _JsonObject(received.get('data', {}))

                if kind == 'ping':
                    await websocket.send_json({'type': 'pong'})
                elif kind == 'serverTime':
                    try:
                        server_time = datetime.fromisoformat(data['currentMs']).timestamp()
                    except (KeyError, TypeError, ValueError) as ex:
                        raise _UnreadableData(f'serverTime {data!r:.200}') from ex
                    self.__clock_offset = server_time - time.monotonic()
                elif kind == 'seat':
                    interval = data.get('keepIntervalSec')
                    if not isinstance(interval, int | float) or isinstance(interval, bool) or interval <= 0:
                        raise _UnreadableData(f'seat {data!r:.200}')
                    if keep_seat is not None:
                        keep_seat.cancel()
                    keep_seat = tasks.create_task(self.__keepSeat(websocket, float(interval)))
                elif kind == 'messageServer':
                    view_uri = data.get('viewUri')
                    if not isinstance(view_uri, str) or not view_uri:
                        raise _UnreadableData(f'messageServer {data!r:.200}')
                    if not comment_stream_started:
                        comment_stream_started = True
                        tasks.create_task(self.__readCommentStream(client, tasks, view_uri))
                elif kind == 'disconnect':
                    reason = data.get('reason')
                    if reason == 'END_PROGRAM':
                        logging.info(f'[NicoliveCommentStream][{self.__nicochannel_id}] The programme ended.')
                        raise _SessionEnded()
                    raise NicoliveUnavailable(f'{DETAIL_DISCONNECTED} ({reason})' if reason else DETAIL_DISCONNECTED)
                elif kind == 'reconnect':
                    wait = data.get('waitTimeSec', 0)
                    if not isinstance(wait, int | float) or isinstance(wait, bool) or wait < 0:
                        raise _UnreadableData(f'reconnect {data!r:.200}')
                    logging.info(f'[NicoliveCommentStream][{self.__nicochannel_id}] niconico asked to reconnect in {wait} s.')
                    await asyncio.sleep(wait)
                    raise _SessionEnded()
                elif kind == 'error':
                    code = data.get('code')
                    raise NicoliveUnavailable(f'{DETAIL_DISCONNECTED} ({code})' if code else DETAIL_DISCONNECTED)
        except (aiohttp.ClientError, ConnectionError) as ex:
            raise NicoliveUnavailable(DETAIL_DISCONNECTED) from ex
        logging.warning(f'[NicoliveCommentStream][{self.__nicochannel_id}] The watch session closed. (code: {websocket.close_code})')
        raise NicoliveUnavailable(DETAIL_DISCONNECTED)


    @staticmethod
    async def __keepSeat(websocket: aiohttp.ClientWebSocketResponse, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await websocket.send_json({'type': 'keepSeat'})
        except (aiohttp.ClientError, ConnectionError) as ex:
            raise NicoliveUnavailable(DETAIL_DISCONNECTED) from ex


    async def __get(self, client: httpx.AsyncClient, url: str, params: dict[str, str] | None = None) -> bytes:
        try:
            response = await client.get(url, params=params)
        except httpx.RequestError as ex:
            raise NicoliveUnavailable(DETAIL_COMMENT_SERVER_UNREACHABLE) from ex
        if response.status_code != 200:
            raise NicoliveUnavailable(f'ニコニコ実況のコメントサーバーでエラーが発生しています。(HTTP Error {response.status_code})')
        return response.content


    async def __readCommentStream(self, client: httpx.AsyncClient, tasks: asyncio.TaskGroup, view_uri: str) -> None:
        """
        Follows the view stream: each response announces the segments of the next ~32 s and where to
        continue. Consecutive segments overlap on the wire, so each one is read by its own task.
        """

        reader = _DelimitedReader()
        at: int | None = None
        for entry in reader.feed(await self.__get(client, view_uri, {'at': 'now'})):
            if (ready := _GetMessage(_ParseMessage(entry), 4)) is not None:
                at = _GetInt(ready, 1)
        reader.finish()
        if at is None:
            raise _UnreadableData('view stream at=now has no next')

        first_response = True
        greatest_segment_from: float | None = None
        while True:
            next_at: int | None = None
            try:
                async with client.stream('GET', view_uri, params={'at': str(at)}) as response:
                    if response.status_code != 200:
                        raise NicoliveUnavailable(f'ニコニコ実況のコメントサーバーでエラーが発生しています。(HTTP Error {response.status_code})')
                    if first_response:
                        self.__connected_at = time.monotonic()
                        self.__setStatus('connected')
                    reader = _DelimitedReader()
                    async for chunk in response.aiter_bytes():
                        for entry in reader.feed(chunk):
                            fields = _ParseMessage(entry)
                            if (backward := _GetMessage(fields, 2)) is not None:
                                if first_response and not self.__recent_comments:
                                    await self.__readBackward(client, backward)
                            elif (previous := _GetMessage(fields, 3)) is not None:
                                if first_response:
                                    await self.__readPrevious(client, previous)
                            elif (segment := _GetMessage(fields, 1)) is not None:
                                segment_from = _GetTimestamp(segment, 1)
                                segment_uri = _GetString(segment, 3)
                                if segment_from is None or not segment_uri:
                                    raise _UnreadableData('segment without from or uri')
                                if greatest_segment_from is None or segment_from > greatest_segment_from:
                                    greatest_segment_from = segment_from
                                    tasks.create_task(self.__readSegment(client, segment_uri))
                            elif (ready := _GetMessage(fields, 4)) is not None:
                                next_at = _GetInt(ready, 1)
                    reader.finish()
            except httpx.RequestError as ex:
                raise NicoliveUnavailable(DETAIL_COMMENT_SERVER_UNREACHABLE) from ex
            if next_at is None:
                raise _UnreadableData('view stream response ended without next')
            first_response = False
            at = next_at


    async def __readBackward(self, client: httpx.AsyncClient, backward: _Fields) -> None:
        """ The packed history before the previous segments; one page is enough to fill the recent list. """

        page = _GetMessage(backward, 2)
        uri = _GetString(page, 1) if page is not None else None
        if not uri:
            raise _UnreadableData('backward without segment uri')
        packed = _ParseMessage(await self.__get(client, uri))
        self.__publishMessages(_GetRepeated(packed, 1))


    async def __readPrevious(self, client: httpx.AsyncClient, previous: _Fields) -> None:
        uri = _GetString(previous, 3)
        if not uri:
            raise _UnreadableData('previous without uri')
        reader = _DelimitedReader()
        messages = reader.feed(await self.__get(client, uri))
        reader.finish()
        self.__publishMessages(messages)


    async def __readSegment(self, client: httpx.AsyncClient, uri: str) -> None:
        try:
            async with client.stream('GET', uri) as response:
                if response.status_code != 200:
                    raise NicoliveUnavailable(f'ニコニコ実況のコメントサーバーでエラーが発生しています。(HTTP Error {response.status_code})')
                reader = _DelimitedReader()
                async for chunk in response.aiter_bytes():
                    self.__publishMessages(reader.feed(chunk))
                reader.finish()
        except httpx.RequestError as ex:
            raise NicoliveUnavailable(DETAIL_COMMENT_SERVER_UNREACHABLE) from ex
