
import { throttle } from '@github/mini-throttle';
import DPlayer, { DPlayerType } from 'dplayer';

import Channels, { IJikkyoSessionInfo } from '@/services/Channels';
import PlayerManager from '@/services/player/PlayerManager';
import useChannelsStore from '@/stores/ChannelsStore';
import usePlayerStore from '@/stores/PlayerStore';
import useSettingsStore from '@/stores/SettingsStore';
import useUserStore from '@/stores/UserStore';
import Utils, { dayjs, CommentUtils } from '@/utils';


export interface ICommentData {
    id: number;
    text: string;
    time: string;
    playback_position: number;
    user_id: string;
    premium: string | number | null;
    my_post: boolean;
}

/** A comment as the server's jikkyo comment stream sends it */
interface IJikkyoLiveComment {
    no: number;
    text: string;
    at: number;
    user_id: string;
    color: string;
    type: 'top' | 'right' | 'bottom';
    size: 'big' | 'medium' | 'small';
}

type JikkyoStreamState = 'connecting' | 'connected' | 'unavailable';


const MESSAGE_SWITCH_OFF = 'ニコニコ実況への接続はオフに設定されています。';
const MESSAGE_NOT_AVAILABLE = 'このチャンネルはニコニコ実況に対応していません。';
const MESSAGE_INFO_FAILED = 'ニコニコ実況の情報を取得できませんでした。';
const MESSAGE_NOT_CONNECTED = 'ニコニコ実況に接続できていないため、コメントを送信できません。';

// The posting session is retried with a growing wait so that a niconico outage costs one
// request per minute at most, and it returns to the short wait once a session worked again.
const POSTING_RETRY_WAIT_MIN = 3;
const POSTING_RETRY_WAIT_MAX = 60;

// An outage that comes and goes would otherwise tell the viewer about every relapse.
const UNAVAILABLE_NOTICE_INTERVAL = 10 * 60;


/**
 * Read through a call because TypeScript keeps a property narrowed across await,
 * and would treat a second check of signal.aborted after an earlier one as impossible.
 */
function isAborted(signal: AbortSignal): boolean {
    return signal.aborted;
}


/**
 * Resolve after the given time, or at once when the signal is aborted.
 * The caller checks signal.aborted afterwards; the timer itself never outlives an abort.
 */
function wait(seconds: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
        if (signal.aborted === true) {
            resolve();
            return;
        }
        const on_abort = () => {
            window.clearTimeout(timeout_id);
            resolve();
        };
        const timeout_id = window.setTimeout(() => {
            signal.removeEventListener('abort', on_abort);
            resolve();
        }, seconds * 1000);
        signal.addEventListener('abort', on_abort, { once: true });
    });
}


/**
 * Live viewing: shows Niconico Jikkyo comments over the picture and in the comment list, and posts the user's comments.
 *
 * Receiving and posting are independent: comments come from the KonomiTV server over Server-Sent Events,
 * and the browser talks to niconico directly only through the user's own watch session, which exists
 * solely to post. Either side can be down without affecting the other.
 */
class LiveCommentManager implements PlayerManager {

    // ユーザー操作により DPlayer 側で画質が切り替わった際、この PlayerManager の再起動が必要かどうかを PlayerController に示す値
    public readonly restart_required_when_quality_switched = false;

    // DPlayer のインスタンス
    // 設計上コンストラクタ以降で変更すべきでないため readonly にしている
    private readonly player: DPlayer;

    // One controller per init() run. destroy() aborts it, which removes every listener of that run,
    // cancels its timers, and tells any init() still awaiting a request to give up. A fresh controller
    // per run means a late continuation of an old run can never act on the next run's state.
    private abort_controller: AbortController = new AbortController();

    // Comment stream from the KonomiTV server
    private eventsource: EventSource | null = null;

    // The user's own nicolive watch session, used only to post
    private watch_session: WebSocket | null = null;

    // vpos を計算する基準となる時刻のタイムスタンプ
    private vpos_base_timestamp: number = 0;

    // Anonymous id of the user in the current programme, to recognise their own comments in the stream
    private hashed_user_id: string | null = null;

    // 座席維持用のタイマーのインターバル ID
    private keep_seat_interval_id: number | null = null;

    private posting_retry_wait = POSTING_RETRY_WAIT_MIN;

    // Why there is no usable watch session right now, shown when the user tries to post.
    // null means a session is open or being opened.
    private posting_unavailable_reason: string | null = null;

    /**
     * コンストラクタ
     * @param player DPlayer のインスタンス
     */
    constructor(player: DPlayer) {
        this.player = player;
    }


    /**
     * Start receiving comments and, when the user can post, open their watch session.
     */
    public async init(): Promise<void> {
        const channels_store = useChannelsStore();
        const player_store = usePlayerStore();
        const settings_store = useSettingsStore();
        const user_store = useUserStore();

        this.abort_controller = new AbortController();
        const signal = this.abort_controller.signal;
        this.posting_unavailable_reason = null;

        // Off means no request at all, so nothing can retry against niconico or the server.
        if (settings_store.settings.connect_to_niconico_jikkyo === false) {
            this.posting_unavailable_reason = MESSAGE_SWITCH_OFF;
            player_store.live_comment_init_failed_message = MESSAGE_SWITCH_OFF;
            console.log('[LiveCommentManager] Connection to Niconico Jikkyo is turned off.');
            return;
        }

        // The channel is fixed for the life of this run; a channel change destroys and re-inits.
        const channel_id = channels_store.channel.current.id;

        await user_store.fetchUser();
        if (isAborted(signal) === true) return;

        const session_info = await Channels.fetchJikkyoSessionInfo(channel_id);
        if (isAborted(signal) === true) return;
        if (session_info === null) {
            this.posting_unavailable_reason = MESSAGE_INFO_FAILED;
            player_store.live_comment_init_failed_message = MESSAGE_INFO_FAILED;
            return;
        }
        if (session_info.is_available === false) {
            this.posting_unavailable_reason = MESSAGE_NOT_AVAILABLE;
            player_store.live_comment_init_failed_message = MESSAGE_NOT_AVAILABLE;
            return;
        }

        this.startReceiving(channel_id, signal);

        this.connectPostingSession(channel_id, signal, session_info);

        console.log('[LiveCommentManager] Initialized.');
    }


    /**
     * Subscribe to the server's comment stream for the channel.
     */
    private startReceiving(channel_id: string, signal: AbortSignal): void {
        const player_store = usePlayerStore();

        const eventsource = new EventSource(`${Utils.api_base_url}/channels/${channel_id}/jikkyo/comments`);
        this.eventsource = eventsource;

        // The server repeats the current state after every reconnection of the EventSource,
        // so acting only on a change is what keeps a notice from showing again each time.
        let last_state: JikkyoStreamState | null = null;
        let last_notice_time: number | null = null;

        // 受信したコメントを一時的に格納するバッファ (スロットル用)
        const comments_buffer: ICommentData[] = [];

        // スロットルを設定し、333ms 未満の間隔でイベントが発火しないようにする
        const emit_comments = throttle(() => {
            if (isAborted(signal) === true) return;
            if (Utils.isSafari() === false) {
                console.debug('[LiveCommentManager][CommentStream] Comments buffer length:', comments_buffer.length);
            }
            player_store.event_emitter.emit('CommentReceived', {
                is_initial_comments: false,
                comments: comments_buffer.slice(),
            });
            comments_buffer.length = 0;
        }, 333);
        signal.addEventListener('abort', () => emit_comments.cancel(), { once: true });

        eventsource.addEventListener('initial', (event: MessageEvent) => {
            const data: { comments: IJikkyoLiveComment[] } = JSON.parse(event.data);
            // The user's own comments stay here: the list is replaced, so they would be lost otherwise.
            const comments = data.comments
                .filter((comment) => this.isMuted(comment) === false)
                .map((comment) => this.toCommentData(comment));
            player_store.event_emitter.emit('CommentReceived', {
                is_initial_comments: true,
                comments: comments,
            });
        }, { signal });

        eventsource.addEventListener('comments', async (event: MessageEvent) => {
            const data: { live: boolean, comments: IJikkyoLiveComment[] } = JSON.parse(event.data);

            // The user's own comments were already added to the list by CommentSendCompleted.
            const comments = data.comments.filter((comment) => this.isMuted(comment) === false && this.isOwnComment(comment) === false);
            if (comments.length === 0) return;

            // Older comments (history or caught up after an interruption) belong to the list only;
            // drawing them now would put them over a picture they were not written about.
            if (data.live === false) {
                player_store.event_emitter.emit('CommentReceived', {
                    is_initial_comments: false,
                    comments: comments.map((comment) => this.toCommentData(comment)),
                });
                return;
            }

            // The picture lags the broadcast by about the buffered length; waiting for it makes
            // each comment appear with the scene it was written about.
            let buffered_end = 0;
            if (this.player.video.buffered.length >= 1) {
                buffered_end = this.player.video.buffered.end(0);
            }
            const comment_delay_time = Math.max(buffered_end - this.player.video.currentTime, 0);
            if (Utils.isSafari() === false) {
                console.debug(`[LiveCommentManager][CommentStream] Delay: ${comment_delay_time} sec.`);
            }
            await wait(comment_delay_time, signal);
            if (isAborted(signal) === true) return;

            for (const comment of comments) {
                comments_buffer.push(this.toCommentData(comment));
                if (this.player.video.paused === false) {
                    this.player.danmaku!.draw({
                        text: comment.text,
                        color: comment.color,
                        type: comment.type,
                        size: comment.size,
                    });
                }
            }
            emit_comments();
        }, { signal });

        eventsource.addEventListener('status', (event: MessageEvent) => {
            const data: { state: JikkyoStreamState, detail: string } = JSON.parse(event.data);
            if (data.state === last_state) return;
            last_state = data.state;
            console.log(`[LiveCommentManager][CommentStream] State: ${data.state} ${data.detail}`);

            if (data.state === 'unavailable') {
                const now = Utils.time();
                if ((last_notice_time === null || now - last_notice_time >= UNAVAILABLE_NOTICE_INTERVAL) &&
                    this.player.template.notice.textContent!.includes('再起動しています…') === false) {
                    this.player.notice(data.detail, undefined, undefined, '#FF6F6A');
                    last_notice_time = now;
                }
                player_store.live_comment_init_failed_message = data.detail;
            } else if (data.state === 'connected') {
                player_store.live_comment_init_failed_message = null;
            }
        }, { signal });

        eventsource.addEventListener('error', () => {
            // While CONNECTING the browser reconnects by itself and the server re-sends the state,
            // so there is nothing to show. CLOSED means the server refused the request; retrying
            // a refused request would only repeat the refusal.
            if (eventsource.readyState === EventSource.CLOSED) {
                console.error('[LiveCommentManager][CommentStream] The server refused the comment stream.');
                player_store.live_comment_init_failed_message = 'コメントの受信に失敗しました。';
                eventsource.close();
                if (this.eventsource === eventsource) {
                    this.eventsource = null;
                }
            } else {
                console.warn('[LiveCommentManager][CommentStream] Connection lost, the browser is reconnecting.');
            }
        }, { signal });
    }


    /**
     * Open the user's own watch session, which is used only to post.
     * @param url single-use watch session URL from the server
     */
    private openPostingSession(channel_id: string, url: string, signal: AbortSignal): void {
        const watch_session = new WebSocket(url);
        this.watch_session = watch_session;
        console.log('[LiveCommentManager][WatchSession] Connecting.');

        // Every handler checks that its socket is still the current one: a lost socket is replaced,
        // and events it still delivers while closing must not touch the new session.
        const is_current = () => this.watch_session === watch_session;

        watch_session.addEventListener('open', () => {
            if (is_current() === false) return;
            watch_session.send(JSON.stringify({
                type: 'startWatching',
                data: {
                    'reconnect': false,
                },
            }));
        }, { signal });

        const on_close = (event: CloseEvent | Event) => {
            if (is_current() === false) return;
            const code = (event instanceof CloseEvent) ? event.code : 'Error';
            this.onPostingSessionLost(channel_id, `Connection closed. (Code: ${code})`, signal);
        };
        watch_session.addEventListener('close', on_close, { signal });
        watch_session.addEventListener('error', on_close, { signal });

        watch_session.addEventListener('message', (event: MessageEvent) => {
            if (is_current() === false) return;
            const message = JSON.parse(event.data);
            switch (message.type) {

                // 座席情報
                case 'seat': {
                    if (this.keep_seat_interval_id !== null) {
                        window.clearInterval(this.keep_seat_interval_id);
                    }
                    // keepIntervalSec の秒数ごとに keepSeat を送信して座席を維持する
                    this.keep_seat_interval_id = window.setInterval(() => {
                        if (watch_session.readyState === WebSocket.OPEN) {
                            watch_session.send(JSON.stringify({type: 'keepSeat'}));
                        }
                    }, message.data.keepIntervalSec * 1000);
                    break;
                }

                // ping-pong
                case 'ping': {
                    // 送り返さなかった場合、勝手にセッションが閉じられてしまう
                    watch_session.send(JSON.stringify({type: 'pong'}));
                    break;
                }

                case 'messageServer': {
                    // vpos は番組開始時間からの累計秒数
                    this.vpos_base_timestamp = dayjs(message.data.vposBaseTime).valueOf();
                    // hashedUserId: 匿名コメント投稿時に用いられる自身のユーザ ID (ログインユーザのみ取得可能)
                    if (typeof message.data.hashedUserId === 'string') {
                        this.hashed_user_id = message.data.hashedUserId;
                    }
                    // The session works, so the next loss is treated as a fresh one.
                    this.posting_retry_wait = POSTING_RETRY_WAIT_MIN;
                    this.posting_unavailable_reason = null;
                    console.log('[LiveCommentManager][WatchSession] Connected.');
                    break;
                }

                case 'error': {
                    // These two answer a post and are reported by sendComment(); the session itself is fine.
                    if (message.data.code === 'COMMENT_POST_NOT_ALLOWED' || message.data.code === 'INVALID_MESSAGE') {
                        break;
                    }
                    this.onPostingSessionLost(channel_id, `Error occurred. (Code: ${message.data.code})`, signal);
                    break;
                }

                // The reconnect message offers a token for resuming, but the watch session URL is
                // single-use anyway, so a fresh URL from the server is the one reliable way back.
                case 'reconnect': {
                    this.onPostingSessionLost(channel_id, 'Reconnect requested.', signal);
                    break;
                }

                // 視聴セッションが閉じられた（4時のリセットなど）
                case 'disconnect': {
                    this.onPostingSessionLost(channel_id, `Disconnected. (Reason: ${message.data.reason})`, signal);
                    break;
                }
            }
        }, { signal });
    }


    /**
     * Get a working watch session, retrying until one opens or the failure cannot change within this run.
     * Both the first answer in init() and a lost session enter here, so there is one retry policy.
     * The retries are silent: the session matters only when the viewer posts, and sendComment() tells them then.
     * @param session_info the server's answer to use first; without it, wait and fetch a fresh one
     */
    private async connectPostingSession(channel_id: string, signal: AbortSignal, session_info?: IJikkyoSessionInfo): Promise<void> {
        let info: IJikkyoSessionInfo | null | undefined = session_info;
        for (;;) {
            if (info === undefined) {
                const wait_seconds = this.posting_retry_wait;
                this.posting_retry_wait = Math.min(this.posting_retry_wait * 2, POSTING_RETRY_WAIT_MAX);
                console.warn(`[LiveCommentManager][WatchSession] Fetching a new watch session in ${wait_seconds} sec.`);
                await wait(wait_seconds, signal);
                if (isAborted(signal) === true) return;
                info = await Channels.fetchJikkyoSessionInfo(channel_id, false);
                if (isAborted(signal) === true) return;
            }

            if (info !== null && info.nicolive_watch_session_url !== null) {
                this.openPostingSession(channel_id, info.nicolive_watch_session_url, signal);
                return;
            }

            // Without an error text the server had no URL because of the channel or the user's account,
            // which no retry can change. An error text (e.g. the next programme not yet on air after the
            // daily reset) or a failed request (e.g. a short network outage) can clear by itself.
            if (info !== null && (info.is_available === false || info.nicolive_watch_session_error === null)) {
                this.posting_unavailable_reason = this.getPostingUnavailableReason(info);
                console.warn(`[LiveCommentManager][WatchSession] Posting is not possible: ${this.posting_unavailable_reason}`);
                return;
            }
            this.posting_unavailable_reason = info?.nicolive_watch_session_error ?? MESSAGE_INFO_FAILED;
            console.warn(`[LiveCommentManager][WatchSession] No watch session: ${this.posting_unavailable_reason}`);
            info = undefined;
        }
    }


    /**
     * @param reason for the console only
     */
    private onPostingSessionLost(channel_id: string, reason: string, signal: AbortSignal): void {
        this.closePostingSession();
        console.warn(`[LiveCommentManager][WatchSession] ${reason}`);
        this.connectPostingSession(channel_id, signal);
    }


    /**
     * Close the current watch session and stop keeping its seat.
     */
    private closePostingSession(): void {
        if (this.keep_seat_interval_id !== null) {
            window.clearInterval(this.keep_seat_interval_id);
            this.keep_seat_interval_id = null;
        }
        if (this.watch_session !== null) {
            const watch_session = this.watch_session;
            // Cleared first, so the close event this triggers is ignored by the session's own handlers.
            this.watch_session = null;
            watch_session.close();
        }
        // A new session may belong to a new programme (the daily reset), so its vpos base must come from its own messageServer.
        this.vpos_base_timestamp = 0;
    }


    /**
     * The reason shown on a post attempt when the server gave no URL and no error text.
     */
    private getPostingUnavailableReason(session_info: IJikkyoSessionInfo): string {
        const user_store = useUserStore();
        if (session_info.is_available === false) {
            return MESSAGE_NOT_AVAILABLE;
        }
        if (user_store.user === null) {
            return 'コメントするには、KonomiTV アカウントにログインしてニコニコアカウントと連携してください。';
        }
        if (user_store.user.niconico_user_id === null) {
            return 'コメントするには、ニコニコアカウントと連携してください。';
        }
        return MESSAGE_NOT_CONNECTED;
    }


    private isMuted(comment: IJikkyoLiveComment): boolean {
        return CommentUtils.isMutedComment(comment.text, comment.user_id, comment.color, comment.type, comment.size, null);
    }


    /**
     * Anonymous posts carry the hashed id of the current programme, named posts the numeric niconico id.
     */
    private isOwnComment(comment: IJikkyoLiveComment): boolean {
        const user_store = useUserStore();
        if (this.hashed_user_id !== null && comment.user_id === this.hashed_user_id) {
            return true;
        }
        const niconico_user_id = user_store.user?.niconico_user_id ?? null;
        return niconico_user_id !== null && comment.user_id === String(niconico_user_id);
    }


    private toCommentData(comment: IJikkyoLiveComment): ICommentData {
        return {
            id: comment.no,
            text: comment.text,
            time: Utils.apply28HourClock(dayjs(comment.at * 1000).format('HH:mm:ss')),
            playback_position: this.player.video.currentTime,
            user_id: comment.user_id,
            premium: null,
            my_post: false,
        };
    }


    /**
     * Post a comment from DPlayer through the user's watch session and report the result through its callbacks.
     */
    public sendComment(options: DPlayerType.APIBackendSendOptions): void {
        const player_store = usePlayerStore();
        const user_store = useUserStore();

        if (this.posting_unavailable_reason !== null) {
            options.error(this.posting_unavailable_reason);
            return;
        }

        if (user_store.user?.niconico_user_premium === false && (options.data.type === 'top' || options.data.type === 'bottom')) {
            options.error('ニコニコ実況でコメントを上下に固定するには、ニコニコアカウントのプレミアム会員登録が必要です。');
            return;
        }
        if (user_store.user?.niconico_user_premium === false && options.data.size === 'big') {
            options.error('ニコニコ実況でコメントサイズを大きめに設定するには、ニコニコアカウントのプレミアム会員登録が必要です。');
            return;
        }

        // Without messageServer there is no vpos base, and niconico rejects a post with a wrong vpos.
        const watch_session = this.watch_session;
        if (watch_session === null || watch_session.readyState !== WebSocket.OPEN || this.vpos_base_timestamp === 0) {
            console.error('[LiveCommentManager][WatchSession] Comment sending failed. (Session is not open.)');
            options.error(MESSAGE_NOT_CONNECTED);
            return;
        }

        // DPlayer 上のコメント色（カラーコード）とニコニコの色コマンド定義のマッピング
        const color_table = {
            '#FFEAEA': 'white',
            '#F02840': 'red',
            '#FD7E80': 'pink',
            '#FDA708': 'orange',
            '#FFE133': 'yellow',
            '#64DD17': 'green',
            '#00D4F5': 'cyan',
            '#4763FF': 'blue',
        };

        // DPlayer 上のコメント位置を表す値とニコニコの位置コマンド定義のマッピング
        const position_table = {
            'top': 'ue',
            'right': 'naka',
            'bottom': 'shita',
        };

        // vpos を計算 (10ミリ秒単位)
        // 番組開始時間からの累計秒らしいけど、なぜ指定しないといけないのかは不明
        // 小数点以下は丸めないとコメントサーバー側で投稿エラーになる
        const vpos = Math.round((dayjs().valueOf() - this.vpos_base_timestamp) / 10);

        // コメントを送信
        watch_session.send(JSON.stringify({
            'type': 'postComment',
            'data': {
                // コメント本文
                'text': options.data.text,
                // コメントの色
                'color': color_table[options.data.color.toUpperCase()],
                // コメント位置
                'position': position_table[options.data.type],
                // コメントサイズ (DPlayer とニコニコで表現が共通なため、変換不要)
                'size': options.data.size,
                // 番組開始時間からの累計秒 (10ミリ秒単位)
                'vpos': vpos,
                // 匿名コメント (184) にするかどうか
                'isAnonymous': true,
            }
        }));

        // コメント送信のレスポンスを取得
        // The per-post listeners also end with the run, so nothing calls back into a player being torn down.
        const abort_controller = new AbortController();
        this.abort_controller.signal.addEventListener('abort', () => abort_controller.abort(), {
            once: true,
            signal: abort_controller.signal,
        });
        watch_session.addEventListener('message', (event) => {
            const message = JSON.parse(event.data);
            switch (message.type) {

                // postCommentResult が送られてきた → コメント送信に成功している
                case 'postCommentResult': {
                    // コメント成功を DPlayer にコールバックで通知
                    options.success();

                    // 送信したコメントを整形してコメントリストに送信
                    player_store.event_emitter.emit('CommentSendCompleted', {
                        comment: {
                            id: Utils.time(),  // ID は取得できないので現在の時間をユニークな ID として利用する
                            text: options.data.text,  // コメント本文
                            time: Utils.apply28HourClock(dayjs().format('HH:mm:ss')),  // 現在時刻
                            playback_position: this.player.video.currentTime,  // 現在の再生位置
                            user_id: `${user_store.user?.niconico_user_id ?? 'Unknown'}`,  // ニコニコユーザー ID
                            premium: null,  // 自分が投稿したコメントの premium フラグは取得できない
                            my_post: true,  // 自分のコメントであることを示すフラグ
                        }
                    });

                    // イベントリスナーを削除
                    abort_controller.abort();
                    break;
                }

                // コメント送信直後に error が送られてきた → コメント送信に失敗している
                case 'error': {
                    // コメント失敗を DPlayer にコールバックで通知
                    let error = `コメントの送信に失敗しました。(${message.data.code})`;
                    switch (message.data.code) {
                        case 'COMMENT_POST_NOT_ALLOWED':
                            error = 'コメントが許可されていません。';
                            break;
                        case 'INVALID_MESSAGE':
                            error = 'コメント内容が無効です。';
                            break;
                    }
                    console.error(`[LiveCommentManager][WatchSession] Comment sending failed. (Code: ${message.data.code})`);
                    options.error(error);

                    // イベントリスナーを解除
                    abort_controller.abort();
                    break;
                }
            }

        }, { signal: abort_controller.signal });

        // A session lost before it answered would otherwise leave the post without any result.
        watch_session.addEventListener('close', () => {
            console.error('[LiveCommentManager][WatchSession] Comment sending failed. (Session closed before the result.)');
            options.error(MESSAGE_NOT_CONNECTED);
            abort_controller.abort();
        }, { signal: abort_controller.signal });
    }


    public async destroy(): Promise<void> {
        const player_store = usePlayerStore();

        // Removes every listener of this run, cancels its waits and stops an init() still in progress.
        this.abort_controller.abort();

        if (this.eventsource !== null) {
            this.eventsource.close();
            this.eventsource = null;
        }

        this.closePostingSession();
        this.hashed_user_id = null;
        this.posting_retry_wait = POSTING_RETRY_WAIT_MIN;
        this.posting_unavailable_reason = null;

        // 初期化に失敗した際のエラーメッセージを削除
        player_store.live_comment_init_failed_message = null;

        console.log('[LiveCommentManager] Destroyed.');
    }
}

export default LiveCommentManager;
