export const SocketCommand = {
    Authenticate: 1,
    Ping: 2,
    PingActive: 4,
    PushUserMessage: 501,
    PushGroupMessage: 521,
    VerifyMessage: 532,
    HasMessage: 533,
    PushMissedMessage: 534,
    GetMessage: 570,
    PushControl: 601,
    PullControl: 603,
    PullUserReaction: 610,
    PullGroupReaction: 611,
    PullUserMessage: 510,
    PullGroupMessage: 511,
} as const;

export const PingSubCommand = {
    KeepConnection: 1,
    KeepActive: 2,
} as const;

export const OFFLINE_QUEUE_COMMANDS: ReadonlySet<number> = new Set([
    SocketCommand.PullUserMessage,
    SocketCommand.PullGroupMessage,
    SocketCommand.PullControl,
    SocketCommand.PullUserReaction,
    SocketCommand.PullGroupReaction,
]);

export const MESSAGE_QUEUE_COMMANDS: ReadonlySet<number> = new Set([
    SocketCommand.PullUserMessage,
    SocketCommand.PullGroupMessage,
]);

export type QueueCommand = {
    cmd: number;
    subCmd: number;
    queueName?: string;
};

export function queueKeyOf(queue: QueueCommand): string {
    return `${queue.cmd}_${queue.subCmd}`;
}
