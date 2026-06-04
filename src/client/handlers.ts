import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState): AckType => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    }
};

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
    return (move: ArmyMove): AckType => {
        try {
            const moveOutcome = handleMove(gs, move);
            switch (moveOutcome) {
                case MoveOutcome.MakeWar:
                case MoveOutcome.Safe:
                    return AckType.Ack;

                case MoveOutcome.SamePlayer:
                default:
                    return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }
    }
};