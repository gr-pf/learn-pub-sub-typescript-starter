import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState): AckType => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    }
};

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove): Promise<AckType> => {
        try {
            const moveOutcome = handleMove(gs, move);
            switch (moveOutcome) {
                case MoveOutcome.MakeWar:
                    try {
                        await publishJSON(
                            ch,
                            ExchangePerilTopic,
                            `${WarRecognitionsPrefix}.${gs.getUsername()}`,
                            {
                                attacker: move.player,
                                defender: gs.getPlayerSnap(),
                            }
                        )
                    } catch (error) {
                        console.error("Error publishing war recognition:", error);
                    } finally {
                        return AckType.NackRequeue
                    }

                case MoveOutcome.Safe:
                case MoveOutcome.SamePlayer:
                    return AckType.Ack;

                default:
                    return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }
    }
};

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => AckType {
    return (rw: RecognitionOfWar): AckType => {
        try {
            const warOutcome = handleWar(gs, rw);
            switch (warOutcome.result) {
                case WarOutcome.NotInvolved:
                    return AckType.NackRequeue

                case WarOutcome.NoUnits:
                    return AckType.NackDiscard

                case WarOutcome.OpponentWon:
                case WarOutcome.YouWon:
                case WarOutcome.Draw:
                    return AckType.Ack

                default:
                    console.error("Error");
                    return AckType.NackDiscard
            }

        } finally {
            process.stdout.write("> ");
        }
    }
}