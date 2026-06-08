import amqp, { type ConfirmChannel } from "amqplib";
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/consume.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const confirmChannel = await conn.createConfirmChannel();

  if (conn) {
    console.log("Connected to RabbitMQ");
  }

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  const userName = await clientWelcome();
  const gameState = new GameState(userName);


  await subscribeJSON(conn, ExchangePerilTopic, `army_moves.${userName}`, "army_moves.*", SimpleQueueType.Transient, handlerMove(gameState, confirmChannel));
  await subscribeJSON(conn, ExchangePerilDirect, `pause.${userName}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
  await subscribeJSON(conn, ExchangePerilTopic, WarRecognitionsPrefix, `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(gameState, confirmChannel));


  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }

    const command = words[0];

    if (command === "spawn") {
      try {
        commandSpawn(gameState, words);
      } catch (error) {
        console.error("Error spawning unit:", error);
      }
    } else if (command === "move") {
      try {
        const move = commandMove(gameState, words);
        await publishJSON(confirmChannel, ExchangePerilTopic, `army_moves.${userName}`, move)

      } catch (error) {
        console.error("Error moving unit:", error);
      }
    } else if (command === "status") {
      await commandStatus(gameState);
    } else if (command === "help") {
      printClientHelp();
    } else if (command === "spam") {
      if (words.length < 2) {
        throw new Error("Command spam missing argument");
      }
      const raw = words[1];
      if (!raw) {
        console.log("usage: spam <n>");
        continue;
      }
      const iter = parseInt(raw, 10);
      if (isNaN(iter)) {
        console.log(`error: ${words[1]} is not a valid number`);
        continue;
      }
      for (let i = 0; i < iter; i++) {
        try {
          await publishGameLog(confirmChannel, gameState.getUsername(), getMaliciousLog());
        } catch (err) {
          console.error(
            "Failed to publish spam message:",
            (err as Error).message,
          );
          continue;
        }
      }

      //console.log("Spamming not allowed yet!");
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else {
      console.log("Invalid command")
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});


export function publishGameLog(channel: ConfirmChannel, username: string, message: string) {

  const gameLog: GameLog = {
    username,
    message,
    currentTime: new Date()
  }

  return publishMsgPack(channel, ExchangePerilTopic, `${GameLogSlug}.${username}`, gameLog)

}