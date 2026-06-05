import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/consume.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";

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
  await subscribeJSON(conn, ExchangePerilTopic, WarRecognitionsPrefix, `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(gameState));

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
      console.log("Spamming not allowed yet!");
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
