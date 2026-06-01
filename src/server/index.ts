import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

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


  const confirmChannel = await conn.createConfirmChannel();

  printServerHelp();
  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }

    const firstWord = words[0];

    if (firstWord === "pause") {
      console.log("Sending pause message")
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
      } catch (error) {
        console.error("Error publishing message:", error);
      }
    } else if (firstWord === "resume") {
      console.log("Sending resume message")
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, { isPaused: false });
      } catch (error) {
        console.error("Error publishing message:", error);
      }
    } else if (firstWord === "quit") {
      console.log("Exiting...")
      process.exit(0);
    } else {
      console.log("Invalid command")
    }

  };
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
