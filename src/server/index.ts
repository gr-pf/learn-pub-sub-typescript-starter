import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";

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
  const data: PlayingState = { isPaused: true }
  try {
    await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, data);
  } catch (error) {
    console.error("Error publishing message:", error);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
