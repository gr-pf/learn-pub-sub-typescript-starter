import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {

    const content = Buffer.from(JSON.stringify(value));

    return new Promise((resolve, reject) => {
        ch.publish(
            exchange,
            routingKey,
            content,
            { contentType: "application/json" },
            (err) => {
                if (err !== null) {
                    reject(new Error("Message was NACKed by the broker"));
                } else {
                    resolve();
                }
            },
        );
    });
};

export async function publishMsgPack<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {

    const encoded: Uint8Array = encode(value);

    const content: Buffer = Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);

    return new Promise((resolve, reject) => {
        ch.publish(
            exchange,
            routingKey,
            content,
            { contentType: "application/x-msgpack" },
            (err) => {
                if (err !== null) {
                    reject(new Error("Message was NACKed by the broker"));
                } else {
                    resolve();
                }
            },
        );
    });
}