import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {

    const channel = await conn.createChannel();

    const durable = queueType === SimpleQueueType.Durable;
    const autoDelete = queueType === SimpleQueueType.Transient;
    const exclusive = queueType === SimpleQueueType.Transient;
    const queue = await channel.assertQueue(queueName, { durable, autoDelete, exclusive, arguments: { "x-dead-letter-exchange": "peril_dlx" } });

    await channel.bindQueue(queue.queue, exchange, key);

    return [channel, queue];
};

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => AckType,
): Promise<void> {

    const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

    await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
        if (message === null) {
            return;
        }

        let data: T;

        try {
            data = JSON.parse(message.content.toString());
        } catch (err) {
            console.error("Error: ", err);
            return;
        }

        try {
            const result = handler(data);
            switch (result) {
                case AckType.Ack:
                    channel.ack(message);
                    console.log("Ack");
                    break;
                case AckType.NackDiscard:
                    channel.nack(message, false, false);
                    console.log("NackDiscard");
                    break;
                case AckType.NackRequeue:
                    channel.nack(message, false, true);
                    console.log("NackRequeue");
                    break;
                default:
                    const unreachable: never = result;
                    console.error("Unexpected ack type:", unreachable);
                    return;
            }
        } catch (err) {
            console.error("Error handling message:", err);
            channel.nack(message, false, false);
            return;
        }
    })
};