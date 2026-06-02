import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
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
    const queue = await channel.assertQueue(queueName, { durable, autoDelete, exclusive });

    await channel.bindQueue(queue.queue, exchange, key);

    return [channel, queue];
};

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
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

        handler(data);
        channel.ack(message);
    })
};