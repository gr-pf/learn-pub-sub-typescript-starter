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