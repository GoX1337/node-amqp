require('dotenv').config();
const amqp = require('amqplib');

let connect = async () => {
    let conn = await amqp.connect(process.env.CLOUDAMQP_URL);
    console.log("[AMQP] connected");
    conn.on("error", (err) => {
        console.error("[AMQP] conn error", err.message);
    });
    conn.on("close", () => {
        console.error("[AMQP] close");
    });

    sendMsg(conn, "queue_1", "hey", 1000);
    sendMsg(conn, "queue_2", "kek", 500);
    sendMsg(conn, "queue_3", "lulz", 1500);
}

let sendMsg = async (conn, queueName, msg, delay) => {
    let channel;
    try {
        channel = await conn.createChannel();
        await channel.assertQueue(queueName);
        let i = 0;
        setInterval(() => {
            let payload = Buffer.from(++i + " "  + msg + "_" + new Date().toISOString());
            channel.sendToQueue(queueName, payload);
            console.log(`[AMQP] send message: '${payload}' to channel ${queueName}`);
        }, delay);
    }
    catch (e) {
        throw e;
    }
    finally {
        setTimeout(() => {
            close(conn, channel);
        }, 10000);
    }
}

let close = (conn, channel) => {
    conn.close();
    console.log("[AMQP] close connection");
    process.exit(0);
}

connect();
