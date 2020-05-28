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

    sendMsg(conn, "queue_1", "hi", 2000);
}

let sendMsg = async (conn, queueName, msg, delay) => {
    try {
        let channel = await conn.createChannel();
        await channel.assertQueue(queueName);
        let i = 0;
        setInterval(() => {
            let payload = Buffer.from(++i + " "  + msg + "_" + new Date().toISOString());
            channel.sendToQueue(queueName, payload);
            console.log(`[AMQP] send message: '${payload}' to queue ${queueName}`);
        }, delay);
    }
    catch (e) {
        throw e;
    }
    finally {
        setTimeout(() => {
            close(conn);
        }, 10000); 
    }
}

let close = (conn) => {
    conn.close();
    console.log("[AMQP] close connection");
    process.exit(0);
}

connect();
