from fastapi import FastAPI
import aio_pika
import asyncio

BROCKER_URL = "amqp://guest:guest@localhost/"

app = FastAPI()


async def send_message(queue_name: str, message: str):
    connection = await aio_pika.connect_robust(BROCKER_URL)
    channel = await connection.channel()
    await channel.declare_queue(queue_name)
    await channel.default_exchange.publish(
        aio_pika.Message(body=message.encode()),
        routing_key=queue_name
    )
    await connection.close()


@app.post("/send_message")
async def send_message_handler(queue_name: str, message: str):
    try:
        await send_message(queue_name, message)
        return {"message": "Сообщение успешно отправлено"}
    except:
        return {"message": "Ошибка отправки сообщения"}


async def consume_messages(queue_name: str):
    connection = await aio_pika.connect_robust(BROCKER_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name)
    try:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    with open("messages.txt", "a") as file:
                        file.write(message.body.decode() + "\n")
        return {"message": "Сообщения успешно обработаны"}
    except:
        return {"message": "Ошибка обработки сообщений"}


@app.get("/start_consumer")
async def start_consumer(queue_name: str):
    await asyncio.create_task(consume_messages(queue_name))
    return {"message": "Консюмер успешно запущен"}
