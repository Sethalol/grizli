import asyncio
import html
import json
import os
import time
 
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from confluent_kafka import Consumer, KafkaError, KafkaException
 
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])  # id чата/канала, куда шлём новости
 
SOURCE_TOPIC = "tg_bot"
IDLE_TIMEOUT = 5.0
 
CONSUMER_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.environ.get("KAFKA_GROUP_ID", "tg-bot-broadcaster"),
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
}
 
 
def format_message(text: str) -> str:
    return f"🆕 {html.escape(text)}"
 
 
def parse_message(raw: bytes):
    if raw is None:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[ОШИБКА ДЕКОДА] {repr(e)}")
        return None
 
    if not isinstance(payload, dict):
        print(f"[ОШИБКА ПАРСИНГА] Неизвестный тип данных: {type(payload)}")
        return None
 
    text = payload.get("text")
    if not text:
        return None
    return text
 
 
async def send_pending() -> None:
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([SOURCE_TOPIC])
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
 
    sent_count = 0
    empty_since = None
 
    try:
        while True:
            msg = consumer.poll(1.0)
 
            if msg is None:
                if empty_since is None:
                    empty_since = time.monotonic()
                elif time.monotonic() - empty_since >= IDLE_TIMEOUT:
                    print("Новых сообщений нет, завершаем работу.")
                    break
                continue
 
            empty_since = None
 
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
 
            text = parse_message(msg.value())
 
            if text is None:
                print("[ПРОПУЩЕНО] пустое или некорректное сообщение")
                consumer.commit(asynchronous=False)
                continue
 
            try:
                await bot.send_message(CHAT_ID, format_message(text))
                sent_count += 1
                print("Новость отправлена")
            except Exception as e:
                print(f"[ОШИБКА ОТПРАВКИ] {repr(e)}")
                # не коммитим офсет — попробуем отправить эту же новость на следующем запуске
                continue
 
            consumer.commit(asynchronous=False)
 
        if sent_count == 0:
            await bot.send_message(CHAT_ID, "Новостей по мобилизации нет!")
 
    finally:
        await bot.session.close()
        consumer.close()
 
    print(f"Итого отправлено: {sent_count}")
 
 
if __name__ == "__main__":
    asyncio.run(send_pending())
