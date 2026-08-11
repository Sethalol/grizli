import asyncio
import html
import os
 
import psycopg2
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
 
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])
 
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "dbname": os.environ.get("DB_NAME", "meduza"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "1111"),
}
 
 
def format_message(title: str, url: str, topic: str | None) -> str:
    text = f"🆕 <b>{html.escape(title or '')}</b>"
    if topic:
        text += f"\n🏷 {html.escape(str(topic))}"
    if url:
        text += f"\n{url}"
    return text
 
 
async def send_pending() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
 
    sent_count = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, url, topic FROM news WHERE sent = false ORDER BY id ASC"
            )
            rows = cur.fetchall()
 
        print(f"К отправке: {len(rows)}")
 
        for news_id, title, url, topic in rows:
            try:
                await bot.send_message(CHAT_ID, format_message(title, url, topic))
            except Exception as e:
                print(f"[ОШИБКА] news_id={news_id}: {repr(e)}")
                continue  # не отмечаем как отправленное — попробуем на следующем запуске DAG
 
            with conn.cursor() as cur:
                cur.execute("UPDATE news SET sent = true WHERE id = %s", (news_id,))
            conn.commit()
            sent_count += 1
            print(f"news_id={news_id} -> отправлено")
        if sent_count == 0:
            await bot.send_message(CHAT_ID, text='Новостей по мобилизации нет!')

    finally:
        await bot.session.close()
        conn.close()
 
    print(f"Итого отправлено: {sent_count}")
 
 
if __name__ == "__main__":
    asyncio.run(send_pending())
