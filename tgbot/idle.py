import asyncio
import html
import logging
import os

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("news_bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
# CHAT_ID = int(os.environ["CHAT_ID"])
POLL_INTERVAL_SECONDS = int(15)
STATE_FILE = os.environ.get("STATE_FILE", "news_bot_last_id.txt")

DB_CONFIG = dict(
    host=os.environ.get("DB_HOST", "localhost"),
    database=os.environ.get("DB_NAME", "meduza"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ.get("DB_PASSWORD", ""),
)


def load_last_id() -> int:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_last_id(value: int) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(value))


def format_message(row: asyncpg.Record) -> str:
    data = dict(row)
    title = html.escape(data.get("title") or "")
    url = data.get("url") or ""
    topic = data.get("topic")

    text = f"🆕 <b>{title}</b>"
    if topic:
        text += f"\n🏷 {html.escape(str(topic))}"
    if url:
        text += f"\n{url}"
    return text


async def poll_news(bot: Bot, pool: asyncpg.Pool) -> None:
    last_id = load_last_id()

    if last_id == 0:
        # При первом запуске не шлём весь исторический архив — стартуем с текущего максимума.
        async with pool.acquire() as conn:
            last_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM news")
        save_last_id(last_id)
        logger.info("Первый запуск: стартуем с id=%s, старые записи отправлены не будут", last_id)

    while True:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, article_id, title, url, topic FROM news WHERE id > $1 ORDER BY id ASC",
                    last_id,
                )

            for row in rows:
                try:
                    await bot.send_message(format_message(row))
                    last_id = row["id"]
                    save_last_id(last_id)
                    logger.info("Отправлено id=%s: %s", row["id"], row["title"])
                except Exception as e:
                    logger.error("Не удалось отправить id=%s: %r", row["id"], e)
                    break  # прервёмся, повторим попытку с этого же id на следующем цикле

        except Exception as e:
            logger.error("Ошибка при опросе БД: %r", e)

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def main() -> None:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start_handler(message: Message) -> None:
        # Удобно, чтобы узнать chat_id для переменной окружения CHAT_ID
        await message.answer(f"Этот чат: <code>{message.chat.id}</code>")

    pool = await asyncpg.create_pool(**DB_CONFIG)

    poll_task = asyncio.create_task(poll_news(bot, pool))
    try:
        await dp.start_polling(bot)
    finally:
        poll_task.cancel()
        await pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())