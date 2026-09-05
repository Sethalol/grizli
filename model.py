"""
Агент-классификатор новостей.

Логика:
  1. Берём необработанные статьи из таблицы articles (обрабатывали ещё не все).
  2. Для каждой статьи запускаем цикл reasoning -> tool_call -> reasoning:
     модель сама решает, хватает ли заголовка, или нужно запросить полный текст
     статьи / проверить, не было ли уже похожей новости.
  3. Финальный вердикт получаем через structured output (JSON Schema) и валидируем pydantic.
  4. Релевантные новости пишем в таблицу news (её читает tgbot/idle.py).
  5. Обработанные статьи помечаем, чтобы не гонять их повторно при следующем запуске DAG.

Предполагаемая схема БД (проверьте/адаптируйте под свою):

  ALTER TABLE articles ADD COLUMN IF NOT EXISTS processed boolean DEFAULT false;

  CREATE TABLE IF NOT EXISTS news (
      id         SERIAL PRIMARY KEY,
      title      TEXT,
      url        TEXT UNIQUE,
      category   TEXT,
      importance TEXT,
      reason     TEXT,
      sent       BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT now()
  );
"""

import json
import os
import re
import sys
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import requests
from openai import OpenAI
from pydantic import BaseModel, ValidationError, field_validator

# ── конфиг ────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "dbname": os.environ.get("DB_NAME", "meduza"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
OPENROUTER_PROXY = os.environ.get("OPENROUTER_PROXY", "")
MODEL_NAME = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INSTRUCTION_PATH = os.path.join(BASE_DIR, "instruction")

MAX_STEPS = 4            # предохранитель от зацикливания агента
MAX_JSON_RETRIES = 2      # повторные попытки получить валидный JSON
SIMILAR_LOOKBACK_HOURS = 48

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)


# ── схема финального вердикта ───────────────────────────────────────────────

class Verdict(BaseModel):
    relevant: bool
    category: str
    importance: str  # HIGH | MEDIUM | LOW | NONE
    reason: str

    @field_validator("importance")
    @classmethod
    def check_importance(cls, v: str) -> str:
        allowed = {"HIGH", "MEDIUM", "LOW", "NONE"}
        if v.upper() not in allowed:
            raise ValueError(f"importance должен быть одним из {allowed}, получено: {v}")
        return v.upper()


VERDICT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "verdict",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "relevant": {"type": "boolean"},
                "category": {"type": "string"},
                "importance": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW", "NONE"]},
                "reason": {"type": "string"},
            },
            "required": ["relevant", "category", "importance", "reason"],
            "additionalProperties": False,
        },
    },
}


# ── инструменты, доступные агенту ───────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "fetch_full_article",
            "description": (
                "Получить полный текст статьи по URL. Используй, если заголовка "
                "недостаточно, чтобы понять релевантность теме по инструкции."
            ),
            "parameters": {
                "type": "object",
                "properties": {"url": {"type": "string"}},
                "required": ["url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_recent_similar",
            "description": (
                "Проверить, не было ли за последние часы уже похожей по смыслу новости "
                "в базе. Используй перед финальным вердиктом, если новость кажется релевантной, "
                "чтобы не дублировать уже отправленное."
            ),
            "parameters": {
                "type": "object",
                "properties": {"title": {"type": "string"}},
                "required": ["title"],
            },
        },
    },
]


# ── реализация инструментов ─────────────────────────────────────────────────

def fetch_full_article(url: str) -> str:
    """Лёгкий парсинг одной статьи (не путать с parcer.py, который скрапит всю ленту)."""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except requests.RequestException as e:
        return f"Не удалось получить статью: {e}"

    # простая эвристика извлечения текста без тяжёлых зависимостей;
    # если сайты требуют JS-рендеринга — замените на playwright/selenium для конкретного источника
    text = re.sub(r"<script.*?</script>|<style.*?</style>", "", resp.text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:6000]  # ограничиваем, чтобы не раздувать контекст


def check_recent_similar(title: str, conn) -> str:
    """Грубая проверка похожести по совпадению значимых слов заголовка.
    Для более точного варианта замените на сравнение embeddings (pgvector / cosine similarity)."""
    words = [w.lower() for w in re.findall(r"\w{4,}", title)]
    if not words:
        return "Похожих новостей не найдено."

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT title FROM news
            WHERE created_at > now() - interval '%s hours'
            """,
            (SIMILAR_LOOKBACK_HOURS,),
        )
        rows = cur.fetchall()

    for (existing_title,) in rows:
        existing_words = set(w.lower() for w in re.findall(r"\w{4,}", existing_title))
        overlap = existing_words.intersection(words)
        if len(overlap) >= max(2, len(words) // 3):
            return f"Похоже, уже отправлялось: «{existing_title}»"

    return "Похожих новостей не найдено."


def execute_tool(name: str, arguments: dict, conn) -> str:
    if name == "fetch_full_article":
        return fetch_full_article(arguments["url"])
    if name == "check_recent_similar":
        return check_recent_similar(arguments["title"], conn)
    return f"Неизвестный инструмент: {name}"


# ── JSON-парсинг с fallback (на случай, если модель не отдала чистую строку) ─

def extract_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("JSON не найден в ответе модели")
    return json.loads(match.group(0))


# ── основной агентный цикл на одну статью ───────────────────────────────────

def classify_article(article: dict, instruction: str, conn) -> Optional[Verdict]:
    messages = [
        {"role": "system", "content": instruction},
        {
            "role": "user",
            "content": (
                f"Заголовок: {article['title']}\n"
                f"URL: {article['url']}\n"
                "Если заголовка достаточно для вердикта — отвечай сразу в JSON. "
                "Если нет — сначала используй доступные инструменты."
            ),
        },
    ]

    for step in range(MAX_STEPS):
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            tools=TOOLS,
        )
        message = response.choices[0].message

        if message.tool_calls:
            # добавляем сообщение ассистента с запросом инструмента в историю
            messages.append(message.model_dump(exclude_none=True))
            for call in message.tool_calls:
                args = json.loads(call.function.arguments)
                result = execute_tool(call.function.name, args, conn)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": result,
                    }
                )
            continue  # даём модели ещё шаг с новым контекстом

        # модель не запросила инструмент — просим финальный структурированный вердикт
        return get_verdict(messages)

    print(f"[WARN] Превышен лимит шагов ({MAX_STEPS}) для статьи: {article['title']}")
    return None


def get_verdict(messages: list) -> Optional[Verdict]:
    local_messages = messages + [
        {"role": "user", "content": "Дай финальный вердикт строго в формате JSON по схеме."}
    ]

    for attempt in range(MAX_JSON_RETRIES + 1):
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=local_messages,
            response_format=VERDICT_SCHEMA,
        )
        raw = response.choices[0].message.content

        try:
            data = extract_json(raw)
            return Verdict.model_validate(data)
        except (ValidationError, ValueError, json.JSONDecodeError) as e:
            print(f"[WARN] Невалидный JSON (попытка {attempt + 1}): {e}")
            local_messages.append(
                {"role": "user", "content": f"Ответ не прошёл валидацию: {e}. Верни строго валидный JSON по схеме."}
            )

    print("[ERROR] Не удалось получить валидный вердикт после всех попыток")
    return None


# ── работа с БД ──────────────────────────────────────────────────────────────

def fetch_unprocessed_articles(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, title, url FROM articles WHERE processed IS NOT TRUE ORDER BY id ASC"
        )
        return cur.fetchall()


def save_verdict(conn, article: dict, verdict: Verdict) -> None:
    with conn.cursor() as cur:
        if verdict.relevant:
            cur.execute(
                """
                INSERT INTO news (title, url, category, importance, reason)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (article["title"], article["url"], verdict.category, verdict.importance, verdict.reason),
            )
        cur.execute("UPDATE articles SET processed = true WHERE id = %s", (article["id"],))
    conn.commit()


# ── точка входа ──────────────────────────────────────────────────────────────

def main() -> None:
    if not os.path.exists(INSTRUCTION_PATH):
        print(f"[ERROR] Не найден файл инструкции: {INSTRUCTION_PATH}")
        sys.exit(1)

    with open(INSTRUCTION_PATH, encoding="utf-8") as f:
        instruction = f.read()

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        articles = fetch_unprocessed_articles(conn)
        print(f"К обработке: {len(articles)}")

        relevant_count = 0
        for article in articles:
            verdict = classify_article(article, instruction, conn)
            if verdict is None:
                print(f"[SKIP] Не удалось классифицировать: {article['title']}")
                continue

            save_verdict(conn, article, verdict)
            if verdict.relevant:
                relevant_count += 1
                print(f"[MATCH] ({verdict.importance}) {article['title']} — {verdict.reason}")
            else:
                print(f"[SKIP] {article['title']} — {verdict.reason}")

        print(f"Итого релевантных: {relevant_count} из {len(articles)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()