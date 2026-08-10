import os
import httpx
from openai import OpenAI

import psycopg2

client = OpenAI(
    api_key=os.environ.get("OPENROUTER_API_KEY"),
    base_url='https://openrouter.ai/api/v1'
)
MODEL_NAME = "openrouter/free"
with open("instruction", encoding='utf-8') as f:
    INSTRUCTION = f.read()
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "dbname": os.environ.get("DB_NAME", "meduza"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "1111"),
}
TOPIC_VALUE = 'mobilization'

def check_title(title: str) -> bool:
    response = client.chat.completions.create(
        model = MODEL_NAME,
        messages = [
            {"role": "system", "content": INSTRUCTION},
            {'role': 'system', 'content': title}
        ],
        max_tokens=40,
        temperature=0,
        extra_body={'reasoning': {"enabled": False}}
    )
    content = response.choices[0].message.content

    if content is None:
        return False
    answer = response.choices[0].message.content.strip().lower()
    return answer.startswith('true')

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT a.id, a.title, a.url
                FROM articles a
                LEFT JOIN news n ON n.article_id = a.id
                WHERE n.id IS NULL
                ORDER BY a.id DESC
                LIMIT 10
                """

            )
            rows = cur.fetchall()
        print(f'Найдено {len(rows)} заголовков')

        for article_id, title, url in rows:
            try:
                passed = check_title(title)
            except Exception as e:
                print(f"[ОШИБКА LLM] article_id={article_id}: {repr(e)}")
                continue

            if passed:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO news (article_id, title, url, topic)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (url) DO NOTHING
                        """,
                        (article_id, title, url, TOPIC_VALUE),
                    )
                conn.commit()
                print(f"id={article_id} -> 'OK, добавлено'")
            else:
                print(f"id={article_id} -> Rejected")



    finally:
        conn.close()
        
if __name__== "__main__":
    main()