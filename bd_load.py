import json
import psycopg2
from psycopg2.extras import execute_values

rows=[]


conn = psycopg2.connect(
    host = 'localhost',
    database = 'meduza',
    user = 'admin',
    password = '1111'
)

cursor = conn.cursor()

with open ('/home/admin/Documents/projects/grizli/meduza_news.json', encoding='utf-8') as f:
    articles = json.load(f)

for a in articles:
    rows.append((
        a.get('title'),
        a.get('url'),
        json.dumps(a, ensure_ascii=False)
    ))

execute_values(cursor, """
    INSERT INTO articles (title, url, raw_json)
    VALUES %s
    ON CONFLICT (url) DO UPDATE SET
        title = EXCLUDED.title,
        raw_json = EXCLUDED.raw_json
""", rows)

conn.commit()
cursor.close()
conn.close()

print(f'Загруэено статей {len(rows)}')