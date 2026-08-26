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

with open('grizli/gazeta_news.json', encoding='utf-8') as f:
    data1=json.load(f)
with open('grizli/mediazona_news.json', encoding='utf-8') as f:
    data2=json.load(f)
with open('grizli/meduza_news.json', encoding='utf-8') as f:
    data3=json.load(f)
articles = data1 + data2 + data3


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

print(f'Загружено статей {len(rows)}')