import os
import json
from openai import OpenAI
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import time
from dotenv import load_dotenv
load_dotenv()


client = OpenAI(
    api_key=os.environ.get("DASHSCOPE_API_KEY"),
    base_url='https://dashscope-intl.aliyuncs.com/compatible-mode/v1'
)

MODEL_NAME = "qwen3.5-flash"
with open("/home/admin/Documents/projects/grizli/instructions/instruction.txt", encoding='utf-8') as f:
    INSTRUCTION = f.read()
TOPIC_VALUE = 'mobilization'
SOURCE_TOPIC="line"
TARGET_TOPIC='catmodel'
CONSUMER_CONFIG={
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
    'group.id': 'my-consumer-group-id'
}
PRODUCER_CONFIG={
     'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}


def delivery_report(err, msg):
    if err:
        print(f"[ОШИБКА ДОСТАВКИ] {err}")



def check_title(title: str) -> bool:
    response = client.chat.completions.create(
        model = MODEL_NAME,
        messages = [
            {"role": "system", "content": INSTRUCTION},
            {'role': 'user', 'content': title}
        ],
        max_tokens=40,
        temperature=0,
        extra_body={"enable_thinking": False}
    )
    content = response.choices[0].message.content

    if content is None:
        return False
    answer = response.choices[0].message.content.strip().lower()
    return answer.startswith('true')

def parse_message(raw_value: bytes):
    if raw_value is None:
        return []
    try:
        payload = json.loads(raw_value.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"ERROR!!! {repr(e)}")
        return []

    if isinstance(payload, dict) and "items" in payload:
        return payload.get("items", [])
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
 
    print(f"[ОШИБКА ПАРСИНГА] неожиданный формат сообщения: {type(payload)}")
    return []

def send_to_catmodel(producer, title, url):
    result = {
        "title": title,
        "url": url,
        "topic": TOPIC_VALUE,
    }

    payload = json.dumps(result, ensure_ascii=False).encode('utf-8')
    producer.produce(
        topic=TARGET_TOPIC,
        key=url.encode('utf-8'),
        value=payload,
        on_delivery=delivery_report,
    )
    producer.poll(0)


def main():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([SOURCE_TOPIC])
    producer = Producer(PRODUCER_CONFIG)
 
    print(f"Слушаем топик '{SOURCE_TOPIC}', результат -> '{TARGET_TOPIC}'...")

    IDLE_TIMEOUT = 5.0
    empty_since = None
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
 
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
 
            articles = parse_message(msg.value())
 
            for article in articles:
                title = article.get("title")
                url = article.get("url")
 
                if not title or not url:
                    print(f"[ПРОПУЩЕНО] неполные данные: {article}")
                    continue
 
                try:
                    passed = check_title(title)
                except Exception as e:
                    print(f"[ОШИБКА LLM] url={url}: {repr(e)}")
                    continue
 
                if passed:
                    send_to_catmodel(producer, title, url)
                    print(f"{url} -> OK, отправлено в '{TARGET_TOPIC}'")
                else:
                    print(f"{url} -> Rejected")
 
            producer.flush()
            consumer.commit(asynchronous=False)
 
    except KeyboardInterrupt:
        print("Остановка консьюмера...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
