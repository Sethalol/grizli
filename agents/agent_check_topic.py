import json
import os
import time
from openai import OpenAI
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

client = OpenAI(
    api_key=os.environ.get('DASHSCOPE_API_KEY'),
    base_url='https://dashscope-intl.aliyuncs.com/compatible-mode/v1'
)
with open('grizli/instruction_for_text', 'r', encoding='utf-8') as f:
    INSTRUCTION=f.read()

MODEL_NAME = "qwen3.5-flash"
SOURCE_TOPIC = 'tg'
TARGET_TOPIC = 'tg_bot'
CONSUMER_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092'),
    "enable.auto.commit": False,
    "auto.offset.reset": 'earliest'
}
PRODUCER_CONFIG = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')}

def check_topic(title: str):
    responce = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0,
        max_tokens=200,
        extra_body={"enable_thinking": False},
        messages=[
            {"role": "system", "content": INSTRUCTION},
            {"role": "user", "content": title}
        ]
    )

    content = responce.choices[0].message.content
    if content is None:
        return False
    return content


def transform(raw: bytes):
    if raw is None:
        return []
    try:
        payload = json.loads(raw.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f'[ОШИБКА ДЕКОДА] {repr(e)}')
        return []


    if isinstance(payload, dict) and "text" in payload:
        return payload.get("text", [])
    else:
        return []


def send_to_tg_bot(producer: Producer, text):
    result = {
        "text": text
    }
    payload = json.dumps(result, ensure_ascii=False).encode('utf-8')
    producer.produce(
        topic=TARGET_TOPIC,
        value=payload
    )
    producer.poll(0)

def main():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([SOURCE_TOPIC])
    producer = Producer(PRODUCER_CONFIG)
    IDLE_TIMEOT=5.0
    empty_since=None

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if empty_since == None:
                    empty_since = time.monotonic()
                elif time.monotonic()-empty_since >= 5.0:
                    print('Новых сообщений нет, завершаю работу')
                    break
                continue

            if msg.error():
                if msg.error() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            articles = transform(msg.value())

            for article in articles:
                text = article.get("text")

                if text is None:
                    print('[НИХУЯ НЕТ]')
                    continue

                try:
                    output_text = check_topic(text)
                except Exception as e:
                    print(f'ОШИБКА: {repr(e)}')
                    continue

                if output_text:
                    send_to_tg_bot(producer, output_text)
                    print(f'Отправлено из {SOURCE_TOPIC} -> {TARGET_TOPIC}')
                else:
                    print('НИХУЯ НЕ ОТПРАВЛЕНО')
            producer.flush()
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print('stopping')
    finally:
        consumer.close()

if __name__ == "__main__":
    main()