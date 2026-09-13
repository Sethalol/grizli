import trafilatura
from trafilatura.settings import use_config
import time
import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

CONSUMER_CONFIG={
    "bootstrap.servers": 'localhost:9092',
    "auto_offset_reset": 'earliest',
}
PRODUCER_CONFIG={"bootstrap.servers": 'localhost:9092'}
SOURCE_TOPIC='catmodel'
TARGET_TOPIC='tg'


def delivery_report(err, msg):
    if err:
        print(f"Delivery error: {err}")
    else:
        print(f"Delivered to: {msg.topic()} [{msg.partition()}]")


def parse_message(raw_message: bytes):
    if raw_message is None:
        return []
    try:
        payload = json.loads(raw_message.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f'Ошибка: {repr(e)}')
        return []
    if isinstance(payload, dict) and 'items' in payload:
        return payload.get('items', [])
    elif isinstance(payload, list):
        return []
    elif isinstance(payload, dict):
        return [payload]

    print(f'[ОШИБКА ПАРСИНГА] Неизвестный тип данных: {type(payload)}')
    return []


def parce_mob(url):
    timeout=15
    config = use_config()
    config.set("DEFAULT", "DOWNLOAD_TIMEOUT", str(timeout))
 
    try:
        downloaded = trafilatura.fetch_url(url, config=config)
        if downloaded is None:
            return None
 
        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=True,
            favor_recall=True,
        )
        return text
 
    except Exception as e:
        print(f'[ERROR]: {repr(e)}')
        return None


def send_to_tg(producer: Producer, text):
    result = {"text":text}
    payload = json.dumps(result, ensure_ascii=False).encode('utf-8')
    producer.produce(
        topic=TARGET_TOPIC,
        value=payload,
        on_delivery=delivery_report
    )
    producer.poll(0)

def main():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([SOURCE_TOPIC])
    producer = Producer(PRODUCER_CONFIG)
    IDLE_TIMEOUT=5.0
    empty_since=None

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

            articles = parse_message(msg.value())

            for article in articles:
                title = article.get('title')
                url = article.get('url')

                if not title or not url:
                    print(f'[ПРОПУЩЕННЫЕ ДАННЫЕ] {article}')
                    continue

                try:
                    passed = parce_mob(url)
                except Exception as e:
                    print(f'[ОШИБКА ПАРСЕРА] article={article}, err={repr(e)}')
                    continue

                if passed:
                    send_to_tg(producer, passed)
                    print(f'{article} -> OK, sent to {TARGET_TOPIC}')
                else:
                    print(f'{article} -> Rejected')

            producer.flush()
            consumer.commit(asynchronous=False)



            
    except KeyboardInterrupt:
        print('Остановка консьюмера...')
    finally:
        consumer.close()

if __name__ == "__main__":
    main()