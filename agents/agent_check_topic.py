import json
import os
from openai import OpenAI

clien = OpenAI(
    api_key=os.environ.get('DASHSCOPE_API_KEY'),
    base_url='https://dashscope-intl.aliyuncs.com/compatible-mode/v1'
)

MODEL_NAME = "qwen3.5-flash"
TOPIC_VALUE = 'mobilization'
SOURCE_TOPIC = 'catmodel'
TARGET_TOPIC = 'tg'
CONSUMER_CONFIG = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092'),
    "enable.auto.commit": False,
}
PRODUCER_CONFIG = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')}
