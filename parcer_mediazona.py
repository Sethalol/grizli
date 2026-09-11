from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options 
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
import time
import json
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}
producer = Producer(config)

def parce_mediazona():
    options = Options()
    options.add_argument("--headless")
    
    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=options)
    try:
        print("Открываем медиазону")
        driver.get('https://zona.media/news')
        time.sleep(5)

        titles = driver.find_elements(By.CSS_SELECTOR, 'article a.feed-item__link')

        news=[]
        for title_elem in titles[:10]:
            title = title_elem.text.strip()[6:]
            href = title_elem.get_attribute("href")
            if title:
                news.append({"title":title, "href":href})
                print(f"Ready: {title}")
        return news
    except Exception as e:
        print(f'Ошибка {e}')
        return []
    finally:
        driver.quit()


def send_message(topic, data):
    producer.produce(topic=topic, value=data)
    producer.flush()


if __name__ == "__main__":
    parcer = parce_mediazona()
    data = json.dump(parcer)
    send_message('line', data)
    print(f'sent data: {data}')