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
        print("Открываем Новую Газету")
        driver.get('https://novayagazeta.eu/news')
        time.sleep(5)

        titles = driver.find_elements(By.CSS_SELECTOR, "article a.flex.flex-col.gap-5")

        news=[]
        for title_elem in titles[:10]:
            title = title_elem.text.strip()
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

def clear_news(news: list[dict]):
    for dct in news:
        dct['title'] = dct['title'].replace('\n','',1)
        print('======================')
        print(dct["title"])
        dct['title'] = dct['title'][dct["title"].index('\n')+1:]
        dct["title"] = dct["title"][:dct["title"].index('\n')]
    print("Очищено")


def send_message(topic, data):
    producer.produce(topic=topic, value=data)
    producer.flush()


if __name__ == "__main__":
    parcer = parce_mediazona()
    clear_news(parcer)
    data = json.dump(parcer)
    send_message('line',     data = json.dump(parcer)
)
    print(f'sent data: {data}')