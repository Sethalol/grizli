from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options 
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
import time
import json
from confluent_kafka import Producer


def delivery_report(err, msg):
    if err:
        print(f"Delivery error: {err}")
    else:
        print(f"Delivered to: {msg.topic()} [{msg.partition()}]")


config = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(config)


def parce_meduza():
    options = Options()
    options.add_argument("--headless")

    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=options)
    
    try:
        print('Окрываем медузу')
        driver.get('https://meduza.io/')
        time.sleep(5)
        titles = driver.find_elements(By.CSS_SELECTOR, "h2 a.Link-module-root")
        
        news = []
        for title_elem in titles[:10]:
            title = title_elem.text.strip()
            href = title_elem.get_attribute("href")
            if title:
                news.append({"title": title, "url": href})
                print(f"✅ {title}")
        
        return news
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return []
    finally:
        driver.quit()

def send_message(topic, data):
    producer.produce(topic=topic, value=data, on_delivery=delivery_report)
    producer.poll(0)
    producer.flush()

if __name__ == "__main__":
    news = parce_meduza()
    with open('/home/admin/Documents/projects/grizli/meduza_news.json', 'w', encoding='utf-8') as f:
        json.dump(news, f, ensure_ascii=False, indent=2)
    
    data = json.dumps(news, ensure_ascii=False).encode('utf-8')
    send_message('line', data)
    print(f'sent data: {data}')