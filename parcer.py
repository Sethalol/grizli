from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options 
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
import time
import json


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

if __name__ == "__main__":
    news = parce_meduza()
    with open("meduza_news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Сохранено {len(news)} новостей")