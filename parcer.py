import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import random


service = Service(GeckoDriverManager().install())
driver = webdriver.Firefox(service=service)

try:
    url = "https://www.avito.ru" # Пример URL
    driver.get(url)
    time.sleep(random.uniform(2, 5)) # Случайная задержка

    # Ждем загрузки элементов (замените селектор на актуальный)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
    )
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    items = soup.select("[data-marker='item']")

    for item in items:
        # Извлечение данных (название, цена, ссылка) — пример
        title_elem = item.select_one("[itemprop='name']")
        price_elem = item.select_one("[itemprop='price']")
        # ... и так далее
        print(title_elem.text.strip() if title_elem else "Нет названия")

finally:
    driver.quit()