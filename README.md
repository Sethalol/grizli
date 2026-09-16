# grizli

Пайплайн для поиска новостей о мобилизации: парсит заголовки с новостных сайтов, фильтрует их через LLM, вытаскивает полный текст отобранных статей, прогоняет текст через второго агента и отправляет результат в Telegram.

Оркестрация — Airflow, передача данных между этапами — Kafka.

## Как это работает

```
parcer_meduza.py  ─┐
parcer_mediazona.py ├─→ [line] ─→ model.py ─→ [catmodel] ─→ parcer_mobilization.py ─┐
parce_gazeta.py   ─┘         (LLM: тема?)                    (trafilatura: текст)   │
                                                                                     ↓
                            idle.py ←─ [tg_bot] ←─ agent_check_topic.py ←────────── [tg]
                          (Telegram)                  (LLM: обработка текста)
```

| Этап | Скрипт | Читает | Пишет |
|---|---|---|---|
| Парсинг заголовков | `parsers/parcer_meduza.py`, `parsers/parcer_mediazona.py`, `parsers/parce_gazeta.py` | — | `line` |
| Фильтр по теме | `agents/model.py` | `line` | `catmodel` |
| Извлечение текста | `parsers/parcer_mobilization.py` | `catmodel` | `tg` |
| Обработка текста | `agents/agent_check_topic.py` | `tg` | `tg_bot` |
| Отправка | `tgbot/idle.py` | `tg_bot` | Telegram |

Все консьюмеры работают в режиме «до опустошения топика»: если сообщений нет `IDLE_TIMEOUT` (5 сек), скрипт завершается. Это позволяет запускать их как разовые задачи в Airflow, а не как демоны.

## Компоненты

### Парсеры заголовков
Selenium + headless Firefox, `webdriver_manager` сам подтягивает geckodriver. Каждый парсер берёт 10 верхних новостей с главной, собирает `[{"title": ..., "url": ...}]`, дублирует в локальный JSON и отправляет в топик `line`.

Источники: [meduza.io](https://meduza.io/), [zona.media](https://zona.media/news), [novayagazeta.eu](https://novayagazeta.eu/news).

### model.py — фильтр заголовков
Qwen (`qwen3.5-flash` через DashScope OpenAI-совместимый endpoint) отвечает `true`/`false` на вопрос, относится ли заголовок к теме. Промпт лежит в файле `grizli/instruction`. Прошедшие заголовки уходят в `catmodel` с добавленным полем `topic: mobilization`, ключ сообщения — url.

### parcer_mobilization.py — извлечение текста
`trafilatura` скачивает страницу по url и достаёт основной текст (без комментариев, меню и футера). Результат `{"text": ...}` идёт в топик `tg`.

### agent_check_topic.py — обработка текста
Второй проход LLM, промпт в `grizli/instruction_for_text`. Результат в `tg_bot`.

### idle.py — Telegram-бот
aiogram, отправляет каждое сообщение в `CHAT_ID` с префиксом 🆕. Офсет коммитится только после успешной отправки, так что при падении Telegram новость переотправится на следующем запуске. Если за весь прогон ничего не отправлено, шлёт «Новостей по мобилизации нет!».

### meduza_pipeline_dag.py
DAG `mobilization_pipeline`, расписание `05 18 * * *` (Europe/Moscow), 2 ретрая с паузой 5 минут. Три парсера идут параллельно, дальше цепочка последовательная.

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install selenium webdriver-manager trafilatura confluent-kafka openai aiogram apache-airflow
```

Нужны Firefox (для Selenium) и запущенная Kafka на `localhost:9092`.

Создать топики:
```bash
for t in line catmodel tg tg_bot; do
  kafka-topics.sh --create --topic $t --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 1
done
```

## Переменные окружения

| Переменная | Где нужна | Описание |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | везде | адрес брокера, по умолчанию `localhost:9092` |
| `DASHSCOPE_API_KEY` | `model.py`, `agent_check_topic.py` | ключ DashScope |
| `BOT_TOKEN` | `idle.py` | токен Telegram-бота |
| `CHAT_ID` | `idle.py` | id чата или канала |

Airflow берёт `DASHSCOPE_API_KEY`, `BOT_TOKEN` и `CHAT_ID` из Variables:
```bash
airflow variables set DASHSCOPE_API_KEY "..."
airflow variables set BOT_TOKEN "..."
airflow variables set CHAT_ID "..."
```

## Файлы с промптами

- `grizli/instruction` — инструкция для фильтра заголовков, модель должна отвечать `true` или `false`
- `grizli/instruction_for_text` — инструкция для обработки полного текста

Оба читаются на старте скрипта, без них будет `FileNotFoundError`.

## Ручной запуск

```bash
python parsers/parcer_meduza.py
python agents/model.py
python parsers/parcer_mobilization.py
python agents/agent_check_topic.py
python tgbot/idle.py
