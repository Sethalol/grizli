# grizli — мониторинг мобилизационных новостей «Медузы»

Airflow-пайплайн, который раз в сутки парсит ленту «Медузы»,
прогоняет заголовки через LLM-фильтр по теме мобилизации в России
и рассылает релевантные новости в Telegram.

## Как это работает

```
parse_meduza → load_bd → run_model → notify_bot
```

| Таск | Скрипт | Что делает |
|---|---|---|
| `parse_meduza` | `parcer.py` | Открывает Медузу через Selenium (Firefox + `webdriver_manager`), собирает свежие заголовки/ссылки, сохраняет в `meduza_news.json` |
| `load_bd` | `bd_load.py` | Читает `meduza_news.json`, апсертит статьи в таблицу `articles` (Postgres, конфликт по `url`) |
| `run_model` | `model.py` | Прогоняет заголовки через LLM по системному промпту из файла `instruction`, размечает тему/важность и пишет результат в таблицу `news` |
| `notify_bot` | `tgbot/idle.py` | Забирает из `news` всё с `sent = false`, шлёт в Telegram-чат, помечает отправленным |

DAG: `meduza_mobilization_pipeline`, расписание `05 18 * * *` (18:05 по Europe/Moscow), `catchup=False`, 2 ретрая с интервалом 5 минут.

## Логика фильтрации

Промпт в `instruction` задаёт агенту-классификатору чёткие критерии:

- **Включаем**: мобилизация/призыв/повестки, изменения в законе о воинской обязанности, ограничения на выезд, электронный реестр военнообязанных, статусы в розыске, альтернативная служба, выплаты — но только если заголовок содержит новостной повод (глагол в прошедшем/будущем времени, конкретная дата), а не общую аналитику.
- **Исключаем**: боевые действия на фронте, геополитику, общую экономику, бытовую криминальную хронику, соцподдержку военных, праздничные поздравления.
- Прошедшим фильтр присваивается важность `HIGH` / `MEDIUM` / `LOW`.
- Модель отвечает одним словом `True`/`False`.

## Структура проекта

```
grizli/
├── parcer.py             # парсинг Медузы (Selenium)
├── bd_load.py             # загрузка JSON в Postgres (articles)
├── model.py                # LLM-классификация по instruction (OpenRouter)
├── instruction              # системный промпт для model.py
├── meduza_news.json          # промежуточный дамп статей (генерируется)
├── api                        # (пусто — зарезервировано)
├── tgbot/
│   └── idle.py                 # рассылка в Telegram (aiogram)
├── meduza_pipeline_dag.py        # DAG для Airflow
└── venv/                          # виртуальное окружение проекта
```

DAG-файл кладётся в `~/airflow/dags/`, сам код проекта — в отдельном
каталоге и запускается через собственный venv (не тот, где стоит Airflow).

## Требования

Два разных окружения:

**Airflow venv** (`/home/admin/Documents/projects/venv`):
- `apache-airflow`

**venv проекта** (`/home/admin/Documents/projects/grizli/venv`) — именно из него запускаются все таски:
- `selenium`
- `webdriver-manager`
- `psycopg2`
- `aiogram`
- клиент для OpenRouter (`openai`-совместимый SDK или `requests`)

```bash
cd grizli
python -m venv venv
venv/bin/pip install selenium webdriver-manager psycopg2-binary aiogram openai
```

Рекомендуется закрепить версии в `requirements.txt` и поставить их одной командой,
чтобы не ловить `ModuleNotFoundError` по одному при каждом прогоне DAG.

## Postgres

Нужна база (по умолчанию `meduza`) минимум с двумя таблицами:

```sql
CREATE TABLE articles (
    id       SERIAL PRIMARY KEY,
    title    TEXT,
    url      TEXT UNIQUE,
    raw_json JSONB
);

CREATE TABLE news (
    id    SERIAL PRIMARY KEY,
    title TEXT,
    url   TEXT,
    topic TEXT,
    sent  BOOLEAN DEFAULT false
);
```

(Точную схему `news`, которую пишет `model.py`, стоит свериться с самим скриптом —
он не входит в этот набор файлов.)

## Переменные окружения / Airflow Variables

DAG читает их через `Variable.get(..., default_var=...)`. **Дефолты в коде — временная затычка,
их нужно убрать и задать значения через Airflow UI (Admin → Variables) или `.env`, не хранить в репозитории:**

| Переменная | Назначение |
|---|---|
| `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | подключение к Postgres |
| `OPENROUTER_API_KEY`, `OPENROUTER_PROXY` | доступ к LLM через OpenRouter |
| `BOT_TOKEN`, `CHAT_ID` | Telegram-бот и чат для рассылки |

## Запуск / тест

```bash
# ручной прогон конкретной даты без создания записи в шедулере
airflow dags test meduza_mobilization_pipeline 2026-08-11

# продовый режим — просто включить DAG в Airflow UI/CLI
airflow dags unpause meduza_mobilization_pipeline
```

## Известные особенности / на что обратить внимание

- Все скрипты в `PROJECT_DIR` используют **относительные пути** к файлам
  (`meduza_news.json`, `instruction`) — при запуске через `BashOperator`
  рабочая директория подпроцесса не гарантированно совпадает с `PROJECT_DIR`.
  Надёжнее переписать чтение/запись на абсолютные пути или на путь
  относительно `os.path.dirname(os.path.abspath(__file__))`.
- Секреты в `meduza_pipeline_dag.py` сейчас захардкожены как `default_var` —
  нужно вынести их в Airflow Variables/Secrets backend и **ротировать**
  токен бота и ключ OpenRouter, если файл когда-либо попадал в публичный репозиторий.
- `parcer.py` и `model.py` не входили в проверяемый набор файлов — их стоит
  задокументировать отдельно (входные/выходные форматы, используемая модель OpenRouter).