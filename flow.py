from datetime import timedelta
# export AIRFLOW_VAR_BOT_TOKEN="8961935363:AAE0MLnpltYJyf0qmaYSpKvqEjdSwoYmn60
    #    export AIRFLOW_VAR_CHAT_ID="1798291844"
    # export OPENROUTER_API_KEY="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"
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Europe/Moscow")
PROJECT_DIR = "/home/admin/Documents/projects/grizli"
PYTHON_BIN = f"{PROJECT_DIR}/venv/bin/python"
ENV_COMMON = {
    "DB_HOST": Variable.get("DB_HOST", default_var="localhost"),
    "DB_NAME": Variable.get("DB_NAME", default_var="meduza"),
    "DB_USER": Variable.get("DB_USER", default_var="postgres"),
    "DB_PASSWORD": Variable.get("DB_PASSWORD", default_var="1111"),
}
ENV_MODEL = {
    **ENV_COMMON,
    "OPENROUTER_API_KEY": Variable.get("OPENROUTER_API_KEY", default_var="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"),
    "OPENROUTER_PROXY": Variable.get("OPENROUTER_PROXY", default_var=""),
}
ENV_BOT = {
    **ENV_COMMON,
    "BOT_TOKEN": Variable.get("BOT_TOKEN", default_var="8961935363:AAE0MLnpltYJyf0qmaYSpKvqEjdSwoYmn60"),
    "CHAT_ID": Variable.get("CHAT_ID", default_var="1798291844"),
}
default_args = {
    "owner": "grizli",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id= "meduza_mobilization_pipeline",
    description="Парсинг медузы",
    schedule="15 13 * * *",
    start_date=pendulum.datetime(2026, 8, 11, tz=LOCAL_TZ),
    catchup=False,
    default_args=default_args,
    tags=['meduza', 'mobilization'],
    ) as dag:

    parse_meduza=BashOperator(
        task_id="parse_meduza",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/parcer.py",
        env=ENV_COMMON,
        append_env=True,
    )

    db_load=BashOperator(
        task_id="load_bd",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/bd_load.py",
        env=ENV_COMMON,
        append_env=True,
    )

    run_model=BashOperator(
        task_id="run_model",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/model.py",
        env=ENV_MODEL,
        append_env=True,
    )

    notify_bot = BashOperator(
        task_id="run_model",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/tgbot/idle.py",
        env=ENV_BOT,
        append_env=True,
    )

    parse_meduza >> db_load >> run_model >> notify_bot