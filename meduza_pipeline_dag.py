from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Europe/Moscow")
PROJECT_PARSER_DIR = "grizli/parsers"
PROJECT_AGENT_DIR = 'grizli/agents'
PROJECT_DIR = 'grizli/'
PYTHON_BIN = f"/home/admin/Documents/projects/venv/bin/python3"
ENV_COMMON = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
}
ENV_MODEL = {
    **ENV_COMMON,
    "DASHSCOPE_API_KEY": Variable.get("DASHSCOPE_API_KEY", default_var=""),
}
ENV_BOT = {
    **ENV_COMMON,
    "BOT_TOKEN": Variable.get("BOT_TOKEN", default_var=""),
    "CHAT_ID": Variable.get("CHAT_ID", default_var=""),
}
default_args = {
    "owner": "grizli",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id= "mobilization_pipeline",
    description="Парсинг новостных сайтов",
    schedule="05 18 * * *",
    start_date=pendulum.datetime(2026, 8, 11, tz=LOCAL_TZ),
    catchup=False,
    default_args=default_args,
    tags=['meduza', 'mediazona', 'gazeta', 'mobilization'],
    ) as dag:

    parse_meduza=BashOperator(
        task_id="parse_meduza",
        bash_command=f"{PYTHON_BIN} {PROJECT_PARSER_DIR}/parcer_meduza.py",
        env=ENV_COMMON,
        append_env=True,
    )

    parse_mediazona=BashOperator(
            task_id="parse_mediazona",
            bash_command=f"{PYTHON_BIN} {PROJECT_PARSER_DIR}/parcer_mediazona.py",
            env=ENV_COMMON,
            append_env=True,
    )

    parse_gazeta=BashOperator(
            task_id="parse_gazeta",
            bash_command=f"{PYTHON_BIN} {PROJECT_PARSER_DIR}/parcer_gazeta.py",
            env=ENV_COMMON,
            append_env=True,
    )

    parse_mobilization = BashOperator(
        task_id='parse_mobilization',
        bash_command=f"{PYTHON_BIN} {PROJECT_PARSER_DIR}/parcer_mobilization.py",
        env=ENV_COMMON,
        append_env=True,
    )

    model_check_topics=BashOperator(
        task_id="model_topic",
        bash_command=f"{PYTHON_BIN} {PROJECT_AGENT_DIR}/model.py",
        env=ENV_MODEL,
        append_env=True,
    )
    model_check_text = BashOperator(
        task_id='model_text',
        bash_command=f"{PYTHON_BIN} {PROJECT_AGENT_DIR}/agent_check_topic.py",
        env=ENV_MODEL,
        append_env=True,
    )

    notify_bot = BashOperator(
        task_id="notify_bot",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/tgbot/idle.py",
        env=ENV_BOT,
        append_env=True,
    )



    [parse_meduza, parse_mediazona, parse_gazeta] >>  model_check_topics >> parse_mobilization >> model_check_text >> notify_bot