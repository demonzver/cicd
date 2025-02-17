# My Test dag
import datetime
from airflow.decorators import dag, task
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LogDatesOperator(BaseOperator):
    """
    Оператор, который выводит логические даты и прочие даты (start_date и т. д.) в логи.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("=== Airflow Context Logical Dates ===")
        # Стандартные поля контекста (работают и в новых версиях Airflow)
        self.log.info("logical_date: %s", context.get("logical_date"))
        self.log.info("execution_date: %s", context.get("execution_date"))
        self.log.info("ds: %s", context.get("ds"))
        self.log.info("data_interval_start: %s", context.get("data_interval_start"))
        self.log.info("data_interval_end: %s", context.get("data_interval_end"))

        # Дополнительные поля, часто используемые для ссылок на предыдущую/следующую дату
        self.log.info("prev_ds: %s", context.get("prev_ds"))
        self.log.info("next_ds: %s", context.get("next_ds"))

        # -------------------------
        #   Получение START_DATE
        # -------------------------
        # 1) start_date DAG-а (указывается при создании DAG)
        dag_obj = context["dag"]
        self.log.info("dag.start_date (из определения DAG): %s", dag_obj.start_date)

        # 2) start_date самого DAG Run (когда Airflow фактически начал этот запуск)
        dag_run_obj = context["dag_run"]
        self.log.info("dag_run.start_date (время начала DAG Run): %s", dag_run_obj.start_date)

        # 3) start_date TaskInstance (когда таска фактически началась)
        task_instance_obj = context["task_instance"]
        self.log.info("task_instance.start_date (время старта задачи): %s", task_instance_obj.start_date)

        # При желании можно вывести все ключи контекста,
        # но учтите, что там может быть много служебных объектов.
        # for key, value in context.items():
        #     self.log.info("context[%s] = %s", key, value)

        return "Dates have been logged."


@dag(
    dag_id='surprise',
    description='Run it and get your gift...',
    start_date=datetime.datetime(2025, 2, 16),
    # schedule=None,
    schedule="* * * * *",
    catchup=False,
    tags=['surprise', 'gift'],
)
def surprise():
    
    @task()
    def look_at_my_logs():
        import os
        import hashlib

        node_selector = os.getenv('ASTRONOMER_NODE_SELECTOR', 'you-must-run-this-dag-on-astro')
        if node_selector == 'you-must-run-this-dag-on-astro':
            print(f"Get your free certification https://academy.astronomer.io/astronomer-certified-apache-airflow-core-exam?pc={node_selector}")
            return

        hash_object = hashlib.sha256(node_selector.encode())
        hex_hash = hash_object.hexdigest()
        
        positions = [10, 27, 12, 18, 19, 15, 3, 18, 13]
        charset = "0123456789abcdefghijklmnopqrstuvwxyz"
        result = ""
        for pos in positions:
            index = int(hex_hash[pos], 16) % len(charset)
            result += charset[index]

        print(f"Get your free certification https://academy.astronomer.io/astronomer-certified-apache-airflow-core-exam?pc={result}")

    log_dates_task = LogDatesOperator(
        task_id="log_dates_task"
    )

    look_at_my_logs() >> log_dates_task


surprise()
