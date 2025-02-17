# My Test dag
import datetime
from airflow.decorators import dag, task
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LogDatesOperator(BaseOperator):
    """
    Оператор, который выводит логические даты (logical_date, execution_date и т.д.) из контекста Airflow в логи.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # В современном Airflow (2.3+), основная дата называется logical_date.
        # Но для совместимости мы выведем и "execution_date".
        self.log.info("=== Airflow Context Logical Dates ===")
        self.log.info("logical_date: %s", context.get("logical_date"))
        self.log.info("execution_date: %s", context.get("execution_date"))
        self.log.info("ds (string formatted logical_date): %s", context.get("ds"))
        self.log.info("data_interval_start: %s", context.get("data_interval_start"))
        self.log.info("data_interval_end: %s", context.get("data_interval_end"))

        # Airflow также предоставляет дополнительные переменные, например, ds_nodash, prev_ds, next_ds и т.д.
        # При необходимости можно их тоже вывести:
        self.log.info("prev_ds: %s", context.get("prev_ds"))
        self.log.info("next_ds: %s", context.get("next_ds"))

        # Возвращаем что-нибудь для наглядности (попадёт в XCom)
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
