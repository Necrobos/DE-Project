from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time



def timer ():
    """Задержка для точного окончания дага"""
    time.sleep(15)
    print("Задержка выполнена")



def create_log_task(task_name, message, operation_type="start"):
    if operation_type == "start":
        start_time = "CURRENT_TIMESTAMP"
        end_time = "NULL"
    elif operation_type == "end":
        start_time = "NULL"
        end_time = "CURRENT_TIMESTAMP"
    else:
        start_time = "NULL"
        end_time = "NULL"
    
    return PostgresOperator(
        task_id=f"log_{task_name}_{operation_type}",
        postgres_conn_id="pg-connection",
        sql=f"""
            CALL logs.insert_dag_log(
                '{{{{ dag.dag_id }}}}', 
                '{task_name}', 
                '{message}',
                {start_time},
                {end_time}
            )
        """
    )


default_args = {
    "owner" : "gavrilov",
    "start_date" : datetime(2025, 8, 25),
    "retries" : 2
}


with DAG(
    "сreate_storefronts",
    default_args = default_args,
    description = "Создание витрин оборотов и остатков",
    catchup = False, #запуск вне работы дага
    schedule = "0 0 * * *"
) as dag2:
    start_log = create_log_task("dag_start", "Запуск DAG расчета витрин DM", "start")

    end_log = create_log_task("dag_end", "Завершение DAG расчета витрин DM", "end")

    calculation_tasks = []

    for day in range (1, 32):
        date_str = f'2018-01-{day:02d}'

        day_start_log = create_log_task(f"day_{day}_start", f"Начало расчета за {date_str}", "start")


        calc_turnover = PostgresOperator(
            task_id=f'calc_turnover_{day}',
            postgres_conn_id='pg-connection',
            sql=f"""
                CALL ds.fill_account_turnover_f(
                    '{date_str}'::DATE
                )
            """
        )
        
        # Расчет остатков
        calc_balance = PostgresOperator(
            task_id=f'calc_balance_{day}',
            postgres_conn_id='pg-connection',
            sql=f"""
                CALL ds.fill_account_balance_f(
                    '{date_str}'::DATE
                )
            """
        )

        day_end_log = create_log_task(f"day_{day}_end", f"Завершение расчета за {date_str}", "end")

        day_start_log >> calc_turnover >> calc_balance >> day_end_log 

        calculation_tasks.append(day_start_log)

    delay_task = PythonOperator(
        task_id = "delay_15s",
        python_callable = timer
    )

    f101_start_log = create_log_task("f101_start", "Запуск рассчета витрины f101", "start")
    dm_f101_round_f_task = PostgresOperator(
        task_id = 'dm_f101',
        postgres_conn_id = 'pg-connection',
        sql = """
            CALL dm.fill_f101_round_f ('2018-02-01')"""
    )
    f101_end_log = create_log_task("f101_end", "Рассчет витрины f101 завершен", "end")


    start_log >> calculation_tasks >> delay_task >> f101_start_log >> dm_f101_round_f_task >> f101_end_log >> end_log