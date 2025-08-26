from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf
from airflow.models import Variable

from datetime import datetime
import pandas as p



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



def insert_data(table_name):

    df = p.read_csv("/files/" + f"{table_name}.csv", delimiter=";", encoding='UTF-8')

    df = df.drop_duplicates() 

    #загрузка
    postgres_hook = PostgresHook("pg-connection")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema = "stage", if_exists="append", index= False)

default_args = {
    "owner" : "gavrilov",
    "start_date" : datetime(2025, 8, 25),
    "retries" : 2
}


def insert_data_currency(table_name):
    df = p.read_csv("/files/" + f"{table_name}.csv", delimiter=";", encoding='latin-1')

    df = df.drop_duplicates() #удаление полных дубликатов

    #загрузка
    postgres_hook = PostgresHook("pg-connection")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema = "stage", if_exists="append", index= False)

with DAG(
    "insert_data",
    default_args = default_args,
    description = "Загрузка данных в Stage",
    catchup = False, #параметр выполнения дага в период отсутствия данных
    schedule = "0 0 * * *" # формат крон
) as macdag:
    
    dag_start_log = create_log_task("dag_start", "Запуск DAG", "start")

    start = DummyOperator(
        task_id = "start"
    )


    ft_balance_f_start = create_log_task("ft_balance_f", "Начало загрузки ft_balance_f", "start")
    ft_balance_f = PythonOperator(
        task_id = "ft_balance_f",
        python_callable = insert_data,
        op_kwargs = {"table_name" : "ft_balance_f"}
    )
    ft_balance_f_end = create_log_task("ft_balance_f", "Конец загрузки ft_balance_f", "end")


    ft_posting_f_start = create_log_task("ft_posting_f", "Начало загрузки ft_posting_f", "start")
    ft_posting_f = PythonOperator(
    task_id = "ft_posting_f",
    python_callable = insert_data,
    op_kwargs = {"table_name" : "ft_posting_f"}
    )
    ft_posting_f_end = create_log_task("ft_posting_f", "Конец загрузки ft_posting_f", "end")



    md_account_d_start = create_log_task("md_account_d", "Начало загрузки md_account_d", "start")
    md_account_d = PythonOperator(
    task_id = "md_account_d",
    python_callable = insert_data,
    op_kwargs = {"table_name" : "md_account_d"}
    )
    md_account_d_end = create_log_task("md_account_d", "Конец загрузки md_account_d", "end")


    md_currency_d_start = create_log_task("md_currency_d", "Начало загрузки md_currency_d", "start")
    md_currency_d = PythonOperator(
    task_id = "md_currency_d",
    python_callable = insert_data_currency,
    op_kwargs = {"table_name" : "md_currency_d"}
    )
    md_currency_d_end = create_log_task("md_currency_d", "Конец загрузки md_currency_d", "end")


    md_exchange_d_start = create_log_task("md_exchange_rate_d", "Начало загрузки md_exchange_rate_d", "start")
    md_exchange_rate_d = PythonOperator(
    task_id = "md_exchange_rate_d",
    python_callable = insert_data,
    op_kwargs = {"table_name" : "md_exchange_rate_d"}
    )
    md_exchange_d_end = create_log_task("md_exchange_rate_d", "Конец загрузки md_exchange_rate_d", "end")


    md_ledger_account_s_start = create_log_task("md_ledger_account_s", "Начало загрузки md_ledger_account_s", "start")
    md_ledger_account_s = PythonOperator(
    task_id = "md_ledger_account_s",
    python_callable = insert_data,
    op_kwargs = {"table_name" : "md_ledger_account_s"}
    )
    md_ledger_account_s_end = create_log_task("md_ledger_account_s", "Конец загрузки md_ledger_account_s", "end")

    dag_end_log = create_log_task("dag_end", "Завершение DAG insert_data", "end") 

    end = DummyOperator(
        task_id = "end"
    )

start >> dag_start_log >> ft_balance_f_start >> ft_balance_f >> ft_balance_f_end >> \
    ft_posting_f_start >> ft_posting_f >> ft_posting_f_end >> \
    md_account_d_start >> md_account_d >> md_account_d_end >> \
    md_currency_d_start >> md_currency_d >> md_currency_d_end >> \
    md_exchange_d_start >> md_exchange_rate_d >> md_exchange_d_end >> \
    md_ledger_account_s_start >> md_ledger_account_s >> md_ledger_account_s_end >> \
    dag_end_log >> end




