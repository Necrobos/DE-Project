import csv
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook



connection = BaseHook.get_connection('pg-connection')

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

def export_to_csv():

    conn = psycopg2.connect(
        database=connection.schema, user= connection.login,
        password =connection.password, host=connection.host, port=connection.port)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM dm.dm_f101_round_f')
    rows = cursor.fetchall()

    cursor.execute("""SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'dm' AND table_name = 'dm_f101_round_f'
    ORDER BY ordinal_position""")

    columns = [row[0] for row in cursor.fetchall()]
    with open('dm_f101_round_f.csv', 'w', newline ='', encoding = 'UTF-8') as csvfile:
        writer = csv.writer(csvfile)

        writer.writerow(columns)

        writer.writerows(rows)

        cursor.close()
        conn.close()






default_args = {
    "owner" : "gavrilov",
    "start_date" : datetime(2025, 8, 30),
    "retries" : 2
}



with DAG(
    "export_in_csv",
    default_args = default_args,
    description = "Выгрузка таблицы dm_f101_round_f в csv файл",
    catchup = False, #запуск вне работы дага
    schedule = "0 0 * * *"
) as macdag3:
    dag_start_log = create_log_task('start_dag', 'Запуск DAG', 'start')
    dag_end_log = create_log_task('end_dag', 'Завершение DAG', 'end')
    export_start_log = create_log_task('export', 'Экспорт файла начат', 'start')
    export_end_log = create_log_task('export', 'Экспорт завершен', 'end')
    start_dag = DummyOperator(
        task_id = "start"
    )
    export_task = PythonOperator(
        task_id = 'export_task',
        python_callable = export_to_csv
    )

    start_dag >> dag_start_log >> export_start_log >> export_task >> export_end_log >> dag_end_log
