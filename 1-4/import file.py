import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

# Получаем подключение к БД
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


def create_empty_table():

    conn = psycopg2.connect(
    database=connection.schema, 
    user=connection.login,
    password=connection.password, 
    host=connection.host, 
    port=connection.port
    )
    conn.autocommit = True  
    cursor = conn.cursor()
    

    cursor.execute("DROP TABLE IF EXISTS dm.dm_f101_round_f_v2")
    
    cursor.execute("""
        CREATE TABLE dm.dm_f101_round_f_v2 AS 
        SELECT * FROM dm.dm_f101_round_f WHERE 1 = 0
    """)
    cursor.close()
    conn.close()

def import_from_csv():

    conn = psycopg2.connect(
    database=connection.schema, 
    user=connection.login,
    password=connection.password, 
    host=connection.host, 
    port=connection.port
    )
    conn.autocommit = True
    cursor = conn.cursor()
    

    copy_query = """
        COPY dm.dm_f101_round_f_v2 FROM STDIN 
        WITH (FORMAT CSV, HEADER true, DELIMITER ',')
    """
    
    with open('dm_f101_round_f.csv', 'r', encoding='UTF-8') as csvfile:
        cursor.copy_expert(copy_query, csvfile)
    
    # Получаем количество импортированных строк
    cursor.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2")
    count = cursor.fetchone()[0]
    print(f"Успешно импортировано {count} строк в таблицу dm.dm_f101_round_f_v2")
    cursor.close()
    conn.close()


default_args = {
    "owner" : "gavrilov",
    "start_date" : datetime(2025, 8, 30),
    "retries" : 2
}



with DAG(
    "import_in_csv",
    default_args = default_args,
    description = "Загрузка данных в копию таблицы через курсор",
    catchup = False, #запуск вне работы дага
    schedule = "0 0 * * *"
) as macdag3:
    
    
    create_empty_table_task = PythonOperator(
        task_id = 'create_table',
        python_callable = create_empty_table
    )
    
    import_csv_task = PythonOperator(
        task_id = 'import_csv',
        python_callable = import_from_csv
    )

    start_dag = DummyOperator(
        task_id = "start"
    )    

    start_dag_log = create_log_task('dag_start', 'Запуск DAG', 'start')
    end_dag_log = create_log_task('dag_end', 'Запуск DAG', 'end')
    import_start_log = create_log_task('import_start', 'Начало загрузки через курсор csv файла, который был создан', 'start')
    import_end_log = create_log_task('import_end', 'Файл успешно загружен в БД', 'end')
    create_table_start_log = create_log_task('create_table_start', 'Создание пустой таблицы начато', 'start')
    create_table_end_log = create_log_task('create_table_end', 'Создание пустой таблицы прошло успешно', 'end')

    start_dag >> start_dag_log >> \
        create_table_start_log >> create_empty_table_task >> create_table_end_log >> \
        import_start_log >> import_csv_task >> import_end_log >> \
        end_dag_log