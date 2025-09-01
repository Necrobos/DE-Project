
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as p
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def insert_data(table_name):

    df = p.read_csv("/files/" + "loan_holiday_info/" + f"{table_name}.csv", delimiter=",", encoding='cp1251')
    
    df = df.drop_duplicates() 

    #загрузка
    postgres_hook = PostgresHook("pg-conn-dwh")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema = "stage", if_exists="append", index= False)

def insert_data2(table_name):

    df = p.read_csv("/files/" + "dict_currency/" + f"{table_name}.csv", delimiter=",", encoding='cp1251')

    df = df.drop_duplicates() 

    #загрузка
    postgres_hook = PostgresHook("pg-conn-dwh")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema = "stage", if_exists="append", index= False)


default_args = {
    "owner" : "gavrilov",
    "start_date" : datetime(2025, 8, 25),
    "retries" : 2
}


with DAG(
    "insert_data_task2",
    default_args = default_args,
    description = "Загрузка данных в rd",
    catchup = False, #параметр выполнения дага в период отсутствия данных
    schedule = "0 0 * * *" # формат крон
) as macdag:


    start = DummyOperator(
        task_id = "start"
    )

    trunc_stage = PostgresOperator(
        task_id = 'trunc_stage',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'TRUNCATE TABLE  stage.deal_info, stage.product_info, stage.dict_currency RESTART IDENTITY'
    )

    deal_info = PythonOperator(
        task_id = 'deal_info',
        python_callable = insert_data,
        op_kwargs = {"table_name" : 'deal_info'}
    )


    product_info = PythonOperator(
    task_id = 'product_info',
    python_callable = insert_data,
    op_kwargs = {"table_name" : 'product_info'}
    )



    dict_currency = PythonOperator(
    task_id = 'dict_currency',
    python_callable = insert_data2,
    op_kwargs = {"table_name" : 'dict_currency'}
    )

    del_dubl_stage_deal_info = PostgresOperator(
        task_id = 'del_dubl_stage_deal_info',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL del_dubl_stage_deal_info()'
    )

    del_dubl_stage_product_info = PostgresOperator(
        task_id = 'del_dubl_stage_product_info',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL del_dubl_stage_product_info()'
    )

    del_dubl_rd_deal_info = PostgresOperator(
        task_id = 'del_dubl_rd_deal_info',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL del_dubl_rd_deal_info()'
    )

    del_dubl_rd_product_info = PostgresOperator(
        task_id = 'del_dubl_rd_product_info',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL del_dubl_rd_product_info()'
    )

    alter_tables_pr_keys = PostgresOperator(
        task_id = 'alter_tables_pr_keys',
        postgres_conn_id = 'pg-conn-dwh',
        sql ="""ALTER TABLE rd.product ADD PRIMARY KEY (product_rk, effective_from_date);
                ALTER TABLE rd.deal_info ADD PRIMARY KEY (deal_rk, effective_from_date);
                ALTER TABLE dm.dict_currency ADD PRIMARY KEY (currency_cd, currency_name);"""
    )

    insert_rd_deal = PostgresOperator(
        task_id = 'insert_rd_deal',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL insert_rd_deal()'
    )

    insert_product_info = PostgresOperator(
        task_id = 'insert_product_info',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL insert_product_info()'
    )

    insert_dict_currency = PostgresOperator(
        task_id = 'insert_dict_currency',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL insert_dict_currency()'
    )


    build_prototype_2_2 = PostgresOperator(
        task_id = 'build_prototype_2_2',
        postgres_conn_id = 'pg-conn-dwh',
        sql = 'CALL build_prototype_2_2()'
    )

    split1 = DummyOperator(
        task_id = 'split1'
    )

    split2 = DummyOperator(
        task_id = 'split2'
    )
    
    start >> trunc_stage >> [deal_info, product_info, dict_currency] >> split1 >> \
    [del_dubl_stage_deal_info, del_dubl_stage_product_info, del_dubl_rd_deal_info, del_dubl_rd_product_info] >> \
    alter_tables_pr_keys >> [insert_rd_deal, insert_product_info, insert_dict_currency] >> build_prototype_2_2