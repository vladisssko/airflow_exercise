"""DAG to import covid19 data to the Postgresql DB
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dag_code.import_covidS3_percountry.import_covidS3_percountry import ImportCovidS3dPercountry


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 1
}

dag = DAG(
    'import_covidS3_percountry'
    , catchup=False
    , default_args=default_args
    , schedule_interval='50 18 * * *'
    , concurrency=1)

##################################################
# DAG Tasks
##################################################


create_table_countries2020_2021 = PythonOperator(
    task_id='Import.covidS3.create_table_countries2020_2021'
    , python_callable=ImportCovidS3dPercountry.run
    , op_kwargs={
         'operation': 'create_table_countries2020_2021'
        , 'table_name': 'countries2020_2021'}
    , provide_context=True
    , dag=dag)


start_etl = PythonOperator(
    task_id='Import.covidS3.start_etl'
    , python_callable=ImportCovidS3dPercountry.run
    , op_kwargs={
         'operation': 'start_etl'
        , 'table_name': 'countries2020_2021'}
    , provide_context=True
    , dag=dag)

create_table_totals2020_2021 = PythonOperator(
    task_id='Import.covidS3.create_table_totals2020_2021'
    , python_callable=ImportCovidS3dPercountry.run
    , op_kwargs={
         'operation': 'create_table_totals2020_2021'
        , 'table_name': 'totals2020_2021'}
    , provide_context=True
    , dag=dag)

start_etl_totals = PythonOperator(
    task_id='Import.covidS3.start_etl_totals'
    , python_callable=ImportCovidS3dPercountry.run
    , op_kwargs={
         'operation': 'start_etl_totals'
        , 'table_name': 'countries2020_2021'}
    , provide_context=True
    , dag=dag)

start_etl_deaths = PythonOperator(
    task_id='Import.covidS3.start_etl_deaths'
    , python_callable=ImportCovidS3dPercountry.run
    , op_kwargs={
         'operation': 'start_etl_deaths'
        , 'table_name': 'totals2020_2021'}
    , provide_context=True
    , dag=dag)



##################################################
# DAG Operator Flow
##################################################

create_table_countries2020_2021 >> start_etl >> create_table_totals2020_2021 >> start_etl_totals >> start_etl_deaths