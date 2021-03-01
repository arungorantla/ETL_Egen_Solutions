from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from task_scripts.extract_task import Extract
from task_scripts.transform_task import Transform
from task_scripts.load_task import Load


# Define the default dag arguments.
default_args = {
    'owner': 'airflow',
    'provide_context': True
}

# [START instantiate_dag]
with DAG(
    dag_id= 'etl_covid_data_dag',
    default_args=default_args,
    description='ETL DAG',
    schedule_interval="0 9 * * *",
    start_date=days_ago(2),
    concurrency=32,
    max_active_runs=2
) as dag:
    # [END instantiate_dag]

    extract_obj = Extract()
    transform_obj = Transform()
    load_obj = Load()

    # Extract task is to extract the covid testing and hospitalization data from API's
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_obj.extract,
    )

    # Transform task is to perform data cleaning and transformation
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_obj.transform,
    )

    # Load task is to update and insert data in the database
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_obj.load,
    )

    # [Start ETL Pipeline Execution]
    extract_task >> transform_task >> load_task

    for county_name in extract_obj.getCountyNamesFromAPI():
        temp = PythonOperator(
            task_id='Loading_data_into_'+county_name,
            python_callable=load_obj.sql_comp.insertData,
            op_kwargs={"county_name": county_name}
        )
        load_task >> temp

