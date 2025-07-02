from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="example_pipeline",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="extract_raw_data",
        bash_command="python extract_raw_data.py"
    )

    task2 = BashOperator(
        task_id="transform_and_validate",
        bash_command="python transform_and_validate.py"
    )

    task3 = BashOperator(
        task_id="load_data",
        bash_command="python load_data_to_redshift.py"
    )
    
    # this option is worth to explore it
    # task3 = PostgresOperator(
    #     task_id="redshift_connector_task",
    #     postgres_conn_id="redshift_conn_id",
    #     sql="""
    #             foo
    #         """,
    # )

    task1 >> task2 >> task3  # define task dependencies
