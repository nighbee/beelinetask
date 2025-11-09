from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='telecom_daily_batch_job',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 8),
    catchup=False,
    tags=['telecom', 'spark'],
    doc_md="""
    ### Telecom Daily Batch Job DAG
    This DAG runs a Spark batch job to process telecom data for the previous day.
    """
) as dag:

    # Task to run the Spark batch job
    run_spark_batch_job = BashOperator(
        task_id='run_spark_batch_job',
        bash_command="""
            docker exec -u root spark-master spark-submit \
              --master spark://spark-master:7077 \
              --packages org.postgresql:postgresql:42.6.0 \
              /opt/bitnamilegacy/spark/jobs/batch_processor.py {{ ds }}
        """,
        doc_md="""
        #### Run Spark Batch Job
        This task submits the Spark batch job to the Spark master.
        The execution date `{{ ds }}` is passed as an argument.
        """
    )