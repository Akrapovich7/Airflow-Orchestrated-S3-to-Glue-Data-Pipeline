from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import timedelta,datetime
from include.tasks import validate_technical,validate_schema
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator



BUCKET = "dealership-alilu-autos"
KEY = "raw/incoming/car_sales_data.csv"
AWS_CONN_ID = "aws_conn"


@dag(
    dag_id="s3_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["prod","aws","etl"],
    default_args={
        "retries":3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True
    }
)

def s3_pipeline():

    wait_for_file=S3KeySensor(
        task_id="wait_for_file",
        bucket_name=BUCKET,
        bucket_key=KEY,
        aws_conn_id=AWS_CONN_ID,
        mode="reschedule",
        timeout=60*60,
        poke_interval=60
    )

    tech = validate_technical(BUCKET, KEY, AWS_CONN_ID)
    schema=validate_schema(BUCKET,KEY,AWS_CONN_ID)

    move_file=S3CopyObjectOperator(
        task_id="move_file",
        source_bucket_name=BUCKET,
        source_bucket_key=KEY,
        dest_bucket_name=BUCKET,
        dest_bucket_key="raw/curated/car_sales_data.csv",
        aws_conn_id=AWS_CONN_ID
    )

    #----------Glue + Crawler to populate Glue Data Catalog
    run_glue_transformation=GlueJobOperator(
        task_id="run_glue_transformation",
        job_name="glue_alilu_autos_etl",
        script_location='s3://dealership-alilu-autos/glue-scripts/glue_alilu_autos_etl.py',
        script_args={
            "--JOB_NAME": "glue_alilu_autos_etl",
            "--source_path": "s3://dealership-alilu-autos/raw/curated/",
            "--target_path": "s3://dealership-alilu-autos/processed/"
        },
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1",
        wait_for_completion=True
    )

    run_crawler=GlueCrawlerOperator(
        task_id="run_crawler",
        config={
        "Name": "dealership_alilu_autos_crawler"
        },
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1",
        wait_for_completion=False
    )

    wait_for_file >> tech >> schema >> move_file>> run_glue_transformation >> run_crawler

s3_pipeline()


