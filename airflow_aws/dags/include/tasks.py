from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
from io import StringIO
import pandas as pd

def get_s3_client(aws_conn_id: str):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return hook.get_conn()


@task
def validate_technical(bucket:str,key:str,aws_conn_id:str):
    s3 = get_s3_client(aws_conn_id)

    meta = s3.head_object(Bucket=bucket, Key=key)

    if meta["ContentLength"]==0:
        raise ValueError("File is empty")
    
    if not key.endswith(".csv"):
        raise ValueError("Wrong extension")

@task
def validate_schema(bucket: str, key: str, aws_conn_id: str):
    s3=get_s3_client(aws_conn_id)

    body=s3.get_object(Bucket=bucket,Key=key)["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(body), nrows=1)

    expected_header = ["sale_id","car_brand","car_model","year","price","customer_name","sale_date"]

    if list(df.columns) != expected_header:
        raise ValueError("Schema mismatch")

