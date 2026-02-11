import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, year, current_date, when

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_path',
    'target_path'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


df = spark.read \
    .option("header", "true") \
    .csv(args['source_path'])


df_typed = df \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))


df_valid = df_typed.filter(
    (col("price") > 0) &
    (col("year") >= 1990) &
    (col("year") <= year(current_date())) &
    (col("sale_date").isNotNull())
)


df_enriched = df_valid.withColumn(
    "car_age_at_sale",
    year(col("sale_date")) - col("year")
)


df_enriched = df_enriched.withColumn(
    "price_segment",
    when(col("price") < 15000, "budget")
    .when(col("price") < 30000, "mid")
    .otherwise("premium")
)


df_enriched.write \
    .mode("overwrite") \
    .parquet(args['target_path'])

job.commit()
