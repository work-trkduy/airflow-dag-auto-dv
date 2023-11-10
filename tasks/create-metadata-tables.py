from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

try:
    spark.sql("create database if not exists auto_dv_metadata")
    spark.sql("create table if not exists auto_dv_metadata.logging_etl_batch_job (run_date date, datelastmaint timestamp) using iceberg")

except Exception as e:
    print("ERROR:", e)
    raise e