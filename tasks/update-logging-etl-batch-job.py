from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

try:
    spark.sql("select current_date() as run_date, current_timestamp() as datelastmaint, '1900-01-01' as prev_datelastmaint") \
        .write.format("iceberg").mode("overwrite").saveAsTable("auto_dv_metadata.logging_etl_batch_job")

except Exception as e:
    print("ERROR:", e)
    raise e