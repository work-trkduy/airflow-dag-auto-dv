from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

def sync_csv(path, table):
    df = spark.read.format("csv") \
        .option("header","true") \
        .option("inferSchema","true") \
        .option("dateFormat", "M/d/yyyy") \
        .option("timestampFormat", "M/d/yyyy HH:mm:ss") \
        .load(path)
    df.write.format("parquet").mode("overwrite").saveAsTable(table)

try:
    sync_csv("/auto_dv/customer.csv", "auto_dv_psa.customer")

except Exception as e:
    print("ERROR:", e)
    raise e