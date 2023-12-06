import sys
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

try:
    for file_name in sys.argv[1:]:
        f = open(file_name, "r")
        sql_text = f.read()
        f.close()

        if "create table" not in sql_text:
            time_row = spark.sql(
                """
                select
                    cast(datelastmaint as string) as datelastmaint,
                    cast(prev_datelastmaint as string) as prev_datelastmaint
                from auto_dv_metadata.logging_etl_batch_job order by run_date desc
                """
            ).head()
            v_from_date = time_row.__getitem__("prev_datelastmaint")
            v_end_date = time_row.__getitem__("datelastmaint")

            sql_text = sql_text \
                .replace("$v_from_date", f"timestamp'{v_from_date}'") \
                .replace("$v_end_date", f"timestamp'{v_end_date}'") \
                .replace("$", "")

        spark.sql(sql_text).show(200, False)

except Exception as e:
    print("ERROR:", e)
    raise e