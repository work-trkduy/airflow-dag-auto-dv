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

        spark.sql(sql_text.replace("$", "")).show(200, False)

except Exception as e:
    print("ERROR:", e)
    raise e