from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LectureParquet") \
    .getOrCreate()

df = spark.read.parquet("elem")
df.show(100000, False)

spark.stop()
