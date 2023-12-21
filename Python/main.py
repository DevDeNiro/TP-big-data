from pyspark.sql import SparkSession

spark_session = SparkSession.builder \
    .appName("KafkaToSparkStreaming") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws: aws-java-sdk-bundle:1.11.563") \
    .getOrCreate()

spark_session.sparkContext.setLogLevel("ERROR")

df = spark_session.read.options(inferSchema="true", delimiter=",", header="true") \
    .parquet("elem")

df.show()
df.printSchema()

bucket_name = "warehouse"
df.write.mode("overwrite") \
    .format("parquet") \
    .option("path", f"s3a://{bucket_name}/elem") \
    .save()
