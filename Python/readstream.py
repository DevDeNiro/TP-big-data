from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

schema = StructType().add("id_transaction", StringType()) \
    .add("type_transaction", StringType()) \
    .add("montant", FloatType()) \
    .add("devise", StringType()) \
    .add("date", TimestampType()) \
    .add("lieu", StringType()) \
    .add("moyen_paiement", StringType()) \
    .add("details", StructType().add("produit", StringType()) \
         .add("quantite", IntegerType()) \
         .add("prix_unitaire", FloatType())) \
    .add("utilisateur", StructType().add("id_utilisateur", StringType()) \
         .add("nom", StringType()) \
         .add("adresse", StringType()) \
         .add("email", StringType()))

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transaction") \
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")
# df = df.withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))

query = df.writeStream.outputMode("append").format("parquet").option("checkpointLocation", "metadata").option("path",
                                                                                                              "elem").start()

query.awaitTermination()
