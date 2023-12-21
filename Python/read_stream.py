from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, when, to_timestamp, to_date

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
    .option("subscribe", "tp-big-data") \
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# --------------------------------------------------------------------
#                     MANIP SUR LES DF
# --------------------------------------------------------------------

# Traitement des données
df = df.withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise"))) # Conversion de la devise
df = df.withColumn("montant_eur", col("montant") * 1.10)  # Conversion USD en EUR
df = df.withColumn("date_timestamp", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))  # Ajouter le TimeZone
df = df.withColumn("date", to_date(col("date")))  # Convertir la date en string en une valeur date
df = df.filter(col("moyen_paiement") != "erreur")  # Supprimer les transactions en erreur
df = df.filter(col("lieu").isNotNull())  # Supprimer les valeurs None pour l'adresse

query = (df.writeStream.outputMode("append")
         .format("parquet")
         .option("checkpointLocation", "metadata")
         .option("path", "elem").start())

query.awaitTermination()
