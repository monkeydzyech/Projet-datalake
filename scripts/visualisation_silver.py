from pyspark.sql import SparkSession

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Visualisation Silver") \
    .getOrCreate()

# Chemins des fichiers Parquet dans Silver
silver_path = "hdfs://localhost:9000/datalake/silver/"

# Lire les fichiers Parquet
df_transactions = spark.read.parquet(f"{silver_path}/transactions")
df_transactions.show(truncate=False)

df_logs = spark.read.parquet(f"{silver_path}/server_logs")
df_logs.show(truncate=False)

df_social = spark.read.parquet(f"{silver_path}/social_media")
df_social.show(truncate=False)

df_pub_stream = spark.read.parquet(f"{silver_path}/pub_stream")
df_pub_stream.show(truncate=False)

# Fermer la session Spark
spark.stop()
