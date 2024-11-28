from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Gold_Metrics") \
    .getOrCreate()

# Chemins HDFS
silver_path = "hdfs://localhost:9000/datalake/silver/"
gold_path = "hdfs://localhost:9000/datalake/gold/"

# =====================
# 1. Total des ventes par produit (transactions)
# =====================
df_transactions = spark.read.parquet(f"{silver_path}/transactions")

# Calculer le total des ventes par produit (quantité * prix)
df_sales = df_transactions.groupBy("produit").agg(
    F.sum(F.col("quantite") * F.col("prix")).alias("total_ventes")
)

# Sauvegarder dans la couche Gold
df_sales.write.parquet(f"{gold_path}/total_sales_per_product", mode="overwrite")

# =====================
# 2. Nombre de requêtes par code de statut HTTP (server_logs)
# =====================
df_logs = spark.read.parquet(f"{silver_path}/server_logs")

# Compter le nombre de requêtes par code de statut
df_status_count = df_logs.groupBy("status_code").agg(
    F.count("*").alias("count_requests")
)

# Sauvegarder dans la couche Gold
df_status_count.write.parquet(f"{gold_path}/status_code_counts", mode="overwrite")

# =====================
# 3. Moyenne des likes par produit (social_media)
# =====================
df_social = spark.read.parquet(f"{silver_path}/social_media")

# Calculer la moyenne des likes par produit
df_likes_per_product = df_social.groupBy("contenu").agg(
    F.avg(F.col("likes").cast("integer")).alias("avg_likes")
)

# Sauvegarder dans la couche Gold
df_likes_per_product.write.parquet(f"{gold_path}/avg_likes_per_product", mode="overwrite")

# =====================
# 4. Nombre total de clics et impressions par campagne (pub_stream)
# =====================
df_pub_stream = spark.read.parquet(f"{silver_path}/pub_stream")

# Calculer le total des clics et impressions par campagne
df_pub_metrics = df_pub_stream.groupBy("campaign_id").agg(
    F.sum("clicks").alias("total_clicks"),
    F.sum("impressions").alias("total_impressions")
)

# Sauvegarder dans la couche Gold
df_pub_metrics.write.parquet(f"{gold_path}/campaign_metrics", mode="overwrite")

print("Calcul des métriques et sauvegarde dans la couche Gold terminés.")
