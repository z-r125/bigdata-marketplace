"""S26 - Analyse RFM (récence, fréquence, montant) et segmentation des clients."""
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import append_timing, get_spark, load, save

spark, startup = get_spark("S26-RFM")
t0 = time.perf_counter()

orders = (load(spark, "orders").select("customer_id", "order_date", "total", "status")
          .filter(F.col("status") != "CANCELLED"))
ref = orders.agg(F.max("order_date")).first()[0]

rfm = orders.groupBy("customer_id").agg(
    F.datediff(F.lit(ref), F.max("order_date")).alias("recence_j"),
    F.count("*").alias("frequence"),
    F.round(F.sum("total").cast("double"), 2).alias("montant"),
)
# Scores de 1 à 5 par quintiles (5 = meilleur). Récence : plus elle est faible, meilleur est le score.
rfm = (rfm.withColumn("score_r", 6 - F.ntile(5).over(Window.orderBy("recence_j")))
          .withColumn("score_f", F.ntile(5).over(Window.orderBy("frequence")))
          .withColumn("score_m", F.ntile(5).over(Window.orderBy("montant"))))
r, f, m = F.col("score_r"), F.col("score_f"), F.col("score_m")
rfm = rfm.withColumn("segment",
                     F.when((r >= 4) & (f >= 4) & (m >= 4), "Champions")
                      .when((r >= 3) & (f >= 3), "Clients fidèles")
                      .when((r >= 4) & (f <= 2), "Nouveaux / prometteurs")
                      .when((r <= 2) & (f >= 3), "À risque")
                      .when((r <= 2) & (f <= 2), "Perdus / hibernants")
                      .otherwise("À surveiller"))

segments = (rfm.groupBy("segment").agg(
    F.count("*").alias("nb_clients"),
    F.round(F.avg("recence_j"), 1).alias("recence_moy_j"),
    F.round(F.avg("frequence"), 2).alias("frequence_moy"),
    F.round(F.avg("montant"), 2).alias("montant_moy_mad"),
    F.round(F.sum("montant"), 2).alias("ca_total_mad"),
).orderBy(F.desc("ca_total_mad"))).toPandas()
clients = rfm.toPandas()
elapsed = (time.perf_counter() - t0) * 1000

print(segments.to_string(index=False))
save(segments, "s26_rfm_segments.csv")
save(clients, "s26_rfm_clients.csv")
append_timing("S26", "Analyse RFM et segmentation", [elapsed], f"1 execution; demarrage session {startup:.1f}s")
spark.stop()