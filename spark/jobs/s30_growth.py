"""S30 - Croissance mensuelle du CA par catégorie (fonction de fenêtre lag)."""
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import append_timing, get_spark, load, save

spark, startup = get_spark("S30-Croissance")
t0 = time.perf_counter()

lines = (load(spark, "orders").filter(F.col("status") != "CANCELLED")
         .select(F.date_format("order_date", "yyyy-MM").alias("mois"), F.explode("lines").alias("l"))
         .select("mois", F.col("l.category").alias("categorie"),
                 (F.col("l.quantity") * F.col("l.unit_price")).cast("double").alias("montant")))
monthly = lines.groupBy("categorie", "mois").agg(F.round(F.sum("montant"), 2).alias("ca_mad"))

w = Window.partitionBy("categorie").orderBy("mois")
growth = (monthly.withColumn("ca_mois_precedent", F.lag("ca_mad", 1).over(w))
          .withColumn("croissance_pct",
                      F.round(100 * (F.col("ca_mad") - F.col("ca_mois_precedent")) / F.col("ca_mois_precedent"), 1))
          .orderBy("categorie", "mois")).toPandas()
elapsed = (time.perf_counter() - t0) * 1000

print(growth[growth["categorie"] == "Ordinateurs"].to_string(index=False))
save(growth, "s30_croissance.csv")
append_timing("S30", "Croissance mensuelle par catégorie (lag)", [elapsed],
              f"1 execution; demarrage session {startup:.1f}s")
spark.stop()