"""S29 - Valeur client moyenne par cohorte mensuelle d'inscription."""
import time

from pyspark.sql import functions as F

from common import append_timing, get_spark, load, save

spark, startup = get_spark("S29-Cohortes")
t0 = time.perf_counter()

customers = (load(spark, "customers").select("customer_id", "signup_date")
             .withColumn("cohorte", F.date_format("signup_date", "yyyy-MM")))
revenue = (load(spark, "orders").filter(F.col("status") != "CANCELLED")
           .groupBy("customer_id").agg(F.sum(F.col("total").cast("double")).alias("ca")))

cohorts = (customers.join(revenue, "customer_id", "left").fillna(0, subset=["ca"])
           .groupBy("cohorte").agg(
               F.count("*").alias("nb_clients"),
               F.sum(F.when(F.col("ca") > 0, 1).otherwise(0)).alias("nb_acheteurs"),
               F.round(F.sum("ca"), 2).alias("ca_total_mad"))
           .withColumn("valeur_moy_client_mad", F.round(F.col("ca_total_mad") / F.col("nb_clients"), 2))
           .orderBy("cohorte")).toPandas()
elapsed = (time.perf_counter() - t0) * 1000

print(cohorts.tail(12).to_string(index=False))
save(cohorts, "s29_cohortes.csv")
append_timing("S29", "Valeur client par cohorte d'inscription", [elapsed],
              f"1 execution; demarrage session {startup:.1f}s")
spark.stop()