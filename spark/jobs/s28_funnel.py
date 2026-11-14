"""S28 - Entonnoir vues -> ajout au panier -> achat, par catégorie (jointure de 3 sources)."""
import time

from pyspark.sql import functions as F

from common import append_timing, get_spark, load, save

spark, startup = get_spark("S28-Entonnoir")
t0 = time.perf_counter()

products = load(spark, "products").select("product_id", "category")
views = (load(spark, "product_views").join(products, "product_id")
         .groupBy("category").agg(F.count("*").alias("vues")))
adds = (load(spark, "cart_events").filter(F.col("action") == "ADD").join(products, "product_id")
        .groupBy("category").agg(F.count("*").alias("ajouts_panier")))
buys = (load(spark, "orders").filter(F.col("status") != "CANCELLED")
        .select(F.explode("lines").alias("l"))
        .groupBy(F.col("l.category").alias("category")).agg(F.count("*").alias("achats")))

funnel = (views.join(adds, "category").join(buys, "category")
          .withColumn("taux_vue_panier_pct", F.round(100 * F.col("ajouts_panier") / F.col("vues"), 1))
          .withColumn("taux_panier_achat_pct", F.round(100 * F.col("achats") / F.col("ajouts_panier"), 1))
          .withColumn("conversion_globale_pct", F.round(100 * F.col("achats") / F.col("vues"), 1))
          .orderBy(F.desc("vues"))).toPandas()
elapsed = (time.perf_counter() - t0) * 1000

print(funnel.to_string(index=False))
save(funnel, "s28_entonnoir.csv")
append_timing("S28", "Entonnoir vues/panier/achat (3 sources)", [elapsed],
              f"1 execution; demarrage session {startup:.1f}s")
spark.stop()