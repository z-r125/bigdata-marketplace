"""S27 - Produits souvent achetés ensemble (FP-Growth, MLlib)."""
import time

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from common import append_timing, get_spark, load, save

spark, startup = get_spark("S27-FPGrowth")
t0 = time.perf_counter()

names = {r.product_id: r.name for r in load(spark, "products").select("product_id", "name").collect()}
baskets = (load(spark, "orders").select("order_id", F.explode("lines").alias("l"))
           .groupBy("order_id")
           .agg(F.collect_set(F.col("l.product_id").cast("string")).alias("items"))
           .filter(F.size("items") >= 2)
           .cache())
n_baskets = baskets.count()

model = FPGrowth(itemsCol="items", minSupport=0.002, minConfidence=0.2).fit(baskets)
n_itemsets = model.freqItemsets.count()
rules = model.associationRules.orderBy(F.desc("lift"), F.desc("confidence")).limit(20).toPandas()
elapsed = (time.perf_counter() - t0) * 1000

to_names = lambda ids: " + ".join(names.get(i, i) for i in ids)
rules["antecedent"] = rules["antecedent"].apply(to_names)
rules["consequent"] = rules["consequent"].apply(to_names)
rules = rules.round(4)

print(f"{n_baskets} paniers de 2 produits ou plus, {n_itemsets} itemsets fréquents, {len(rules)} règles retenues")
print(rules.head(10).to_string(index=False))
save(rules, "s27_regles.csv")
append_timing("S27", "FP-Growth (achats conjoints)", [elapsed], f"1 execution; demarrage session {startup:.1f}s")
spark.stop()