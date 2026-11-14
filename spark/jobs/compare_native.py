"""
Comparaison natif vs Spark : A16, A17, A19 calculées avec Spark (mêmes tables, même logique).
Seul le calcul est chronométré (lecture Cassandra -> collect), pas le démarrage de la session.
1 exécution de chauffe + 5 mesures.
"""
import time
from datetime import date, timedelta

from pyspark.sql import functions as F

from common import REF_DATE, append_timing, get_spark, load

spark, startup = get_spark("Comparaison-natif-Spark")
end = date.fromisoformat(REF_DATE)
start = end - timedelta(days=29)
months = sorted({start.strftime("%Y-%m"), end.strftime("%Y-%m")})


def a16():
    return (load(spark, "orders_by_month").filter(F.col("month").isin(months))
            .filter((F.col("status") != "CANCELLED") & F.col("day").between(start, end))
            .groupBy("day").agg(F.sum("total").alias("ca")).orderBy("day").collect())


def a17():
    return (load(spark, "lines_by_product").groupBy("product_id")
            .agg(F.sum("quantity").alias("qty")).orderBy(F.desc("qty")).limit(10).collect())


def a19():
    return (load(spark, "lines_by_category").filter(F.col("status") != "CANCELLED")
            .groupBy("category").agg(F.sum("amount").alias("ca")).orderBy(F.desc("ca")).collect())


for qid, desc, fn in [("A16", "CA par jour sur 30 jours", a16),
                      ("A17", "Top 10 produits vendus", a17),
                      ("A19", "CA par catégorie", a19)]:
    samples, result = [], None
    for i in range(6):
        t0 = time.perf_counter()
        result = fn()
        if i > 0:
            samples.append((time.perf_counter() - t0) * 1000)
    append_timing(qid, f"{desc} (Spark)", samples, f"demarrage session {startup:.1f}s")
    print(f"   {qid} - aperçu du résultat : {[tuple(r) for r in result[:3]]}")

spark.stop()