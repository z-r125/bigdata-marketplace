"""Outils partagés par les jobs Spark : session, lecture Cassandra, export CSV, chronométrage."""
import csv
import math
import os
import statistics
import time
from datetime import datetime

from pyspark.sql import SparkSession

KEYSPACE = "marketplace"
RESULTS = "/opt/app/results"                  # = spark/results/ dans le dépôt
REF_DATE = os.getenv("REF_DATE", "2026-11-13")  # même date de référence que le générateur
FIELDS = ["timestamp", "query", "description", "variant", "cl", "nodes_up", "runs", "ok", "errors",
          "median_ms", "p95_ms", "min_ms", "max_ms", "mean_ms", "last_error", "note"]


def get_spark(app_name):
    """Crée la session et renvoie (spark, durée de démarrage en secondes)."""
    t0 = time.perf_counter()
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark, time.perf_counter() - t0


def load(spark, table):
    """Lit une table Cassandra (lecture répartie par plages de tokens entre les workers)."""
    return (spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace=KEYSPACE, table=table).load())


def save(pdf, name):
    os.makedirs(RESULTS, exist_ok=True)
    pdf.to_csv(os.path.join(RESULTS, name), index=False)
    print(f"-> résultat écrit : spark/results/{name}")


def append_timing(query, description, samples_ms, note=""):
    """Ajoute une ligne à spark/results/timings.csv (même format que bench/results/all.csv)."""
    s = sorted(samples_ms)
    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"), "query": query,
        "description": description, "variant": "spark", "cl": "LOCAL_ONE (connecteur)", "nodes_up": "",
        "runs": len(s), "ok": len(s), "errors": 0,
        "median_ms": round(statistics.median(s), 2),
        "p95_ms": round(s[max(0, math.ceil(0.95 * len(s)) - 1)], 2),
        "min_ms": round(s[0], 2), "max_ms": round(s[-1], 2), "mean_ms": round(statistics.fmean(s), 2),
        "last_error": "", "note": note,
    }
    path = os.path.join(RESULTS, "timings.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)
    print(f"-> temps {query} : médiane {row['median_ms']} ms ({len(s)} exécution(s))")