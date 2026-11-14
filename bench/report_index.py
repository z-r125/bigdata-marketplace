"""
Synthèse de l'expérience d'indexation : avec index SAI vs sans index (ALLOW FILTERING).
Usage : docker exec -u $(id -u):$(id -g) app python -m bench.report_index
"""
from pathlib import Path

import pandas as pd

from app.analytics.index_queries import a21_low_stock, a23_computers, a24_inactive_customers
from app.db import shutdown

SRC = Path(__file__).resolve().parent / "results" / "index.csv"
OUT = SRC.with_name("index_summary.csv")

df = pd.read_csv(SRC)
df["requete"] = df["query"].str.replace("_NOIDX", "", regex=False)
last = df.sort_values("timestamp").groupby(["requete", "variant"]).last().reset_index()

summary = last.pivot(index="requete", columns="variant", values=["median_ms", "p95_ms"])
summary.columns = [f"{m}_{v}" for m, v in summary.columns]
summary["facteur_gain"] = (summary["median_ms_sans_index"] / summary["median_ms_sai"]).round(1)

try:  # nombre de lignes renvoyées (index recréés)
    summary["lignes_renvoyees"] = [len(a21_low_stock()), len(a23_computers()), len(a24_inactive_customers())]
except Exception as exc:
    print(f"(comptage ignoré : {exc})")
finally:
    shutdown()

summary.to_csv(OUT)
print("\n=== Expérience d'indexation (ms) ===")
print(summary.to_string())
print(f"\nSynthèse écrite dans : {OUT}")