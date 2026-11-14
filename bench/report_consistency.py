"""
Synthèse de l'expérience de cohérence : médiane (ms) et taux d'erreur par requête, CL et situation.
Usage : docker exec -u $(id -u):$(id -g) app python -m bench.report_consistency
"""
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parent / "results" / "consistency.csv"
OUT = SRC.with_name("consistency_summary.csv")
ORDER_CL = ["ONE", "QUORUM", "ALL"]
ORDER_SITU = ["3_noeuds", "1_arrete", "2_arretes"]

df = pd.read_csv(SRC)
df["erreurs_%"] = (100 * df["errors"] / df["runs"]).round(0)
df["cl"] = pd.Categorical(df["cl"], ORDER_CL, ordered=True)
df["note"] = pd.Categorical(df["note"], ORDER_SITU, ordered=True)

summary = (df.sort_values("timestamp")
             .groupby(["query", "note", "cl"], observed=True).last()
             .reset_index()[["query", "note", "cl", "nodes_up", "median_ms", "p95_ms", "erreurs_%"]])
summary.to_csv(OUT, index=False)

for metric, title in [("median_ms", "MÉDIANE (ms)"), ("erreurs_%", "TAUX D'ERREUR (%)")]:
    pivot = summary.pivot_table(index="query", columns=["note", "cl"], values=metric, observed=True)
    print(f"\n=== {title} ===")
    print(pivot.to_string(na_rep="-"))
print(f"\nSynthèse écrite dans : {OUT}")