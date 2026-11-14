"""
Lanceur du bench.

  make bench                                   # toutes les requêtes du catalogue
  make bench ARGS="--only T3,T4 --runs 20"     # sous-ensemble, 20 mesures
  make bench ARGS="--only T1,T3 --cl ALL --note '3 noeuds'"   # expérience de cohérence
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from app.db import shutdown
from bench.harness import RESULTS_DIR, measure, print_table, write_csv
from bench.workload import WORKLOAD, build_context


def main():
    ap = argparse.ArgumentParser(description="Banc de mesure des requêtes")
    ap.add_argument("--only", help="liste d'identifiants séparés par des virgules (ex. T1,T3,A16)")
    ap.add_argument("--runs", type=int, help="nombre de mesures (défaut : 10, ou valeur du catalogue)")
    ap.add_argument("--cl", choices=["ONE", "QUORUM", "LOCAL_QUORUM", "ALL"],
                    help="niveau de cohérence (appliqué aux requêtes qui le supportent)")
    ap.add_argument("--variant", default="native", help="étiquette : native, spark, sans_index...")
    ap.add_argument("--note", default="", help="commentaire libre (ex. '1 noeud arrêté')")
    ap.add_argument("--out", default=str(RESULTS_DIR / "all.csv"))
    args = ap.parse_args()

    ids = [i.strip().upper() for i in args.only.split(",")] if args.only else list(WORKLOAD)
    unknown = [i for i in ids if i not in WORKLOAD]
    if unknown:
        sys.exit(f"Requêtes inconnues : {unknown}. Disponibles : {list(WORKLOAD)}")

    ctx = build_context()
    results = []
    try:
        for qid in ids:
            spec = WORKLOAD[qid]
            cl = args.cl if (args.cl and spec.supports_cl) else None
            print(f"-> {qid:<4} {spec.description}", flush=True)
            results.append(measure(
                qid, spec.fn, spec.make_args, ctx,
                runs=args.runs or spec.runs or 10,
                warmup=1 if spec.warmup is None else spec.warmup,
                variant=args.variant, cl=cl, description=spec.description, note=args.note,
            ))
    finally:
        if results:
            write_csv(results, Path(args.out))
            print_table(results)
            print(f"\nRésultats ajoutés à : {args.out}")
        shutdown()


if __name__ == "__main__":
    main()