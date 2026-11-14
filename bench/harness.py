"""
Chronomètre commun à toutes les requêtes (T1-T15, A16-A25, S26-S30).

Méthode : `warmup` exécutions de chauffe (non comptées), puis `runs` exécutions mesurées.
On retient médiane, p95, min, max et moyenne (en ms), plus le nombre d'erreurs.
Les arguments de chaque exécution sont préparés HORS chronométrage (make_args).
"""
from __future__ import annotations

import csv
import math
import statistics
import time
from datetime import datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"
FIELDS = ["timestamp", "query", "description", "variant", "cl", "nodes_up", "runs", "ok", "errors",
          "median_ms", "p95_ms", "min_ms", "max_ms", "mean_ms", "last_error", "note"]


def percentile(sorted_values: list[float], p: float):
    """Percentile par rang le plus proche (adapté aux petits échantillons)."""
    if not sorted_values:
        return None
    k = max(0, math.ceil(p / 100 * len(sorted_values)) - 1)
    return sorted_values[k]


def nodes_up():
    """Nombre de nœuds Cassandra vus comme actifs par le driver."""
    try:
        from app.db import session
        return sum(1 for h in session().cluster.metadata.all_hosts() if h.is_up)
    except Exception:
        return None


def measure(query: str, fn, make_args=None, ctx=None, runs: int = 10, warmup: int = 1,
            variant: str = "native", cl: str | None = None, description: str = "", note: str = "") -> dict:
    """Mesure `fn` et renvoie une ligne de résultat (dict, colonnes = FIELDS)."""
    samples, errors, last_error = [], 0, ""
    for i in range(warmup + runs):
        args, kwargs = make_args(ctx) if make_args else ((), {})
        if cl is not None:
            kwargs = {**kwargs, "cl": cl}
        t0 = time.perf_counter()
        try:
            fn(*args, **kwargs)
            ok = True
        except Exception as exc:
            ok = False
            last_error = f"{type(exc).__name__}: {exc}"[:200]
        elapsed = (time.perf_counter() - t0) * 1000
        if i >= warmup:
            if ok:
                samples.append(elapsed)
            else:
                errors += 1

    s = sorted(samples)
    rnd = lambda v: None if v is None else round(v, 2)
    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "query": query,
        "description": description,
        "variant": variant,
        "cl": cl or "LOCAL_QUORUM (défaut)",
        "nodes_up": nodes_up(),
        "runs": runs,
        "ok": len(s),
        "errors": errors,
        "median_ms": rnd(statistics.median(s)) if s else None,
        "p95_ms": rnd(percentile(s, 95)),
        "min_ms": rnd(s[0]) if s else None,
        "max_ms": rnd(s[-1]) if s else None,
        "mean_ms": rnd(statistics.fmean(s)) if s else None,
        "last_error": last_error,
        "note": note,
    }


def write_csv(results: list[dict], path: Path):
    """Ajoute les résultats au CSV (en-tête écrit seulement si le fichier est nouveau)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerows(results)


def print_table(results: list[dict]):
    fmt = lambda v: "-" if v is None else f"{v:.1f}"
    print(f"\n{'Req':<5}{'Variante':<10}{'CL':<22}{'Nœuds':>6}{'Médiane':>10}{'p95':>10}"
          f"{'Min':>9}{'Max':>10}{'Err':>5}  Description")
    print("-" * 120)
    for r in results:
        print(f"{r['query']:<5}{r['variant']:<10}{r['cl']:<22}{str(r['nodes_up']):>6}"
              f"{fmt(r['median_ms']):>10}{fmt(r['p95_ms']):>10}{fmt(r['min_ms']):>9}{fmt(r['max_ms']):>10}"
              f"{r['errors']:>5}  {r['description']}")
        if r["last_error"]:
            print(f"      dernière erreur : {r['last_error']}")