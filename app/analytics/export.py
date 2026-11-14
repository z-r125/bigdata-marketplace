"""
Exécute les requêtes analytiques natives, affiche les résultats et les écrit en CSV
dans bench/results/ (utilisés par le tableau de bord et le rapport).
Usage : make analytics
"""
import csv
from pathlib import Path

from app.analytics import index_queries as iq
from app.analytics import queries as aq
from app.db import execute, shutdown

OUT = Path("bench/results")


def write(name, header, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"   -> {OUT / name}")


def show(title, header, rows, limit=12):
    print(f"\n=== {title} ===")
    print(" | ".join(header))
    for r in rows[:limit]:
        print(" | ".join(str(v) for v in r))


def main():
    sections = [
        ("A16 CA par jour (30 j)", "a16_ca_par_jour.csv", ["jour", "ca_mad"], aq.a16_revenue_per_day()),
        ("A17 Top 10 produits", "a17_top_produits.csv", ["product_id", "nom", "categorie", "quantite"],
         aq.a17_top_products()),
        ("A19 CA par catégorie", "a19_ca_categorie.csv", ["categorie", "ca_mad"], aq.a19_revenue_by_category()),
        ("A20 Panier moyen par ville", "a20_panier_ville.csv", ["ville", "panier_moyen_mad", "nb_commandes"],
         aq.a20_avg_basket_by_city()),
        ("A22 Commandes par paiement", "a22_paiement.csv", ["mode_paiement", "nb_commandes"],
         aq.a22_orders_by_payment()),
        ("A25 Taux d'annulation", "a25_annulation.csv", ["categorie", "lignes", "lignes_annulees", "taux_pct"],
         aq.a25_cancellation_rate()),
    ]
    for title, fname, header, rows in sections:
        show(title, header, rows)
        write(fname, header, rows)

    pid = execute("SELECT product_id FROM review_helpful LIMIT 1").one()
    prod = execute("SELECT product_id FROM reviews_by_product LIMIT 1").one().product_id
    print(f"\n=== A18 Note d'un produit ({prod}) ===\n{aq.a18_product_rating(prod)}")
    print(f"\nA21 produits stock < 10 : {len(iq.a21_low_stock())}")
    print(f"A23 PC RAM >= 16 Go et < 8000 MAD : {len(iq.a23_computers())}")
    print(f"A24 clients inactifs 90 j : {len(iq.a24_inactive_customers())}")
    shutdown()


if __name__ == "__main__":
    main()