"""
Requêtes analytiques natives A16-A20, A22, A25.
Les agrégats (SUM, AVG, COUNT, GROUP BY) sont calculés par Cassandra à l'intérieur d'une partition.
Les filtres sur une colonne hors clé (status, payment_method) restent limités à UNE partition
(ALLOW FILTERING mono-partition : pas de scan du cluster).
A21, A23, A24 (index SAI) sont dans index_queries.py.
"""
from __future__ import annotations

from datetime import timedelta

from app.analytics.index_queries import ref_date
from app.db import execute

CATEGORIES = ["Ordinateurs", "Smartphones", "TV & Audio", "Vêtements",
              "Chaussures", "Meubles", "Cuisine", "Décoration"]
CITIES = ["Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Agadir",
          "Meknès", "Oujda", "Kénitra", "Tétouan", "El Jadida", "Safi"]
PAYMENTS = ["carte", "cash_livraison", "paypal", "virement"]


def months_between(start, end) -> list[str]:
    months, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def a16_revenue_per_day(days=30, cl=None):
    """A16 - CA par jour sur 30 jours. Table orders_by_month (partition = mois, clustering = day).
    SUM(total) GROUP BY day sur 2 partitions, moins le CA des commandes annulées."""
    end = ref_date().date()
    start = end - timedelta(days=days - 1)
    revenue = {}
    for month in months_between(start, end):
        for r in execute("SELECT day, SUM(total) AS ca FROM orders_by_month WHERE month = ? GROUP BY day",
                         (month,), cl, timeout=60):
            revenue[r.day.date()] = float(r.ca)
        for r in execute("SELECT day, SUM(total) AS ca FROM orders_by_month WHERE month = ? "
                         "AND status = 'CANCELLED' GROUP BY day ALLOW FILTERING", (month,), cl, timeout=60):
            revenue[r.day.date()] = revenue.get(r.day.date(), 0.0) - float(r.ca)
    return [(d, round(revenue.get(d, 0.0), 2)) for d in (start + timedelta(days=i) for i in range(days))]


def a17_top_products(n=10, cl=None):
    """A17 - Top 10 des produits vendus (quantité). Table lines_by_product :
    SUM(quantity) GROUP BY product_id sur toute la table, puis tri côté client (CQL ne trie pas un agrégat)."""
    rows = execute("SELECT product_id, SUM(quantity) AS qty FROM lines_by_product GROUP BY product_id",
                   (), cl, timeout=120)
    top = sorted(((r.product_id, r.qty) for r in rows), key=lambda x: -x[1])[:n]
    out = []
    for pid, qty in top:
        p = execute("SELECT name, category FROM products WHERE product_id = ?", (pid,), cl).one()
        out.append((str(pid), p.name if p else "?", p.category if p else "?", int(qty)))
    return out


def a18_product_rating(product_id, cl=None):
    """A18 - Note moyenne et nombre d'avis d'un produit. Table reviews_by_product (partition = produit).
    CAST obligatoire : avg() sur un int renvoie un entier en CQL."""
    r = execute("SELECT AVG(CAST(rating AS float)) AS note_moy, COUNT(*) AS nb "
                "FROM reviews_by_product WHERE product_id = ?", (product_id,), cl).one()
    return (round(r.note_moy, 2) if r.nb else None, r.nb)


def a19_revenue_by_category(cl=None):
    """A19 - CA par catégorie (hors annulations). Table lines_by_category : 1 partition par catégorie."""
    out = []
    for c in CATEGORIES:
        total = execute("SELECT SUM(amount) FROM lines_by_category WHERE category = ?",
                        (c,), cl, timeout=60).one()[0] or 0
        cancelled = execute("SELECT SUM(amount) FROM lines_by_category WHERE category = ? "
                            "AND status = 'CANCELLED' ALLOW FILTERING", (c,), cl, timeout=60).one()[0] or 0
        out.append((c, round(float(total) - float(cancelled), 2)))
    return sorted(out, key=lambda x: -x[1])


def a20_avg_basket_by_city(cl=None):
    """A20 - Panier moyen par ville. Table orders_by_city : status est la 1re colonne de clustering,
    donc status IN (...) exclut les annulations sans filtrage."""
    out = []
    for city in CITIES:
        r = execute("SELECT AVG(total) AS panier_moy, COUNT(*) AS nb FROM orders_by_city "
                    "WHERE city = ? AND status IN ('DELIVERED', 'PENDING', 'SHIPPED')", (city,), cl).one()
        out.append((city, round(float(r.panier_moy or 0), 2), r.nb))
    return sorted(out, key=lambda x: -x[1])


def a22_orders_by_payment(cl=None):
    """A22 - Nombre de commandes par mode de paiement sur 12 mois. Table orders_by_month :
    COUNT(*) filtré dans chaque partition mensuelle."""
    end = ref_date().date()
    start = end - timedelta(days=365)
    counts = dict.fromkeys(PAYMENTS, 0)
    for month in months_between(start, end):
        for pm in PAYMENTS:
            counts[pm] += execute("SELECT COUNT(*) FROM orders_by_month WHERE month = ? "
                                  "AND payment_method = ? ALLOW FILTERING", (month, pm), cl, timeout=60).one()[0]
    return sorted(counts.items(), key=lambda kv: -kv[1])


def a25_cancellation_rate(cl=None):
    """A25 - Taux d'annulation par catégorie (par ligne de commande). Table lines_by_category."""
    out = []
    for c in CATEGORIES:
        total = execute("SELECT COUNT(*) FROM lines_by_category WHERE category = ?",
                        (c,), cl, timeout=60).one()[0]
        cancelled = execute("SELECT COUNT(*) FROM lines_by_category WHERE category = ? "
                            "AND status = 'CANCELLED' ALLOW FILTERING", (c,), cl, timeout=60).one()[0]
        out.append((c, total, cancelled, round(100 * cancelled / total, 2) if total else 0.0))
    return sorted(out, key=lambda x: -x[3])