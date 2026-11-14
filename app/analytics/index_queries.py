"""
Requêtes analytiques reposant sur des index SAI : A21, A23, A24.
allow_filtering=True : variante SANS index (scan complet + filtrage), pour l'expérience d'indexation.
"""
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from app.db import execute

AF = " ALLOW FILTERING"


def ref_date() -> datetime:
    """Date de référence du jeu de données (manifest du générateur), sinon aujourd'hui."""
    try:
        m = json.loads(Path("data/manifest.json").read_text(encoding="utf-8"))
        return datetime.fromisoformat(m["ref_date"])
    except Exception:
        return datetime.now()


def a21_low_stock(threshold=10, allow_filtering=False, cl=None):
    """A21 - Produits dont le stock est < 10.
    Avec SAI : recherche par plage dans l'index de chaque nœud. Sans index : scan de toute la table."""
    cql = "SELECT product_id, name, category, stock FROM products WHERE stock < ?"
    return list(execute(cql + (AF if allow_filtering else ""), (threshold,), cl))


def a23_computers(min_ram=16, max_price=8000, allow_filtering=False, cl=None):
    """A23 - Ordinateurs avec RAM >= 16 Go à moins de 8 000 MAD (filtre sur attributs variables).
    ram_gb est extrait de la map attributes au chargement pour être indexable numériquement.
    Avec SAI : intersection de 3 index (category, ram_gb, price)."""
    cql = ("SELECT product_id, name, ram_gb, price FROM products "
           "WHERE category = 'Ordinateurs' AND ram_gb >= ? AND price < ?")
    return list(execute(cql + (AF if allow_filtering else ""), (min_ram, Decimal(max_price)), cl))


def a24_inactive_customers(days=90, allow_filtering=False, cl=None):
    """A24 - Clients sans commande depuis 90 jours (clients sans aucune commande inclus :
    leur last_order_at vaut leur date d'inscription)."""
    limit = ref_date() - timedelta(days=days)
    cql = "SELECT customer_id, name, city, last_order_at FROM customers WHERE last_order_at < ?"
    return list(execute(cql + (AF if allow_filtering else ""), (limit,), cl))