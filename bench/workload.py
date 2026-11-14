"""
Catalogue des requêtes mesurées : fonction + générateur d'arguments.

make_args(ctx) -> (args, kwargs) est appelé AVANT chaque exécution, HORS chronométrage :
il tire des identifiants réels et prépare l'état nécessaire (ex. un article au panier pour T6).

Pour ajouter une requête (ex. M3 pour A16) :
    from app.analytics.queries import a16_revenue_per_day
    WORKLOAD["A16"] = Spec(a16_revenue_per_day, lambda ctx: ((), {}), "CA par jour sur 30 jours")
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable
from uuid import uuid4

from app.db import execute
from app.transactional import queries as q
from app.transactional.demo import fake_products
from app.analytics.index_queries import a21_low_stock, a23_computers, a24_inactive_customers

CITIES = ["Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Agadir",
          "Meknès", "Oujda", "Kénitra", "Tétouan", "El Jadida", "Safi"]
CATEGORIES = ["Ordinateurs", "Smartphones", "TV & Audio", "Vêtements",
              "Chaussures", "Meubles", "Cuisine", "Décoration"]
DEMO_ADDRESS = {"label": "bench", "street": "1 rue du Bench", "city": "Rabat", "postal_code": "10000"}


@dataclass
class Spec:
    fn: Callable
    make_args: Callable          # ctx -> (args, kwargs), appelé hors chronométrage
    description: str
    runs: int | None = None      # None = valeur par défaut du lanceur (10)
    warmup: int | None = None    # None = 1
    supports_cl: bool = False    # True si la fonction accepte cl= (expérience de cohérence)


# ------------------------------------------------------------------ contexte (échantillons réels)
def build_context(seed: int = 7) -> dict:
    products = [r.product_id for r in execute("SELECT product_id, stock FROM products LIMIT 1000")
                if r.stock > 100][:100]
    customers = list(dict.fromkeys(r.customer_id for r in
                                   execute("SELECT customer_id FROM orders_by_customer LIMIT 500")))
    reviews = [r.review_id for r in execute("SELECT review_id FROM review_helpful LIMIT 100")]
    if not (products and customers and reviews):
        raise RuntimeError("Données absentes : lance `make load` d'abord")
    return {"rnd": random.Random(seed), "products": products, "customers": customers, "reviews": reviews,
            "import_sellers": [], "bench_order": None, "toggle": 0}


def pick(ctx, key):
    return ctx["rnd"].choice(ctx[key])


def lines_for(product_id, qty=1):
    p = q.t03_get_product(product_id)
    return [{"product_id": p.product_id, "sku": p.sku, "name": p.name, "category": p.category,
             "quantity": qty, "unit_price": p.price}]


# ------------------------------------------------------------------ préparation des arguments
def args_t1(ctx):
    return (pick(ctx, "customers"), lines_for(pick(ctx, "products"))), {"payment_method": "carte"}


def args_t2(ctx):
    seller = uuid4()
    ctx["import_sellers"].append(seller)  # ces lots seront supprimés par T14
    return (fake_products(5000, seller, "RETIRED"),), {}


def args_t6(ctx):
    c, p = pick(ctx, "customers"), pick(ctx, "products")
    q.t05_add_to_cart(c, p, 1)  # l'article doit être au panier avant de le retirer
    return (c, p), {}


def args_t7(ctx):
    c = pick(ctx, "customers")
    q.t05_add_to_cart(c, pick(ctx, "products"), 1)
    return (c,), {}


def args_t8(ctx):
    if ctx["bench_order"] is None:  # commande dédiée au bench, pour ne pas modifier les données
        ctx["bench_order"] = q.t01_create_order(ctx["customers"][0], lines_for(ctx["products"][0]))
    ctx["toggle"] ^= 1
    return (ctx["bench_order"], "SHIPPED" if ctx["toggle"] else "PENDING"), {}


def args_t14(ctx):
    if not ctx["import_sellers"]:  # T14 lancé seul : on prépare un lot hors chronométrage
        seller = uuid4()
        q.t02_import_products(fake_products(5000, seller, "RETIRED"))
        ctx["import_sellers"].append(seller)
    return (ctx["import_sellers"].pop(0),), {}


# ------------------------------------------------------------------ catalogue
WORKLOAD: dict[str, Spec] = {
    "T1":  Spec(q.t01_create_order, args_t1, "Créer une commande (BATCH LOGGED, 6 tables)", supports_cl=True),
    "T2":  Spec(q.t02_import_products, args_t2, "Importer 5 000 produits (3 tables, parallèle)", runs=3, warmup=0),
    "T3":  Spec(q.t03_get_product, lambda c: ((pick(c, "products"),), {}), "Fiche produit", supports_cl=True),
    "T4":  Spec(q.t04_customer_orders, lambda c: ((pick(c, "customers"),), {}),
                "Commandes d'un client, récentes d'abord", supports_cl=True),
    "T5":  Spec(q.t05_add_to_cart, lambda c: ((pick(c, "customers"), pick(c, "products"), 1), {}),
                "Ajouter au panier"),
    "T6":  Spec(q.t06_remove_from_cart, args_t6, "Retirer du panier"),
    "T7":  Spec(q.t07_cart_ttl, args_t7, "Lire le TTL du panier (expiration 7 j)"),
    "T8":  Spec(q.t08_update_order_status, args_t8, "Changer le statut (BATCH LOGGED, 5 tables)",
                supports_cl=True),
    "T9":  Spec(q.t09_decrement_stock, lambda c: ((pick(c, "products"), 1), {}), "Décrémenter le stock (LWT)"),
    "T10": Spec(q.t10_publish_review, lambda c: ((pick(c, "products"), pick(c, "customers"), 4, "bench"), {}),
                "Publier un avis"),
    "T11": Spec(q.t11_vote_helpful, lambda c: ((pick(c, "reviews"),), {}), "Vote « utile » (counter)"),
    "T12": Spec(q.t12_add_address, lambda c: ((pick(c, "customers"), DEMO_ADDRESS), {}),
                "Ajouter une adresse (list<UDT>)"),
    "T13": Spec(q.t13_products_by_category, lambda c: ((c["rnd"].choice(CATEGORIES),), {}),
                "Catégorie triée par prix, page de 20", supports_cl=True),
    "T14": Spec(q.t14_delete_retired_products, args_t14, "Supprimer 5 000 produits retirés d'un vendeur",
                runs=3, warmup=0),
    "T15": Spec(q.t15_pending_orders_in_city, lambda c: ((c["rnd"].choice(CITIES),), {}),
                "Commandes PENDING d'une ville", supports_cl=True), 
    # --- M3 : ajouter ici A16 à A25 (natif) et les variantes Spark ---
    }
    # ------------------------------------------------------------------ A21 / A23 / A24 (index SAI)

NO_ARGS = lambda c: ((), {})
NO_INDEX = lambda c: ((), {"allow_filtering": True})

WORKLOAD.update({
    "A21": Spec(a21_low_stock, NO_ARGS, "Stock < 10 (index SAI)", supports_cl=True),
    "A23": Spec(a23_computers, NO_ARGS, "PC RAM >= 16 Go et < 8000 MAD (3 index SAI)", supports_cl=True),
    "A24": Spec(a24_inactive_customers, NO_ARGS, "Clients inactifs depuis 90 j (index SAI)", supports_cl=True),
    "A21_NOIDX": Spec(a21_low_stock, NO_INDEX, "Stock < 10 (scan + ALLOW FILTERING)"),
    "A23_NOIDX": Spec(a23_computers, NO_INDEX, "PC RAM >= 16 Go et < 8000 MAD (scan + ALLOW FILTERING)"),
    "A24_NOIDX": Spec(a24_inactive_customers, NO_INDEX, "Clients inactifs 90 j (scan + ALLOW FILTERING)"),
})
