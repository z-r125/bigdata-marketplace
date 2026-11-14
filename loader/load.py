#!/usr/bin/env python3
"""
Loader - Mini-projet Big Data (Marketplace, Cassandra)

Charge les fichiers data/*.jsonl (produits par generator/generate.py) dans les
15 tables du keyspace `marketplace`, avec des requêtes préparées exécutées en
parallèle (execute_concurrent_with_args).

Le chargement commence par vider les tables : il est donc rejouable
(indispensable à cause du compteur review_helpful, non idempotent).

Usage (toujours dans le conteneur app) :  make load
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path
from uuid import UUID

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import UNSET_VALUE

TABLES = [
    "customers", "products", "products_by_category_price", "products_by_seller",
    "orders", "orders_by_customer", "orders_by_city", "orders_by_month",
    "lines_by_category", "lines_by_product", "carts", "cart_events",
    "product_views", "reviews_by_product", "review_helpful",
]


# =============================================================================
# Conversions JSON -> types Cassandra
# =============================================================================
def read_jsonl(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def U(s: str) -> UUID:
    return UUID(s)


def TS(s: str) -> datetime:
    return datetime.fromisoformat(s)


def D(x) -> Decimal:
    return Decimal(str(round(float(x), 2)))


def unset_if_none(v):
    """Une colonne absente est envoyée en UNSET (pas de tombstone), et non en NULL."""
    return UNSET_VALUE if v is None else v


# Les UDT sont passés sous forme de tuples, dans l'ordre des champs du CREATE TYPE
def udt_address(a: dict) -> tuple:
    return (a["label"], a["street"], a["city"], a["postal_code"], a["country"])


def udt_order_line(l: dict) -> tuple:
    return (U(l["product_id"]), l["sku"], l["name"], l["category"], l["quantity"], D(l["unit_price"]))


def udt_cart_item(i: dict) -> tuple:
    return (i["quantity"], D(i["unit_price"]), TS(i["added_at"]))


# =============================================================================
# Loader
# =============================================================================
class Loader:
    def __init__(self, hosts: list[str], data_dir: Path, concurrency: int):
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="dc1")),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=60,
        )
        self.cluster = Cluster(hosts, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.session = self.cluster.connect("marketplace")
        self.data = data_dir
        self.concurrency = concurrency
        self.writes: dict[str, int] = {}

    # ------------------------------------------------------------- moteur
    def run(self, table: str, cql: str, rows, chunk_size: int = 20_000):
        """Exécute `cql` (préparée) pour chaque tuple de `rows`, par paquets, en parallèle."""
        stmt = self.session.prepare(cql)
        it, n, t0 = iter(rows), 0, time.perf_counter()
        while True:
            chunk = list(islice(it, chunk_size))
            if not chunk:
                break
            execute_concurrent_with_args(self.session, stmt, chunk,
                                         concurrency=self.concurrency, raise_on_first_error=True)
            n += len(chunk)
            print(f"\r  {table:<28}{n:>9,} lignes".replace(",", " "), end="", flush=True)
        dt = time.perf_counter() - t0
        rate = n / dt if dt > 0 else 0
        print(f"\r  {table:<28}{n:>9,} lignes  {dt:6.1f} s  ({rate:,.0f} écritures/s)".replace(",", " "))
        self.writes[table] = self.writes.get(table, 0) + n

    def truncate(self):
        print("Vidage des 15 tables (chargement rejouable)...")
        for t in TABLES:
            self.session.execute(f"TRUNCATE {t}", timeout=120)

    # ------------------------------------------------------------- clients
    def load_customers(self):
        rows = (
            (U(c["customer_id"]), c["name"], c["email"], c["city"],
             date.fromisoformat(c["signup_date"]), c["segment"],
             [udt_address(a) for a in c["addresses"]], c["preferences"], TS(c["last_order_at"]))
            for c in read_jsonl(self.data / "customers.jsonl")
        )
        self.run("customers",
                 "INSERT INTO customers (customer_id, name, email, city, signup_date, segment, "
                 "addresses, preferences, last_order_at) VALUES (?,?,?,?,?,?,?,?,?)", rows)

    # ------------------------------------------------------------- produits (3 tables)
    def load_products(self):
        prods = list(read_jsonl(self.data / "products.jsonl"))
        self.run("products",
                 "INSERT INTO products (product_id, sku, name, category, family, brand, seller_id, "
                 "price, stock, tags, attributes, ram_gb, status, created_at) "
                 "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ((U(p["product_id"]), p["sku"], p["name"], p["category"], p["family"], p["brand"],
                   U(p["seller_id"]), D(p["price"]), p["stock"], set(p["tags"]), p["attributes"],
                   unset_if_none(p["ram_gb"]), p["status"], TS(p["created_at"])) for p in prods))
        self.run("products_by_category_price",
                 "INSERT INTO products_by_category_price (category, price, product_id, sku, name, brand, stock) "
                 "VALUES (?,?,?,?,?,?,?)",
                 ((p["category"], D(p["price"]), U(p["product_id"]), p["sku"], p["name"], p["brand"],
                   p["stock"]) for p in prods))
        self.run("products_by_seller",
                 "INSERT INTO products_by_seller (seller_id, product_id, category, price, name, status) "
                 "VALUES (?,?,?,?,?,?)",
                 ((U(p["seller_id"]), U(p["product_id"]), p["category"], D(p["price"]), p["name"],
                   p["status"]) for p in prods))

    # ------------------------------------------------------------- commandes (6 tables)
    def load_orders(self):
        orders = list(read_jsonl(self.data / "orders.jsonl"))
        self.run("orders",
                 "INSERT INTO orders (order_id, customer_id, order_date, lines, shipping_address, city, "
                 "payment_method, status, total) VALUES (?,?,?,?,?,?,?,?,?)",
                 ((U(o["order_id"]), U(o["customer_id"]), TS(o["order_date"]),
                   [udt_order_line(l) for l in o["lines"]], udt_address(o["shipping_address"]),
                   o["city"], o["payment_method"], o["status"], D(o["total"])) for o in orders))
        self.run("orders_by_customer",
                 "INSERT INTO orders_by_customer (customer_id, order_date, order_id, status, total, nb_items) "
                 "VALUES (?,?,?,?,?,?)",
                 ((U(o["customer_id"]), TS(o["order_date"]), U(o["order_id"]), o["status"], D(o["total"]),
                   sum(l["quantity"] for l in o["lines"])) for o in orders))
        self.run("orders_by_city",
                 "INSERT INTO orders_by_city (city, status, order_date, order_id, customer_id, total, "
                 "payment_method) VALUES (?,?,?,?,?,?,?)",
                 ((o["city"], o["status"], TS(o["order_date"]), U(o["order_id"]), U(o["customer_id"]),
                   D(o["total"]), o["payment_method"]) for o in orders))
        self.run("orders_by_month",
                 "INSERT INTO orders_by_month (month, day, order_id, total, payment_method, status) "
                 "VALUES (?,?,?,?,?,?)",
                 ((o["order_date"][:7], TS(o["order_date"]).date(), U(o["order_id"]), D(o["total"]),
                   o["payment_method"], o["status"]) for o in orders))
        self.run("lines_by_category",
                 "INSERT INTO lines_by_category (category, order_id, line_no, order_date, product_id, "
                 "quantity, amount, status) VALUES (?,?,?,?,?,?,?,?)",
                 ((l["category"], U(o["order_id"]), i, TS(o["order_date"]), U(l["product_id"]),
                   l["quantity"], D(l["quantity"] * l["unit_price"]), o["status"])
                  for o in orders for i, l in enumerate(o["lines"])))
        self.run("lines_by_product",
                 "INSERT INTO lines_by_product (product_id, order_id, line_no, category, quantity, amount) "
                 "VALUES (?,?,?,?,?,?)",
                 ((U(l["product_id"]), U(o["order_id"]), i, l["category"], l["quantity"],
                   D(l["quantity"] * l["unit_price"])) for o in orders for i, l in enumerate(o["lines"])))

    # ------------------------------------------------------------- paniers + événements + vues
    def load_carts_events_views(self):
        self.run("carts",
                 "INSERT INTO carts (customer_id, items, updated_at) VALUES (?,?,?)",
                 ((U(c["customer_id"]), {U(pid): udt_cart_item(it) for pid, it in c["items"].items()},
                   TS(c["updated_at"])) for c in read_jsonl(self.data / "carts.jsonl")))
        self.run("cart_events",
                 "INSERT INTO cart_events (customer_id, event_time, product_id, action, quantity) "
                 "VALUES (?,?,?,?,?)",
                 ((U(e["customer_id"]), TS(e["event_time"]), U(e["product_id"]), e["action"], e["quantity"])
                  for e in read_jsonl(self.data / "cart_events.jsonl")))
        self.run("product_views",
                 "INSERT INTO product_views (customer_id, viewed_at, product_id) VALUES (?,?,?)",
                 ((U(v["customer_id"]), TS(v["viewed_at"]), U(v["product_id"]))
                  for v in read_jsonl(self.data / "views.jsonl")))

    # ------------------------------------------------------------- avis + compteur
    def load_reviews(self):
        reviews = list(read_jsonl(self.data / "reviews.jsonl"))
        self.run("reviews_by_product",
                 "INSERT INTO reviews_by_product (product_id, created_at, review_id, customer_id, rating, comment) "
                 "VALUES (?,?,?,?,?,?)",
                 ((U(r["product_id"]), TS(r["created_at"]), U(r["review_id"]), U(r["customer_id"]),
                   r["rating"], r["comment"]) for r in reviews))
        self.run("review_helpful",
                 "UPDATE review_helpful SET helpful = helpful + ? WHERE review_id = ?",
                 ((r["helpful_votes"], U(r["review_id"])) for r in reviews if r["helpful_votes"] > 0))

    # ------------------------------------------------------------- vérification
    def verify(self, manifest: dict):
        print("\nVérification (COUNT dans Cassandra vs manifest du générateur) :")
        expected = manifest["counts"]
        checks = [("customers", "customers"), ("products", "products"),
                  ("orders", "orders"), ("carts", "carts")]
        ok = True
        for table, key in checks:
            n = self.session.execute(f"SELECT COUNT(*) FROM {table}", timeout=120).one()[0]
            status = "OK" if n == expected[key] else "ÉCART"
            ok &= status == "OK"
            print(f"  {table:<12} Cassandra={n:>7}  manifest={expected[key]:>7}  {status}")
        return ok


# =============================================================================
# Point d'entrée
# =============================================================================
def main():
    ap = argparse.ArgumentParser(description="Charge data/*.jsonl dans Cassandra")
    ap.add_argument("--data", default="data")
    ap.add_argument("--hosts", default="cass1,cass2,cass3")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--no-truncate", action="store_true", help="ne pas vider les tables avant chargement")
    args = ap.parse_args()

    data_dir = Path(args.data)
    manifest_path = data_dir / "manifest.json"
    if not manifest_path.exists():
        sys.exit(f"Introuvable : {manifest_path}. Lance d'abord `make gen`.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    t0 = time.perf_counter()
    loader = Loader(args.hosts.split(","), data_dir, args.concurrency)
    try:
        if not args.no_truncate:
            loader.truncate()
        print("Chargement :")
        loader.load_customers()
        loader.load_products()
        loader.load_orders()
        loader.load_carts_events_views()
        loader.load_reviews()
        ok = loader.verify(manifest)
        total = sum(loader.writes.values())
        print(f"\n=== Chargement terminé : {total:,} écritures en {time.perf_counter() - t0:.0f} s "
              f"(seed={manifest['seed']}, ref={manifest['ref_date']}) ===".replace(",", " "))
        print("Résultat :", "OK" if ok else "ÉCARTS DÉTECTÉS - voir ci-dessus")
    finally:
        loader.cluster.shutdown()


if __name__ == "__main__":
    main()