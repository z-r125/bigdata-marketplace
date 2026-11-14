"""
Démonstration de T1-T15 sur les données chargées (résultat + temps de chaque requête).
Usage : make tx
"""
import time
from decimal import Decimal
from uuid import uuid4

from app.db import execute, now_utc, shutdown
from app.transactional import queries as q


def step(label, fn, *args, show=lambda r: r, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    ms = (time.perf_counter() - t0) * 1000
    print(f"{label:<42}{ms:>9.1f} ms | {show(result)}")
    return result


def fake_products(n, seller_id, status):
    """Lot de test pour T2, rangé dans une catégorie isolée pour ne pas polluer les vraies."""
    now = now_utc()
    return [{"product_id": uuid4(), "sku": f"IMP-{i:05d}", "name": f"Produit importé {i}",
             "category": "Import-Test", "family": "maison", "brand": "Import", "seller_id": seller_id,
             "price": Decimal("99.90") + i % 50, "stock": 100, "tags": {"import"},
             "attributes": {"lot": "demo"}, "ram_gb": None, "status": status, "created_at": now}
            for i in range(n)]


def main():
    print("=== Échantillons tirés des données chargées ===")
    prods = list(execute("SELECT product_id, name, stock FROM products WHERE stock > 50 LIMIT 2"))
    p1, p2 = prods[0].product_id, prods[1].product_id
    cust = execute("SELECT customer_id FROM orders_by_customer LIMIT 1").one().customer_id
    review = execute("SELECT review_id FROM review_helpful LIMIT 1").one().review_id
    print(f"produits={p1}, {p2} | client={cust}\n")
    print(f"{'Requête':<42}{'Temps':>12} | Résultat")
    print("-" * 110)

    step("T3  Fiche produit", q.t03_get_product, p1,
         show=lambda p: f"{p.name} | prix={p.price} | stock={p.stock} | attributs={dict(p.attributes)}")
    step("T4  Commandes du client (récentes d'abord)", q.t04_customer_orders, cust,
         show=lambda r: f"{len(r)} commandes, la plus récente : {r[0].order_date:%Y-%m-%d} ({r[0].status})")
    step("T5  Ajout au panier", q.t05_add_to_cart, cust, p1, 2, show=lambda i: f"{len(i)} article(s) au panier")
    step("T5  Ajout au panier (2e produit)", q.t05_add_to_cart, cust, p2, 1, show=lambda i: f"{len(i)} article(s)")
    step("T6  Retrait du panier", q.t06_remove_from_cart, cust, p2, show=lambda i: f"{len(i)} article(s) restant(s)")
    step("T7  TTL restant du panier", q.t07_cart_ttl, cust,
         show=lambda s: f"{s} s avant expiration (~{s / 86400:.1f} jours)")
    step("T7  Démo expiration (TTL 5 s)", q.t07_demo_expiration, 5,
         show=lambda r: f"présent avant={r[0]}, présent après 6 s={r[1]}")
    stock_before = q.t03_get_product(p2).stock
    step("T9  Décrément de stock (LWT)", q.t09_decrement_stock, p2, 1,
         show=lambda r: f"stock {stock_before} -> {r[0]} (tentative n°{r[1]})")
    order_id = step("T1  Commande (T9 x2 + BATCH LOGGED)", q.place_order, cust, [(p1, 1), (p2, 2)],
                    show=lambda oid: f"commande {oid} créée")
    step("T4  Vérification : nouvelle commande en tête", q.t04_customer_orders, cust,
         show=lambda r: f"1re = {r[0].order_id == order_id} | statut={r[0].status} | total={r[0].total}")
    step("T8  Statut PENDING -> SHIPPED", q.t08_update_order_status, order_id, "SHIPPED",
         show=lambda old: f"ancien statut={old}")
    step("T10 Publier un avis", q.t10_publish_review, p1, cust, 5, "Démo : excellent produit",
         show=lambda rid: f"avis {rid}")
    step("T11 Vote « utile »", q.t11_vote_helpful, review, show=lambda n: f"compteur = {n}")
    step("T12 Ajouter une adresse", q.t12_add_address, cust,
         {"label": "démo", "street": "1 rue de la Démo", "city": "Rabat", "postal_code": "10000"},
         show=lambda n: f"{n} adresse(s)")
    rows1, state = step("T13 Catégorie triée par prix (page 1)", q.t13_products_by_category, "Ordinateurs",
                        show=lambda r: f"{len(r[0])} produits, {r[0][0].price} -> {r[0][-1].price} MAD")
    step("T13 Page 2 (paging_state)", q.t13_products_by_category, "Ordinateurs", paging_state=state,
         show=lambda r: f"{len(r[0])} produits, {r[0][0].price} -> {r[0][-1].price} MAD")
    step("T15 Commandes PENDING à Casablanca", q.t15_pending_orders_in_city, "Casablanca",
         show=lambda r: f"{len(r)} commandes en attente")

    seller = uuid4()
    batch_products = fake_products(5000, seller, "RETIRED")
    step("T2  Import de 5 000 produits", q.t02_import_products, batch_products, show=lambda n: f"{n} importés")
    step("T14 Suppression des produits retirés", q.t14_delete_retired_products, seller,
         show=lambda n: f"{n} supprimés")
    left = execute("SELECT COUNT(*) FROM products_by_seller WHERE seller_id = ?", (seller,)).one()[0]
    print(f"{'    Contrôle après T14':<42}{'':>12} | {left} produit(s) restant(s) pour ce vendeur")
    shutdown()


if __name__ == "__main__":
    main()