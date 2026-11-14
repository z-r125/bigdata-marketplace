"""
Requêtes transactionnelles T1-T15.

Chaque fonction indique la table utilisée et sa clé :
c'est la justification « requête -> table » demandée par l'énoncé.
"""
from __future__ import annotations

import time
from uuid import uuid4

from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.query import UNSET_VALUE

from app.db import (batch, execute, make_batch, money, now_utc, prepared, session,
                    udt_address, udt_order_line)

ORDER_STATUSES = {"PENDING", "SHIPPED", "DELIVERED", "CANCELLED"}


class InsufficientStock(Exception):
    """Stock insuffisant pour la quantité demandée."""


# =============================================================================
# T1 - Créer une commande avec ses lignes
# =============================================================================
def t01_create_order(customer_id, lines, payment_method="carte", shipping_address=None, cl=None):
    """T1 - Écriture atomique (BATCH LOGGED) dans les 6 tables « commande »
    + mise à jour de customers.last_order_at (utile pour A24).
    lines : [{product_id, sku, name, category, quantity, unit_price}] (prix déjà fixés)."""
    cust = execute("SELECT addresses FROM customers WHERE customer_id = ?", (customer_id,), cl).one()
    if cust is None:
        raise ValueError(f"Client inconnu : {customer_id}")
    if shipping_address is None:
        if not cust.addresses:
            raise ValueError("Aucune adresse de livraison")
        shipping_address = cust.addresses[0]
    addr = udt_address(shipping_address)
    city = addr[2]
    order_id, now, status = uuid4(), now_utc(), "PENDING"
    total = money(sum(money(l["unit_price"]) * l["quantity"] for l in lines))
    nb_items = sum(int(l["quantity"]) for l in lines)

    stmts = [
        ("INSERT INTO orders (order_id, customer_id, order_date, lines, shipping_address, city, "
         "payment_method, status, total) VALUES (?,?,?,?,?,?,?,?,?)",
         (order_id, customer_id, now, [udt_order_line(l) for l in lines], addr, city,
          payment_method, status, total)),
        ("INSERT INTO orders_by_customer (customer_id, order_date, order_id, status, total, nb_items) "
         "VALUES (?,?,?,?,?,?)", (customer_id, now, order_id, status, total, nb_items)),
        ("INSERT INTO orders_by_city (city, status, order_date, order_id, customer_id, total, payment_method) "
         "VALUES (?,?,?,?,?,?,?)", (city, status, now, order_id, customer_id, total, payment_method)),
        ("INSERT INTO orders_by_month (month, day, order_id, total, payment_method, status) "
         "VALUES (?,?,?,?,?,?)", (now.strftime("%Y-%m"), now.date(), order_id, total, payment_method, status)),
        ("UPDATE customers SET last_order_at = ? WHERE customer_id = ?", (now, customer_id)),
    ]
    for i, l in enumerate(lines):
        amount = money(money(l["unit_price"]) * l["quantity"])
        stmts.append(("INSERT INTO lines_by_category (category, order_id, line_no, order_date, product_id, "
                      "quantity, amount, status) VALUES (?,?,?,?,?,?,?,?)",
                      (l["category"], order_id, i, now, l["product_id"], int(l["quantity"]), amount, status)))
        stmts.append(("INSERT INTO lines_by_product (product_id, order_id, line_no, category, quantity, amount) "
                      "VALUES (?,?,?,?,?,?)",
                      (l["product_id"], order_id, i, l["category"], int(l["quantity"]), amount)))
    batch(stmts, cl)
    return order_id


def place_order(customer_id, items, payment_method="carte", cl=None):
    """Parcours d'achat complet : T9 (stock) pour chaque article, puis T1.
    items : [(product_id, quantité)]. En cas d'échec, le stock déjà réservé est restitué."""
    lines, reserved = [], []
    try:
        for product_id, qty in items:
            p = t03_get_product(product_id)
            if p is None:
                raise ValueError(f"Produit inconnu : {product_id}")
            t09_decrement_stock(product_id, qty)
            reserved.append((product_id, qty))
            lines.append({"product_id": product_id, "sku": p.sku, "name": p.name,
                          "category": p.category, "quantity": qty, "unit_price": p.price})
        return t01_create_order(customer_id, lines, payment_method, cl=cl)
    except Exception:
        for product_id, qty in reserved:  # compensation
            t09_decrement_stock(product_id, -qty)
        raise


# =============================================================================
# T2 - Importer des produits en lot
# =============================================================================
def t02_import_products(products, concurrency=64):
    """T2 - Écritures parallèles dans products, products_by_category_price et products_by_seller.
    Pas de gros BATCH : un batch multi-partitions de 5 000 lignes dépasserait la limite (50 Ko)."""
    s = session()
    p_main = prepared("INSERT INTO products (product_id, sku, name, category, family, brand, seller_id, "
                      "price, stock, tags, attributes, ram_gb, status, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    p_cat = prepared("INSERT INTO products_by_category_price (category, price, product_id, sku, name, brand, stock) "
                     "VALUES (?,?,?,?,?,?,?)")
    p_sel = prepared("INSERT INTO products_by_seller (seller_id, product_id, category, price, name, status) "
                     "VALUES (?,?,?,?,?,?)")
    rows_main = [(p["product_id"], p["sku"], p["name"], p["category"], p["family"], p["brand"], p["seller_id"],
                  money(p["price"]), p["stock"], set(p["tags"]), p["attributes"],
                  UNSET_VALUE if p.get("ram_gb") is None else p["ram_gb"], p["status"], p["created_at"])
                 for p in products]
    rows_cat = [(p["category"], money(p["price"]), p["product_id"], p["sku"], p["name"], p["brand"], p["stock"])
                for p in products]
    rows_sel = [(p["seller_id"], p["product_id"], p["category"], money(p["price"]), p["name"], p["status"])
                for p in products]
    for stmt, rows in ((p_main, rows_main), (p_cat, rows_cat), (p_sel, rows_sel)):
        execute_concurrent_with_args(s, stmt, rows, concurrency=concurrency, raise_on_first_error=True)
    return len(products)


# =============================================================================
# T3 - Fiche produit
# =============================================================================
def t03_get_product(product_id, cl=None):
    """T3 - Table products, clé product_id. Attributs variables (map) et tags (set)."""
    return execute("SELECT product_id, sku, name, category, brand, price, stock, tags, attributes, ram_gb, status "
                   "FROM products WHERE product_id = ?", (product_id,), cl).one()


# =============================================================================
# T4 - Commandes d'un client, les plus récentes d'abord
# =============================================================================
def t04_customer_orders(customer_id, limit=20, cl=None):
    """T4 - Table orders_by_customer : partition customer_id, clustering order_date DESC.
    Le tri est fait par le stockage : aucune opération de tri à la lecture."""
    return list(execute("SELECT order_date, order_id, status, total, nb_items FROM orders_by_customer "
                        "WHERE customer_id = ? LIMIT ?", (customer_id, limit), cl))


# =============================================================================
# T5 / T6 / T7 - Panier
# =============================================================================
def _read_cart(customer_id) -> dict:
    row = execute("SELECT items FROM carts WHERE customer_id = ?", (customer_id,)).one()
    return dict(row.items) if row is not None and row.items else {}


def _write_cart(customer_id, items: dict):
    """Réécrit la ligne entière : le TTL de 7 jours repart de zéro (T7)."""
    if items:
        execute("INSERT INTO carts (customer_id, items, updated_at) VALUES (?,?,?)",
                (customer_id, {k: tuple(v) for k, v in items.items()}, now_utc()))
    else:
        execute("DELETE FROM carts WHERE customer_id = ?", (customer_id,))


def _cart_event(customer_id, product_id, action, quantity):
    execute("INSERT INTO cart_events (customer_id, event_time, product_id, action, quantity) "
            "VALUES (?,?,?,?,?)", (customer_id, now_utc(), product_id, action, quantity))


def t05_add_to_cart(customer_id, product_id, quantity=1):
    """T5 - Table carts : partition customer_id, map<uuid, frozen<cart_item>>.
    Trace aussi l'événement dans cart_events (utilisé par l'entonnoir S28)."""
    p = execute("SELECT price FROM products WHERE product_id = ?", (product_id,)).one()
    if p is None:
        raise ValueError(f"Produit inconnu : {product_id}")
    items = _read_cart(customer_id)
    previous = items.get(product_id)
    new_qty = quantity + (previous[0] if previous else 0)
    items[product_id] = (new_qty, p.price, now_utc())
    _write_cart(customer_id, items)
    _cart_event(customer_id, product_id, "ADD", quantity)
    return items


def t06_remove_from_cart(customer_id, product_id):
    """T6 - Retire un article ; un panier vide est supprimé."""
    items = _read_cart(customer_id)
    removed = items.pop(product_id, None)
    if removed is not None:
        _write_cart(customer_id, items)
        _cart_event(customer_id, product_id, "REMOVE", removed[0])
    return items


def t07_cart_ttl(customer_id):
    """T7 - Expiration automatique : default_time_to_live = 604800 s (7 jours) sur carts.
    Renvoie les secondes restantes avant expiration, ou None si le panier a expiré."""
    row = execute("SELECT TTL(updated_at) AS remaining FROM carts WHERE customer_id = ?", (customer_id,)).one()
    return row.remaining if row is not None else None


def t07_demo_expiration(ttl_seconds=5):
    """Démo accélérée de T7 : panier avec un TTL court, relu avant et après expiration."""
    cid = uuid4()
    execute("INSERT INTO carts (customer_id, items, updated_at) VALUES (?,?,?) USING TTL ?",
            (cid, {uuid4(): (1, money(10), now_utc())}, now_utc(), ttl_seconds))
    before = bool(_read_cart(cid))
    time.sleep(ttl_seconds + 1)
    after = bool(_read_cart(cid))
    return before, after


# =============================================================================
# T8 - Mettre à jour le statut d'une commande
# =============================================================================
def t08_update_order_status(order_id, new_status, cl=None):
    """T8 - Le statut est dupliqué dans 5 tables. Dans orders_by_city, il fait partie de la clé :
    DELETE de l'ancienne ligne + INSERT de la nouvelle. Le tout dans un BATCH LOGGED (atomique)."""
    if new_status not in ORDER_STATUSES:
        raise ValueError(f"Statut invalide : {new_status}")
    o = execute("SELECT customer_id, order_date, city, status, total, payment_method, lines "
                "FROM orders WHERE order_id = ?", (order_id,), cl).one()
    if o is None:
        raise ValueError(f"Commande inconnue : {order_id}")
    if o.status == new_status:
        return o.status
    stmts = [
        ("UPDATE orders SET status = ? WHERE order_id = ?", (new_status, order_id)),
        ("UPDATE orders_by_customer SET status = ? WHERE customer_id = ? AND order_date = ? AND order_id = ?",
         (new_status, o.customer_id, o.order_date, order_id)),
        ("DELETE FROM orders_by_city WHERE city = ? AND status = ? AND order_date = ? AND order_id = ?",
         (o.city, o.status, o.order_date, order_id)),
        ("INSERT INTO orders_by_city (city, status, order_date, order_id, customer_id, total, payment_method) "
         "VALUES (?,?,?,?,?,?,?)",
         (o.city, new_status, o.order_date, order_id, o.customer_id, o.total, o.payment_method)),
        ("UPDATE orders_by_month SET status = ? WHERE month = ? AND day = ? AND order_id = ?",
         (new_status, o.order_date.strftime("%Y-%m"), o.order_date.date(), order_id)),
    ]
    for i, line in enumerate(o.lines or []):
        stmts.append(("UPDATE lines_by_category SET status = ? WHERE category = ? AND order_id = ? AND line_no = ?",
                      (new_status, line.category, order_id, i)))
    batch(stmts, cl)
    return o.status


# =============================================================================
# T9 - Décrémenter le stock (sans risque de stock négatif)
# =============================================================================
def t09_decrement_stock(product_id, quantity=1, max_retries=5):
    """T9 - Lightweight Transaction (Paxos) : UPDATE ... IF stock = <valeur lue>.
    Si un autre client a modifié le stock entre-temps, la condition échoue et on recommence.
    quantity négative = restitution du stock (compensation)."""
    for attempt in range(1, max_retries + 1):
        row = execute("SELECT stock, category, price FROM products WHERE product_id = ?",
                      (product_id,), cl="SERIAL").one()
        if row is None:
            raise ValueError(f"Produit inconnu : {product_id}")
        if row.stock < quantity:
            raise InsufficientStock(f"Stock {row.stock} < {quantity} pour {product_id}")
        new_stock = row.stock - quantity
        res = execute("UPDATE products SET stock = ? WHERE product_id = ? IF stock = ?",
                      (new_stock, product_id, row.stock))
        if res.was_applied:
            execute("UPDATE products_by_category_price SET stock = ? "
                    "WHERE category = ? AND price = ? AND product_id = ?",
                    (new_stock, row.category, row.price, product_id))
            return new_stock, attempt
    raise RuntimeError(f"T9 : conflit persistant après {max_retries} tentatives")


# =============================================================================
# T10 / T11 - Avis
# =============================================================================
def t10_publish_review(product_id, customer_id, rating, comment):
    """T10 - Table reviews_by_product : partition product_id, clustering created_at DESC."""
    if not 1 <= int(rating) <= 5:
        raise ValueError("La note doit être comprise entre 1 et 5")
    review_id = uuid4()
    execute("INSERT INTO reviews_by_product (product_id, created_at, review_id, customer_id, rating, comment) "
            "VALUES (?,?,?,?,?,?)", (product_id, now_utc(), review_id, customer_id, int(rating), comment))
    return review_id


def t11_vote_helpful(review_id):
    """T11 - Compteur dans une table dédiée (review_helpful) : incrément sans lecture préalable."""
    execute("UPDATE review_helpful SET helpful = helpful + 1 WHERE review_id = ?", (review_id,))
    row = execute("SELECT helpful FROM review_helpful WHERE review_id = ?", (review_id,)).one()
    return row.helpful if row else 0


# =============================================================================
# T12 - Ajouter une adresse à un client
# =============================================================================
def t12_add_address(customer_id, address):
    """T12 - Ajout en fin de liste d'UDT (addresses = addresses + [...]), sans lecture préalable."""
    execute("UPDATE customers SET addresses = addresses + ? WHERE customer_id = ?",
            ([udt_address(address)], customer_id))
    row = execute("SELECT addresses FROM customers WHERE customer_id = ?", (customer_id,)).one()
    return len(row.addresses or [])


# =============================================================================
# T13 - Produits d'une catégorie triés par prix, 20 par page
# =============================================================================
def t13_products_by_category(category, page_size=20, paging_state=None, cl=None):
    """T13 - Table products_by_category_price : partition category, clustering price ASC.
    Pagination native du driver (fetch_size + paging_state) : pas d'OFFSET en CQL."""
    rs = execute("SELECT price, product_id, name, brand, stock FROM products_by_category_price "
                 "WHERE category = ?", (category,), cl, fetch_size=page_size, paging_state=paging_state)
    return rs.current_rows, rs.paging_state


# =============================================================================
# T14 - Supprimer les produits retirés d'un vendeur
# =============================================================================
def t14_delete_retired_products(seller_id, concurrency=32):
    """T14 - Lecture de la partition products_by_seller, puis, pour chaque produit RETIRED,
    un BATCH LOGGED qui le supprime des 3 tables produit. Les batchs sont exécutés en parallèle."""
    rows = execute("SELECT product_id, category, price, status FROM products_by_seller WHERE seller_id = ?",
                   (seller_id,))
    retired = [r for r in rows if r.status == "RETIRED"]
    batches = [(make_batch([
        ("DELETE FROM products WHERE product_id = ?", (r.product_id,)),
        ("DELETE FROM products_by_category_price WHERE category = ? AND price = ? AND product_id = ?",
         (r.category, r.price, r.product_id)),
        ("DELETE FROM products_by_seller WHERE seller_id = ? AND product_id = ?", (seller_id, r.product_id)),
    ]), None) for r in retired]
    if batches:
        execute_concurrent(session(), batches, concurrency=concurrency, raise_on_first_error=True)
    return len(retired)


# =============================================================================
# T15 - Commandes en attente de livraison dans une ville
# =============================================================================
def t15_pending_orders_in_city(city, limit=100, cl=None):
    """T15 - Table orders_by_city : partition city, clustering (status, order_date DESC).
    Lecture d'une tranche contiguë de la partition (status = 'PENDING')."""
    return list(execute("SELECT order_date, order_id, customer_id, total FROM orders_by_city "
                        "WHERE city = ? AND status = 'PENDING' LIMIT ?", (city, limit), cl))