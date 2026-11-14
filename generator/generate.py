#!/usr/bin/env python3
"""
Générateur de données - Mini-projet Big Data (Marketplace, Cassandra + Spark)

Produit des fichiers JSON Lines (un objet JSON par ligne) dans --out (par défaut data/) :
  customers, sellers, products, orders, reviews, views, carts, cart_events + manifest.json

Déterministe : même --seed et même --ref-date => exactement les mêmes données
sur les 3 machines (comparer le fichier manifest.json).

Usage :
  python generator/generate.py --seed 42 --ref-date 2026-11-30
  python generator/generate.py --scale 0.05 --out /tmp/data_test   # petit jeu de test
"""
from __future__ import annotations

import argparse
import json
import math
import random
import time as chrono
import unicodedata
import uuid
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path

from faker import Faker

# =============================================================================
# 1. PARAMÈTRES
# =============================================================================
BASE_VOLUMES = {
    "customers": 10_000,
    "sellers": 200,
    "products": 8_000,
    "orders": 30_000,
    "reviews": 20_000,
    "views": 120_000,  # > 30 000 exigées : nécessaire pour un entonnoir (S28) réaliste
    "carts": 3_000,
}

CITIES = {"Casablanca": 25, "Rabat": 12, "Marrakech": 10, "Fès": 9, "Tanger": 9, "Agadir": 7,
          "Meknès": 6, "Oujda": 5, "Kénitra": 5, "Tétouan": 4, "El Jadida": 4, "Safi": 4}
SEGMENTS = {"standard": 70, "premium": 22, "vip": 8}
SEGMENT_ACTIVITY = {"standard": 1.0, "premium": 2.0, "vip": 4.0}
PAYMENTS = {"carte": 45, "cash_livraison": 30, "paypal": 15, "virement": 10}

# catégorie : (code SKU, famille, poids dans le catalogue, taux d'annulation, marques)
CATEGORIES = {
    "Ordinateurs": ("ORD", "electronique", 10, 0.04, ["Lenovo", "HP", "Dell", "Asus", "Acer", "Apple", "MSI"]),
    "Smartphones": ("SMA", "electronique", 12, 0.05, ["Samsung", "Apple", "Xiaomi", "Oppo", "Huawei", "Realme"]),
    "TV & Audio": ("TVA", "electronique", 8, 0.05, ["Samsung", "LG", "Sony", "TCL", "JBL", "Bose"]),
    "Vêtements": ("VET", "mode", 20, 0.11, ["Zara", "H&M", "LC Waikiki", "Mango", "Defacto", "Nike"]),
    "Chaussures": ("CHA", "mode", 12, 0.09, ["Nike", "Adidas", "Puma", "Skechers", "Geox"]),
    "Meubles": ("MEU", "maison", 10, 0.07, ["Kitea", "Ikea", "Mobilia", "Atlas Home"]),
    "Cuisine": ("CUI", "maison", 14, 0.06, ["Moulinex", "Tefal", "Bosch", "Philips", "Kenwood"]),
    "Décoration": ("DEC", "maison", 14, 0.06, ["Maisons du Monde", "Kitea", "Zara Home", "Artisanat Fès"]),
}
FAMILY_TAGS = {
    "electronique": ["high-tech", "garantie-2-ans", "gaming", "bureautique", "sans-fil"],
    "mode": ["coton", "été", "hiver", "unisexe", "tendance"],
    "maison": ["fait-main", "écologique", "design", "compact", "artisanat"],
}
COMMON_TAGS = ["promo", "nouveauté", "best-seller", "livraison-gratuite"]
COLORS = ["noir", "blanc", "bleu", "rouge", "vert", "beige", "gris", "marron"]
COMMENTS = {
    1: ["Très déçu, produit non conforme.", "Qualité médiocre, je ne recommande pas.", "Arrivé endommagé."],
    2: ["Pas à la hauteur de la description.", "Livraison très lente.", "Qualité moyenne pour le prix."],
    3: ["Correct sans plus.", "Fait le travail, rien d'exceptionnel.", "Bon produit mais emballage abîmé."],
    4: ["Bon rapport qualité/prix.", "Satisfait, livraison rapide.", "Conforme à la description."],
    5: ["Excellent produit, je recommande !", "Parfait, rien à redire.", "Très bonne qualité, vendeur sérieux."],
}


# =============================================================================
# 2. OUTILS
# =============================================================================
def weighted(rnd: random.Random, mapping: dict):
    return rnd.choices(list(mapping), weights=list(mapping.values()))[0]


def log_uniform(rnd: random.Random, lo: float, hi: float) -> float:
    return math.exp(rnd.uniform(math.log(lo), math.log(hi)))


def slug(text: str) -> str:
    t = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return "".join(ch for ch in t if ch.isalnum())


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def cumulative(weights):
    acc, out = 0.0, []
    for w in weights:
        acc += w
        out.append(acc)
    return out


# =============================================================================
# 3. GÉNÉRATEUR
# =============================================================================
class MarketplaceGenerator:
    def __init__(self, seed: int, ref_date: date, scale: float):
        self.seed, self.ref_date, self.scale = seed, ref_date, scale
        self.rnd = random.Random(seed)
        self.fake = Faker("fr_FR")
        self.fake.seed_instance(seed)
        self.ref = datetime.combine(ref_date, time(23, 59, 59))
        self.vol = {k: max(1, int(v * scale)) for k, v in BASE_VOLUMES.items()}
        self.data: dict[str, list] = {}
        self.cart_events: list = []

    # ---------------------------------------------------------------- helpers
    def uid(self) -> str:
        return str(uuid.UUID(int=self.rnd.getrandbits(128), version=4))

    def rand_dt(self, start: datetime, end: datetime, recent_bias: float = 1.0) -> datetime:
        """Date aléatoire entre start et end ; recent_bias > 1 favorise les dates récentes."""
        if end <= start:
            return end
        u = self.rnd.random() ** (1 / recent_bias)
        return start + timedelta(seconds=(end - start).total_seconds() * u)

    def clamp(self, dt: datetime) -> datetime:
        return min(dt, self.ref)

    def address(self, label: str, city: str) -> dict:
        return {"label": label, "street": self.fake.street_address(), "city": city,
                "postal_code": f"{self.rnd.randint(10, 93)}{self.rnd.randint(0, 999):03d}",
                "country": "Maroc"}

    # ---------------------------------------------------------------- clients
    def gen_customers(self):
        rnd, fake = self.rnd, self.fake
        cities = list(CITIES)
        customers = []
        for i in range(self.vol["customers"]):
            first, last = fake.first_name(), fake.last_name()
            city = weighted(rnd, CITIES)
            signup = self.rand_dt(self.ref - timedelta(days=3 * 365), self.ref - timedelta(days=1), 1.2)
            addresses = [self.address("domicile", city)]
            n_extra = rnd.choices([0, 1, 2], weights=[60, 30, 10])[0]
            for label in rnd.sample(["travail", "famille", "résidence secondaire"], k=n_extra):
                addresses.append(self.address(label, city if rnd.random() < 0.6 else rnd.choice(cities)))
            segment = weighted(rnd, SEGMENTS)
            customers.append({
                "customer_id": self.uid(),
                "name": f"{first} {last}",
                "email": f"{slug(first)}.{slug(last)}{i}@"
                         f"{rnd.choice(['gmail.com', 'outlook.fr', 'yahoo.fr', 'menara.ma'])}",
                "city": city,
                "signup_date": signup.date().isoformat(),
                "segment": segment,
                "addresses": addresses,
                "preferences": {
                    "langue": rnd.choice(["fr", "fr", "ar", "en"]),
                    "newsletter": rnd.choice(["oui", "non"]),
                    "categorie_favorite": rnd.choice(list(CATEGORIES)),
                    "livraison": rnd.choice(["domicile", "point_relais"]),
                },
                "_signup_dt": signup,
                # 12 % de clients qui ne commandent jamais (utile pour A24)
                "_activity": 0.0 if rnd.random() < 0.12
                else SEGMENT_ACTIVITY[segment] * rnd.lognormvariate(0, 0.6),
            })
        self.data["customers"] = customers

    # ---------------------------------------------------------------- vendeurs
    def gen_sellers(self):
        self.data["sellers"] = [{
            "seller_id": self.uid(),
            "name": self.fake.company(),
            "city": weighted(self.rnd, CITIES),
            "since": self.rand_dt(self.ref - timedelta(days=5 * 365),
                                  self.ref - timedelta(days=60)).date().isoformat(),
        } for _ in range(self.vol["sellers"])]

    # ---------------------------------------------------------------- produits
    def spec(self, cat: str, brand: str):
        """Retourne (nom, prix MAD, attributs variables {str: str}, ram_gb ou None)."""
        rnd = self.rnd
        if cat == "Ordinateurs":
            ram = rnd.choice([4, 8, 8, 16, 16, 16, 32, 64])
            sto = rnd.choice([256, 512, 512, 1024, 2048])
            cpu = rnd.choice(["Intel i5", "Intel i7", "Ryzen 5", "Ryzen 7", "Apple M3"])
            price = 1800 + ram * 220 + sto * 1.2 + rnd.uniform(-600, 2500)
            attrs = {"ram_gb": str(ram), "stockage_go": str(sto), "cpu": cpu,
                     "ecran_pouces": rnd.choice(["13.3", "14", "15.6", "16", "17.3"])}
            return f"PC portable {brand} {cpu} {ram}Go/{sto}Go", price, attrs, ram
        if cat == "Smartphones":
            ram = rnd.choice([4, 6, 8, 8, 12])
            sto = rnd.choice([64, 128, 128, 256, 512])
            color = rnd.choice(COLORS[:5])
            price = 500 + ram * 450 + sto * 7 + rnd.uniform(-300, 1500)
            attrs = {"ram_gb": str(ram), "stockage_go": str(sto), "couleur": color}
            return f"Smartphone {brand} {sto}Go {color}", price, attrs, ram
        if cat == "TV & Audio":
            if rnd.random() < 0.5:
                size = rnd.choice([32, 43, 50, 55, 65, 75])
                reso = "Full HD" if size <= 43 else rnd.choice(["4K UHD", "4K UHD", "8K"])
                price = size * 95 + (3000 if reso == "8K" else 0) + rnd.uniform(-500, 1500)
                attrs = {"type": "TV", "taille_pouces": str(size), "resolution": reso}
                return f"TV {brand} {size} pouces {reso}", price, attrs, None
            kind = rnd.choice(["casque", "enceinte", "barre de son", "écouteurs"])
            conn = rnd.choice(["Bluetooth", "filaire", "Wi-Fi"])
            attrs = {"type": kind, "connectivite": conn}
            return f"{kind.capitalize()} {brand} {conn}", log_uniform(rnd, 150, 4500), attrs, None
        if cat == "Vêtements":
            item = rnd.choice(["T-shirt", "Jean", "Veste", "Robe", "Chemise", "Pull", "Jogging"])
            color = rnd.choice(COLORS)
            attrs = {"taille": rnd.choice(["XS", "S", "M", "L", "XL", "XXL"]), "couleur": color,
                     "matiere": rnd.choice(["coton", "lin", "polyester", "laine", "denim"])}
            return f"{item} {brand} {color}", log_uniform(rnd, 60, 1500), attrs, None
        if cat == "Chaussures":
            item = rnd.choice(["Baskets", "Sandales", "Bottines", "Mocassins", "Chaussures de sport"])
            color = rnd.choice(COLORS)
            attrs = {"pointure": str(rnd.randint(36, 46)), "couleur": color}
            return f"{item} {brand} {color}", log_uniform(rnd, 150, 2500), attrs, None
        if cat == "Meubles":
            item = rnd.choice(["Canapé", "Table", "Chaise", "Armoire", "Lit", "Étagère"])
            mat = rnd.choice(["bois massif", "MDF", "métal", "verre", "rotin"])
            attrs = {"materiau": mat, "dimensions": f"{rnd.randint(40, 220)}x{rnd.randint(40, 200)} cm"}
            return f"{item} {brand} {mat}", log_uniform(rnd, 300, 15000), attrs, None
        if cat == "Cuisine":
            item = rnd.choice(["Robot pâtissier", "Mixeur", "Cocotte", "Poêle", "Cafetière", "Bouilloire"])
            attrs = {"capacite_l": str(rnd.choice([0.5, 1, 1.5, 2, 3, 5]))}
            if item in ("Robot pâtissier", "Mixeur", "Cafetière", "Bouilloire"):
                attrs["puissance_w"] = str(rnd.choice([600, 800, 1000, 1200, 1500, 2000]))
            return f"{item} {brand}", log_uniform(rnd, 80, 6000), attrs, None
        # Décoration
        item = rnd.choice(["Vase", "Tapis", "Miroir", "Lampe", "Coussin", "Tableau", "Bougeoir"])
        style = rnd.choice(["berbère", "moderne", "bohème", "scandinave", "classique"])
        attrs = {"style": style, "couleur": rnd.choice(COLORS)}
        return f"{item} {style} {brand}", log_uniform(rnd, 40, 3000), attrs, None

    def gen_products(self):
        rnd = self.rnd
        cats = list(CATEGORIES)
        cat_weights = [CATEGORIES[c][2] for c in cats]
        sellers = [s["seller_id"] for s in self.data["sellers"]]
        products = []
        for i in range(self.vol["products"]):
            cat = rnd.choices(cats, weights=cat_weights)[0]
            code, family, _, _, brands = CATEGORIES[cat]
            brand = rnd.choice(brands)
            name, price, attrs, ram = self.spec(cat, brand)
            tags = set(rnd.sample(FAMILY_TAGS[family], k=rnd.randint(1, 2)))
            if rnd.random() < 0.4:
                tags.add(rnd.choice(COMMON_TAGS))
            products.append({
                "product_id": self.uid(),
                "sku": f"{code}-{i:05d}",
                "name": name,
                "category": cat,
                "family": family,
                "brand": brand,
                "seller_id": rnd.choice(sellers),
                "price": round(max(price, 20), 2),
                "stock": rnd.randint(0, 9) if rnd.random() < 0.06 else rnd.randint(10, 500),
                "tags": sorted(tags),
                "attributes": attrs,
                "ram_gb": ram,
                "status": "RETIRED" if rnd.random() < 0.03 else "ACTIVE",
                "created_at": iso(self.rand_dt(self.ref - timedelta(days=3 * 365),
                                               self.ref - timedelta(days=30))),
                "_quality": rnd.choice([-1, 0, 0, 0, 1]),
            })
        # Popularité type Zipf : quelques best-sellers + longue traîne (A17, S27)
        ranks = list(range(len(products)))
        rnd.shuffle(ranks)
        weights = [0.0] * len(products)
        for rank, idx in enumerate(ranks):
            weights[idx] = 1 / (rank + 1) ** 0.75
        self.prod_cum = cumulative(weights)
        self.data["products"] = products
        # "Lots" souvent achetés ensemble, pris parmi les 20 % les plus populaires (FP-Growth, S27)
        top = sorted(range(len(products)), key=lambda j: -weights[j])[: max(10, len(products) // 5)]
        self.bundles = []
        for _ in range(max(5, int(60 * self.scale))):
            fam = rnd.choice(["electronique", "mode", "maison"])
            pool = [products[j] for j in top if products[j]["family"] == fam]
            if len(pool) >= 3:
                self.bundles.append(rnd.sample(pool, k=rnd.choice([2, 2, 3])))

    # ---------------------------------------------------------------- commandes
    def order_status(self, odt: datetime, main_cat: str) -> str:
        rnd = self.rnd
        age = (self.ref - odt).days
        if rnd.random() < CATEGORIES[main_cat][3]:
            return "CANCELLED"
        if age <= 3:
            return "PENDING" if rnd.random() < 0.75 else "SHIPPED"
        if age <= 10:
            return rnd.choices(["PENDING", "SHIPPED", "DELIVERED"], weights=[10, 45, 45])[0]
        return "DELIVERED"

    def gen_orders(self):
        rnd = self.rnd
        customers, products = self.data["customers"], self.data["products"]
        window_start = self.ref - timedelta(days=365)
        buyers = rnd.choices(customers, cum_weights=cumulative(c["_activity"] for c in customers),
                             k=self.vol["orders"])
        orders = []
        for c in buyers:
            odt = self.rand_dt(max(c["_signup_dt"], window_start), self.ref, recent_bias=1.4)
            items = list(rnd.choice(self.bundles)) if self.bundles and rnd.random() < 0.3 else []
            k = (rnd.choices([0, 1, 2, 3], weights=[30, 40, 20, 10])[0] if items
                 else rnd.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0])
            items += rnd.choices(products, cum_weights=self.prod_cum, k=k)
            seen, lines = set(), []
            for p in items:
                if p["product_id"] in seen:
                    continue
                seen.add(p["product_id"])
                qmax = 3 if p["family"] == "mode" else 2
                qty = rnd.choices([1, 2, 3][:qmax], weights=[80, 15, 5][:qmax])[0]
                unit = round(p["price"] * (0.9 if rnd.random() < 0.12 else 1.0), 2)
                lines.append({"product_id": p["product_id"], "sku": p["sku"], "name": p["name"],
                              "category": p["category"], "quantity": qty, "unit_price": unit})
            main_cat = max(lines, key=lambda l: l["unit_price"] * l["quantity"])["category"]
            addr = rnd.choice(c["addresses"])
            orders.append({
                "order_id": self.uid(),
                "customer_id": c["customer_id"],
                "order_date": iso(odt),
                "lines": lines,
                "shipping_address": addr,
                "city": addr["city"],
                "payment_method": weighted(rnd, PAYMENTS),
                "status": self.order_status(odt, main_cat),
                "total": round(sum(l["unit_price"] * l["quantity"] for l in lines), 2),
            })
        orders.sort(key=lambda o: o["order_date"])
        self.data["orders"] = orders
        last = {}
        for o in orders:
            last[o["customer_id"]] = o["order_date"]
        for c in customers:  # A24 : date de la dernière commande (ou d'inscription si aucune)
            c["last_order_at"] = last.get(c["customer_id"], iso(c["_signup_dt"]))

    # ---------------------------------------------------------------- avis
    def gen_reviews(self):
        rnd = self.rnd
        prod = {p["product_id"]: p for p in self.data["products"]}
        pairs = {}
        for o in self.data["orders"]:
            if o["status"] == "DELIVERED":
                for l in o["lines"]:
                    pairs.setdefault((o["customer_id"], l["product_id"]), o["order_date"])
        candidates = sorted(pairs.items())
        chosen = rnd.sample(candidates, k=min(self.vol["reviews"], len(candidates)))
        reviews = []
        for (cid, pid), od in chosen:
            base = rnd.choices([1, 2, 3, 4, 5], weights=[4, 6, 15, 35, 40])[0]
            rating = min(5, max(1, base + prod[pid]["_quality"]))
            created = self.clamp(datetime.fromisoformat(od) + timedelta(days=rnd.uniform(2, 20)))
            reviews.append({
                "review_id": self.uid(), "product_id": pid, "customer_id": cid, "rating": rating,
                "comment": rnd.choice(COMMENTS[rating]), "created_at": iso(created),
                "helpful_votes": min(200, int(rnd.paretovariate(1.5)) - 1),
            })
        self.data["reviews"] = reviews

    # ---------------------------------------------------------------- vues + événements panier
    def cart_event(self, cid, pid, dt, action, qty=1):
        self.cart_events.append({"customer_id": cid, "product_id": pid, "event_time": iso(self.clamp(dt)),
                                 "action": action, "quantity": qty})

    def gen_views_and_cart_events(self):
        rnd = self.rnd
        customers, products = self.data["customers"], self.data["products"]
        views = []
        # 1) Parcours d'achat : vue + ajout au panier avant chaque ligne de commande
        for o in self.data["orders"]:
            odt = datetime.fromisoformat(o["order_date"])
            for l in o["lines"]:
                if rnd.random() < 0.9:
                    vdt = odt - timedelta(minutes=rnd.uniform(30, 3 * 24 * 60))
                    views.append({"customer_id": o["customer_id"], "product_id": l["product_id"],
                                  "viewed_at": iso(vdt)})
                if rnd.random() < 0.9:
                    self.cart_event(o["customer_id"], l["product_id"],
                                    odt - timedelta(minutes=rnd.uniform(1, 25)), "ADD", l["quantity"])
        # 2) Navigation sans achat, dont une partie finit en panier abandonné
        start = self.ref - timedelta(days=365)
        n_extra = max(0, self.vol["views"] - len(views))
        for p in rnd.choices(products, cum_weights=self.prod_cum, k=n_extra):
            c = rnd.choice(customers)
            vdt = self.rand_dt(max(start, c["_signup_dt"]), self.ref, 1.4)
            views.append({"customer_id": c["customer_id"], "product_id": p["product_id"],
                          "viewed_at": iso(vdt)})
            if rnd.random() < 0.25:
                adt = vdt + timedelta(minutes=rnd.uniform(1, 120))
                self.cart_event(c["customer_id"], p["product_id"], adt, "ADD")
                if rnd.random() < 0.3:
                    self.cart_event(c["customer_id"], p["product_id"],
                                    adt + timedelta(minutes=rnd.uniform(1, 600)), "REMOVE")
        views.sort(key=lambda v: v["viewed_at"])
        self.data["views"] = views

    # ---------------------------------------------------------------- paniers en cours
    def gen_carts(self):
        rnd = self.rnd
        products = self.data["products"]
        owners = rnd.sample(self.data["customers"], k=min(self.vol["carts"], len(self.data["customers"])))
        carts = []
        for c in owners:
            items = {}
            for p in rnd.choices(products, cum_weights=self.prod_cum, k=rnd.randint(1, 5)):
                added = self.rand_dt(self.ref - timedelta(days=6), self.ref)
                qty = rnd.choices([1, 2, 3], weights=[75, 20, 5])[0]
                items[p["product_id"]] = {"quantity": qty, "unit_price": p["price"], "added_at": iso(added)}
                self.cart_event(c["customer_id"], p["product_id"], added, "ADD", qty)
            carts.append({"customer_id": c["customer_id"], "items": items,
                          "updated_at": max(i["added_at"] for i in items.values())})
        self.data["carts"] = carts
        self.data["cart_events"] = sorted(self.cart_events, key=lambda e: e["event_time"])

    # ---------------------------------------------------------------- écriture + contrôles
    def write(self, out: Path):
        out.mkdir(parents=True, exist_ok=True)
        for name, rows in self.data.items():
            with open(out / f"{name}.jsonl", "w", encoding="utf-8") as f:
                for r in rows:
                    clean = {k: v for k, v in r.items() if not k.startswith("_")}
                    f.write(json.dumps(clean, ensure_ascii=False) + "\n")

    def report(self, out: Path, elapsed: float):
        d = self.data
        counts = {k: len(v) for k, v in d.items()}
        enonce = sum(counts[k] for k in ["customers", "products", "orders", "reviews", "views", "carts"])

        # Intégrité référentielle
        cids = {c["customer_id"] for c in d["customers"]}
        pids = {p["product_id"] for p in d["products"]}
        assert all(o["customer_id"] in cids for o in d["orders"]), "commande -> client inconnu"
        assert all(l["product_id"] in pids for o in d["orders"] for l in o["lines"]), "ligne -> produit inconnu"
        assert all(r["product_id"] in pids and r["customer_id"] in cids for r in d["reviews"]), "avis invalide"
        assert all(v["product_id"] in pids and v["customer_id"] in cids for v in d["views"]), "vue invalide"

        statuses = Counter(o["status"] for o in d["orders"])
        n_lines = sum(len(o["lines"]) for o in d["orders"])
        revenue = round(sum(o["total"] for o in d["orders"] if o["status"] != "CANCELLED"), 2)
        limit_90 = iso(self.ref - timedelta(days=90))
        checks = {
            "A21_stock_inf_10": sum(1 for p in d["products"] if p["stock"] < 10),
            "A23_pc_ram16_prix_inf_8000": sum(1 for p in d["products"] if p["category"] == "Ordinateurs"
                                             and (p["ram_gb"] or 0) >= 16 and p["price"] < 8000),
            "A24_clients_inactifs_90j": sum(1 for c in d["customers"] if c["last_order_at"] < limit_90),
            "T14_produits_retires": sum(1 for p in d["products"] if p["status"] == "RETIRED"),
            "T15_commandes_pending": statuses.get("PENDING", 0),
        }
        manifest = {"seed": self.seed, "ref_date": self.ref_date.isoformat(), "scale": self.scale,
                    "counts": counts, "total_elements_enonce": enonce, "order_lines": n_lines,
                    "statuses": dict(sorted(statuses.items())), "checks": checks,
                    "revenue_hors_annulations_mad": revenue}
        (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"\n=== Génération terminée en {elapsed:.1f} s (seed={self.seed}, ref={self.ref_date}) ===")
        for k, v in counts.items():
            print(f"  {k:<12} {v:>8,}".replace(",", " "))
        ok = "OK" if enonce >= 50_000 else "INSUFFISANT"
        print(f"  Total exigé par l'énoncé : {enonce:,} (>= 50 000 : {ok})".replace(",", " "))
        print(f"  Lignes de commande : {n_lines:,}".replace(",", " "))
        print(f"  Statuts : {dict(sorted(statuses.items()))}")
        print(f"  Contrôles requêtes : {checks}")
        print(f"  Intégrité référentielle : OK")
        print(f"  CA hors annulations : {revenue:,.2f} MAD  <- doit être identique sur les 3 machines"
              .replace(",", " "))
        print(f"  Fichiers écrits dans : {out.resolve()}")


# =============================================================================
# 4. POINT D'ENTRÉE
# =============================================================================
def main():
    ap = argparse.ArgumentParser(description="Générateur de données de la marketplace")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ref-date", default="2026-11-30", help="date de fin de la période simulée")
    ap.add_argument("--out", default="data")
    ap.add_argument("--scale", type=float, default=1.0, help="facteur de volume (0.05 = test rapide)")
    args = ap.parse_args()

    t0 = chrono.perf_counter()
    g = MarketplaceGenerator(args.seed, date.fromisoformat(args.ref_date), args.scale)
    for step in (g.gen_customers, g.gen_sellers, g.gen_products, g.gen_orders,
                 g.gen_reviews, g.gen_views_and_cart_events, g.gen_carts):
        print(f"-> {step.__name__}")
        step()
    out = Path(args.out)
    g.write(out)
    g.report(out, chrono.perf_counter() - t0)


if __name__ == "__main__":
    main()