USE marketplace;

-- ============================ CLIENTS ============================
-- T12 (ajout d'adresse), A24 (index SAI sur last_order_at), source Spark S26/S29
CREATE TABLE IF NOT EXISTS customers (
  customer_id   uuid PRIMARY KEY,
  name          text,
  email         text,
  city          text,
  signup_date   date,
  segment       text,
  addresses     list<frozen<address>>,
  preferences   map<text, text>,
  last_order_at timestamp
);

-- ============================ PRODUITS ============================
-- T2 (import), T3 (fiche), T9 (stock en LWT), A21/A23 (index SAI)
CREATE TABLE IF NOT EXISTS products (
  product_id  uuid PRIMARY KEY,
  sku         text,
  name        text,
  category    text,
  family      text,
  brand       text,
  seller_id   uuid,
  price       decimal,
  stock       int,
  tags        set<text>,
  attributes  map<text, text>,
  ram_gb      int,
  status      text,
  created_at  timestamp
);

-- T13 : produits d'une catégorie triés par prix, paginés par 20
CREATE TABLE IF NOT EXISTS products_by_category_price (
  category    text,
  price       decimal,
  product_id  uuid,
  sku         text,
  name        text,
  brand       text,
  stock       int,
  PRIMARY KEY ((category), price, product_id)
) WITH CLUSTERING ORDER BY (price ASC, product_id ASC);

-- T14 : produits d'un vendeur (pour supprimer les produits retirés)
CREATE TABLE IF NOT EXISTS products_by_seller (
  seller_id   uuid,
  product_id  uuid,
  category    text,
  price       decimal,
  name        text,
  status      text,
  PRIMARY KEY ((seller_id), product_id)
);

-- ============================ COMMANDES ============================
-- T1 (création), T8 (statut), source Spark
CREATE TABLE IF NOT EXISTS orders (
  order_id          uuid PRIMARY KEY,
  customer_id       uuid,
  order_date        timestamp,
  lines             list<frozen<order_line>>,
  shipping_address  frozen<address>,
  city              text,
  payment_method    text,
  status            text,
  total             decimal
);

-- T4 : commandes d'un client, les plus récentes d'abord
CREATE TABLE IF NOT EXISTS orders_by_customer (
  customer_id  uuid,
  order_date   timestamp,
  order_id     uuid,
  status       text,
  total        decimal,
  nb_items     int,
  PRIMARY KEY ((customer_id), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id ASC);

-- T15 : commandes en attente dans une ville ; A20 : panier moyen par ville
CREATE TABLE IF NOT EXISTS orders_by_city (
  city            text,
  status          text,
  order_date      timestamp,
  order_id        uuid,
  customer_id     uuid,
  total           decimal,
  payment_method  text,
  PRIMARY KEY ((city), status, order_date, order_id)
) WITH CLUSTERING ORDER BY (status ASC, order_date DESC, order_id ASC);

-- A16 : CA par jour (GROUP BY month, day) ; A22 : commandes par mode de paiement
CREATE TABLE IF NOT EXISTS orders_by_month (
  month           text,      -- 'YYYY-MM'
  day             date,
  order_id        uuid,
  total           decimal,
  payment_method  text,
  status          text,
  PRIMARY KEY ((month), day, order_id)
);

-- A19 : CA par catégorie ; A25 : taux d'annulation ; S30 : croissance mensuelle
CREATE TABLE IF NOT EXISTS lines_by_category (
  category    text,
  order_id    uuid,
  line_no     int,
  order_date  timestamp,
  product_id  uuid,
  quantity    int,
  amount      decimal,
  status      text,
  PRIMARY KEY ((category), order_id, line_no)
);

-- A17 : top 10 des produits vendus (SUM(quantity) GROUP BY product_id)
CREATE TABLE IF NOT EXISTS lines_by_product (
  product_id  uuid,
  order_id    uuid,
  line_no     int,
  category    text,
  quantity    int,
  amount      decimal,
  PRIMARY KEY ((product_id), order_id, line_no)
);

-- ============================ PANIERS ============================
-- T5/T6 : une ligne par client, réécrite à chaque modification pour réarmer le TTL
-- T7 : expiration automatique 7 jours (604 800 s) après la dernière modification
CREATE TABLE IF NOT EXISTS carts (
  customer_id  uuid PRIMARY KEY,
  items        map<uuid, frozen<cart_item>>,
  updated_at   timestamp
) WITH default_time_to_live = 604800;

-- S28 : historique des ajouts/retraits au panier (sans TTL)
CREATE TABLE IF NOT EXISTS cart_events (
  customer_id  uuid,
  event_time   timestamp,
  product_id   uuid,
  action       text,      -- 'ADD' | 'REMOVE'
  quantity     int,
  PRIMARY KEY ((customer_id), event_time, product_id)
);

-- S28 : vues de produits
CREATE TABLE IF NOT EXISTS product_views (
  customer_id  uuid,
  viewed_at    timestamp,
  product_id   uuid,
  PRIMARY KEY ((customer_id), viewed_at, product_id)
);

-- ============================ AVIS ============================
-- T10 (publier) ; A18 (note moyenne + nombre d'avis par produit)
CREATE TABLE IF NOT EXISTS reviews_by_product (
  product_id   uuid,
  created_at   timestamp,
  review_id    uuid,
  customer_id  uuid,
  rating       int,
  comment      text,
  PRIMARY KEY ((product_id), created_at, review_id)
) WITH CLUSTERING ORDER BY (created_at DESC, review_id ASC);

-- T11 : votes « utile » (un compteur doit vivre dans une table dédiée)
CREATE TABLE IF NOT EXISTS review_helpful (
  review_id  uuid PRIMARY KEY,
  helpful    counter
);