USE marketplace;

-- Index SAI (Storage-Attached Index, Cassandra 5.0) : filtres et plages hors clé primaire
-- A21 : produits dont le stock est inférieur à 10
CREATE INDEX IF NOT EXISTS products_stock_sai    ON products (stock)    USING 'sai';
-- A23 : ordinateurs avec RAM >= 16 Go et prix < 8 000 MAD
CREATE INDEX IF NOT EXISTS products_category_sai ON products (category) USING 'sai';
CREATE INDEX IF NOT EXISTS products_ram_sai      ON products (ram_gb)   USING 'sai';
CREATE INDEX IF NOT EXISTS products_price_sai    ON products (price)    USING 'sai';
-- A24 : clients sans commande depuis 90 jours
CREATE INDEX IF NOT EXISTS customers_last_order_sai ON customers (last_order_at) USING 'sai';