USE marketplace;

-- Adresse : utilisée dans customers.addresses (liste) et orders.shipping_address
CREATE TYPE IF NOT EXISTS address (
  label text, street text, city text, postal_code text, country text
);

-- Ligne de commande : imbriquée dans orders.lines
CREATE TYPE IF NOT EXISTS order_line (
  product_id uuid, sku text, name text, category text, quantity int, unit_price decimal
);

-- Article de panier : valeur de la map carts.items
CREATE TYPE IF NOT EXISTS cart_item (
  quantity int, unit_price decimal, added_at timestamp
);