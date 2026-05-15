-- Create tables for the product catalog
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL,
    category_id INT REFERENCES categories(category_id)
);
-- REPLICA IDENTITY FULL is crucial!
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT UNIQUE REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity >= 0)
);
ALTER TABLE inventory REPLICA IDENTITY FULL;

CREATE PUBLICATION my_publication FOR ALL TABLES;

DO $`$
BEGIN
    FOR i IN 1..5000 LOOP
        INSERT INTO categories (name) VALUES ('Category ' || i) ON CONFLICT DO NOTHING;
        INSERT INTO products (name, description, price, category_id) VALUES ('Product ' || i, 'Description for product ' || i, (random() * 100 + 1)::numeric(10, 2), (i % 100) + 1);
        INSERT INTO inventory (product_id, quantity) VALUES (i, floor(random() * 100));
    END LOOP;
END $`$;
