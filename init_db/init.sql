CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price NUMERIC(10,2),
    category_id INT REFERENCES categories(category_id)
);
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT UNIQUE REFERENCES products(product_id),
    quantity INT
);
ALTER TABLE inventory REPLICA IDENTITY FULL;

CREATE PUBLICATION my_publication FOR ALL TABLES;