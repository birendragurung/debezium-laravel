-- =============================================================================
-- Runs automatically on first MySQL startup (docker-entrypoint-initdb.d).
-- Creates the `inventory` schema with dummy data and the Debezium user.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Debezium replication user. `mysql_native_password` avoids public-key
-- retrieval issues with the binlog client. Grants are the minimum Debezium
-- needs to snapshot and stream from the binlog.
-- ----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- ----------------------------------------------------------------------------
-- Schema + dummy data
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

CREATE TABLE customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(50)  NOT NULL,
  last_name  VARCHAR(50)  NOT NULL,
  email      VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE products (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(100)  NOT NULL,
  description VARCHAR(512),
  weight      DECIMAL(8,2)
);

CREATE TABLE orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  order_date  DATE NOT NULL,
  customer_id INT  NOT NULL,
  product_id  INT  NOT NULL,
  quantity    INT  NOT NULL DEFAULT 1,
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
  CONSTRAINT fk_orders_product  FOREIGN KEY (product_id)  REFERENCES products(id)
);

INSERT INTO customers (first_name, last_name, email) VALUES
  ('Sally',  'Thomas',    'sally.thomas@example.com'),
  ('George', 'Bailey',    'gbailey@foobar.com'),
  ('Edward', 'Walker',    'ed@walker.com'),
  ('Anne',   'Kretchmar', 'annek@noanswer.org');

INSERT INTO products (name, description, weight) VALUES
  ('scooter',    'Small 2-wheel scooter',          3.14),
  ('car battery','12V car battery',                8.10),
  ('hammer',     '16oz carpenter''s hammer',       1.00),
  ('spare tire', '24 inch spare tire',            22.20);

INSERT INTO orders (order_date, customer_id, product_id, quantity) VALUES
  ('2024-01-16', 1, 1, 1),
  ('2024-01-17', 2, 2, 2),
  ('2024-02-19', 3, 3, 1),
  ('2024-02-21', 4, 4, 4);
