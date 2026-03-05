-- Initialize test database for CDC pipeline E2E tests

-- Create test tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10, 2),
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10, 2),
    inventory INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some initial data (will be captured by CDC)
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');

INSERT INTO products (name, price, inventory) VALUES
    ('Widget', 19.99, 100),
    ('Gadget', 29.99, 50);

-- Create publication for CDC
-- Note: The source code uses 'pglogrepl_publication' as the publication name
CREATE PUBLICATION pglogrepl_publication FOR TABLE users, orders, products;

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO iris;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO iris;
