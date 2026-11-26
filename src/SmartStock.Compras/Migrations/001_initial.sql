CREATE TABLE IF NOT EXISTS replenishment_needs (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    occurred_at timestamptz NOT NULL,
    sku text NOT NULL,
    previous_stock integer NOT NULL,
    current_stock integer NOT NULL,
    threshold integer NOT NULL,
    sale_id text NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS suppliers (
    id text PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS quotations (
    id text PRIMARY KEY,
    supplier_id text NOT NULL REFERENCES suppliers(id),
    sku text NOT NULL,
    unit_price numeric(18, 2) NOT NULL CHECK (unit_price > 0),
    currency text NOT NULL,
    lead_time_days integer NOT NULL CHECK (lead_time_days >= 0),
    minimum_quantity integer NOT NULL CHECK (minimum_quantity > 0)
);

INSERT INTO suppliers (id, name) VALUES
    ('supplier-a', 'Proveedor A'),
    ('supplier-b', 'Proveedor B'),
    ('supplier-c', 'Proveedor C')
ON CONFLICT (id) DO NOTHING;

INSERT INTO quotations (id, supplier_id, sku, unit_price, currency, lead_time_days, minimum_quantity) VALUES
    ('quote-a', 'supplier-a', 'SKU-001', 12.00, 'USD', 2, 10),
    ('quote-b', 'supplier-b', 'SKU-001', 10.50, 'USD', 5, 20),
    ('quote-c', 'supplier-c', 'SKU-001', 9.75, 'USD', 10, 50)
ON CONFLICT (id) DO NOTHING;
