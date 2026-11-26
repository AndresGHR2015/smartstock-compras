CREATE TABLE purchase_requests (
    request_key text PRIMARY KEY,
    request_id text NOT NULL,
    request_document jsonb NOT NULL,
    result_json text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE purchase_orders (
    order_key text PRIMARY KEY,
    order_id text NOT NULL,
    need_event_id text NOT NULL REFERENCES replenishment_needs(event_id),
    offer jsonb NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    status text NOT NULL CHECK (status IN ('DRAFT', 'APPROVED', 'REJECTED')),
    revision integer NOT NULL CHECK (revision > 0),
    decided_by text,
    CHECK (
        (status = 'DRAFT' AND revision = 1 AND decided_by IS NULL)
        OR (status IN ('APPROVED', 'REJECTED') AND revision = 2 AND decided_by IS NOT NULL)
    )
);

CREATE TABLE purchase_cursors (
    token text PRIMARY KEY,
    operation text NOT NULL CHECK (operation IN ('list_needs', 'list_offers')),
    sku text NOT NULL,
    after_id text NOT NULL
);
