CREATE TABLE IF NOT EXISTS daily_sales (
    sale_date DATE NOT NULL, country TEXT NOT NULL,
    net_revenue NUMERIC(18,4) NOT NULL, gross_revenue NUMERIC(18,4) NOT NULL,
    return_value NUMERIC(18,4) NOT NULL, invoice_count BIGINT NOT NULL, line_count BIGINT NOT NULL,
    PRIMARY KEY (sale_date, country)
);
CREATE TABLE IF NOT EXISTS product_sales (
    stock_code TEXT NOT NULL, description TEXT NOT NULL,
    net_quantity BIGINT NOT NULL, net_revenue NUMERIC(18,4) NOT NULL, invoice_count BIGINT NOT NULL,
    PRIMARY KEY (stock_code, description)
);
CREATE TABLE IF NOT EXISTS country_sales (
    country TEXT PRIMARY KEY, net_revenue NUMERIC(18,4) NOT NULL,
    invoice_count BIGINT NOT NULL, customer_count BIGINT NOT NULL
);

-- Incremental line state and exactly-once batch ledger.
CREATE TABLE IF NOT EXISTS retail_line_items (
    line_id TEXT PRIMARY KEY, invoice_no TEXT NOT NULL, stock_code TEXT NOT NULL,
    description TEXT NOT NULL, quantity BIGINT NOT NULL, invoice_ts TIMESTAMP NOT NULL,
    sale_date DATE NOT NULL, unit_price NUMERIC(12,4) NOT NULL, customer_id BIGINT,
    country TEXT NOT NULL, is_return BOOLEAN NOT NULL, is_cancellation BOOLEAN NOT NULL,
    line_amount NUMERIC(18,4) NOT NULL, source_fingerprint TEXT NOT NULL,
    source_file TEXT, run_id TEXT, ingested_at TIMESTAMP
);
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE retail_line_items ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;
CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id TEXT PRIMARY KEY, source_name TEXT NOT NULL, row_count BIGINT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_retail_line_daily ON retail_line_items(sale_date,country);
CREATE INDEX IF NOT EXISTS ix_retail_line_product ON retail_line_items(stock_code,description);
CREATE INDEX IF NOT EXISTS ix_retail_line_country ON retail_line_items(country);
