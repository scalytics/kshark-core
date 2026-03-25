-- =============================================================================
-- PostgreSQL Initialization Script for kshark Integration Testbed
-- =============================================================================
--
-- This script runs on first startup of the PostgreSQL container.
-- The database 'testdb' is already created by POSTGRES_DB env var.
-- The user 'pguser' is already created by POSTGRES_USER env var.
--
-- This script creates:
--   1. The 'customers' table
--   2. Seed data for integration tests

-- Ensure we are connected to testdb
\connect testdb;

-- Create the customers table
CREATE TABLE IF NOT EXISTS customers (
    id              SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    company         VARCHAR(200),
    country         VARCHAR(100),
    signup_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    plan            VARCHAR(50) NOT NULL DEFAULT 'free',
    monthly_spend   NUMERIC(10, 2) DEFAULT 0.00,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create an index on email for connector queries
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers (email);

-- Create an index on signup_date for incremental queries
CREATE INDEX IF NOT EXISTS idx_customers_signup ON customers (signup_date);

-- Insert seed data
INSERT INTO customers (first_name, last_name, email, company, country, signup_date, plan, monthly_spend, is_active) VALUES
    ('Alice',   'Johnson',  'alice@example.com',    'Acme Corp',        'US',   '2026-01-10', 'professional', 99.00,  TRUE),
    ('Bob',     'Smith',    'bob@example.com',      'TechStart GmbH',   'DE',   '2026-01-12', 'enterprise',   499.00, TRUE),
    ('Charlie', 'Lee',      'charlie@example.com',  'DataFlow Inc',     'US',   '2026-01-15', 'starter',      29.00,  TRUE),
    ('Diana',   'Patel',    'diana@example.com',    'CloudNine Ltd',    'GB',   '2026-01-18', 'professional', 99.00,  TRUE),
    ('Eve',     'Martinez', 'eve@example.com',      'StreamOps SAS',    'FR',   '2026-01-20', 'enterprise',   499.00, TRUE),
    ('Frank',   'Wilson',   'frank@example.com',    NULL,               'CA',   '2026-01-22', 'free',         0.00,   TRUE),
    ('Grace',   'Kim',      'grace@example.com',    'Nexus Analytics',  'KR',   '2026-01-25', 'starter',      29.00,  FALSE),
    ('Hank',    'Mueller',  'hank@example.com',     'Kafka Kraft AG',   'CH',   '2026-01-28', 'professional', 99.00,  TRUE)
ON CONFLICT (email) DO NOTHING;

-- Grant all privileges on the table to pguser (already owner, but explicit)
GRANT ALL PRIVILEGES ON TABLE customers TO pguser;
GRANT USAGE, SELECT ON SEQUENCE customers_id_seq TO pguser;
