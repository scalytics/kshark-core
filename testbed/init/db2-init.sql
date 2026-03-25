-- =============================================================================
-- DB2 Initialization Script for kshark Integration Testbed
-- =============================================================================
--
-- This script is executed after the DB2 container has created the TESTDB
-- database (via DB2INST1_PASSWORD + DBNAME env vars).
--
-- It creates:
--   1. The ORDERS table
--   2. Seed data for integration tests
--
-- Note: DB2 uses uppercase identifiers by default. The table and column
-- names are kept uppercase for consistency with DRDA probing.

CONNECT TO TESTDB;

CREATE TABLE ORDERS (
    ID              INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
    ORDER_NUMBER    VARCHAR(50) NOT NULL,
    CUSTOMER_NAME   VARCHAR(200) NOT NULL,
    PRODUCT         VARCHAR(200) NOT NULL,
    QUANTITY        INTEGER NOT NULL DEFAULT 1,
    UNIT_PRICE      DECIMAL(10, 2) NOT NULL,
    TOTAL_AMOUNT    DECIMAL(12, 2) NOT NULL,
    ORDER_STATUS    VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    ORDER_DATE      DATE NOT NULL DEFAULT CURRENT DATE,
    SHIP_COUNTRY    VARCHAR(100),
    CREATED_AT      TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    PRIMARY KEY (ID)
);

INSERT INTO ORDERS (ORDER_NUMBER, CUSTOMER_NAME, PRODUCT, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, ORDER_STATUS, ORDER_DATE, SHIP_COUNTRY) VALUES
    ('ORD-10001', 'Acme Corp',        'Kafka Cluster License',    1,  5000.00,  5000.00,  'SHIPPED',    '2026-01-10', 'US'),
    ('ORD-10002', 'TechStart GmbH',   'Confluent Cloud Credits',  5,  1000.00,  5000.00,  'DELIVERED',  '2026-01-12', 'DE'),
    ('ORD-10003', 'DataFlow Inc',     'Connect Addon Pack',       2,  750.00,   1500.00,  'PENDING',    '2026-01-15', 'US'),
    ('ORD-10004', 'CloudNine Ltd',    'Schema Registry Pro',      1,  2000.00,  2000.00,  'SHIPPED',    '2026-01-18', 'GB'),
    ('ORD-10005', 'StreamOps SAS',    'ksqlDB Enterprise',        3,  3000.00,  9000.00,  'PROCESSING', '2026-01-20', 'FR');

CONNECT RESET;
