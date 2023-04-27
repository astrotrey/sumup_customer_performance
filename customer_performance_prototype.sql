/* =============================================== */
/* Load the data into (persistent?) staging tables */
/* =============================================== */
DROP TABLE IF EXISTS staging_devices CASCADE;
CREATE TABLE staging_devices (
    id integer,
    type integer,
    store_id integer
);
COPY staging_devices FROM 'csvs/devices.csv' CSV HEADER;

DROP TABLE IF EXISTS staging_stores CASCADE;
CREATE TABLE staging_stores (
    id integer,
    name text,
    address text,
    city text,
    country text,
    created_at timestamp,
    typology text,
    customer_id integer
);
COPY staging_stores FROM 'csvs/stores.csv' CSV HEADER;

DROP TABLE IF EXISTS staging_transactions CASCADE;
CREATE TABLE staging_transactions (
    id integer,
    device_id integer,
    product_name1 text,
    product_sku text,
    product_name2 text,
    amount integer,
    status text,
    card_number text,
    cvv integer,
    created_at timestamp,
    happened_at timestamp
);
COPY staging_transactions FROM 'csvs/transactions.csv' CSV HEADER;

/* ================================================= */
/* Create the `transactions` table from the staging  */
/* tables. The `transactions` table will answer the  */
/* following questions:                              */
/* 1. Top 10 stores per transacted amount            */
/* 2. Top 10 products sold                           */
/* 3. Average transacted amount per store typology   */
/*    and country                                    */
/* 4. Percentage of transactions per device type     */
/* 5. Average time for a store to perform its 5      */
/*    first transactions                             */
/* ================================================= */
DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE transactions AS
SELECT
    t.id AS transaction_id,
    t.device_id,
    d.store_id,
    s.customer_id,
    d.type AS device_type,
    s.name AS store_name,
    s.typology AS store_type,
    s.country AS store_country,
    s.created_at AS store_created_at,
    t.created_at AS transaction_created_at,
    t.happened_at AS transaction_happened_at,
    t.amount AS transaction_amount,
    t.status AS transaction_status,
    t.product_sku,
    t.product_name1,
    t.product_name2
FROM
    staging_transactions t
    LEFT JOIN staging_devices d ON t.device_id = d.id
    LEFT JOIN staging_stores s ON d.store_id = s.id
;

/* ================================================= */
/* Top 10 stores by transacted amount                */
/* ================================================= */
SELECT
    store_id,
    SUM(transaction_amount) AS total_amount
FROM
    transactions
WHERE
    transaction_status='accepted'
GROUP BY
    store_id
ORDER BY
    total_amount DESC
LIMIT 10
;

SELECT pg_sleep(10);

/* ================================================= */
/* Top 10 products sold                              */
/* ================================================= */
SELECT
    product_sku,
    COUNT(*) AS num_sold
FROM
    transactions
WHERE
    transaction_status='accepted'
GROUP BY
    product_sku
ORDER BY
    num_sold DESC
LIMIT 10
;

SELECT pg_sleep(10);

/* ================================================= */
/* Avg transaction amount by store_type and country  */
/* ================================================= */
SELECT
    store_type,
    store_country,
    ROUND(AVG(transaction_amount),2) AS avg_amount
FROM
    transactions
WHERE
    transaction_status='accepted'
GROUP BY
    store_type,
    store_country
ORDER BY
    store_type,
    avg_amount DESC,
    store_country
;

SELECT pg_sleep(10);

/* ================================================= */
/* Percentage of transactions by device_type         */
/* ================================================= */
WITH
total_transactions AS (
SELECT
    COUNT(*)
FROM
    transactions
WHERE
    transaction_status='accepted'
)
SELECT
    t.device_type,
    COUNT(t.*) AS num_transactions,
    tt.count AS total_transactions,
    COALESCE(ROUND(
        COUNT(t.*)::numeric / NULLIF(tt.count,0)
    ,2),0) AS percent_transactions
FROM
    transactions t
    CROSS JOIN total_transactions tt
WHERE
    t.transaction_status='accepted'
GROUP BY
    t.device_type,
    tt.count
ORDER BY
    num_transactions DESC
;

SELECT pg_sleep(10);

/* ================================================= */
/* Average time to 5 transactions                    */
/* ================================================= */
WITH
first_5_transactions AS (
SELECT
    t.customer_id,
    t.store_id,
    t.transaction_happened_at
FROM (
    SELECT
        customer_id,
        store_id,
        transaction_id,
        transaction_happened_at,
        row_number() OVER (PARTITION BY store_id ORDER BY transaction_happened_at) AS transaction_num
    FROM transactions
) t
WHERE
    transaction_num = 1 OR transaction_num = 5
ORDER BY
    t.customer_id,
    t.store_id,
    t.transaction_happened_at
),
transaction_rate AS (
SELECT
    customer_id,
    store_id,
    days_to_5_transactions
FROM (
SELECT
    customer_id,
    store_id,
    transaction_happened_at AS first_transaction_at,
    lead(transaction_happened_at) OVER w AS fifth_transaction_at,
    (lead(transaction_happened_at) OVER w - transaction_happened_at) AS days_to_5_transactions
FROM
    first_5_transactions
WINDOW w AS (PARTITION BY store_id ORDER BY transaction_happened_at)
) subq
WHERE
    days_to_5_transactions IS NOT NULL
ORDER BY
    customer_id, days_to_5_transactions DESC, store_id
)
--SELECT * FROM first_5_transactions
SELECT
    customer_id,
    COUNT(DISTINCT store_id) AS num_stores,
    EXTRACT(DAY FROM AVG(days_to_5_transactions)) AS avg_days_to_5_transactions
FROM transaction_rate
GROUP BY customer_id
ORDER BY avg_days_to_5_transactions
;
