INSERT INTO transaction_rate
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
    FROM
        transactions
    WHERE
        -- Any store that is in the transaction_rate table has
        -- already had 5 transactions, so its transaction_rate
        -- does not need to be calculated again
        store_id NOT IN (SELECT store_id FROM transaction_rate)
) t
WHERE
    transaction_num = 1 OR transaction_num = 5
ORDER BY
    t.customer_id,
    t.store_id,
    t.transaction_happened_at
)
SELECT
    customer_id,
    store_id,
    first_transaction_at,
    fifth_transaction_at,
    EXTRACT(DAY FROM days_to_5_transactions) AS days_to_5_transactions
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
-- As a precaution, use the primary key constraint to prevent
-- inserting mulitple rows for the same store_id
ON CONFLICT (store_id) DO NOTHING
;
