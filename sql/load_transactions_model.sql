INSERT INTO transactions
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
    t.card_number,
    t.cvv,
    t.product_sku,
    t.product_name1,
    t.product_name2
FROM
    staging_transactions t
    LEFT JOIN staging_devices d ON t.device_id = d.id
    LEFT JOIN staging_stores s ON d.store_id = s.id
/* Use primary key constraint to avoid inserting multiple
   rows for the same transaction_id and to update fields
   that could potentially change */
ON CONFLICT (transaction_id) DO UPDATE SET
    transaction_created_at = EXCLUDED.transaction_created_at,
    transaction_amount = EXCLUDED.transaction_amount,
    transaction_status = EXCLUDED.transaction_status
/* Only update if at least one of the fields actually changed */
WHERE
    transactions.transaction_created_at != EXCLUDED.transaction_created_at
    OR transactions.transaction_amount != EXCLUDED.transaction_amount
    OR transactions.transaction_status != EXCLUDED.transaction_status
;
