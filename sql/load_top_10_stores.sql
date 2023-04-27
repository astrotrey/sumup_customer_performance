DROP TABLE IF EXISTS top_10_stores CASCADE;
CREATE TABLE top_10_stores AS
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
