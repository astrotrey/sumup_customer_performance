DROP TABLE IF EXISTS transactions_per_device CASCADE;
CREATE TABLE transactions_per_device AS
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
