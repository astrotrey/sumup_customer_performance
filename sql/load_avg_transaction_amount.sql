DROP TABLE IF EXISTS avg_transaction_amount CASCADE;
CREATE TABLE avg_transaction_amount AS
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
