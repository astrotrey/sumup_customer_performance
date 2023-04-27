DROP TABLE IF EXISTS avg_time_to_5_transactions CASCADE;
CREATE TABLE avg_time_to_5_transactions AS
SELECT
    customer_id,
    COUNT(DISTINCT store_id) AS num_stores,
    ROUND(AVG(days_to_5_transactions)) AS avg_days_to_5_transactions
FROM transaction_rate
GROUP BY customer_id
ORDER BY avg_days_to_5_transactions
;
