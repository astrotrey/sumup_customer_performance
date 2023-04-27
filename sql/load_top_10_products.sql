DROP TABLE IF EXISTS top_10_products CASCADE;
CREATE TABLE top_10_products AS
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
