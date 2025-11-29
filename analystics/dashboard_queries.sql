USE banking_real;

-- top merchants by transaction volume in last 30 days
SELECT merchant_id, merchant_city, COUNT(*) AS txns, SUM(amount) AS volume
FROM transactions
WHERE txn_date BETWEEN (CURRENT_DATE - INTERVAL 30 DAY) AND CURRENT_DATE
GROUP BY merchant_id, merchant_city
ORDER BY volume DESC
LIMIT 50;

-- high-value cards in last 30 days
SELECT card_id, COUNT(*) AS txns_count, SUM(amount) AS total_spend
FROM transactions
WHERE txn_date >= (CURRENT_DATE - INTERVAL 30 DAY)
GROUP BY card_id
ORDER BY total_spend DESC
LIMIT 50;
