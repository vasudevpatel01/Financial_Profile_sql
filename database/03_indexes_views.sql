USE banking_real;

-- indexes
CREATE INDEX idx_txn_card_date ON transactions(card_id, txn_date);
CREATE INDEX idx_txn_date_amount ON transactions(txn_date, amount);
CREATE INDEX idx_customers_clientid ON customers(client_id);
CREATE INDEX idx_merchant_city ON merchants(merchant_city);

-- simple views for dashboards
DROP VIEW IF EXISTS vw_daily_amounts;
CREATE VIEW vw_daily_amounts AS
SELECT txn_date,
       COUNT(*) AS total_txns,
       SUM(amount) AS total_amount,
       AVG(amount) AS avg_amount
FROM transactions
GROUP BY txn_date;

DROP VIEW IF EXISTS vw_high_value_txns_7d;
CREATE VIEW vw_high_value_txns_7d AS
SELECT txn_date, transaction_id, card_id, amount, merchant_id, merchant_city
FROM transactions
WHERE amount >= 5000 AND txn_date >= (CURRENT_DATE - INTERVAL 7 DAY)
ORDER BY amount DESC;
