USE banking_real;
DELIMITER $$

-- simple insertion trigger: if amount very high, create a fraud_alert
DROP TRIGGER IF EXISTS trg_after_txn_insert;
CREATE TRIGGER trg_after_txn_insert AFTER INSERT ON transactions
FOR EACH ROW
BEGIN
  DECLARE high_threshold DECIMAL(18,2) DEFAULT 5000.00;
  IF NEW.amount >= high_threshold THEN
    INSERT INTO fraud_alerts(account_or_card, transaction_id, alert_type, alert_score, details)
    VALUES(NEW.card_id, NEW.transaction_id, 'HIGH_VALUE_TXN', 90.0,
           JSON_OBJECT('amount', NEW.amount, 'merchant', NEW.merchant_id, 'txn_date', NEW.txn_date));
  END IF;
END $$

DELIMITER ;
