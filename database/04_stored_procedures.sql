USE banking_real;
DELIMITER $$

-- daily summary returning basic KPIs
DROP PROCEDURE IF EXISTS proc_daily_summary;
CREATE PROCEDURE proc_daily_summary(IN p_date DATE)
BEGIN
  SELECT
    p_date AS report_date,
    (SELECT COUNT(*) FROM transactions WHERE txn_date = p_date) AS total_txns,
    (SELECT SUM(amount) FROM transactions WHERE txn_date = p_date) AS total_amount,
    (SELECT COUNT(*) FROM transactions WHERE txn_date = p_date AND amount >= 5000) AS high_value_count;
END $$

-- monthly statement for a card
DROP PROCEDURE IF EXISTS proc_monthly_statement;
CREATE PROCEDURE proc_monthly_statement(IN p_card VARCHAR(50), IN p_year INT, IN p_month INT)
BEGIN
  SELECT transaction_id, txn_date, txn_ts, amount, channel, merchant_id, merchant_city
  FROM transactions
  WHERE card_id = p_card AND YEAR(txn_date) = p_year AND MONTH(txn_date) = p_month
  ORDER BY txn_ts ASC;
END $$

-- quick loader helper for marking ETL logs (call from ETL or use DB insert)
DROP PROCEDURE IF EXISTS proc_mark_etl;
CREATE PROCEDURE proc_mark_etl(
  IN p_name VARCHAR(100), IN p_file VARCHAR(255),
  IN p_status ENUM('SUCCESS','FAILURE','PARTIAL'), IN p_rows INT, IN p_failed INT, IN p_message TEXT)
BEGIN
  INSERT INTO etl_logs(etl_name,file_name,run_finished,status,rows_processed,rows_failed,message)
  VALUES(p_name,p_file,CURRENT_TIMESTAMP,p_status,p_rows,p_failed,p_message);
END $$

DELIMITER ;
