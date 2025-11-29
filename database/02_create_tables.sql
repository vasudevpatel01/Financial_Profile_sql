USE banking_real;

-- customers table (extracted from address / client_id)
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  customer_id INT AUTO_INCREMENT PRIMARY KEY,
  client_id VARCHAR(50) UNIQUE,      -- e.g., CLTIND00104
  full_name VARCHAR(200),            -- if available; else null
  birth_year INT,
  birth_month TINYINT,
  gender VARCHAR(20),
  address TEXT,
  per_capita_income BIGINT,
  yearly_income BIGINT,
  total_debt BIGINT,
  credit_score INT,
  num_credit_cards INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- cards/accounts table (one card per account row)
DROP TABLE IF EXISTS cards;
CREATE TABLE cards (
  card_id VARCHAR(50) PRIMARY KEY,   -- CRD001042 etc.
  client_id VARCHAR(50),
  card_created_at TIMESTAMP NULL,
  last_active_at TIMESTAMP NULL,
  use_chip ENUM('Yes','No') DEFAULT 'Yes',
  FOREIGN KEY (client_id) REFERENCES customers(client_id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- merchants table
DROP TABLE IF EXISTS merchants;
CREATE TABLE merchants (
  merchant_id VARCHAR(50) PRIMARY KEY, -- MCH00496 etc.
  merchant_city VARCHAR(100),
  merchant_state VARCHAR(100),
  zip VARCHAR(20)
) ENGINE=InnoDB;

-- transactions table (main fact)
DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
  transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  raw_transaction_id VARCHAR(100), -- original transaction id column (if unique)
  client_id VARCHAR(50),
  card_id VARCHAR(50),
  txn_date DATE,
  txn_ts TIMESTAMP,                -- if time included
  txn_type ENUM('SALE','REFUND','FEE','REVERSAL','UNKNOWN') DEFAULT 'SALE',
  amount DECIMAL(18,2) NOT NULL,
  channel VARCHAR(50),             -- e.g., UPI/ATM/NetBanking/Branch as in dataset
  use_chip ENUM('Yes','No'),
  merchant_id VARCHAR(50),
  merchant_city VARCHAR(100),
  merchant_state VARCHAR(100),
  zip VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE SET NULL,
  FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- etl logs
DROP TABLE IF EXISTS etl_logs;
CREATE TABLE etl_logs (
  log_id INT AUTO_INCREMENT PRIMARY KEY,
  etl_name VARCHAR(100),
  file_name VARCHAR(255),
  run_started TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  run_finished TIMESTAMP NULL,
  status ENUM('SUCCESS','FAILURE','PARTIAL') DEFAULT 'SUCCESS',
  rows_processed INT DEFAULT 0,
  rows_failed INT DEFAULT 0,
  message TEXT
) ENGINE=InnoDB;

-- fraud alerts
DROP TABLE IF EXISTS fraud_alerts;
CREATE TABLE fraud_alerts (
  fraud_id INT AUTO_INCREMENT PRIMARY KEY,
  account_or_card VARCHAR(50),
  transaction_id BIGINT NULL,
  alert_type VARCHAR(100),
  alert_score DECIMAL(5,2) DEFAULT 0,
  details JSON,
  alert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  resolved BOOLEAN DEFAULT FALSE,
  resolved_at TIMESTAMP NULL
) ENGINE=InnoDB;
