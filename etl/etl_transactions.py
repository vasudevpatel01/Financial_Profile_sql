# etl/etl_transactions.py
import csv, os
from datetime import datetime
from etl.db import get_conn, insert_etl_log
from etl.helpers import parse_date

ETL_NAME = "transactions_etl"
FILE = "data/fin_profiles.csv"
BATCH = 500

def parse_amount(s):
    try:
        return float(s)
    except:
        return 0.0

def run():
    conn = get_conn()
    cur = conn.cursor()
    rows_processed = 0
    rows_failed = 0
    batch = []
    try:
        with open(FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=',')
            for r in reader:
                try:
                    txn_date = parse_date(r.get('date'))
                    amount = parse_amount(r.get('amount'))
                    merchant_id = r.get('merchant_id') or None
                    # ensure merchant exists (simple upsert)
                    if merchant_id:
                        cur.execute("""
                          INSERT INTO merchants (merchant_id, merchant_city, merchant_state, zip)
                          VALUES (%s,%s,%s,%s)
                          ON DUPLICATE KEY UPDATE merchant_city=VALUES(merchant_city), merchant_state=VALUES(merchant_state)
                        """, (merchant_id, r.get('merchant_city'), r.get('merchant_state'), r.get('zip')))
                    batch.append((
                        r.get('transaction'),          # raw_transaction_id if present
                        r.get('client_id'),
                        r.get('card_id'),
                        txn_date,
                        None,                          # txn_ts optional
                        'SALE',                        # default type
                        amount,
                        r.get('use_chip'),
                        merchant_id,
                        r.get('merchant_city'),
                        r.get('merchant_state'),
                        r.get('zip')
                    ))
                    if len(batch) >= BATCH:
                        cur.executemany("""
                          INSERT INTO transactions (raw_transaction_id, client_id, card_id, txn_date, txn_ts, txn_type,
                                                    amount, use_chip, merchant_id, merchant_city, merchant_state, zip)
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, batch)
                        conn.commit()
                        rows_processed += len(batch)
                        batch = []
                except Exception as e:
                    rows_failed += 1
        # remaining batch
        if batch:
            cur.executemany("""
              INSERT INTO transactions (raw_transaction_id, client_id, card_id, txn_date, txn_ts, txn_type,
                                        amount, use_chip, merchant_id, merchant_city, merchant_state, zip)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch)
            conn.commit()
            rows_processed += len(batch)
        insert_etl_log(cur, ETL_NAME, os.path.basename(FILE), 'SUCCESS', rows_processed, rows_failed, None)
    except Exception as e:
        conn.rollback()
        insert_etl_log(cur, ETL_NAME, os.path.basename(FILE), 'FAILURE', rows_processed, rows_failed, str(e))
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    run()
