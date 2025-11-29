# etl/etl_customers.py
import csv, os
from etl.db import get_conn, insert_etl_log
from etl.helpers import sha256_hash, mask_id, parse_date

ETL_NAME = "customers_etl"
FILE = "data/fin_profiles.csv"  

def run():
    conn = get_conn()
    cur = conn.cursor()
    rows_processed = 0
    rows_failed = 0
    try:
        seen = set()
        with open(FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=',')
            for r in reader:
                client_id = r.get('client_id')
                if not client_id or client_id in seen:
                    continue
                seen.add(client_id)
                try:
                    cur.execute("""
                      INSERT INTO customers (client_id, birth_year, birth_month, gender, address,
                                             per_capita_income, yearly_income, total_debt, credit_score, num_credit_cards)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON DUPLICATE KEY UPDATE updated_at = NOW()
                    """, (
                        client_id,
                        int(r.get('birth_year')) if r.get('birth_year') else None,
                        int(r.get('birth_mon')) if r.get('birth_mon') else None,
                        r.get('gender'),
                        r.get('address'),
                        int(r.get('per_capita')) if r.get('per_capita') else None,
                        int(r.get('yearly_inc')) if r.get('yearly_inc') else None,
                        int(r.get('total_debt')) if r.get('total_debt') else None,
                        int(r.get('credit_sco')) if r.get('credit_sco') else None,
                        int(r.get('num_credi')) if r.get('num_credi') else None
                    ))
                    rows_processed += 1
                except Exception as e:
                    rows_failed += 1
        conn.commit()
        insert_etl_log(cur, ETL_NAME, os.path.basename(FILE), 'SUCCESS', rows_processed, rows_failed, None)
    except Exception as e:
        conn.rollback()
        insert_etl_log(cur, ETL_NAME, os.path.basename(FILE), 'FAILURE', rows_processed, rows_failed, str(e))
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    run()
