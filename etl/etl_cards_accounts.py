# etl/etl_accounts.py
import csv, os
from etl.db import get_conn, insert_etl_log
from etl.helpers import parse_date

ETL_NAME = "cards_etl"
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
                card_id = r.get('card_id')
                if not card_id or card_id in seen:
                    continue
                seen.add(card_id)
                try:
                    use_chip = r.get('use_chip') if r.get('use_chip') in ('Yes','No') else 'Yes'
                    cur.execute("""
                      INSERT INTO cards (card_id, client_id, last_active_at, use_chip)
                      VALUES (%s,%s,%s,%s)
                      ON DUPLICATE KEY UPDATE last_active_at = VALUES(last_active_at), use_chip = VALUES(use_chip)
                    """, (
                        card_id,
                        r.get('client_id'),
                        parse_date(r.get('date')),
                        use_chip
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
