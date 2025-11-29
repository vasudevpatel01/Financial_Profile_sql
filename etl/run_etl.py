# etl/run_etl.py
from etl.etl_customers import run as run_customers
from etl.etl_cards_accounts import run as run_cards
from etl.etl_transactions import run as run_txns

def main():
    print("Starting customers ETL...")
    run_customers()
    print("Customers ETL completed.")
    print("Starting cards ETL...")
    run_cards()
    print("Cards ETL completed.")
    print("Starting transactions ETL...")
    run_txns()
    print("Transactions ETL completed.")

if __name__ == '__main__':
    main()
