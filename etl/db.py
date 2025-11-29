# etl/db.py
import mysql.connector
import json
from mysql.connector import errorcode

_conf = json.load(open("etl/config.json"))

def get_conn():
    return mysql.connector.connect(
        host=_conf["host"],
        port=_conf.get("port",3306),
        user=_conf["user"],
        password=_conf["password"],
        database=_conf["database"],
        autocommit=False
    )

def insert_etl_log(cursor, etl_name, file_name, status, rows_processed, rows_failed, message=None):
    cursor.execute("""
      INSERT INTO etl_logs (etl_name, file_name, run_finished, status, rows_processed, rows_failed, message)
      VALUES (%s, %s, NOW(), %s, %s, %s, %s)
    """, (etl_name, file_name, status, rows_processed, rows_failed, message))
