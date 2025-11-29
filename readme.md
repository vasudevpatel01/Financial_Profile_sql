# Banking Real Dataset — End-to-End SQL Developer Project

## Overview
This project ingests card transaction CSV data, normalizes it into MySQL, provides stored procedures and triggers for daily reports and basic fraud alerts, and contains analytics queries suitable for dashboards.

## Requirements
- MySQL 8+
- Python 3.9+
- python packages: mysql-connector-python

## Setup
1. Create DB and schema:
   - Open MySQL client and run `database/01_create_database.sql`
   - Then run `database/02_create_tables.sql`
   - Run `database/03_indexes_views.sql`
   - Run `database/04_stored_procedures.sql`
   - Run `database/05_triggers.sql`


3. Edit `etl/config.json` with DB credentials.

4. Install Python package:
