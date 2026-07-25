from prefect import flow, task , get_run_logger
import sqlite3
import pandas as pd

@task
def extract_data(db_path: str = "shopdata.db")-> pd.DataFrame:
    logger = get_run_logger()
    try:
        conn = sqlite3.connect(db_path)

        df_exchange_rates = pd.read_sql("exchange_rates", conn)
        df_customers = pd.read_sql("raw_customers", conn)
        df_orders = pd.read_sql("raw_orders", conn)

        conn.close()

        logger.info(f"Extracted {len(df_exchange_rates)} rows from exchange_rates")
        logger.info(f"Extracted {len(df_customers)} rows from raw_customers")
        logger.info(f"Extracted {len(df_orders)} rows from raw_orders")
        return  {"exchange_rates": df_exchange_rates, "customers": df_customers, "orders": df_orders}
    except sqlite3.Error as e:
        logger.exception(f"SQLite error: {e}")
        raise

    except Exception as e:
        logger.exception("Unexpected error while extracting orders", exc_info=e)
        raise

@flow
def main_etl():
    logger = get_run_logger()
    try:
        logger.info("Starting ETL flow")
        logger.info("Extracting data from SQLite database")
        source_data = extract_data()
        print(source_data)
    except Exception as e:
        logger.exception("ETL flow failed", exc_info=e)
        raise

if __name__ == "__main__":
    main_etl()
