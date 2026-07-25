from prefect import flow, task , get_run_logger
import sqlite3
import pandas as pd

@task
def extract_data(db_path: str = "shopdata.db")-> pd.DataFrame:
    logger = get_run_logger()
    try:
        conn = sqlite3.connect(db_path)

        df_exchange_rates = pd.read_sql("SELECT * FROM exchange_rates", conn)
        df_customers = pd.read_sql("SELECT * FROM raw_customers", conn)
        df_orders = pd.read_sql("SELECT * FROM raw_orders", conn)

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

@task
def transform_orders(df_orders: pd.DataFrame, df_exchange_rates: pd.DataFrame)-> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info("Starting transformation orders")

        df_orders = df_orders[df_orders["total_amount"] > 0]
        df_orders["currency"] = df_orders["currency"].fillna("USD")
        df_orders = df_orders.merge(
            df_exchange_rates,
            left_on=["currency", "order_date"],
            right_on=["currency", "date"],
            how="left"
        )
        df_orders["usd_amount"] = df_orders["total_amount"] * df_orders["rate_to_usd"]

        logger.info("Finished transformation orders")
        return df_orders
    except Exception as e:
        logger.exception("Unexpected error while transforming orders")
        raise
      

@task
def transform_customers(df_customers: pd.DataFrame)-> pd.DataFrame:
    logger = get_run_logger()
    try:
        logger.info("Starting transformation customers")

        df_customers = df_customers.sort_values("signup_date").drop_duplicates(subset='customer_id', keep="last")
        df_customers["phone"] = df_customers["phone"].str.replace(r"\D", "", regex=True)
        df_customers["email"] = df_customers["email"].fillna("unknown@domain.com")

        logger.info("Finished transformation customers")
        return df_customers


    except Exception as e:  
        logger.exception("Unexpected error while transforming data")
        raise
    
@flow
def main_etl():
    logger = get_run_logger()
    try:
        logger.info("Starting ETL flow")
        logger.info("Extracting data from SQLite database")
        source_data = extract_data()
        print(source_data)
        customers = transform_customers(source_data)
        orders = transform_orders(source_data)
    except Exception as e:
        logger.exception("ETL flow failed", exc_info=e)
        raise

if __name__ == "__main__":
    main_etl()
