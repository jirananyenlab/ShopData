from prefect import flow, task , get_run_logger
import sqlite3
import pandas as pd

@task
def extract_data(db_path: str = "shopdata.db")-> pd.DataFrame:
    logger = get_run_logger()
    try:
        conn = sqlite3.connect(db_path)

        df_exchange_rates = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)
        df_customers = pd.read_sql("SELECT * FROM raw_customers", conn)
        df_orders = pd.read_sql("SELECT * FROM raw_orders", conn)

        conn.close()

        logger.info(f"Extracted {len(df_exchange_rates)} rows from vw_exchange_rates")
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
        logger.info(f"columns after merge: {df_orders}")
        logger.info(f"total_amount: {df_orders['total_amount']}, rate_to_usd: {df_orders['rate_to_usd']}")
        df_orders["usd_amount"] = df_orders["total_amount"]

        mask = (df_orders["currency"] != "USD") & (df_orders["rate_to_usd"].notna())

        df_orders.loc[mask, "usd_amount"] = (
            df_orders.loc[mask, "total_amount"]
            * df_orders.loc[mask, "rate_to_usd"]
        )

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

def filter_orphan_orders(customers_data, orders_data, logger):
    valid_orders = orders_data[
        orders_data["customer_id"].isin(customers_data["customer_id"])
    ]

    skipped = len(orders_data) - len(valid_orders)

    if skipped > 0:
        logger.warning(
            "Skipping %d orders because customer_id does not exist in dim_customers",
            skipped,
        )

    return valid_orders

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            signup_date TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fct_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total_amount REAL NOT NULL,
            usd_amount REAL NOT NULL,
            currency TEXT NOT NULL,
            status TEXT NOT NULL,
            order_date TEXT,
            FOREIGN KEY (customer_id)
                REFERENCES dim_customers(customer_id)
        )
    """)

@task
def load_data(data_dict, db_path: str = "analytics.db"):
    logger = get_run_logger()
    try:
        logger.info("Starting connection to analytics.db")

        conn = sqlite3.connect(db_path)
        logger.info("Connected to analytics.db")

        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()
        customers_data = data_dict["customers"]
        orders_data = data_dict["orders"]

        orders_data = filter_orphan_orders(customers_data, orders_data, logger)
        
        logger.info("Creating tables dim_customers and fct_orders if they do not exist")
        create_tables(cursor)


        logger.info("Inserting data into dim_customers")
        for _, customer in customers_data.iterrows():
           cursor.execute(
                """
                INSERT INTO dim_customers (
                    customer_id,
                    full_name,
                    email,
                    phone,
                    signup_date
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    full_name = excluded.full_name,
                    email = excluded.email,
                    phone = excluded.phone,
                    signup_date = excluded.signup_date

                """,
                (
                    customer["customer_id"],
                    customer["full_name"],
                    customer["email"],
                    customer["phone"],
                    customer["signup_date"],
                )
           )
    
        logger.info("Inserting data into fct_orders")
        for _, order in orders_data.iterrows():
            cursor.execute(
                """
                INSERT INTO fct_orders (
                    order_id,
                    customer_id,
                    total_amount,
                    usd_amount,
                    currency,
                    status,
                    order_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    total_amount = excluded.total_amount,
                    usd_amount = excluded.usd_amount,
                    currency = excluded.currency,
                    status = excluded.status,
                    order_date = excluded.order_date
                """,
                (
                    order["order_id"],
                    order["customer_id"],
                    order["total_amount"],
                    order["usd_amount"],
                    order["currency"],
                    order["status"],
                    order["order_date"],
                )
            )

        conn.commit()
        logger.info("Data loaded successfully")
    except Exception as e:
        conn.rollback()
        logger.exception("Unexpected error while loading data")
        raise
    finally:
        conn.close()
    


@flow
def main_etl():
    logger = get_run_logger()
    try:
        logger.info("Starting ETL flow")
        source_data = extract_data()

        customers = transform_customers(source_data["customers"])

        orders = transform_orders(source_data["orders"], source_data["exchange_rates"])

        load_data(
       { "customers": customers, "orders": orders }
        )
        
        logger.info("ETL flow completed")
    except Exception as e:
        logger.exception("ETL flow failed", exc_info=e)
        raise

if __name__ == "__main__":
    main_etl()
