# Data Exploration

The following potential anomalies were found during data exploration.

1. **Null values**

   * `vw_raw_orders`: `order_date`, `currency`
   * `vw_raw_customers`: `email`, `phone`

2. **Negative values**

   * `total_amount` in `vw_raw_orders` contains negative values.

3. **Missing customer references**

   * Some `customer_id` values in `vw_raw_orders` do not exist in `vw_raw_customers`.

4. **Duplicate records**

   * Duplicate `customer_id` values were found in `vw_raw_customers`.
