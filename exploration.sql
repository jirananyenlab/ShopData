-- Orders
-- negative value 
SELECT *
FROM vw_raw_orders
WHERE total_amount < 0;

-- null value
SELECT *
FROM vw_raw_orders
WHERE order_date IS NULL or total_amount IS NULL or currency IS NULL or status IS NULL ;

-- customer is not in customers table
SELECT *
FROM vw_raw_orders
WHERE customer_id NOT IN (SELECT customer_id FROM vw_raw_customers);

-- duplicate values
SELECT order_id, COUNT(*) AS duplicate_count
FROM vw_raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Customer
select * from vw_raw_customers;

-- null value
select * from vw_raw_customers
where full_name IS NULL or email IS NULL or phone IS NULL or signup_date IS NULL;

-- format of email is not correct
select * from vw_raw_customers
where email NOT LIKE '%_@__%.__%';

-- duplicate values
SELECT customer_id, COUNT(*) AS duplicate_count
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;