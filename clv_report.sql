select c.customer_id ,full_name,
    COUNT(o.order_id) AS total_orders_placed, 
    ROUND(sum(o.usd_amount), 2) as lifetime_value_usd, 
    STRFTIME('%Y-%m', c.signup_date) AS customer_cohort
from dim_customers c JOIN fct_orders o on c.customer_id = o.customer_id
group by  o.customer_id , STRFTIME('%Y-%m', c.signup_date)
order by lifetime_value_usd desc; 
