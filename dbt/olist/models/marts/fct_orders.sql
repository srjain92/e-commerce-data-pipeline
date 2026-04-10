with order_payments as (
select order_id,
       sum(payment_value) as total_order_revenue
from {{ ref('stg_order_payments') }}
group by 1
),

orders as (
select extract(year from purchased_at) as purchase_year,
       extract(month from purchased_at) as purchase_month,
       order_id
from {{ ref('stg_orders') }}
where order_status = 'delivered'
)

select o.purchase_year,
       o.purchase_month,
       count(distinct o.order_id) as total_orders,
       sum(coalesce(p.total_order_revenue, 0)) as total_revenue
from orders o
left join order_payments p on o.order_id = p.order_id
group by 1, 2