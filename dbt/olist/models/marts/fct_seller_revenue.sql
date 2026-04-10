with delivered_orders as (
select order_id
from {{ ref('stg_orders') }}
where order_status = 'delivered'
),

order_items as (
select seller_id,
       item_price
from {{ ref('stg_order_items') }} i
inner join delivered_orders o on i.order_id = o.order_id
)

select seller_id,
       sum(item_price) as gross_revenue
from order_items
group by 1