select state,
       round(avg(days_to_delivery), 2) as average_delivery_days
from {{ ref('int_orders_enriched') }}
group by 1