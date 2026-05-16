with customers as (
    select * from {{ ref('stg_acme_customers') }}
),

orders as (
    select * from {{ ref('stg_acme_customer_orders') }}
),

invoices as (
    select * from {{ ref('stg_acme_invoices') }}
),

order_agg as (
    select
        customer_id,
        count(distinct order_id)        as total_orders,
        sum(line_total)                 as total_order_value,
        min(ordered_at)                 as first_order_at,
        max(ordered_at)                 as latest_order_at
    from orders
    where status = 'completed'
    group by customer_id
),

invoice_agg as (
    select
        customer_id,
        count(distinct invoice_id)                              as total_invoices,
        sum(amount) filter (where status = 'paid')              as total_paid,
        sum(amount) filter (where status in ('unpaid','overdue')) as total_outstanding
    from invoices
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.status                                                as customer_status,
    c.created_at,
    coalesce(ia.total_invoices, 0)                          as total_invoices,
    coalesce(ia.total_paid, 0)                              as total_paid,
    coalesce(ia.total_outstanding, 0)                       as total_outstanding,
    coalesce(oa.total_orders, 0)                            as total_completed_orders,
    coalesce(oa.total_order_value, 0)                       as total_order_value,
    oa.first_order_at,
    oa.latest_order_at
from customers c
left join invoice_agg ia using (customer_id)
left join order_agg  oa using (customer_id)
