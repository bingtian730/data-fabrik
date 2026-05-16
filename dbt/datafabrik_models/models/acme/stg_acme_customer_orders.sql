with source as (
    select * from {{ source('raw', 'customer_orders') }}
),

cleaned as (
    select
        trim(order_id)                                      as order_id,
        customer_id::integer                                as customer_id,
        trim(invoice_id)                                    as invoice_id,
        -- Normalize product name: trim spaces, title-case
        initcap(trim(product_name))                         as product_name,
        -- Quantity stored as float text (e.g. "2.0") — round to integer
        round(quantity::numeric)::integer                   as quantity,
        round(unit_price::numeric, 2)                       as unit_price,
        -- Derive line total
        round(quantity::numeric, 0) * round(unit_price::numeric, 2)
                                                            as line_total,
        case
            when lower(trim(status)) = 'completed'          then 'completed'
            when lower(trim(status)) = 'pending'            then 'pending'
            when lower(trim(status)) = 'cancelled'          then 'cancelled'
            else 'unknown'
        end                                                 as status,
        ordered_at::timestamptz                             as ordered_at,
        updated_at
    from source
    where trim(order_id) != ''
      and customer_id is not null
      and quantity::numeric > 0
      and unit_price::numeric >= 0
)

select
    order_id,
    customer_id,
    invoice_id,
    product_name,
    quantity,
    unit_price,
    line_total,
    status,
    ordered_at,
    updated_at
from cleaned
