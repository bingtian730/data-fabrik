with source as (
    select * from {{ source('raw', 'invoices') }}
),

cleaned as (
    select
        trim(invoice_id)                                    as invoice_id,
        customer_id::integer                                as customer_id,
        -- Round to 2 decimal places and cast to NUMERIC
        round(amount::numeric, 2)                           as amount,
        upper(trim(currency))                               as currency,
        -- Normalize status to lowercase canonical form
        case
            when lower(trim(status)) = 'paid'               then 'paid'
            when lower(trim(status)) = 'unpaid'             then 'unpaid'
            when lower(trim(status)) = 'overdue'            then 'overdue'
            when lower(trim(status)) = 'cancelled'          then 'cancelled'
            else 'unknown'
        end                                                 as status,
        issue_date::date                                    as issue_date,
        -- paid_at is NULL for unpaid/overdue invoices — keep as nullable timestamp
        nullif(trim(paid_at), '')::timestamptz              as paid_at,
        updated_at
    from source
    where trim(invoice_id) != ''
      and customer_id is not null
      and amount::numeric >= 0
)

select
    invoice_id,
    customer_id,
    amount,
    currency,
    status,
    issue_date,
    paid_at,
    updated_at
from cleaned
