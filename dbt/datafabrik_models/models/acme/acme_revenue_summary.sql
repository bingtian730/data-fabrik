with invoices as (
    select * from {{ ref('stg_acme_invoices') }}
)

select
    issue_date,
    currency,
    count(distinct invoice_id)                                  as invoice_count,
    sum(amount)                                                 as total_invoiced,
    sum(amount) filter (where status = 'paid')                  as total_paid,
    sum(amount) filter (where status in ('unpaid', 'overdue'))  as total_outstanding,
    count(*) filter (where status = 'paid')                     as paid_count,
    count(*) filter (where status = 'unpaid')                   as unpaid_count,
    count(*) filter (where status = 'overdue')                  as overdue_count,
    round(
        100.0 * count(*) filter (where status = 'paid') / nullif(count(*), 0),
        1
    )                                                           as paid_pct
from invoices
group by issue_date, currency
order by issue_date, currency
