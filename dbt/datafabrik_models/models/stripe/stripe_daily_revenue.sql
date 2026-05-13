SELECT
    charge_date,
    status,
    COUNT(*)                        AS charge_count,
    SUM(amount_usd)                 AS gross_revenue_usd,
    SUM(CASE WHEN refunded THEN amount_usd ELSE 0 END) AS refunded_usd,
    SUM(CASE WHEN status = 'succeeded' AND NOT refunded THEN amount_usd ELSE 0 END) AS net_revenue_usd
FROM {{ ref('stg_stripe_charges') }}
GROUP BY charge_date, status
ORDER BY charge_date, status
