with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id::integer                                as customer_id,
        trim(first_name)                                    as first_name,
        trim(last_name)                                     as last_name,
        lower(trim(email))                                  as email,
        -- Normalize phone: strip all non-digits then format as +1XXXXXXXXXX
        regexp_replace(phone, '[^0-9]', '', 'g')            as phone_digits,
        upper(trim(status))                                 as status_raw,
        -- Map any variant to canonical active/inactive/suspended
        case
            when upper(trim(status)) in ('ACTIVE')          then 'active'
            when upper(trim(status)) in ('INACTIVE')        then 'inactive'
            when upper(trim(status)) = 'SUSPENDED'          then 'suspended'
            else 'unknown'
        end                                                 as status,
        created_at::timestamptz                             as created_at,
        updated_at
    from source
    where customer_id is not null
      and trim(first_name) != ''
      and trim(last_name)  != ''
)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone_digits,
    status,
    created_at,
    updated_at
from cleaned
