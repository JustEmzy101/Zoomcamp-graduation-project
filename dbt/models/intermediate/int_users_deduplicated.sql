with ranked as (
    select
        *,
        row_number() over (
            partition by user_id
            order by _ab_cdc_lsn desc, _airbyte_extracted_at desc
        ) as row_num
    from {{ ref('stg_users') }}
    where _ab_cdc_deleted_at is null
)

select
    user_id,
    email,
    first_name,
    last_name,
    country,
    marital_status,
    kyc_status,
    created_at,
    _ab_cdc_updated_at as last_updated_at,
    _airbyte_extracted_at as ingested_at
from ranked
where row_num = 1
