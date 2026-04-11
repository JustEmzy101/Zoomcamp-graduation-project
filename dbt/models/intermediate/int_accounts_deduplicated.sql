with ranked as (
    select
        *,
        row_number() over (
            partition by account_id
            order by _ab_cdc_lsn desc, _airbyte_extracted_at desc
        ) as row_num
    from {{ ref('dbt_stg_accounts') }}
    where _ab_cdc_deleted_at is null  -- exclude soft-deleted records (if desired)
)

select
    account_id,
    user_id,
    balance,
    currency,
    _ab_cdc_updated_at as last_updated_at,
    _airbyte_extracted_at as ingested_at
from ranked
where row_num = 1
