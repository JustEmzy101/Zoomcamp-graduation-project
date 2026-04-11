with ranked as (
    select
        *,
        row_number() over (
            partition by log_id
            order by _ab_cdc_lsn desc, _airbyte_extracted_at desc
        ) as row_num
    from {{ ref('stg_audit') }}
    where _ab_cdc_deleted_at is null
)

select
    log_id,
    action,
    performed_by,
    transaction_id,
    event_timestamp,
    _ab_cdc_updated_at as last_updated_at,
    _airbyte_extracted_at as ingested_at
from ranked
where row_num = 1
