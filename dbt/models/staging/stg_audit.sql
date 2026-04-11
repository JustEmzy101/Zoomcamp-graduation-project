with source as (
    select * from {{ source('raw', 'stg_audit') }}
),

renamed as (
    select
        cast(log_id as int64) as log_id,
        action,
        performed_by,
        cast(transaction_id as int64) as transaction_id,
        cast(timestamp as timestamp) as event_timestamp,
        
        -- CDC metadata
        cast(_ab_cdc_lsn as numeric) as _ab_cdc_lsn,
        _ab_cdc_deleted_at,
        _ab_cdc_updated_at,
        
        -- Airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        _airbyte_generation_id
        
    from source
)

select * from renamed
