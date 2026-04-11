with source as (
    select * from {{ source('raw', 'stg_transactions') }}
),

renamed as (
    select
        cast(transaction_id as int64) as transaction_id,
        cast(from_account as int64) as from_account_id,
        cast(to_account as int64) as to_account_id,  -- Assumed field; adjust if named differently
        cast(amount as numeric) as amount,
        type as transaction_type,
        status,
        cast(timestamp as timestamp) as transaction_timestamp,
        
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
