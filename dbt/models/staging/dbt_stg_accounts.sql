with source as (
    select * from {{ source('raw', 'stg_accounts') }}
),

renamed as (
    select
        -- Primary key (account_id is natural key)
        cast(account_id as int64) as account_id,
        cast(user_id as int64) as user_id,
        cast(balance as numeric) as balance,
        currency,
        
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
