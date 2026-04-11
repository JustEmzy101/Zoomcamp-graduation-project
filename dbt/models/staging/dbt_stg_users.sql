with source as (
    select * from {{ source('raw', 'stg_users') }}
),

renamed as (
    select
        cast(user_id as int64) as user_id,
        email,
        first_name,
        last_name,
        country,
        marital_status,
        kyc_status,
        cast(created_at as timestamp) as created_at,
        
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
