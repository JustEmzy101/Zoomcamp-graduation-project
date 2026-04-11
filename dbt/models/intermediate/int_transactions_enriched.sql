with transactions_raw as (
    select *
    from {{ ref('stg_transactions') }}
    where _ab_cdc_deleted_at is null
),

-- Deduplicate by transaction_id (if there could be updates)
deduped as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by _ab_cdc_lsn desc, _airbyte_extracted_at desc
        ) as row_num
    from transactions_raw
),

latest_transactions as (
    select *
    from deduped
    where row_num = 1
)

select
    t.transaction_id,
    t.from_account_id,
    t.to_account_id,
    t.amount,
    t.transaction_type,
    t.status,
    t.transaction_timestamp,
    t._ab_cdc_updated_at as last_updated_at,
    t._airbyte_extracted_at as ingested_at,
    
    -- Enrich with account/user info if needed in later models
    fa.user_id as from_user_id,
    ta.user_id as to_user_id
    
from latest_transactions t
left join {{ ref('int_accounts_deduplicated') }} fa
    on t.from_account_id = fa.account_id
left join {{ ref('int_accounts_deduplicated') }} ta
    on t.to_account_id = ta.account_id
