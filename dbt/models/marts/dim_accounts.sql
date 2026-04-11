{{ config(materialized='table') }}

select
    account_id,
    user_id,
    balance,
    currency,
    last_updated_at,
    ingested_at
from {{ ref('int_accounts_deduplicated') }}
