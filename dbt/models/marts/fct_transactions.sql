{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    partition_by={
        "field": "transaction_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['transaction_type', 'status']
) }}

select
    transaction_id,
    from_account_id,
    to_account_id,
    from_user_id,
    to_user_id,
    amount,
    transaction_type,
    status,
    transaction_timestamp,
    date(transaction_timestamp) as transaction_date,
    last_updated_at,
    ingested_at
from {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
  -- Only process new/updated records
  where last_updated_at > (select max(last_updated_at) from {{ this }})
     or ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
