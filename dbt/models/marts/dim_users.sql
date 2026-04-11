{{ config(materialized='table') }}

select
    user_id,
    email,
    first_name,
    last_name,
    country,
    marital_status,
    kyc_status,
    created_at,
    last_updated_at,
    ingested_at
from {{ ref('int_users_deduplicated') }}
