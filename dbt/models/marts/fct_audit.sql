{{ config(
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['action']
) }}

select
    log_id,
    action,
    performed_by,
    transaction_id,
    event_timestamp,
    date(event_timestamp) as event_date,
    last_updated_at,
    ingested_at
from {{ ref('int_audit_logs') }}
