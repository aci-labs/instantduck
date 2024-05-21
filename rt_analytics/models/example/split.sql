

-- where id is not null
{{ config(
    materialized='table',
    tags=['split']
) }}

with raw_data as (
    -- Replace this with the actual table or source you are reading from
    select name as full_name, 34 as age from raw.ingest.messages
)

select
    full_name,
    split_part(full_name, ' ', 1) as first_name,
    split_part(full_name, ' ', 2) as last_name,
    age
from
    raw_data
