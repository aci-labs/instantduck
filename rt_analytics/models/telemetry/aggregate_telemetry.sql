{{ config(
    materialized='table',
    tags=['telemetry']
) }}

with telemetry as (
    select
        deviceID,
        tripID,
        avg(speed::DOUBLE) as avg_speed,
        avg(kpl::DOUBLE) as avg_fuel_efficiency
    from
        raw.ingest.messages
    group by
        deviceID, tripID
)

select
    deviceID,
    tripID,
    avg_speed,
    avg_fuel_efficiency
from
    telemetry
