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
),

updated_telemetry as (
    select
        t.deviceID,
        t.tripID,
        t.avg_speed,
        t.avg_fuel_efficiency
    from
        telemetry t
    union all
    select
        existing.deviceID,
        existing.tripID,
        existing.avg_speed,
        existing.avg_fuel_efficiency
    from
        {{ ref('aggregate_telemetry') }} existing
    left join telemetry t
    on existing.deviceID = t.deviceID and existing.tripID = t.tripID
    where t.deviceID is null
)

select
    deviceID,
    tripID,
    avg_speed,
    avg_fuel_efficiency
from
    updated_telemetry
