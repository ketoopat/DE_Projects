{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='append'
 ) }}

with main as (
    select
        distinct
        concat("stationId",'_',"stationCode") as station_key,
        "stationId" as station_id,
        "stationCode" as station_code,
        "waterLevel" as water_level,
        "waterlevelStatus" as waterlevel_status,
        case
            when "waterlevelStatus" = 3 then '🚨 Beyond Danger'
            when "waterlevelStatus" = 2 then '⚠️ Between Warning and Danger'
            when "waterlevelStatus" = 1 then '🔶 Between Alert and Warning'
            when "waterlevelStatus" = 0 then '✅ Between Normal and Alert'
            when "waterlevelStatus" = -1 then '🟢 Below Normal (Safe)'
            else '❓ Unknown Status'
        end as waterlevel_condition,
        "stationName",
        "districtName",
        to_timestamp("lastUpdate", 'DD/MM/YYYY HH24:MI:SS')::timestamp AS last_update
    from
        raw.waterlevels w
    where
        "stationStatus" = 1

        {% if is_incremental() %}
        /* This condition is important to avoid reprocessing old data */
        and to_timestamp("lastUpdate", 'DD/MM/YYYY HH24:MI:SS')::timestamp > (
            select max(last_update) from {{ this }}
        )
        {% endif %}
)

select * from main