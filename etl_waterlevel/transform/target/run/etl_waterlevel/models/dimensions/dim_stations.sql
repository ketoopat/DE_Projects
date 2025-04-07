
  create view "airflow"."dim"."dim_stations__dbt_tmp"
    
    
  as (
    -- models/dimensions/dim_stations.sql

with station_names as (
  select distinct
    "stationId"     as station_id,
    "stationName"   as station_name,
    "stationCode"   as station_code,
    "stationStatus" as station_status
  from raw.waterlevels
)

select * from station_names
  );