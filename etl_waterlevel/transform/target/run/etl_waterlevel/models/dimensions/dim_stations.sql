
  create view "airflow"."dim"."dim_stations__dbt_tmp"
    
    
  as (
    


with station_names as (
select
	distinct
  "stationId" as station_id,
	"stationName" as station_name,
	"stationCode" as station_code,
	"districtName" as district_name,
	"latitude" as latitude,
	"longitude" as longitude,
	"stationStatus" as station_status
from
	raw.waterlevels
)

select
	*
from
	station_names
order by
	district_name,
	station_code asc
  );