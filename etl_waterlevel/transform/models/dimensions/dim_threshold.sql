{{ config(
    materialized='view',
    tags=['static']
) }}


select
	distinct 
	concat("stationId",'_',"stationCode") as station_key,
	wlth_normal,
	wlth_alert,
	wlth_warning,
	wlth_danger
from
	raw.waterlevels w
order by
	"station_key" asc