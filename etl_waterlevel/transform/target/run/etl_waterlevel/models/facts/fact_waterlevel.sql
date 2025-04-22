
      insert into "airflow"."fact"."fact_waterlevel" ("station_key", "station_id", "station_code", "water_level", "waterlevel_status", "waterlevel_condition", "stationName", "districtName", "last_update")
    (
        select "station_key", "station_id", "station_code", "water_level", "waterlevel_status", "waterlevel_condition", "stationName", "districtName", "last_update"
        from "fact_waterlevel__dbt_tmp000506015384"
    )


  