{{  config(materialized='table')  }}

select *,
CONCAT(Latitude,",",Longitude) As Coordinates 

from {{source("airquality","air_quality_india_daily")}}
