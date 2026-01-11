select 
    ride_id,
    rideable_type as bike_type,
    DATE(started_at) as start_date,
    TIME(started_at) as start_time,
    DATE(ended_at) as end_date,
    TIME(ended_at) as end_time,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual as user_type
from
    {{ source('citibike_trips', 'raw_trips') }}