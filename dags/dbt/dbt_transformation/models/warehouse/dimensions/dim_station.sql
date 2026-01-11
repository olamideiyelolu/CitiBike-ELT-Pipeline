with all_stations as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        start_lat as latitude,
        start_lng as longitude
    from {{ ref('stg_citibike_rides') }}

    union all

    select
        end_station_id as station_id,
        end_station_name as station_name,
        end_lat as latitude,
        end_lng as longitude
    from {{ ref('stg_citibike_rides') }}
),

deduplicated as (
    select
        station_id,
        max(station_name) as station_name,
        max(latitude) as latitude,
        max(longitude) as longitude
    from all_stations
    where station_id is not null
    group by station_id
),

final as (
    select
        {{ 
            dbt_utils.generate_surrogate_key([
                'station_id',
            ])
        }} as station_sk,
        station_id,
        station_name,
        latitude,
        longitude
    from deduplicated
)

select * from final