with rides as (
    select
        ride_id,
        bike_type,
        start_date,
        end_date,
        start_time,
        end_time,
        start_station_id,
        end_station_id,
        user_type
    from {{ref('stg_citibike_rides')}}
),

dim_start_station as (
    select * from {{ref('dim_station')}}
),

dim_end_station as (
    select * from {{ref('dim_station')}}
),

dim_start_date as (
    select *  from {{ref('dim_date')}}
),

dim_end_date as (
    select *  from {{ref('dim_date')}}
),

final as (
    select
        rides.ride_id,
        rides.bike_type,
        rides.start_time,
        rides.end_time,
        rides.user_type,
        dim_start_station.station_sk as start_station_sk,
        dim_end_station.station_sk as end_station_sk,
        dim_start_date.date_sk as start_date_sk,
        dim_end_date.date_sk as end_date_sk

    from rides

    left join dim_start_station
        on rides.start_station_id = dim_start_station.station_id

    left join dim_end_station
        on rides.end_station_id = dim_end_station.station_id

    left join dim_start_date
        on rides.start_date = dim_start_date.date
    
    left join dim_end_date
        on rides.end_date = dim_end_date.date
)

select * from final