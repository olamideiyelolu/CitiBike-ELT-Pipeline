with dates as (
    select
        start_date as date
    from {{ ref('stg_citibike_rides') }}
    union all
    select
        end_date as date
    from {{ ref('stg_citibike_rides') }}
),
final as (
    select distinct
        {{
            dbt_utils.generate_surrogate_key(['date'])
        }} as date_sk,
        date
    from dates
)
select * from final