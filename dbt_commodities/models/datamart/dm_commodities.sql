


with commodities as (
    select 
        *
    from 
        {{ ref("stg_commodities") }}
)

,transform as (
    select 
        ticker,
        sum(close_value) as total_value
    from 
        commodities
    group by 
        ticker
)

select * from transform