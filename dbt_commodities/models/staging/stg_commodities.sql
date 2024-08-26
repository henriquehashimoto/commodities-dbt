-- 1) Import data sources

with source as (
    select 
        "Date",
        "Close",
        ticker
    from 
        {{ source('commodities', 'commodities') }}
),

-- 2) Renamed
renamed as (
    select
        cast("Date" as date) as date,
        "Close" as close_value,
        ticker
    from 
        source
)

-- 3) Final select
select * from renamed