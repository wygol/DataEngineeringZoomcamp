{{ config(materialized='table') }}

with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    -- 1. Create a Unique Primary Key
    to_hex(md5(concat(cast(vendorid as string), cast(pickup_datetime as string)))) as trip_id,
    
    -- 2. Basic Trip Info
    trips_unioned.*, 
    
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,

    {{ get_payment_type_description('trips_unioned.payment_type') }} as payment_type_description

from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_location_id = dropoff_zone.location_id