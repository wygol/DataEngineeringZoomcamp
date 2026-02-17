with source as (select * from {{source("raw_data", "fhv_tripdata")}}),
source_step as (

select

-- identifier
cast(unique_row_id as string) as unique_row_id, -- it is hexcode so string feels wrong, fix at a later stage
cast(filename as string) as filename,
cast(dispatching_base_num as string) as dispatching_base_id, -- is not a number but actually a code like "B00123"
cast(pickup_datetime as timestamp) as pickup_datetime,
cast(dropOff_datetime as timestamp) as dropoff_datetime,
cast(PUlocationID as integer) as pulocationid,
cast(DOlocationID as integer) as dolocationid,
cast(SR_Flag as string) as sr_flag,
cast(affiliated_base_number as string) as aff_base_id -- is also not a number but a code

from source
)
select * from source_step
-- normally 1:1 mapping in staging, but here I will already reduce by null dispatching_base_nums
where dispatching_base_id is not null