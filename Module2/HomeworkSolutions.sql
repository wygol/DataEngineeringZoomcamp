-- 1) 128.25 MB

-- 2) green_tripdata_2020-04.csv

-- 3) bigquery sql query:
SELECT count(1) FROM `wygolde.ny_taxi_data.yellow_tripdata` WHERE filename LIKE '%2020%' 
-- answer: 24648499

-- 4) bigquery sql query:
SELECT count(1) FROM `wygolde.ny_taxi_data.green_tripdata` WHERE filename LIKE '%2020%'
-- answer: 1734051

-- 5) bigquery sql query:
select count(1) from `ny_taxi_data.yellow_tripdata_2021_03`
-- answer: 1925152

--6) Add a timezone property set to America/New_York in the Schedule trigger configuration