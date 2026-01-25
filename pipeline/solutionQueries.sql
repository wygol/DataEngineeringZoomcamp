
-- 3 
-- For the trips in November 2025 (lpep_pickup_datetime between 
-- '2025-11-01' and '2025-12-01', exclusive of the upper bound), 
-- how many trips had a trip_distance of less than or equal to 1 mile?
SELECT COUNT(*) FROM green_taxi_trips 
WHERE lpep_pickup_datetime::DATE >= '2025-11-01' AND lpep_pickup_datetime::DATE <= '2025-11-30'
AND trip_distance <= 1;

-- works the same like this:
-- SELECT COUNT(*) FROM green_taxi_trips 
-- WHERE lpep_pickup_datetime::DATE >= '2025-11-01' AND lpep_pickup_datetime::DATE < '2025-12-01'
-- AND trip_distance <= 1;




-- 4
-- Which was the pick up day with the longest trip distance? 
-- Only consider trips with trip_distance less than 100 miles (to exclude data errors).
SELECT lpep_pickup_datetime::DATE AS dateSelection, max(trip_distance) as maxed FROM green_taxi_trips
WHERE trip_distance < 101 GROUP BY dateSelection ORDER BY maxed DESC LIMIT 1;
   


-- 5
-- Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
WITH pickupIdentifier AS (SELECT lpep_pickup_datetime::DATE AS yeardate ,"PULocationID", sum(total_amount) AS total_amount_day
FROM green_taxi_trips WHERE lpep_pickup_datetime::DATE='2025-11-18' 
GROUP BY yeardate, "PULocationID" ORDER BY total_amount_day DESC LIMIT 1)

SELECT p."PULocationID", p.total_amount_day, t.* FROM pickupIdentifier AS p INNER JOIN taxi_zone_lookup AS t
ON p."PULocationID" = t."LocationID";

-- 6
-- For the passengers picked up in the zone named "East Harlem North"
-- in November 2025, which was the drop off zone that had the largest tip?
WITH selection AS(
SELECT max(g.tip_amount) AS tip, g."DOLocationID" FROM green_taxi_trips AS g INNER JOIN taxi_zone_lookup AS t 
ON g."PULocationID" = t."LocationID" WHERE t."Zone"='East Harlem North'
AND g.lpep_pickup_datetime::DATE >= '2025-11-01' AND 
g.lpep_pickup_datetime::DATE <= '2025-11-30'
GROUP BY (g."DOLocationID") ORDER BY tip DESC
)

SELECT s.tip, s."DOLocationID", t."Zone" FROM selection AS s INNER JOIN taxi_zone_lookup AS t
ON s."DOLocationID" = t."LocationID" ORDER BY s.tip DESC LIMIT 1;

-- not the best designed query but I will read into how I can make this better
-- it does the job for now though
