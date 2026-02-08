-- setup)

-- creating external table
CREATE OR REPLACE EXTERNAL TABLE wygolde.ny_taxi_data.extTaxiData
OPTIONS (
  format = 'Parquet',
  uris = ['gs://wygolde_terraform_demo/*.parquet']
);

-- creating materialized table
CREATE OR REPLACE TABLE wygolde.ny_taxi_data.matTaxiData AS
SELECT * FROM wygolde.ny_taxi_data.extTaxiData

 -- 1)
SELECT COUNT(1) FROM wygolde.ny_taxi_data.extTaxiData

-- answer: 20332093

-- 2)
SELECT COUNT(DISTINCT ) FROM wygolde.ny_taxi_data.extTaxiData
-- estimated amount: 0 B

SELECT COUNT(DISTINCT ) FROM wygolde.ny_taxi_data.matTaxiData
-- estimated amount: 155.12 MB

-- answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

-- 3)
SELECT PULocationID FROM wygolde.ny_taxi_data.matTaxiData 
-- estimated size: 155.12 MB

SELECT PULocationID, DOLocationID FROM wygolde.ny_taxi_data.matTaxiData
-- estimated size: 310.24 MB

-- answer: BigQuery is a columnar database, and it only scans the specific 
-- columns requested in the query. Querying two columns
-- (PULocationID, DOLocationID) requires reading more data
-- than querying one column (PULocationID), leading to a higher estimated
-- number of bytes processed.

-- 4) 
SELECT COUNT(1) FROM wygolde.ny_taxi_data.matTaxiData WHERE fare_amount = 0

-- answer: 8333

-- 5)
CREATE OR REPLACE TABLE wygolde.ny_taxi_data.matTaxiDataPartClust
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY(VendorID) AS
SELECT * FROM wygolde.ny_taxi_data.extTaxiData
 
-- answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

-- 6)
-- non-partitioned and non-clustered
SELECT DISTINCT VendorID FROM wygolde.ny_taxi_data.matTaxiData
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND
DATE(tpep_dropoff_datetime) <= '2024-03-15'
-- estimated size: 310.24 MB


-- partitioned and clustered
SELECT DISTINCT VendorID FROM wygolde.ny_taxi_data.matTaxiDataPartClust
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01' AND
DATE(tpep_dropoff_datetime) <= '2024-03-15'
-- estimated size: 26.84 MB

-- this is a great improvement (and impacts the cost side)

-- answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

-- 7)
-- answer: GCP Bucket

-- 8)
-- answer: False
-- It really depends on the use case and size of the data

-- 9) 
SELECT COUNT(*) FROM wygolde.ny_taxi_data.matTaxiData
-- estimated size: 0 B 

SELECT COUNT(*) FROM wygolde.ny_taxi_data.matTaxiDataPartClust
-- estimated size: 0 B 

-- after research I found the following answer:
-- BigQuery does not actually look at the data for a simple COUNT(*) query
-- but it looks at the metadata catalogue which stored the row number 
-- into the catalogue during the creation of the materialized table
-- so bigQuery does not look at the data but just at an entry in the catalogue.
-- Note: This breaks if we use a filter though
-- like: SELECT COUNT(*) FROM T WHERE X = 1
-- now bigQuery actually has to look at the data