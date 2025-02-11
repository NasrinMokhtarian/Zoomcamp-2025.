## Nasrin Mokhtarian HW #3 Answers
Question 1:
What is the count of records for the 2024 Yellow Taxi Data?

1: 65,623
2: 840,402
3: 20,332,093
4: 85,431,289

✅ ANSWER 20,332,093



```python
-- Creating an external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi_west4.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp_m3_2025/ny_yellow_2024/yellow_tripdata_2024-*.parquet']
);

--Create a materialized table from the external table
CREATE OR REPLACE TABLE `kestra-sandbox-449808.ny_taxi_west4.yellow_tripdata_2024` AS SELECT * FROM `kestra-sandbox-449808.ny_taxi_west4.external_yellow_tripdata`;

-- the number of records(Jan to Jun 2024 only)
SELECT COUNT(*) FROM `ny_taxi_west4.yellow_tripdata_2024`
```

Question 2.
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

1: 18.82 MB for the External Table and 47.60 MB for the Materialized Table
2: 0 MB for the External Table and 155.12 MB for the Materialized Table
3: 2.14 GB for the External Table and 0MB for the Materialized Table
4: 0 MB for the External Table and 0MB for the Materialized Table

✅ ANSWER 0 MB for the External Table and 155.12 MB for the Materialized Table


```python
SELECT DISTINCT COUNT(PULocationID) FROM `ny_taxi_west4.yellow_tripdata_2024` 
 UNION ALL 
SELECT DISTINCT COUNT(PULocationID) FROM `ny_taxi_west4.external_yellow_tripdata`;

# to answer check the Bytes processed.
```

Question 3:
1: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

2: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

3: BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.

4:BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned. When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

✅ ANSWER BigQuery is a columnar database,and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


```python
# Check the bytes processed
SELECT PULocationID FROM `ny_taxi_west4.yellow_tripdata_2024`;
# Check the bytes processed
SELECT PULocationID, DOLocationID FROM `ny_taxi_west4.yellow_tripdata_2024`;

```

Question 4.
How many records have a fare_amount of 0?

1: 128,210
2: 546,578
3: 20,188,016
4: 8,333

✅ ANSWER 8,333


```python
# Count fare_amount=0 
SELECT COUNT(*) FROM `ny_taxi_west4.yellow_tripdata_2024` WHERE fare_amount=0;

```


```python
Question 5.
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

1: Partition by tpep_dropoff_datetime and Cluster on VendorID
2: Cluster on by tpep_dropoff_datetime and Cluster on VendorID
3: Cluster on tpep_dropoff_datetime Partition by VendorID
4: Partition by tpep_dropoff_datetime and Partition by VendorID

✅ ANSWER Partition by tpep_dropoff_datetime and Cluster on VendorID
```


```python
# Creating a partitioned and clustered table from the materialized
CREATE OR REPLACE TABLE `ny_taxi_west4.partitioned_yellow_tripdata_2024` 
PARTITION BY DATE (tpep_dropoff_datetime)
CLUSTER BY VendorID AS
  SELECT* FROM `ny_taxi_west4.yellow_tripdata_2024`;

```


```python
Question 6.
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
Choose the answer which most closely matches.

1: 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
2: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
3: 5.87 MB for non-partitioned table and 0 MB for the partitioned table
4: 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

✅ ANSWER 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
```


```python
# Check the the bytes processed and compare
SELECT DISTINCT (VendorID) FROM `ny_taxi_west4.yellow_tripdata_2024` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
SELECT DISTINCT (VendorID) FROM `ny_taxi_west4.partitioned_yellow_tripdata_2024` WHERE tpep_dropoff_datetime  BETWEEN '2024-03-01' AND '2024-03-15';

```


```python
Question 7:
Where is the data stored in the External Table you created?

1: Big Query
2: Container Registry
3: GCP Bucket
4: Big Table
✅ ANSWER GCP Bucket

```


```python
Question 8:
It is best practice in Big Query to always cluster your data:

1: True
2: False
✅ ANSWER False
```


```python
Bonus
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

✅ ANSWER 0B cause it uses caches result feature.
```
