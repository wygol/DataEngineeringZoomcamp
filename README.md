# Dataclub.talks Data Engineering Zoomcamp

## Introduction
This repository reflects the learnings from the different modules taught in the Data Engineering course offered by [datatalks.club](https://datatalks.club/). In the following sections you will see everything that I learned. During the course I made heavy use of the New York City Taxi Comission Dataset which has a rich data history with multiple hundred million rows. During the scope of the course I learned different methods of data ingestion (e.g. via Python, DuckDB, Kestra) to different target systems (e.g. to PostgreSQL, DuckDB, Parquet, Google Cloud Storage) and also how to transport data efficiently over the internet. Furthermore, I learned about data quality and tools that ensure that data is received and transformed to the highest quality standards needed for my project (e.g. with dbt test I could check whether my data schemas broke over time and I could define time errors, so that I receive an error when my data is older than 15 hours). This course has teached me a lot about the entire data engineering pipeline and has allowed me to deepen my knowledge in this interesting field alone in the future.


## [Module 1](./Module1/)
In this module you will see my first docker container setups and my docker compose for a PostgreSQL database and a pgadmin container. There is also a python script to ingest NYC taxi data into the PostgreSQL entity.
Afterwards I analyzed the data with some sql queries.

Here you will find:
* the [ingestion](./Module1/pipeline/ingest_data.py) script
* the [sql](./Module1/pipeline/solutionQueries.sql) queries for the homework
* the [Dockerfile](./Module1/pipeline/Dockerfile) for the ingestion script
* the [Docker-Compose](./Module1/pipeline/docker-compose.yaml) for the database and pgadmin container
* the [Terraform](./Module2/main.tf) setup is already stored in Module 2 because it will be used there


## [Module 2](./Module2/)
In this module I reused some code from Module 1 (and also switched my Docker setup from blind mounts to volumes, so that I dont have to care about my local paths in the future Modules). I enhanced my
[Docker Compose](./Module2/docker-compose.yml) file with the kestra service and learned how to use kestra efficiently to orchestrate my workflows. 
I also implemented a gemini pro api key in kestra so that I can use the power of AI inside the orchestration tool (this works especially good with retrieval augmented generation (RAG))
The answers to the homework can be found [here](./Module2/HomeworkSolutions.sql)


## [Module 3](./Module3/)
In this module I was introduced to the general architecture of BigQuery, how it works, and how to optimize bq queries. I learned when to use partitions and clusters and in which cases such optimization techniques are not helpful. I also got a short introduction into BigQuery ML techniques. To see my answers for the homework (with the bq queries) see [this](./Module3/bigQuerySQL.sql). I set up the GCP infrastructure with terraform from [Module 2](./Module/) again and used the python ingestion script presented in the DataEngineering Zoomcamp course. I changed the script to fit my needs, but there was not much coding to do there.


## [Module 4](./Module4/)
In this module I was introduced to Analytics Engineering and learned the tool dbt and got better with bigquery. The course offered to use dbt via a local or remote setup. The local setup makes use of duckdb while the remote setup uses dbt labs and connects via their web gui to my bigquery instance. I used the remote approach for this task, so that I can benefit from my previously developed setup (terraform etc.). You can see all my models ([staging](./Module4/ny_taxi_data/models/staging), [intermediate](./Module4/ny_taxi_data/models/intermediate), and [marts](./Module4/ny_taxi_data/models/marts) in this dedicated dbt [folder](./Module4/ny_taxi_data). I reused some of the code from the lecturer (Juan Manuel Perafan) and wrote the rest myself. 

## [Module 5](./Module5/)
This module combines everything I have done in Module 1 to 4 (with the exception of me not using BigQuery here but duckDB) by bulding an end to end ETL [pipeline](./Module5/my-pipeline/pipeline) in bruin. Bruin is a useful all in one tool which uses the best features of dbt, dlt, kestra, duckdb/postgesql, docker, and other such tools. I have rebuild the pipeline with data [ingestion](./Module5/my-pipeline/pipeline/assets/ingestion) (from NYC TLC data), to [staging](./Module5/my-pipeline/pipeline/assets/staging) (transformation), and to [reporting](./Module5/my-pipeline/pipeline/assets/reports) (displaying data in a fitting format). I also used lots of checks in bruin to ensure that my data quality will not degrade. This module was especially useful to me because it allowed me to streamline my development to focusing on the pipeline instead of orchestrating 5 different tools. Bruin is a free and open source tool (in its basic version) and allows me to get coding fast.

### Note
The repository is still growing and more Modules will be added by me in the future.
