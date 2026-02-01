# Dataclub.talks Data Engineering Zoomcamp
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
