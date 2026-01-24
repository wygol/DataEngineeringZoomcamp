
# so I changed the named volume to a blind mount (with 
# an absolute path on my vps) where I can actually see it
# instead of the internal docker storage when I do not reference a real path...
# this is for me to understand it better but the functionality is the same and I will
# gitignore the folder that will be created with the physical database data 

sudo docker run -it \
  --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/ \
  -p 5432:5432 \
  --network=pg-network\
  --name=pgdatabase
  postgres:18
# bring the database with the same blind mount to the network
# and then the script and pgadmin can also use the network if needed to connect to the db

# IMPORTANT: a container needs a name on a network so that it can be seen by other containers

# ingestion container 
# on the pg-network network so that it can connect to the correct localhost
sudo docker run -it --rm  --name=ingestion --network=pipeline_default \ 
zoomcamp1:v2 --user="root" \ 
--password="root" --host="pgdatabase" \ 
--port=5432 --db="ny_taxi" \ --table="yellow_taxi_data" 

 # the containers dont have to be build again. instead they just live now in a network
 # the dockerfile/image is still correct, it is just that the living environment has changed
 # and the living environment is defined in the docker run
 # host has to be adapted to the db containers name on the network (pgdatabase here)


 # make a new container for pgadmin

sudo docker run -it --rm -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
 -e PGADMIN_DEFAULT_PASSWORD="root" -v $(pwd)/pgadmin_data:/var/lib/pgadmin \
 -p 8085:80 --network=pg-network --name=pgadmin dpage/pgadmin4


 # script for calling the dockerized ingestion script (ingetst_data.py) and use its custom url link ingestion (parquet to db)
 sudo docker run -it --rm  --network=pipeline_default ingestion:latest link  --user=root --password=root --host=pgdatabase --port=5432 --db=ny_taxi --table=green_taxi_trips --url=https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet --schema --dtype=parquet