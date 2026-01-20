
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
  postgres:18
