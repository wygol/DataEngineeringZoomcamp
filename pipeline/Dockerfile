# the base image we start from
FROM python:3.13.11-slim

# installing modules that we need for our scripts, i.e. dependencies
# install uv first so that we can use it
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# installing /uv from source into /bin/ of our docker container
# bin is a system folder and uv is therefore visible to the terminal 

# mkdir /app & cd /app
WORKDIR /app

# we define this path string inside /app as a future environment path
# there is nothing in this location at this point of execution
ENV PATH="/app/.venv//bin:$PATH"
# so the /app/.venv/bin from my local system is pushed to the containers path or not?

# here we copy the relevant uv files so that we can rebuild the uv environment 
# that was used for developments
# the files are stored in app/ -> app/pyprojects.toml 
COPY "pyproject.toml"  "uv.lock" ".python-version" ./

# now uv is executed as a tool. this builds the entire environment, so also the 
# .venv/bin/ (where our python and packages are)
# this also means that 2 python interpreters exist in my container.
# one global python from the image and one uv python from uv
RUN uv sync --locked


# this copies the .../pipeline/pipeline.py to the /app/pipeline.py
COPY pipeline.py ./

# this will trigger that the container runs python pipeline.py in its CLI and 
# then I can write the asked system argument vector in the initial docker run
# so the month specification 
# the entrypoint is a default command to run. so normally we would have a main.py
# or something with a similar logic here
ENTRYPOINT ["python", "pipeline.py"]
