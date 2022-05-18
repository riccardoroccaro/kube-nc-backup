# set base image (host OS)
FROM python:3.10.4-slim-bullseye

# install required software
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget curl gcc && \
    wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup && \
    echo "b9e90cde27affc2a44f9fc60e302ccfcacf71f4ae02071f30d570e6048c28597  mariadb_repo_setup" | sha256sum -c - && \
    chmod +x mariadb_repo_setup && \
    ./mariadb_repo_setup --mariadb-server-version="mariadb-10.6" && \
    apt-get update && apt-get install -y libmariadb3 libmariadb-dev && \
    rm ./mariadb_repo_setup && \
    apt-get autoremove --purge -y wget curl

# set the working directory in the container
WORKDIR /usr/src/app

# copy the dependencies file to the working directory
COPY app/requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# clean the no-more-useful apt-get installed software and the apt-get cache
RUN apt-get autoremove --purge -y gcc && \
    apt-get autopurge -y && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# copy the content of the local src directory to the working directory
COPY app/* .

# set PYTHONPATH to run kubencbackup as a module
ENV PYTHONPATH=/usr/src/app

# command to run on container start
CMD [ "python3", "-m", "kubencbackup" ]
