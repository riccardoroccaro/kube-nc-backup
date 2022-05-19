# set base image (host OS)
FROM python:3.10.4-slim-bullseye

# install required software
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y wget curl gcc && \
    wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup && \
    echo "b9e90cde27affc2a44f9fc60e302ccfcacf71f4ae02071f30d570e6048c28597  mariadb_repo_setup" | sha256sum -c - && \
    chmod +x mariadb_repo_setup && \
    ./mariadb_repo_setup --mariadb-server-version="mariadb-10.6" && \
    apt-get install -y libmariadb3 libmariadb-dev && \
    rm ./mariadb_repo_setup

# set the app destination directory
ARG APP_DIR=/usr/src

# set the working directory in the container
WORKDIR $APP_DIR

# copy the dependencies file to the working directory
COPY app/requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# clean the no-more-useful apt-get installed software and the apt-get cache
RUN apt-get autoremove --purge -y wget curl gcc && \
    apt-get autopurge -y && apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

# copy the content of the local src directory to the working directory
COPY app/* .

# add the longhorn external library
ADD https://raw.githubusercontent.com/longhorn/longhorn-tests/885646e5fc1a1d9b904257251eae4101e35a78ff/manager/integration/tests/longhorn.py \
    ./kubencbackup/extlib/longhornlib.py

# set PYTHONPATH to run kubencbackup as a module
ENV PYTHONPATH=$APP_DIR

# command to run on container start
CMD [ "python3", "-m", "kubencbackup" ]
