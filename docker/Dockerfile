# set base image (host OS)
FROM python:3.10.4

# set the working directory in the container
WORKDIR /root/app

# copy the dependencies file to the working directory
COPY app/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY app/* .

# command to run on container start
CMD [ "python3", "-m", "kubencbackup" ]
