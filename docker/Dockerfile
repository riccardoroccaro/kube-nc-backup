# set base image (host OS)
FROM python:3.10.4

# set the working directory in the container
WORKDIR /root/code

# copy the dependencies file to the working directory
COPY code/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY code/* .

# command to run on container start
CMD [ "python", "./main.py" ]
