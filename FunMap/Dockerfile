# Use an official Python runtime as a parent image
FROM python:3

RUN apt-get update && apt-get install -y htop vim bc unzip
RUN mkdir /results
RUN mkdir /data
RUN mkdir /mappings
RUN mkdir /sql

# Set the working directory to /funmap
WORKDIR /funmap

# Copy the current directory contents into the container at /funmap
COPY . /funmap

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r /funmap/requirements.txt

# Run app.py when the container launches
CMD ["tail", "-f", "/dev/null"]