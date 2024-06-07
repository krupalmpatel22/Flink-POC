# Use the official Flink image as the base image
FROM flink:latest

# Install dependencies for adding a new repository
RUN apt-get update && apt-get install -y software-properties-common

# Add deadsnakes PPA for Python 3.10
RUN add-apt-repository ppa:deadsnakes/ppa

# Install Python 3.10 and related packages
RUN apt-get update && apt-get install -y python3.10 python3.10-dev python3.10-distutils curl

# Install pip for Python 3.10
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

# Update alternatives to make Python 3.10 the default python3 and python
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

# Install pyflink and ruamel.yaml
RUN python3.10 -m pip install apache-flink ruamel.yaml CurrencyConverter

# Copy local scripts into the container
COPY path/to/your/scripts /opt/flink/scripts

# Clean up APT when done
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Verify installation
RUN python3 --version
RUN python --version
RUN pip3 --version
RUN python -m pip show apache-flink
RUN python -m pip show ruamel.yaml
RUN python -m pip show CurrencyConverter
