FROM ubuntu:18.04
COPY . /app
WORKDIR /app
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install \
	python3 \
	python3-tk
RUN apt-get -y install python3-pip
RUN pip3 install -r requirements.txt
CMD["python", "src/data.py"]