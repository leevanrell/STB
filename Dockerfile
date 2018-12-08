FROM python:3.6.7

COPY . /app
WORKDIR /app

RUN apt-get install python3-tk

RUN pip install virtualenv
RUN virtualenv .
RUN source ./bin/activate


CMD ["python", "data.py"]