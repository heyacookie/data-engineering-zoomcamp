FROM python:3

RUN apt-get install wget
RUN python -m pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_taxi.py ingest_taxi.py 

ENTRYPOINT [ "python", "ingest_taxi.py" ]