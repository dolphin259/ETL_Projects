FROM apache/airflow:3.1.1-python3.13

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip
RUN pip install  -r /requirements.txt