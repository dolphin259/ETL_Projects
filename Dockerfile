FROM apache/airflow:2.10.3-python3.10

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r /requirements.txt
