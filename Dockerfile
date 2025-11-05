FROM apache/airflow:2.10.3-python3.10

# Copy file requirements
COPY requirements.txt /requirements.txt

# Upgrade pip trước khi cài
RUN pip install --upgrade pip

# Cài các package từ requirements
RUN pip install --no-cache-dir -r /requirements.txt
