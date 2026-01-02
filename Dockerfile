FROM apache/airflow:2.9.3
COPY . /airflow
WORKDIR /airflow
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r /requirements.txt
