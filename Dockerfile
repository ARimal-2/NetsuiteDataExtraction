FROM apache/airflow:2.9.3

# Copy only requirements first for caching
COPY requirements.txt /airflow/requirements.txt
WORKDIR /airflow
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY src/ /airflow/src/
