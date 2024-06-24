FROM apache/airflow:2.9.0-python3.9


RUN pip install --upgrade pip
RUN pip install openpyxl


COPY requirements.txt .
RUN pip install -r requirements.txt

