FROM apache/airflow:2.7.1

# ติดตั้ง library ที่จำเป็นสำหรับงาน Data Science / Cleaning
USER airflow
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    openpyxl \
    mlxtend