FROM apache/airflow:3.3.2-python3.13
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" "apache-airflow-providers-docker==4.6.1" \
    && pip check
# Docker exec must resolve executables accessible to the non-root airflow user.
ENV PATH="/home/airflow/.local/bin:/usr/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
