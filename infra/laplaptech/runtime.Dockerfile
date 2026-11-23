FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /app
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin pipeline
COPY requirements.txt requirements.lock.txt ./
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY --chown=pipeline:pipeline ingestion ./ingestion
COPY --chown=pipeline:pipeline dbt ./dbt
COPY --chown=pipeline:pipeline scripts/run_airflow_stage.py ./scripts/run_airflow_stage.py
COPY --chown=pipeline:pipeline scripts/run_cosmos.py ./scripts/run_cosmos.py
COPY --chown=pipeline:pipeline scripts/recover_pipeline.py ./scripts/recover_pipeline.py
COPY --chown=pipeline:pipeline scripts/provision_access.py ./scripts/provision_access.py
RUN mkdir -p /artifacts /app/cosmos && chown -R pipeline:pipeline /app /artifacts
USER pipeline
RUN POSTGRES_PASSWORD=parse_only dbt parse --no-partial-parse \
    --project-dir /app/dbt --profiles-dir /app/dbt \
    --target-path /app/cosmos --log-path /tmp/cosmos-parse \
    && rm -r /tmp/cosmos-parse
ENTRYPOINT ["python", "-m", "scripts.run_airflow_stage"]
