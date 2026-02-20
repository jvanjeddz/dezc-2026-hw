FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl git && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl https://install.duckdb.org | sh && \
    ln -s /root/.duckdb/cli/latest/duckdb /usr/local/bin/duckdb

RUN pip install --no-cache-dir \
    duckdb \
    dbt-core \
    dbt-duckdb \
    jupyterlab \
    numpy \
    pandas \
    requests

CMD ["bash"]