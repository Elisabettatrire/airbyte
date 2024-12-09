FROM airbyte/python-connector-base:2.0.0

WORKDIR /airbyte/integration_code

COPY pyproject.toml poetry.lock ./
COPY metadata.yaml ./
COPY main.py ./
COPY README.md ./
COPY source_google_ad_manager ./source_google_ad_manager

RUN pip install poetry --no-cache-dir

RUN poetry config virtualenvs.create false
RUN poetry install --only main --no-cache