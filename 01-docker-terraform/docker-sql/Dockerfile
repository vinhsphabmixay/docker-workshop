FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock README.md ./
RUN uv lock

COPY ingest_data.py .
COPY src/ src/

ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]