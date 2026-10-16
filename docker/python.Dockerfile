# FastAPI control plane, LangGraph agent, DuckDB OLAP and the Qdrant indexer.
FROM python:3.13-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PITWALL_ROOT=/app FASTEMBED_CACHE_PATH=/cache/fastembed
WORKDIR /app/python
COPY python/requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY python/pyproject.toml ./
COPY python/pitwall ./pitwall
RUN pip install --no-cache-dir --no-deps -e .
COPY scripts/measure-pit-loss.py /app/scripts/
EXPOSE 8080
CMD ["uvicorn", "pitwall.api.app:app", "--host", "0.0.0.0", "--port", "8080"]
