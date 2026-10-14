FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY asyncpulse ./asyncpulse
RUN pip install --no-cache-dir .
USER 10001
EXPOSE 8000
CMD ["uvicorn", "asyncpulse.main:app", "--host", "0.0.0.0", "--port", "8000"]
