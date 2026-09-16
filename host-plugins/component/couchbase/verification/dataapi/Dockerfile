# The Data API server. Pinned so a rebuild does not silently change the SDK
# whose behaviour the tests are reading as ground truth.
FROM python:3.12-slim

RUN pip install --no-cache-dir couchbase==4.6.2

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 9000
CMD ["python3", "/app/server.py", "9000", "0.0.0.0"]
