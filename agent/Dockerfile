FROM python:3.12-slim

WORKDIR /app

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collect.py .

# Run as non-root
RUN useradd -u 1000 -r agent && chown -R agent /app
USER agent

ENTRYPOINT ["python", "-u", "collect.py"]
