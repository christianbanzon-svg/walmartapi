FROM python:3.11-slim

# Prevent Python from writing .pyc files and ensure unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (better layer caching)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Install playwright browsers
RUN playwright install chromium

# Copy project files
COPY . /app

# Ensure output directory exists for mounted volumes
RUN mkdir -p /app/walmart/output

# Expose port for API service
EXPOSE 8000

# Default entrypoint runs the API service
ENTRYPOINT ["python", "-u", "walmart/api.py"]


