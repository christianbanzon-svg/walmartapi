FROM python:3.11-slim

# Prevent Python from writing .pyc files and ensure unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching)
COPY walmart/requirements.txt /app/walmart/requirements.txt
RUN pip install --no-cache-dir -r walmart/requirements.txt

# Copy project files
COPY . /app

# Ensure output directory exists for mounted volumes
RUN mkdir -p /app/walmart/output

# Expose port for API service
EXPOSE 8000

# Default entrypoint runs the main scraper; pass flags via `docker run ... -- <flags>` or compose `command:`
ENTRYPOINT ["python", "-u", "walmart/run_walmart.py"]


