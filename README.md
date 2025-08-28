# Walmart Scraper

A comprehensive Walmart product and seller scraping tool using BlueCart API with FastAPI web service.

## Features

- **Product Search**: Search Walmart products by keywords across multiple domains
- **Seller Information**: Extract seller contact details (email, phone, address)
- **Multi-domain Support**: US (walmart.com), Canada (walmart.ca)
- **Web API**: FastAPI service for remote execution
- **Docker Support**: Containerized deployment
- **Data Export**: CSV and JSON output formats

## Quick Start

### 1. Setup Environment

Copy the example environment file and configure your API key:

```bash
cp .env.example .env
# Edit .env and add your BLUECART_API_KEY
```

### 2. Run with Docker

Start the FastAPI service:

```bash
docker compose up walmart-api
```

The API will be available at `http://localhost:8000`

### 3. API Endpoints

#### Start a Scraping Task
```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "keywords": "nike,adidas",
    "max_per_keyword": 10,
    "export": "csv"
  }'
```

#### Check Task Status
```bash
curl "http://localhost:8000/tasks/{task_id}"
```

#### Download Results
```bash
curl "http://localhost:8000/download/{task_id}/{filename}"
```

#### Get Latest Results
```bash
curl "http://localhost:8000/latest"
```

### 4. Direct Command Line Usage

Run the scraper directly:

```bash
docker compose up walmart
```

Or run locally:

```bash
python walmart/run_walmart.py --keywords "nike" --max-per-keyword 10 --export csv
```

## Configuration

### Environment Variables

- `BLUECART_API_KEY`: Your BlueCart API key (required)
- `WALMART_DOMAIN`: Target domain (walmart.com, walmart.ca)
- `OUTPUT_DIR`: Output directory for results
- `DATABASE_PATH`: SQLite database path

### Command Line Arguments

- `--keywords`: Comma-separated search keywords
- `--max-per-keyword`: Maximum products per keyword
- `--export`: Output format (csv, json, or both)
- `--debug`: Enable debug mode
- `--zipcode`: Location for localized results
- `--retry-seller-passes`: Number of seller profile retry attempts

## API Documentation

Once the service is running, visit `http://localhost:8000/docs` for interactive API documentation.

## Output Files

- `walmart_scan_YYYYMMDD_HHMMSS.csv`: Main product listings with seller info
- `walmart_scan_YYYYMMDD_HHMMSS.json`: Raw API responses
- `walmart.db`: SQLite database with historical data

## Troubleshooting

### BlueCart API Issues
- Ensure your API key is valid and has sufficient credits
- Some domains (MX, CN, AU) may not be supported
- Seller profile API may be rate-limited

### Anti-bot Measures
- The scraper includes anti-bot detection handling
- For persistent issues, consider using proxies or manual intervention

## Development

### Local Development
```bash
pip install -r requirements.txt
python walmart/api.py
```

### Testing
```bash
# Test API health
curl http://localhost:8000/health

# Test scraping
curl -X POST http://localhost:8000/scrape -H "Content-Type: application/json" -d '{"keywords": "test"}'
```

## License

Private project - not for public distribution.
