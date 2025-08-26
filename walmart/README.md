Walmart Scraper (BlueCart API)

This project queries the Traject Data BlueCart API for Walmart (US) to search by keyword, fetch product and seller/offer details, store change history in SQLite, and export JSON/CSV.

Requirements
- Python 3.10+
- BlueCart API key

Setup
1) Create and activate a virtual environment.
2) Install dependencies:
   pip install -r requirements.txt
3) Configure environment:
   - Copy `.env.example` to `.env` and set `BLUECART_API_KEY`.

Quick start
- Run a one-off scan for keywords (comma-separated):
  python run_walmart.py --keywords "instyler,instyler rotating iron" --max-per-keyword 10 --export json csv

- Or provide a file with one keyword per line:
  python run_walmart.py --keywords-file keywords.txt --max-per-keyword 10 --export json csv

Notes
- History: Each run stores full snapshots of listing and seller data with timestamps so you can audit changes over time.
- Regions: Default is Walmart US (`walmart.com`). You can change the domain in `config.py` if needed.
- API limits: Respect your BlueCart plan limits; use `--sleep` to back off between requests if necessary.

Environment variables
- BLUECART_API_KEY: Your Traject Data BlueCart API key
- BLUECART_BASE_URL: Optional. Defaults to https://api.bluecartapi.com/request
- WALMART_DOMAIN: Optional. Defaults to walmart.com

Outputs
- Exports are written under `walmart/output/` as timestamped JSON and CSV files.

Disclaimer
This client relies on BlueCart’s Walmart endpoints. Field availability can vary by product/seller. Code is defensive and will store whatever fields are returned.


Docker

- Build the image:
  docker build -t walmartscraper:latest .

- Run a quick scan (US by default):
  docker run --rm -e BLUECART_API_KEY=YOUR_KEY -v %cd%/walmart/output:/data/output walmartscraper:latest --keywords nike --max-per-keyword 5 --export csv json

- Canada run:
  docker run --rm -e BLUECART_API_KEY=YOUR_KEY -e WALMART_DOMAIN=walmart.ca -v %cd%/walmart/output:/data/output walmartscraper:latest --keywords nike --max-per-keyword 5 --export csv json

- Using docker compose (reads your host .env):
  1) Copy `.env.example` to `.env` and set `BLUECART_API_KEY` (and optional vars)
  2) Edit `docker-compose.yml` command if needed
  3) Run:
     docker compose up --build

Repository requirements compliance
- Includes Dockerfile and docker-compose.yml at repo root.
- All config values are read from `.env` (not committed). An `.env.example` is provided with placeholders.
- `docker compose up` launches the service with no extra setup once `.env` exists.




