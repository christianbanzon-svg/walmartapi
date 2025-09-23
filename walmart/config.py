import os
from dataclasses import dataclass
from typing import Optional

try:
	from dotenv import load_dotenv
	load_dotenv()
except Exception:
	# If python-dotenv is not installed, proceed with OS environment only
	pass


@dataclass(frozen=True)
class AppConfig:
	api_key: str
	base_url: str = "https://api.bluecartapi.com/request"
	site: str = "walmart.com"
	output_dir: str = os.path.join(os.path.dirname(__file__), "output")
	database_path: str = os.path.join(os.path.dirname(__file__), "walmart.sqlite3")


def get_config() -> AppConfig:
	api_key: Optional[str] = os.getenv("BLUECART_API_KEY")
	if not api_key:
		raise RuntimeError("BLUECART_API_KEY is not set. Create .env or set the environment variable.")

	base_url = os.getenv("BLUECART_BASE_URL", "https://api.bluecartapi.com/request")
	site = os.getenv("WALMART_DOMAIN", "walmart.com")
	# Fix the output directory path to use the correct path relative to config file
	output_dir = os.getenv("OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "output"))
	database_path = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "walmart.sqlite3"))

	return AppConfig(
		api_key=api_key,
		base_url=base_url,
		site=site,
		output_dir=output_dir,
		database_path=database_path,
	)




