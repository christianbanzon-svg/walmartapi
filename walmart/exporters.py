import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List

from config import get_config


def _timestamp() -> str:
	return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def ensure_output_dir() -> str:
	cfg = get_config()
	os.makedirs(cfg.output_dir, exist_ok=True)
	return cfg.output_dir


def export_json(records: List[Dict[str, Any]], name_prefix: str) -> str:
	output_dir = ensure_output_dir()
	path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.json")
	with open(path, "w", encoding="utf-8") as f:
		json.dump(records, f, ensure_ascii=False, indent=2)
	return path


def export_csv(records: List[Dict[str, Any]], name_prefix: str) -> str:
	if not records:
		output_dir = ensure_output_dir()
		path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.csv")
		with open(path, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(f)
			writer.writerow(["no_records"])
		return path

	headers = sorted({k for r in records for k in r.keys()})
	output_dir = ensure_output_dir()
	path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.csv")
	with open(path, "w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
		writer.writeheader()
		for r in records:
			writer.writerow(r)
	return path


def write_debug_json(obj: Any, filename: str) -> str:
	output_dir = ensure_output_dir()
	path = os.path.join(output_dir, filename)
	with open(path, "w", encoding="utf-8") as f:
		json.dump(obj, f, ensure_ascii=False, indent=2)
	return path


