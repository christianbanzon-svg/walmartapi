import argparse
import csv
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any

from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
MAILTO_RE = re.compile(r'mailto:([^"\'>\s]+)', re.I)
PHONE_RE = re.compile(r"(\+?\d[\d\s\-()]{7,}\d)")


def _digits_only(s: str) -> str:
	return re.sub(r"\D", "", s or "")


def _valid_phone(p: str) -> bool:
	d = _digits_only(p)
	# keep common lengths 10-13, exclude 12+ repetitive garbage like tracking IDs
	return 10 <= len(d) <= 13


def _collect_from_json(node: Any, emails: List[str], phones: List[str]) -> None:
	"""Walk arbitrary JSON and collect emails/phones.

	The function is defensive against schema variations and failures.
	"""
	try:
		if isinstance(node, dict):
			for k, v in node.items():
				if isinstance(v, (dict, list)):
					_collect_from_json(v, emails, phones)
				elif isinstance(v, str):
					for e in EMAIL_RE.findall(v):
						emails.append(e)
					kl = k.lower()
					if ("phone" in kl) or ("telephone" in kl) or ("tel" in kl):
						phones.append(v)
		elif isinstance(node, list):
			for it in node:
				_collect_from_json(it, emails, phones)
	except Exception:
		# ignore malformed fragments
		pass


def scan_file(path: str) -> Dict[str, str]:
	try:
		with open(path, "r", encoding="utf-8", errors="ignore") as f:
			text = f.read()
	except Exception:
		return {"file": path, "email_address": "", "phone_number": "", "notes": "read_error"}

	emails: List[str] = []
	phones: List[str] = []

	# 1) Raw regex over full text
	emails.extend(EMAIL_RE.findall(text))
	phones.extend(PHONE_RE.findall(text))
	for m in MAILTO_RE.findall(text):
		emails.append(m)

	# 2) Parse JSON-LD and inline JSON blobs
	try:
		soup = BeautifulSoup(text, "html.parser")
		for sc in soup.find_all("script"):
			content = sc.string or sc.text or ""
			if not content:
				continue
			if sc.get("type", "").lower() == "application/ld+json" or "sellerEmail" in content or "__WML_" in content or '"email"' in content:
				try:
					data = json.loads(content)
					_collect_from_json(data, emails, phones)
				except Exception:
					# tolerate JSON issues; still regex scan this block
					emails.extend(EMAIL_RE.findall(content))
					phones.extend(PHONE_RE.findall(content))
	except Exception:
		pass

	# Normalize & filter phones
	phones = [p for p in phones if _valid_phone(p)]
	# Deduplicate preserving order
	def _dedupe(seq: List[str]) -> List[str]:
		seen = set(); out: List[str] = []
		for x in seq:
			x2 = x.strip()
			if not x2 or x2 in seen:
				continue
			seen.add(x2); out.append(x2)
		return out

	emails = _dedupe(emails)
	phones = _dedupe(phones)

	return {
		"file": path,
		"email_address": emails[0] if emails else "",
		"phone_number": phones[0] if phones else "",
		"notes": "" if (emails or phones) else "no_matches",
	}


def write_csv(rows: List[Dict[str, str]], out_dir: str) -> str:
	os.makedirs(out_dir, exist_ok=True)
	ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
	path = os.path.join(out_dir, f"offline_scan_{ts}.csv")
	with open(path, "w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=["file", "email_address", "phone_number", "notes"])
		w.writeheader()
		for r in rows:
			w.writerow(r)
	return path


def main() -> None:
	parser = argparse.ArgumentParser(description="Scan saved HTML files for emails/phones")
	parser.add_argument("--dir", type=str, required=True, help="Directory containing saved HTML files")
	args = parser.parse_args()

	rows: List[Dict[str, str]] = []
	for root, _, files in os.walk(args.dir):
		for name in files:
			if not name.lower().endswith((".html", ".htm", ".txt")):
				continue
			rows.append(scan_file(os.path.join(root, name)))
	out = write_csv(rows, args.dir)
	print(f"Saved: {out}")


if __name__ == "__main__":
	main()
