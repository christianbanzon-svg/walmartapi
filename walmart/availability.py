from typing import Optional, Dict, Any, List

import json
import re
import requests
from bs4 import BeautifulSoup


def _safe_int(value: Any) -> Optional[int]:
	try:
		iv = int(str(value).strip())
		return iv if iv >= 0 else None
	except Exception:
		return None


def probe_walmart_availability(listing_url: Optional[str]) -> Dict[str, Optional[int]]:
	"""Best-effort probe for units available / max order quantity by parsing product page.

	Returns:
		{"units_available": Optional[int], "max_order_quantity": Optional[int]}
	"""
	result: Dict[str, Optional[int]] = {"units_available": None, "max_order_quantity": None}
	if not listing_url:
		return result
	try:
		resp = requests.get(listing_url, timeout=30)
		resp.raise_for_status()
		html = resp.text
		soup = BeautifulSoup(html, "html.parser")
		# JSON-LD often contains stock info
		for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
			try:
				data = json.loads(script.string or '{}')
			except Exception:
				continue
			nodes: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
			for node in nodes:
				if not isinstance(node, dict):
					continue
				# Look for offers -> inventory/availability or order limits
				offers = node.get('offers')
				if isinstance(offers, dict):
					# Some schemas include inventoryLevel, availableQuantity, or maxOrderQuantity
					qty = offers.get('inventoryLevel') or offers.get('availableQuantity') or offers.get('availableDeliveryQuantity')
					max_qty = offers.get('maxOrderQuantity') or offers.get('orderLimit') or offers.get('maxPurchaseQuantity')
					qty_i = _safe_int(qty)
					max_i = _safe_int(max_qty)
					if qty_i is not None:
						result["units_available"] = qty_i
					if max_i is not None:
						result["max_order_quantity"] = max_i
		# Heuristic: search raw HTML for known keys
		for key in ["maxOrderQuantity", "orderLimit", "maxPurchaseQuantity", "availableQuantity", "inventoryLevel", "availableDeliveryQuantity"]:
			m = re.search(rf"\b{key}\b\s*[:=]\s*(\d+)", html)
			if m:
				val = _safe_int(m.group(1))
				if val is not None:
					if key in ("maxOrderQuantity", "orderLimit", "maxPurchaseQuantity"):
						result["max_order_quantity"] = result["max_order_quantity"] or val
					else:
						result["units_available"] = result["units_available"] or val
		return result
	except Exception:
		return result



