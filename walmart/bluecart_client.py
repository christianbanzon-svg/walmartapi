import random
import time
from typing import Any, Dict, List, Optional

import requests

from config import get_config


class BlueCartClient:
	def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, source: Optional[str] = None, site: Optional[str] = None, sleep_seconds: float = 0.0, max_retries: int = 4, retry_backoff_seconds: float = 1.75, request_timeout_seconds: float = 60.0):
		cfg = get_config()
		self.api_key = api_key or cfg.api_key
		self.base_url = base_url or cfg.base_url
		self.source = source or "walmart"
		self.site = site or cfg.site
		self.sleep_seconds = sleep_seconds
		self.max_retries = max_retries
		self.retry_backoff_seconds = retry_backoff_seconds
		self.request_timeout_seconds = request_timeout_seconds

	def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
		merged = {
			"api_key": self.api_key,
			"source": self.source,
			"walmart_domain": self.site,
		}
		merged.update(params)
		attempt = 0
		while True:
			attempt += 1
			try:
				response = requests.get(self.base_url, params=merged, timeout=self.request_timeout_seconds)
				status = response.status_code
				if status == 429 or status >= 500:
					if attempt <= self.max_retries:
						delay = self.retry_backoff_seconds * (2 ** (attempt - 1))
						delay = delay * (0.75 + random.random() * 0.5)
						time.sleep(delay)
						continue
					response.raise_for_status()
				elif 400 <= status < 500:
					# On client errors, BlueCart often returns a JSON body with request_info/message.
					# Try to return that JSON so callers can handle gracefully.
					try:
						data = response.json()
						return data
					except ValueError:
						response.raise_for_status()
				# success
				if self.sleep_seconds:
					time.sleep(self.sleep_seconds)
				return response.json()
			except requests.RequestException:
				if attempt <= self.max_retries:
					delay = self.retry_backoff_seconds * (2 ** (attempt - 1))
					delay = delay * (0.75 + random.random() * 0.5)
					time.sleep(delay)
					continue
				raise

	def search(self, query: str, page: int = 1, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		# BlueCart standard: type=search
		payload: Dict[str, Any] = {
			"type": "search",
			"search_term": query,
			"page": page,
		}
		if extra:
			# Map known aliases
			mapped: Dict[str, Any] = dict(extra)
			payload.update(mapped)
		return self._request(payload)

	def product(self, product_id: str) -> Dict[str, Any]:
		# BlueCart standard: type=product
		return self._request({
			"type": "product",
			"item_id": product_id,
		})

	def offers(self, product_id: str, page: int = 1) -> Dict[str, Any]:
		# BlueCart standard: type=offers (if supported for site)
		return self._request({
			"type": "offers",
			"item_id": product_id,
			"page": page,
		})

	def seller_profile(self, seller_id: Optional[str] = None, url: Optional[str] = None) -> Dict[str, Any]:
		payload: Dict[str, Any] = {"type": "seller_profile"}
		if seller_id:
			payload["seller_id"] = seller_id
		if url:
			payload["url"] = url
		return self._request(payload)


