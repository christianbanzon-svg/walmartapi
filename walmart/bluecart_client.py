import random
import time
from typing import Any, Dict, List, Optional

import requests

from config import get_config

# Phase 3: Import performance optimization modules
try:
	from api_cache import InMemoryCache, RequestDeduplicator, RateLimitMonitor
	PERFORMANCE_OPTIMIZATION_AVAILABLE = True
except ImportError:
	PERFORMANCE_OPTIMIZATION_AVAILABLE = False


class BlueCartClient:
	def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, source: Optional[str] = None, site: Optional[str] = None, sleep_seconds: float = 0.0, max_retries: int = 4, retry_backoff_seconds: float = 1.75, request_timeout_seconds: float = 300.0, enable_cache: bool = True, enable_deduplication: bool = True, enable_rate_limit: bool = True):
		cfg = get_config()
		self.api_key = api_key or cfg.api_key
		self.base_url = base_url or cfg.base_url
		self.source = source or "walmart"
		self.site = site or cfg.site
		self.sleep_seconds = sleep_seconds
		self.max_retries = max_retries
		self.retry_backoff_seconds = retry_backoff_seconds
		self.request_timeout_seconds = request_timeout_seconds
		
		# Phase 3: Initialize performance optimization
		if PERFORMANCE_OPTIMIZATION_AVAILABLE:
			self.cache = InMemoryCache(max_size=1000, default_ttl=3600) if enable_cache else None
			self.deduplicator = RequestDeduplicator(dedup_window_seconds=5) if enable_deduplication else None
			self.rate_limiter = RateLimitMonitor(max_calls_per_minute=60, max_calls_per_hour=1000) if enable_rate_limit else None
		else:
			self.cache = None
			self.deduplicator = None
			self.rate_limiter = None

	def _request(self, params: Dict[str, Any], endpoint: str = "api") -> Dict[str, Any]:
		"""
		Make API request with caching, deduplication, and rate limiting.
		
		Args:
			params: Request parameters
			endpoint: Endpoint name for caching/deduplication (e.g., "search", "product")
		"""
		merged = {
			"api_key": self.api_key,
			"source": self.source,
			"walmart_domain": self.site,
		}
		merged.update(params)
		
		# Phase 3: Check cache first
		if self.cache:
			cached = self.cache.get(endpoint, merged)
			if cached:
				return cached
		
		# Phase 3: Check for duplicate requests
		if self.deduplicator:
			if self.deduplicator.should_skip(endpoint, merged):
				# Duplicate request - return cached if available, otherwise skip
				if self.cache:
					cached = self.cache.get(endpoint, merged)
					if cached:
						return cached
				# No cache available - proceed with request anyway
		
		# Phase 3: Rate limiting
		if self.rate_limiter:
			delay = self.rate_limiter.record_call()
			if delay > 0:
				time.sleep(delay)
		
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
				result = response.json()
				
				# Phase 3: Cache successful response
				if self.cache:
					self.cache.set(endpoint, merged, result)
				
				return result
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
		return self._request(payload, endpoint="search")

	def product(self, product_id: str) -> Dict[str, Any]:
		# BlueCart standard: type=product
		return self._request({
			"type": "product",
			"item_id": product_id,
		}, endpoint="product")

	def offers(self, product_id: str, page: int = 1) -> Dict[str, Any]:
		# BlueCart standard: type=offers (if supported for site)
		return self._request({
			"type": "offers",
			"item_id": product_id,
			"page": page,
		}, endpoint="offers")

	def seller_profile(self, seller_id: Optional[str] = None, url: Optional[str] = None) -> Dict[str, Any]:
		payload: Dict[str, Any] = {"type": "seller_profile"}
		if seller_id:
			payload["seller_id"] = seller_id
		if url:
			payload["url"] = url
		return self._request(payload, endpoint="seller_profile")
	
	def get_performance_stats(self) -> Dict[str, Any]:
		"""Get performance optimization statistics"""
		stats = {}
		if self.cache:
			stats["cache"] = self.cache.get_stats()
		if self.deduplicator:
			stats["deduplication"] = self.deduplicator.get_stats()
		if self.rate_limiter:
			stats["rate_limiting"] = self.rate_limiter.get_stats()
		return stats


