"""
Phase 3: Performance Optimization - API Caching Layer
In-memory caching with optional Redis support for API responses
"""
import json
import hashlib
import time
from typing import Any, Dict, Optional, Tuple
from threading import Lock
from collections import OrderedDict


class InMemoryCache:
	"""Thread-safe in-memory cache with LRU eviction"""
	
	def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
		"""
		Initialize in-memory cache.
		
		Args:
			max_size: Maximum number of cached items
			default_ttl: Default time-to-live in seconds (1 hour)
		"""
		self.max_size = max_size
		self.default_ttl = default_ttl
		self._cache: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
		self._lock = Lock()
		self.stats = {
			"hits": 0,
			"misses": 0,
			"sets": 0,
			"evictions": 0,
		}
	
	def _generate_key(self, endpoint: str, params: Dict[str, Any]) -> str:
		"""Generate cache key from endpoint and params"""
		# Sort params for consistent keys
		key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
		return hashlib.md5(key_data.encode()).hexdigest()
	
	def get(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
		"""Get cached response"""
		with self._lock:
			key = self._generate_key(endpoint, params)
			
			if key not in self._cache:
				self.stats["misses"] += 1
				return None
			
			expiry_time, data = self._cache[key]
			
			# Check if expired
			if time.time() > expiry_time:
				del self._cache[key]
				self.stats["misses"] += 1
				return None
			
			# Move to end (LRU)
			self._cache.move_to_end(key)
			self.stats["hits"] += 1
			return data
	
	def set(self, endpoint: str, params: Dict[str, Any], data: Dict[str, Any], ttl: Optional[int] = None) -> None:
		"""Cache response"""
		with self._lock:
			key = self._generate_key(endpoint, params)
			ttl = ttl or self.default_ttl
			expiry_time = time.time() + ttl
			
			# Evict if at max size
			if len(self._cache) >= self.max_size and key not in self._cache:
				# Remove oldest item (LRU)
				self._cache.popitem(last=False)
				self.stats["evictions"] += 1
			
			self._cache[key] = (expiry_time, data)
			self._cache.move_to_end(key)  # Move to end (most recently used)
			self.stats["sets"] += 1
	
	def clear(self) -> None:
		"""Clear all cached items"""
		with self._lock:
			self._cache.clear()
			self.stats = {"hits": 0, "misses": 0, "sets": 0, "evictions": 0}
	
	def get_stats(self) -> Dict[str, Any]:
		"""Get cache statistics"""
		with self._lock:
			total_requests = self.stats["hits"] + self.stats["misses"]
			hit_rate = (self.stats["hits"] / total_requests * 100) if total_requests > 0 else 0
			return {
				**self.stats,
				"total_requests": total_requests,
				"hit_rate_percent": round(hit_rate, 2),
				"cache_size": len(self._cache),
				"max_size": self.max_size,
			}


class RequestDeduplicator:
	"""Prevent duplicate API requests within a short time window"""
	
	def __init__(self, dedup_window_seconds: int = 5):
		"""
		Initialize request deduplicator.
		
		Args:
			dedup_window_seconds: Time window to consider requests as duplicates
		"""
		self.dedup_window = dedup_window_seconds
		self._recent_requests: Dict[str, float] = {}  # key -> timestamp
		self._lock = Lock()
		self.stats = {
			"deduplicated": 0,
			"unique_requests": 0,
		}
	
	def _generate_key(self, endpoint: str, params: Dict[str, Any]) -> str:
		"""Generate request key"""
		key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
		return hashlib.md5(key_data.encode()).hexdigest()
	
	def should_skip(self, endpoint: str, params: Dict[str, Any]) -> bool:
		"""
		Check if request should be skipped (recent duplicate).
		
		Args:
			endpoint: API endpoint name
			params: Request parameters
		
		Returns:
			True if request should be skipped (duplicate)
		"""
		key = self._generate_key(endpoint, params)
		now = time.time()
		
		with self._lock:
			# Clean old entries
			self._recent_requests = {
				k: v for k, v in self._recent_requests.items()
				if now - v < self.dedup_window
			}
			
			# Check if duplicate
			if key in self._recent_requests:
				self.stats["deduplicated"] += 1
				return True
			
			# Record new request
			self._recent_requests[key] = now
			self.stats["unique_requests"] += 1
			return False
	
	def get_stats(self) -> Dict[str, Any]:
		"""Get deduplication statistics"""
		with self._lock:
			now = time.time()
			# Clean old entries for accurate count
			self._recent_requests = {
				k: v for k, v in self._recent_requests.items()
				if now - v < self.dedup_window
			}
			return {
				**self.stats,
				"recent_requests": len(self._recent_requests),
				"dedup_window_seconds": self.dedup_window,
			}


class RateLimitMonitor:
	"""Monitor API call rate and provide adaptive delays"""
	
	def __init__(self, max_calls_per_minute: int = 60, max_calls_per_hour: int = 1000):
		"""
		Initialize rate limit monitor.
		
		Args:
			max_calls_per_minute: Maximum API calls per minute
			max_calls_per_hour: Maximum API calls per hour
		"""
		self.max_per_minute = max_calls_per_minute
		self.max_per_hour = max_calls_per_hour
		self._call_times: list = []
		self._lock = Lock()
		self.stats = {
			"total_calls": 0,
			"rate_limited": 0,
			"average_delay": 0.0,
		}
	
	def record_call(self) -> float:
		"""
		Record an API call and return required delay.
		
		Returns:
			Delay in seconds before next call
		"""
		with self._lock:
			now = time.time()
			self._call_times.append(now)
			self.stats["total_calls"] += 1
			
			# Clean old entries (older than 1 hour)
			one_hour_ago = now - 3600
			self._call_times = [t for t in self._call_times if t > one_hour_ago]
			
			# Check rate limits
			one_minute_ago = now - 60
			calls_last_minute = len([t for t in self._call_times if t > one_minute_ago])
			calls_last_hour = len(self._call_times)
			
			# Calculate required delay
			delay = 0.0
			
			if calls_last_minute >= self.max_per_minute:
				# Rate limited - need to wait
				oldest_in_minute = min([t for t in self._call_times if t > one_minute_ago])
				delay = max(0, 60 - (now - oldest_in_minute))
				self.stats["rate_limited"] += 1
			elif calls_last_hour >= self.max_per_hour:
				# Hourly limit approaching
				delay = 0.1  # Small delay to slow down
			
			# Update average delay
			if delay > 0:
				current_avg = self.stats["average_delay"]
				total = self.stats["total_calls"]
				self.stats["average_delay"] = ((current_avg * (total - 1)) + delay) / total
			
			return delay
	
	def get_stats(self) -> Dict[str, Any]:
		"""Get rate limit statistics"""
		with self._lock:
			now = time.time()
			one_minute_ago = now - 60
			one_hour_ago = now - 3600
			
			calls_last_minute = len([t for t in self._call_times if t > one_minute_ago])
			calls_last_hour = len([t for t in self._call_times if t > one_hour_ago])
			
			return {
				**self.stats,
				"calls_last_minute": calls_last_minute,
				"calls_last_hour": calls_last_hour,
				"max_per_minute": self.max_per_minute,
				"max_per_hour": self.max_per_hour,
				"utilization_percent_minute": round((calls_last_minute / self.max_per_minute * 100), 2),
				"utilization_percent_hour": round((calls_last_hour / self.max_per_hour * 100), 2),
			}

