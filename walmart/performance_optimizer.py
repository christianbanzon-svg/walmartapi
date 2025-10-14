"""
Performance Optimization System
Handles Redis caching, batch processing, connection pooling, and rate limiting
"""
import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
import redis.asyncio as redis

logger = logging.getLogger(__name__)

@dataclass
class CacheStats:
    """Cache performance statistics"""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    hit_rate: float = 0.0
    total_requests: int = 0
    cache_savings: int = 0  # Number of API calls saved

@dataclass
class BatchStats:
    """Batch processing statistics"""
    total_batches: int = 0
    total_items: int = 0
    avg_batch_size: float = 0.0
    processing_time: float = 0.0
    items_per_second: float = 0.0

class RedisCache:
    """Intelligent Redis caching layer"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", default_ttl: int = 3600):
        self.redis_url = redis_url
        self.default_ttl = default_ttl
        self.redis_client: Optional[redis.Redis] = None
        self.stats = CacheStats()
        self.cache_prefix = "walmart_scraper:"
    
    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            logger.info("Connected to Redis cache")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Cache disabled.")
            self.redis_client = None
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis_client:
            await self.redis_client.close()
    
    def _generate_cache_key(self, query_type: str, **params) -> str:
        """Generate cache key from parameters"""
        # Sort parameters for consistent keys
        sorted_params = sorted(params.items())
        param_str = "&".join(f"{k}={v}" for k, v in sorted_params)
        key_hash = hashlib.md5(param_str.encode()).hexdigest()
        return f"{self.cache_prefix}{query_type}:{key_hash}"
    
    async def get(self, query_type: str, **params) -> Optional[Dict[str, Any]]:
        """Get cached data"""
        if not self.redis_client:
            return None
        
        cache_key = self._generate_cache_key(query_type, **params)
        
        try:
            cached_data = await self.redis_client.get(cache_key)
            if cached_data:
                self.stats.hits += 1
                self.stats.total_requests += 1
                self.stats.hit_rate = self.stats.hits / self.stats.total_requests
                logger.debug(f"Cache HIT: {query_type}")
                return json.loads(cached_data)
            else:
                self.stats.misses += 1
                self.stats.total_requests += 1
                self.stats.hit_rate = self.stats.hits / self.stats.total_requests
                logger.debug(f"Cache MISS: {query_type}")
                return None
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    async def set(self, data: Dict[str, Any], query_type: str, ttl: Optional[int] = None, **params):
        """Cache data"""
        if not self.redis_client:
            return
        
        cache_key = self._generate_cache_key(query_type, **params)
        ttl = ttl or self.default_ttl
        
        try:
            await self.redis_client.setex(
                cache_key,
                ttl,
                json.dumps(data, ensure_ascii=False)
            )
            self.stats.sets += 1
            logger.debug(f"Cache SET: {query_type}")
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    async def invalidate_pattern(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        if not self.redis_client:
            return
        
        try:
            keys = await self.redis_client.keys(f"{self.cache_prefix}{pattern}")
            if keys:
                await self.redis_client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache entries")
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'hits': self.stats.hits,
            'misses': self.stats.misses,
            'sets': self.stats.sets,
            'hit_rate': f"{self.stats.hit_rate:.1%}",
            'total_requests': self.stats.total_requests,
            'api_calls_saved': self.stats.hits,
            'cache_efficiency': 'Excellent' if self.stats.hit_rate > 0.3 else 'Good' if self.stats.hit_rate > 0.1 else 'Poor'
        }

class ConnectionPool:
    """Optimized HTTP connection pooling"""
    
    def __init__(self, max_connections: int = 100, max_connections_per_host: int = 30):
        self.max_connections = max_connections
        self.max_connections_per_host = max_connections_per_host
        self.session: Optional[ClientSession] = None
        self.connector: Optional[TCPConnector] = None
    
    async def create_session(self):
        """Create optimized HTTP session"""
        self.connector = TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections_per_host,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = ClientTimeout(total=60, connect=10, sock_read=30)
        
        self.session = ClientSession(
            connector=self.connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Walmart-Scraper/1.0',
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate'
            }
        )
        
        logger.info(f"Created HTTP session with {self.max_connections} max connections")
    
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()

class RateLimiter:
    """Advanced rate limiting system"""
    
    def __init__(self, requests_per_minute: int = 60, burst_limit: int = 10):
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self.request_times: List[float] = []
        self.burst_counter = 0
        self.last_burst_reset = time.time()
    
    async def acquire(self) -> bool:
        """Acquire permission to make a request"""
        current_time = time.time()
        
        # Reset burst counter every minute
        if current_time - self.last_burst_reset > 60:
            self.burst_counter = 0
            self.last_burst_reset = current_time
        
        # Check burst limit
        if self.burst_counter >= self.burst_limit:
            await asyncio.sleep(1)
            return False
        
        # Check rate limit
        minute_ago = current_time - 60
        self.request_times = [t for t in self.request_times if t > minute_ago]
        
        if len(self.request_times) >= self.requests_per_minute:
            # Calculate wait time
            oldest_request = min(self.request_times)
            wait_time = 60 - (current_time - oldest_request) + 1
            await asyncio.sleep(wait_time)
            return False
        
        # Allow request
        self.request_times.append(current_time)
        self.burst_counter += 1
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """Get rate limiter status"""
        current_time = time.time()
        minute_ago = current_time - 60
        recent_requests = len([t for t in self.request_times if t > minute_ago])
        
        return {
            'requests_last_minute': recent_requests,
            'requests_per_minute_limit': self.requests_per_minute,
            'burst_usage': self.burst_counter,
            'burst_limit': self.burst_limit,
            'status': 'Normal' if recent_requests < self.requests_per_minute * 0.8 else 'Approaching Limit'
        }

class BatchProcessor:
    """Optimized batch processing system"""
    
    def __init__(self, batch_size: int = 50, max_concurrent: int = 10):
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.stats = BatchStats()
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
    
    async def process_batches(self, items: List[Any], process_func, **kwargs) -> List[Any]:
        """Process items in optimized batches"""
        start_time = time.time()
        total_items = len(items)
        
        # Create batches
        batches = [items[i:i + self.batch_size] for i in range(0, total_items, self.batch_size)]
        self.stats.total_batches = len(batches)
        self.stats.total_items = total_items
        
        logger.info(f"Processing {total_items} items in {len(batches)} batches of {self.batch_size}")
        
        # Process batches concurrently
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_batch(batch):
            async with semaphore:
                return await process_func(batch, **kwargs)
        
        # Execute batches
        batch_tasks = [process_batch(batch) for batch in batches]
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Flatten results
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing error: {result}")
            else:
                if isinstance(result, list):
                    processed_results.extend(result)
                else:
                    processed_results.append(result)
        
        # Update statistics
        processing_time = time.time() - start_time
        self.stats.processing_time = processing_time
        self.stats.avg_batch_size = total_items / len(batches)
        self.stats.items_per_second = total_items / processing_time if processing_time > 0 else 0
        
        logger.info(f"Batch processing complete: {self.stats.items_per_second:.1f} items/sec")
        return processed_results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch processing statistics"""
        return {
            'total_batches': self.stats.total_batches,
            'total_items': self.stats.total_items,
            'avg_batch_size': f"{self.stats.avg_batch_size:.1f}",
            'processing_time': f"{self.stats.processing_time:.2f}s",
            'items_per_second': f"{self.stats.items_per_second:.1f}",
            'efficiency': 'Excellent' if self.stats.items_per_second > 10 else 'Good' if self.stats.items_per_second > 5 else 'Poor'
        }

class PerformanceOptimizer:
    """Main performance optimization system"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.cache = RedisCache(redis_url)
        self.connection_pool = ConnectionPool()
        self.rate_limiter = RateLimiter()
        self.batch_processor = BatchProcessor()
        self.optimization_enabled = True
    
    async def initialize(self):
        """Initialize all optimization components"""
        await self.cache.connect()
        await self.connection_pool.create_session()
        logger.info("Performance optimizer initialized")
    
    async def shutdown(self):
        """Shutdown all optimization components"""
        await self.cache.disconnect()
        await self.connection_pool.close()
        logger.info("Performance optimizer shutdown")
    
    async def get_cached_search(self, query: str, page: int = 1, domain: str = "walmart.com") -> Optional[Dict[str, Any]]:
        """Get cached search results"""
        return await self.cache.get("search", query=query, page=page, domain=domain)
    
    async def cache_search_result(self, data: Dict[str, Any], query: str, page: int = 1, domain: str = "walmart.com", ttl: int = 1800):
        """Cache search results (30 minutes TTL)"""
        await self.cache.set(data, "search", ttl, query=query, page=page, domain=domain)
    
    async def get_cached_product(self, product_id: str, domain: str = "walmart.com") -> Optional[Dict[str, Any]]:
        """Get cached product data"""
        return await self.cache.get("product", product_id=product_id, domain=domain)
    
    async def cache_product_data(self, data: Dict[str, Any], product_id: str, domain: str = "walmart.com", ttl: int = 7200):
        """Cache product data (2 hours TTL)"""
        await self.cache.set(data, "product", ttl, product_id=product_id, domain=domain)
    
    async def rate_limit_check(self) -> bool:
        """Check if request is allowed by rate limiter"""
        return await self.rate_limiter.acquire()
    
    async def batch_process_items(self, items: List[Any], process_func, **kwargs) -> List[Any]:
        """Process items in optimized batches"""
        return await self.batch_processor.process_batches(items, process_func, **kwargs)
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive performance report"""
        return {
            'cache_stats': self.cache.get_stats(),
            'rate_limiter_status': self.rate_limiter.get_status(),
            'batch_processing_stats': self.batch_processor.get_stats(),
            'optimization_status': '✅ Active' if self.optimization_enabled else '❌ Disabled',
            'estimated_api_savings': f"{self.cache.stats.hits} calls saved",
            'performance_grade': self._calculate_performance_grade()
        }
    
    def _calculate_performance_grade(self) -> str:
        """Calculate overall performance grade"""
        cache_hit_rate = self.cache.stats.hit_rate
        items_per_sec = self.batch_processor.stats.items_per_second
        
        if cache_hit_rate > 0.3 and items_per_sec > 10:
            return "A+ (Excellent)"
        elif cache_hit_rate > 0.2 and items_per_sec > 5:
            return "A (Very Good)"
        elif cache_hit_rate > 0.1 and items_per_sec > 2:
            return "B (Good)"
        else:
            return "C (Needs Improvement)"

# Global performance optimizer instance
performance_optimizer = PerformanceOptimizer()

