"""
Enhanced Reliability System
Comprehensive error handling, retry logic with exponential backoff, and circuit breaker patterns
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import traceback
import json
from functools import wraps

logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class RetryStrategy(Enum):
    """Retry strategies"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    NO_RETRY = "no_retry"

@dataclass
class RetryConfig:
    """Retry configuration"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF

@dataclass
class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back

@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: type = Exception

@dataclass
class ErrorContext:
    """Error context information"""
    error_type: str
    error_message: str
    severity: ErrorSeverity
    timestamp: datetime
    context: Dict[str, Any]
    stack_trace: Optional[str] = None
    retry_count: int = 0
    is_recoverable: bool = True

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.next_attempt_time: Optional[datetime] = None
    
    def can_execute(self) -> bool:
        """Check if execution is allowed"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            if self.next_attempt_time and datetime.now() >= self.next_attempt_time:
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
        
        # HALF_OPEN state
        return True
    
    def on_success(self):
        """Handle successful execution"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            logger.info("Circuit breaker: Service recovered, circuit closed")
    
    def on_failure(self, error: Exception):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            # Half-open failed, go back to open
            self.state = CircuitBreakerState.OPEN
            self.next_attempt_time = datetime.now() + timedelta(seconds=self.config.recovery_timeout)
            logger.warning("Circuit breaker: Service still failing, circuit reopened")
        elif self.failure_count >= self.config.failure_threshold:
            # Too many failures, open circuit
            self.state = CircuitBreakerState.OPEN
            self.next_attempt_time = datetime.now() + timedelta(seconds=self.config.recovery_timeout)
            logger.error(f"Circuit breaker: Circuit opened after {self.failure_count} failures")
    
    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status"""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'next_attempt': self.next_attempt_time.isoformat() if self.next_attempt_time else None,
            'is_healthy': self.state == CircuitBreakerState.CLOSED
        }

class RetryManager:
    """Advanced retry management with exponential backoff"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.retry_stats = {
            'total_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'circuit_breaker_hits': 0
        }
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        if self.config.strategy == RetryStrategy.NO_RETRY:
            return 0
        
        if self.config.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.config.base_delay
        elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * attempt
        else:  # EXPONENTIAL_BACKOFF
            delay = self.config.base_delay * (self.config.backoff_multiplier ** (attempt - 1))
        
        # Apply jitter to prevent thundering herd
        if self.config.jitter:
            jitter = random.uniform(0.1, 0.3) * delay
            delay += jitter
        
        return min(delay, self.config.max_delay)
    
    async def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        last_error = None
        
        for attempt in range(1, self.config.max_retries + 1):
            self.retry_stats['total_attempts'] += 1
            
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                if attempt > 1:
                    self.retry_stats['successful_retries'] += 1
                    logger.info(f"Retry successful on attempt {attempt}")
                
                return result
            
            except Exception as e:
                last_error = e
                
                if attempt == self.config.max_retries:
                    self.retry_stats['failed_retries'] += 1
                    logger.error(f"All {self.config.max_retries} retry attempts failed")
                    break
                
                delay = self.calculate_delay(attempt)
                logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay:.2f}s")
                await asyncio.sleep(delay)
        
        # All retries failed
        raise last_error
    
    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics"""
        total_retries = self.retry_stats['successful_retries'] + self.retry_stats['failed_retries']
        success_rate = (self.retry_stats['successful_retries'] / total_retries * 100) if total_retries > 0 else 0
        
        return {
            'total_attempts': self.retry_stats['total_attempts'],
            'successful_retries': self.retry_stats['successful_retries'],
            'failed_retries': self.retry_stats['failed_retries'],
            'success_rate': f"{success_rate:.1f}%",
            'retry_efficiency': 'Excellent' if success_rate > 80 else 'Good' if success_rate > 60 else 'Poor'
        }

class ErrorHandler:
    """Comprehensive error handling system"""
    
    def __init__(self):
        self.error_log: List[ErrorContext] = []
        self.error_counts: Dict[str, int] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
    
    def create_circuit_breaker(self, name: str, config: CircuitBreakerConfig) -> CircuitBreaker:
        """Create a named circuit breaker"""
        breaker = CircuitBreaker(config)
        self.circuit_breakers[name] = breaker
        return breaker
    
    def log_error(self, error: Exception, context: Dict[str, Any], severity: ErrorSeverity = ErrorSeverity.MEDIUM) -> ErrorContext:
        """Log error with context"""
        error_context = ErrorContext(
            error_type=type(error).__name__,
            error_message=str(error),
            severity=severity,
            timestamp=datetime.now(),
            context=context,
            stack_trace=traceback.format_exc(),
            is_recoverable=self._is_recoverable_error(error)
        )
        
        self.error_log.append(error_context)
        self.error_counts[error_context.error_type] = self.error_counts.get(error_context.error_type, 0) + 1
        
        logger.error(f"Error logged: {error_context.error_type} - {error_context.error_message}")
        return error_context
    
    def _is_recoverable_error(self, error: Exception) -> bool:
        """Determine if error is recoverable"""
        recoverable_errors = (
            ConnectionError,
            TimeoutError,
            OSError,
            # Add more recoverable error types
        )
        return isinstance(error, recoverable_errors)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary statistics"""
        total_errors = len(self.error_log)
        critical_errors = len([e for e in self.error_log if e.severity == ErrorSeverity.CRITICAL])
        recoverable_errors = len([e for e in self.error_log if e.is_recoverable])
        
        return {
            'total_errors': total_errors,
            'critical_errors': critical_errors,
            'recoverable_errors': recoverable_errors,
            'error_types': self.error_counts,
            'recent_errors': [self._format_error(e) for e in self.error_log[-5:]],
            'health_status': 'Healthy' if critical_errors == 0 else 'Degraded' if critical_errors < 3 else 'Critical'
        }
    
    def _format_error(self, error_context: ErrorContext) -> Dict[str, Any]:
        """Format error for reporting"""
        return {
            'type': error_context.error_type,
            'message': error_context.error_message,
            'severity': error_context.severity.value,
            'timestamp': error_context.timestamp.isoformat(),
            'recoverable': error_context.is_recoverable
        }
    
    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """Get all circuit breaker statuses"""
        return {
            name: breaker.get_status() 
            for name, breaker in self.circuit_breakers.items()
        }

def retry_with_circuit_breaker(
    retry_config: RetryConfig = RetryConfig(),
    circuit_breaker_config: CircuitBreakerConfig = CircuitBreakerConfig(),
    circuit_breaker_name: str = "default"
):
    """Decorator for retry with circuit breaker"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get or create circuit breaker
            error_handler = ReliabilityManager.get_instance().error_handler
            if circuit_breaker_name not in error_handler.circuit_breakers:
                error_handler.create_circuit_breaker(circuit_breaker_name, circuit_breaker_config)
            
            circuit_breaker = error_handler.circuit_breakers[circuit_breaker_name]
            
            # Check if circuit allows execution
            if not circuit_breaker.can_execute():
                logger.warning(f"Circuit breaker {circuit_breaker_name} is open, skipping execution")
                return None
            
            # Create retry manager
            retry_manager = RetryManager(retry_config)
            
            try:
                result = await retry_manager.execute_with_retry(func, *args, **kwargs)
                circuit_breaker.on_success()
                return result
            
            except Exception as e:
                circuit_breaker.on_failure(e)
                error_handler.log_error(e, {'function': func.__name__}, ErrorSeverity.MEDIUM)
                raise
        
        return wrapper
    return decorator

class ReliabilityManager:
    """Main reliability management system"""
    
    _instance = None
    
    def __init__(self):
        self.error_handler = ErrorHandler()
        self.retry_configs: Dict[str, RetryConfig] = {}
        self.is_initialized = False
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def initialize(self):
        """Initialize reliability system"""
        # Create default circuit breakers
        self.error_handler.create_circuit_breaker(
            "bluecart_api",
            CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=60.0,
                expected_exception=Exception
            )
        )
        
        self.error_handler.create_circuit_breaker(
            "database_operations",
            CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30.0,
                expected_exception=Exception
            )
        )
        
        # Setup default retry configurations
        self.retry_configs = {
            "api_calls": RetryConfig(
                max_retries=3,
                base_delay=1.0,
                max_delay=30.0,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            ),
            "database_operations": RetryConfig(
                max_retries=2,
                base_delay=0.5,
                max_delay=10.0,
                strategy=RetryStrategy.LINEAR_BACKOFF
            ),
            "file_operations": RetryConfig(
                max_retries=2,
                base_delay=0.2,
                max_delay=5.0,
                strategy=RetryStrategy.FIXED_DELAY
            )
        }
        
        self.is_initialized = True
        logger.info("Reliability system initialized")
    
    def get_reliability_report(self) -> Dict[str, Any]:
        """Get comprehensive reliability report"""
        return {
            'error_summary': self.error_handler.get_error_summary(),
            'circuit_breaker_status': self.error_handler.get_circuit_breaker_status(),
            'system_health': self._calculate_system_health(),
            'recommendations': self._get_recommendations()
        }
    
    def _calculate_system_health(self) -> str:
        """Calculate overall system health"""
        error_summary = self.error_handler.get_error_summary()
        circuit_status = self.error_handler.get_circuit_breaker_status()
        
        if error_summary['critical_errors'] > 0:
            return "Critical"
        
        open_circuits = sum(1 for cb in circuit_status.values() if cb['state'] == 'open')
        if open_circuits > 0:
            return "Degraded"
        
        if error_summary['total_errors'] > 10:
            return "Warning"
        
        return "Healthy"
    
    def _get_recommendations(self) -> List[str]:
        """Get system recommendations based on current state"""
        recommendations = []
        error_summary = self.error_handler.get_error_summary()
        
        if error_summary['critical_errors'] > 0:
            recommendations.append("🚨 Address critical errors immediately")
        
        if error_summary['total_errors'] > 20:
            recommendations.append("⚠️ High error rate detected - review error logs")
        
        circuit_status = self.error_handler.get_circuit_breaker_status()
        open_circuits = [name for name, cb in circuit_status.items() if cb['state'] == 'open']
        if open_circuits:
            recommendations.append(f"🔧 Circuit breakers open: {', '.join(open_circuits)}")
        
        if not recommendations:
            recommendations.append("✅ System operating normally")
        
        return recommendations

# Global reliability manager instance
reliability_manager = ReliabilityManager()

