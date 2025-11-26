"""Comprehensive error handling and recovery system for scrapers"""
import time
import logging
from enum import Enum
from typing import Optional, Callable, Any
from datetime import datetime, timedelta


class ErrorType(Enum):
    """Classification of different error types"""
    NETWORK = "network"
    TIMEOUT = "timeout"
    BLOCKED = "blocked"
    INVALID_SESSION = "invalid_session"
    PAGE_ERROR = "page_error"
    ELEMENT_NOT_FOUND = "element_not_found"
    MODAL_INTERRUPTION = "modal_interruption"
    RATE_LIMIT = "rate_limit"
    CAPTCHA = "captcha"
    CLOUDFLARE = "cloudflare"
    REDIRECT = "redirect"
    JAVASCRIPT_ERROR = "javascript_error"
    MEMORY_ERROR = "memory_error"
    UNKNOWN = "unknown"


class ErrorHandler:
    """Smart error handler with classification and recovery strategies"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.error_counts = {error_type: 0 for error_type in ErrorType}
        self.last_error_time = {}
        self.circuit_breaker_threshold = 20  # Increased from 5 to 20
        self.circuit_breaker_reset_time = 300  # Reset after 5 minutes
        
    def classify_error(self, error: Exception) -> ErrorType:
        """Classify error type based on exception message and type"""
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        # Network errors
        if any(keyword in error_str for keyword in ['connection', 'network', 'dns', 'err_', '10060', 'timeout', 'timed out']):
            if 'timeout' in error_str or 'timed out' in error_str:
                return ErrorType.TIMEOUT
            return ErrorType.NETWORK
        
        # Session errors
        if 'invalid session' in error_str or 'session id' in error_str:
            return ErrorType.INVALID_SESSION
        
        # Blocking errors
        if any(keyword in error_str for keyword in ['blocked', 'access denied', '403', 'forbidden']):
            return ErrorType.BLOCKED
        
        # Rate limiting
        if any(keyword in error_str for keyword in ['rate limit', 'too many requests', '429']):
            return ErrorType.RATE_LIMIT
        
        # CAPTCHA
        if 'captcha' in error_str:
            return ErrorType.CAPTCHA
        
        # Cloudflare
        if any(keyword in error_str for keyword in ['cloudflare', 'cf-ray', 'checking your browser', 'just a moment', 'ddos protection']):
            return ErrorType.CLOUDFLARE
        
        # Page errors
        if any(keyword in error_str for keyword in ['404', 'not found', 'page error']):
            return ErrorType.PAGE_ERROR
        
        # Element not found
        if any(keyword in error_str for keyword in ['no such element', 'element not found', 'element not visible']):
            return ErrorType.ELEMENT_NOT_FOUND
        
        # JavaScript errors
        if 'javascript' in error_str or 'js error' in error_str:
            return ErrorType.JAVASCRIPT_ERROR
        
        # Memory errors
        if 'memory' in error_str or 'out of memory' in error_str:
            return ErrorType.MEMORY_ERROR
        
        return ErrorType.UNKNOWN
    
    def get_recovery_strategy(self, error_type: ErrorType, retry_count: int) -> dict:
        """Get recovery strategy based on error type and retry count"""
        strategies = {
            ErrorType.NETWORK: {
                'should_retry': retry_count < 3,
                'wait_time': (5, 10),
                'action': 'wait_and_retry',
                'requires_reinit': False
            },
            ErrorType.TIMEOUT: {
                'should_retry': retry_count < 3,
                'wait_time': (3, 8),
                'action': 'get_partial_content',
                'requires_reinit': False
            },
            ErrorType.BLOCKED: {
                'should_retry': retry_count < 5,
                'wait_time': (30 + retry_count * 10, 45 + retry_count * 15),
                'action': 'extended_wait',
                'requires_reinit': retry_count >= 2
            },
            ErrorType.INVALID_SESSION: {
                'should_retry': retry_count < 3,
                'wait_time': (2, 5),
                'action': 'reinitialize_driver',
                'requires_reinit': True
            },
            ErrorType.RATE_LIMIT: {
                'should_retry': retry_count < 3,
                'wait_time': (60, 120),
                'action': 'extended_wait',
                'requires_reinit': False
            },
            ErrorType.CAPTCHA: {
                'should_retry': False,
                'wait_time': (0, 0),
                'action': 'manual_intervention',
                'requires_reinit': False
            },
            ErrorType.CLOUDFLARE: {
                'should_retry': True,
                'wait_time': (5, 15),
                'action': 'wait_for_cloudflare',
                'requires_reinit': False
            },
            ErrorType.PAGE_ERROR: {
                'should_retry': retry_count < 2,
                'wait_time': (2, 5),
                'action': 'skip',
                'requires_reinit': False
            },
            ErrorType.ELEMENT_NOT_FOUND: {
                'should_retry': retry_count < 2,
                'wait_time': (1, 3),
                'action': 'use_fallback',
                'requires_reinit': False
            },
            ErrorType.UNKNOWN: {
                'should_retry': retry_count < 2,
                'wait_time': (3, 6),
                'action': 'wait_and_retry',
                'requires_reinit': False
            }
        }
        
        return strategies.get(error_type, strategies[ErrorType.UNKNOWN])
    
    def should_continue(self, error_type: ErrorType) -> bool:
        """Check if we should continue after this error type"""
        # Circuit breaker: stop if too many consecutive errors
        if self.error_counts[error_type] >= self.circuit_breaker_threshold:
            last_time = self.last_error_time.get(error_type)
            if last_time:
                time_since = (datetime.now() - last_time).total_seconds()
                if time_since < self.circuit_breaker_reset_time:
                    self.logger.error(f"Circuit breaker triggered for {error_type.value} - too many consecutive errors")
                    return False
        
        # Don't continue for certain error types
        if error_type in [ErrorType.CAPTCHA, ErrorType.MEMORY_ERROR]:
            return False
        
        return True
    
    def record_error(self, error_type: ErrorType):
        """Record error occurrence"""
        self.error_counts[error_type] += 1
        self.last_error_time[error_type] = datetime.now()
        
        # Reset counter if enough time has passed
        last_time = self.last_error_time.get(error_type)
        if last_time:
            time_since = (datetime.now() - last_time).total_seconds()
            if time_since > self.circuit_breaker_reset_time:
                self.error_counts[error_type] = 0
    
    def handle_error(self, error: Exception, retry_count: int, context: dict = None) -> dict:
        """Comprehensive error handling"""
        error_type = self.classify_error(error)
        strategy = self.get_recovery_strategy(error_type, retry_count)
        
        self.record_error(error_type)
        
        if not self.should_continue(error_type):
            return {
                'should_retry': False,
                'action': 'stop',
                'reason': f'Circuit breaker triggered for {error_type.value}',
                'message': f'Circuit breaker triggered for {error_type.value} - too many consecutive errors'
            }
        
        return {
            'error_type': error_type,
            'should_retry': strategy['should_retry'],
            'wait_time': strategy['wait_time'],
            'action': strategy['action'],
            'requires_reinit': strategy['requires_reinit'],
            'message': f"Error type: {error_type.value}, Strategy: {strategy['action']}"
        }

