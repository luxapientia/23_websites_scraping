"""Base scraper class for all site scrapers"""
import requests
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from fake_useragent import UserAgent
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import logging
from datetime import datetime
from abc import ABC, abstractmethod
import os
import re
import urllib.error
from urllib.parse import urlparse
import traceback
import sys
import atexit
from typing import Callable, Any
from scrapers.error_handler import ErrorHandler, ErrorType
from selenium.webdriver.common.action_chains import ActionChains

# Suppress harmless undetected_chromedriver cleanup errors during shutdown
_original_stderr_write = sys.stderr.write
_cleanup_registered = False

def _filtered_stderr_write(s):
    """Filter out harmless ChromeDriver cleanup errors"""
    if isinstance(s, (str, bytes)):
        s_str = s if isinstance(s, str) else s.decode('utf-8', errors='ignore')
        # Suppress specific harmless errors from undetected_chromedriver during shutdown
        if ('OSError: [WinError 6] The handle is invalid' in s_str or
            'Exception ignored in: <function Chrome.__del__' in s_str or
            ('undetected_chromedriver' in s_str and '__del__' in s_str and 'WinError 6' in s_str)):
            return  # Suppress these harmless errors
    return _original_stderr_write(s)

def _suppress_chromedriver_cleanup_errors():
    """Suppress harmless ChromeDriver cleanup errors during Python shutdown"""
    global _cleanup_registered
    if not _cleanup_registered:
        sys.stderr.write = _filtered_stderr_write
        _cleanup_registered = True

# Register the error suppression
_suppress_chromedriver_cleanup_errors()


class BaseScraper(ABC):
    """Base scraper class for all site scrapers"""
    
    def __init__(self, site_name, use_selenium=False, headless=False):
        self.site_name = site_name
        self.use_selenium = use_selenium
        self.headless = headless  # Initialize headless attribute
        self.session = requests.Session()
        self.ua = UserAgent()
        self.driver = None
        self.page_load_timeout = 30  # Store timeout value for later use
        
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            filename=f'logs/{site_name}_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(site_name)
        
        # Initialize error handler
        self.error_handler = ErrorHandler(self.logger)
        
        # Health monitoring
        self.health_status = {
            'consecutive_failures': 0,
            'total_requests': 0,
            'successful_requests': 0,
            'last_success_time': None,
            'last_failure_time': None
        }
        
        # Headers for requests
        self.headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        if use_selenium:
            self.setup_selenium()
    
    def setup_selenium(self):
        """
        Setup undetected ChromeDriver with maximum anti-detection measures - improved initialization
        
        Uses undetected_chromedriver (uc) instead of regular selenium.webdriver.Chrome
        to bypass bot detection and Cloudflare challenges.
        """
        # Don't start if browser already exists
        if self.driver is not None:
            self.logger.info("Browser already initialized, skipping...")
            return
        
        try:
            # IMPORTANT: Using undetected_chromedriver (uc) instead of regular selenium.webdriver.Chrome
            # This provides automatic anti-detection features and helps bypass Cloudflare
            options = uc.ChromeOptions()  # uc = undetected_chromedriver

            if self.headless:
                # options.add_argument('--headless=new')  # Disabled - causes issues
                # Move window off-screen
                options.add_argument("--window-position=0,0")
            
            # Anti-detection arguments
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-software-rasterizer")
            
            # Use normal page load strategy (more human-like, less suspicious)
            # Changed from 'eager' to 'normal' to avoid Cloudflare detection
            options.page_load_strategy = 'normal'
            
            # Performance optimizations - made optional to avoid detection
            # Note: Blocking images/CSS can create suspicious fingerprints
            # Only block if really needed for speed, otherwise let them load normally
            # options.add_argument('--blink-settings=imagesEnabled=false')  # DISABLED - suspicious
            
            # Preferences - minimal blocking to avoid detection
            options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.notifications": 2,
                "credentials_enable_service": False,
                "profile.password_manager_enabled": False,
                "profile.default_content_settings.popups": 0,
                # Images and CSS blocking disabled - can trigger Cloudflare detection
                # "profile.managed_default_content_settings.images": 2,  # DISABLED
                # "profile.managed_default_content_settings.stylesheets": 2,  # DISABLED
                "profile.managed_default_content_settings.plugins": 2,  # Block plugins (safe)
                "profile.managed_default_content_settings.media_stream": 2,  # Block media (safe)
            })
            
            # Note: excludeSwitches and useAutomationExtension are handled automatically by undetected_chromedriver
            # Don't set them manually as they cause compatibility issues
            
            # Try to use local ChromeDriver first to avoid network timeout
            chromedriver_path = os.path.join(os.getcwd(), 'chromedriver-win32', 'chromedriver.exe')
            driver_executable_path = chromedriver_path if os.path.exists(chromedriver_path) else None
            
            # Create undetected ChromeDriver with stealth settings
            # IMPORTANT: Using uc.Chrome() from undetected_chromedriver, NOT selenium.webdriver.Chrome()
            # use_subprocess=True helps avoid detection by running Chrome in a subprocess
            max_retries = 2
            retry_count = 0
            driver_initialized = False
            
            while retry_count < max_retries and not driver_initialized:
                try:
                    if driver_executable_path and os.path.exists(driver_executable_path):
                        # Use local ChromeDriver to avoid network timeout
                        self.logger.info(f"Using local ChromeDriver with undetected_chromedriver: {driver_executable_path}")
                        # uc.Chrome() is from undetected_chromedriver module - provides anti-detection
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=None,  # Auto-detect Chrome version
                            use_subprocess=True,  # Run in subprocess to avoid detection
                            driver_executable_path=driver_executable_path,  # Use local ChromeDriver
                            browser_executable_path=None  # Use system Chrome
                        )
                    else:
                        # No local ChromeDriver, try auto-download
                        self.logger.info("No local ChromeDriver found, attempting auto-download with undetected_chromedriver...")
                        # uc.Chrome() is from undetected_chromedriver module - provides anti-detection
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=None,  # Auto-detect Chrome version
                            use_subprocess=True,  # Run in subprocess to avoid detection
                            driver_executable_path=None,  # Let it auto-detect and download
                            browser_executable_path=None  # Use system Chrome
                        )
                    
                    # Maximize window if not headless
                    try:
                        self.driver.maximize_window()
                    except:
                        pass  # Window maximization is optional
                    
                    driver_initialized = True
                    self.logger.info(f"Browser initialized successfully for {self.site_name}")
                    
                except (urllib.error.URLError, TimeoutError, OSError, Exception) as e:
                    error_msg = str(e).lower()
                    # Check if it's a network/timeout error
                    if any(keyword in error_msg for keyword in ['timeout', 'connection', 'failed', 'urlopen', '10060']):
                        retry_count += 1
                        if driver_executable_path and os.path.exists(driver_executable_path) and retry_count < max_retries:
                            self.logger.warning(f"Network error during initialization (attempt {retry_count}/{max_retries}): {str(e)}")
                            self.logger.info("Retrying with local ChromeDriver...")
                            time.sleep(2)  # Brief wait before retry
                            continue
                        else:
                            self.logger.error(f"Failed to initialize ChromeDriver after {retry_count} attempts: {str(e)}")
                            if not driver_executable_path or not os.path.exists(driver_executable_path):
                                self.logger.error("No local ChromeDriver found. Please ensure chromedriver-win32/chromedriver.exe exists.")
                            raise
                    else:
                        # Other errors - raise immediately
                        self.logger.error(f"Failed to initialize ChromeDriver: {str(e)}")
                        raise
            
            # Set timeouts - optimized for faster fetching
            self.page_load_timeout = 30  # Store timeout value
            self.driver.set_page_load_timeout(30)  # Optimized: reduced from 90 to 30 seconds
            self.driver.implicitly_wait(2)  # Optimized: reduced from 15 to 2 seconds for element finding
            self.driver.set_script_timeout(10)  # Optimized: reduced from 60 to 10 seconds for JavaScript execution
            
            # Additional anti-detection scripts - Enhanced
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    // Remove webdriver property
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Fake plugins
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    
                    // Set languages
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                    
                    // Add chrome object
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    // Override permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // Override getBattery
                    if (navigator.getBattery) {
                        navigator.getBattery = () => Promise.resolve({
                            charging: true,
                            chargingTime: 0,
                            dischargingTime: Infinity,
                            level: 1
                        });
                    }
                '''
            })
            
            # Additional CDP commands for better stealth
            # REMOVED: Hardcoded origin - this was suspicious and site-specific
            # CDP permissions are now handled dynamically per site if needed
            # Note: undetected_chromedriver handles most stealth features automatically
            
            self.logger.info(f"Undetected ChromeDriver initialized with anti-detection measures for {self.site_name}")
        except Exception as e:
            self.logger.error(f"Error setting up ChromeDriver: {str(e)}")
            raise
    
    def simulate_human_behavior(self):
        """Simulate human-like behavior to avoid detection"""
        import random
        try:
            if not self.driver:
                return
            
            # Human-like behavior simulation with realistic delays
            # Random scroll to simulate reading
            scroll_amount = random.randint(200, 400)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.5))  # Increased to 0.5-1.5s for more human-like timing
            
            # Random scroll back up a bit (like humans do)
            scroll_back = random.randint(50, 100)
            self.driver.execute_script(f"window.scrollBy(0, -{scroll_back});")
            time.sleep(random.uniform(0.3, 0.8))  # Increased to 0.3-0.8s for more human-like timing
            
            # Skip mouse movement for speed (optional, can be re-enabled if needed)
            # Mouse simulation removed for speed optimization
            
        except Exception as e:
            # Don't fail if human simulation fails
            self.logger.debug(f"Human behavior simulation skipped: {str(e)}")
    
    def is_driver_valid(self):
        """Check if the driver session is still valid"""
        try:
            if not self.driver:
                return False
            # Try to get current URL - if session is invalid, this will raise an exception
            _ = self.driver.current_url
            return True
        except Exception:
            return False
    
    def ensure_driver(self):
        """Ensure driver is initialized and valid, reinitialize if needed"""
        if self.driver and self.is_driver_valid():
            return True
        
        max_reinit_attempts = 3
        attempt = 0
        
        while attempt < max_reinit_attempts:
            self.logger.warning(f"Driver session invalid or missing, reinitializing (attempt {attempt + 1}/{max_reinit_attempts})...")
            try:
                if self.driver:
                    old_driver = self.driver  # Keep reference
                    self.driver = None  # Clear reference FIRST
                    try:
                        old_driver.quit()
                    except (OSError, AttributeError):
                        # Handle already invalid - harmless
                        pass
                    except Exception:
                        pass
                    finally:
                        # Explicitly delete the reference to help garbage collection
                        try:
                            del old_driver
                        except:
                            pass
                
                self.setup_selenium()
                self.logger.info("✓ Driver reinitialized successfully")
                return True
                    
            except Exception as e:
                attempt += 1
                if attempt >= max_reinit_attempts:
                    self.logger.error(f"Failed to reinitialize driver after {max_reinit_attempts} attempts: {str(e)}")
                    raise
                time.sleep(2 * attempt)  # Progressive backoff
        
        return False
    
    def safe_driver_get(self, property_name, default=None):
        """Safely get a driver property, handling invalid session errors"""
        try:
            if not self.driver:
                self.ensure_driver()
            if property_name == 'current_url':
                return self.driver.current_url
            elif property_name == 'title':
                return self.driver.title
            elif property_name == 'page_source':
                return self.driver.page_source
            else:
                return getattr(self.driver, property_name, default)
        except Exception as e:
            error_str = str(e).lower()
            if 'invalid session' in error_str or 'session id' in error_str:
                try:
                    self.ensure_driver()
                    if property_name == 'current_url':
                        return self.driver.current_url
                    elif property_name == 'title':
                        return self.driver.title
                    elif property_name == 'page_source':
                        return self.driver.page_source
                    else:
                        return getattr(self.driver, property_name, default)
                except:
                    return default
            else:
                return default
    
    def has_cloudflare_challenge(self):
        """
        Check if the current page has a Cloudflare challenge - STRICT DETECTION
        
        Only returns True if we're ACTUALLY on a Cloudflare challenge page.
        Many sites use Cloudflare CDN but don't show challenges - this avoids false positives.
        """
        try:
            if not self.driver:
                return False
            
            try:
                current_url = self.driver.current_url.lower()
                
                # PRIMARY CHECK: URL-based (MOST RELIABLE - only this is definitive)
                # If we're on challenges.cloudflare.com or /cdn-cgi/challenge, definitely a challenge
                if 'challenges.cloudflare.com' in current_url or '/cdn-cgi/challenge' in current_url:
                    return True
                
                # SECONDARY CHECK: Only check for challenge if page is SMALL (<5KB)
                # Challenge pages are typically very small, normal pages are much larger
                page_source = self.driver.page_source
                
                # If page has substantial content (>5KB), it's NOT a challenge page
                # Even if Cloudflare is mentioned (which is common in CDN links)
                if len(page_source) > 5000:
                    return False  # Definitely not a challenge page
                
                # Only check for challenge indicators if page is small (<5KB)
                # This is the only case where we might have a challenge
                page_lower = page_source.lower()
                
                # STRICT indicators - only these mean actual challenge page
                strict_challenge_indicators = [
                    "just a moment",  # Must be exact match
                    "checking your browser",  # Must be exact match
                ]
                
                # Check for strict indicators
                has_strict_indicator = False
                for indicator in strict_challenge_indicators:
                    if indicator in page_lower:
                        has_strict_indicator = True
                        break
                
                # Only return True if BOTH conditions: small page AND strict indicator
                if has_strict_indicator and len(page_source) < 5000:
                    # Additional verification: Check for challenge-specific DOM elements
                    try:
                        from selenium.webdriver.common.by import By
                        # Look for actual challenge form elements
                        challenge_form = self.driver.find_elements(By.CSS_SELECTOR, "#challenge-form, form[action*='challenge']")
                        if challenge_form:
                            for form in challenge_form:
                                try:
                                    if form.is_displayed():
                                        return True  # Found visible challenge form
                                except:
                                    continue
                    except:
                        pass
                    
                    # If we have strict indicator but no form, still might be challenge
                    # But be more cautious - only return True if page is very small (<2KB)
                    if len(page_source) < 2000:
                        return True
                
                # TERTIARY CHECK: Check for challenge-specific visible elements (only if page is small)
                if len(page_source) < 5000:
                    try:
                        from selenium.webdriver.common.by import By
                        challenge_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                            ".cf-browser-verification, .challenge-container")
                        if challenge_elements:
                            # Check if any are visible AND page is small
                            for elem in challenge_elements:
                                try:
                                    if elem.is_displayed() and len(page_source) < 3000:
                                        return True
                                except:
                                    continue
                    except:
                        pass
                
                # If we get here, no challenge detected
                return False
                
            except Exception:
                # If we can't check, assume no challenge to avoid false positives
                return False
            
        except Exception as e:
            self.logger.debug(f"Error checking for Cloudflare: {str(e)}")
            return False
    
    def wait_for_cloudflare(self, timeout=30, target_url=None, max_retries=1):
        """
        Wait for Cloudflare challenge to complete - OPTIMIZED FAST VERSION
        
        Optimized for speed: Simple check every 1-2 seconds, exits as soon as page is accessible.
        Based on successful approach from jiji_service.py (4 seconds average).
        
        Args:
            timeout: Maximum time to wait in seconds per attempt (optimized: 30s)
            target_url: Expected URL to verify we're on the correct site
            max_retries: Maximum number of retries if Cloudflare bypass fails (optimized: 1)
            
        Returns:
            bool: True if Cloudflare challenge was bypassed and we're on correct site, False otherwise
        """
        if not self.driver:
            return False
        
        self.logger.info(f"⏳ Cloudflare challenge detected - waiting up to {timeout}s (optimized for speed)...")
        start_time = time.time()
        check_interval = 1.5  # Check every 1.5 seconds (faster than before)
        retry_count = 0
        import random
        
        # Initial brief wait for Cloudflare to start processing
        time.sleep(2)  # Reduced from 5s
        
        while retry_count <= max_retries:
            attempt_start = time.time()
            
            while time.time() - attempt_start < timeout:
                try:
                    # FAST CHECK: Simple page source check (like jiji_service.py)
                    page_source = self.driver.page_source
                    current_url = self.driver.current_url.lower()
                    
                    # Quick check: If we're on Cloudflare challenge URL, definitely blocked
                    if 'challenges.cloudflare.com' in current_url or '/cdn-cgi/challenge' in current_url:
                        # Still on challenge page, continue waiting
                        pass
                    else:
                        # Not on challenge URL - check if challenge indicators are gone
                        page_preview = page_source[:2000].lower()
                        has_challenge_text = ("just a moment" in page_preview or "checking your browser" in page_preview)
                        
                        # If no challenge text AND we have substantial content, we're good
                        if not has_challenge_text and len(page_source) > 5000:
                            # Simple domain check if target URL provided
                            on_target = True
                            if target_url:
                                try:
                                    target_domain = urlparse(target_url).netloc.split(':')[0].lower().replace('www.', '')
                                    current_domain = urlparse(current_url if '//' in current_url else 'http://' + current_url).netloc.split(':')[0].lower().replace('www.', '')
                                    on_target = target_domain == current_domain or target_domain in current_domain
                                except:
                                    # Simple fallback check
                                    target_domain = target_url.split('//')[1].split('/')[0].split(':')[0].lower().replace('www.', '')
                                    on_target = target_domain in current_url.replace('www.', '')
                            
                            if on_target:
                                elapsed = time.time() - start_time
                                self.logger.info(f"✅ Cloudflare bypassed successfully! (took {elapsed:.1f}s)")
                                return True
                    
                    # Brief wait before next check
                    time.sleep(check_interval)
                    
                except Exception as e:
                    self.logger.debug(f"Error while waiting for Cloudflare: {str(e)}")
                    time.sleep(check_interval)
            
            # Timeout reached for this attempt
            elapsed = time.time() - attempt_start
            retry_count += 1
            
            if retry_count <= max_retries:
                self.logger.warning(f"⚠️ Cloudflare timeout after {elapsed:.1f}s (attempt {retry_count}/{max_retries + 1})")
                
                # Simple retry: Just refresh and wait again
                try:
                    if target_url:
                        self.logger.info("🔄 Refreshing page...")
                        self.driver.refresh()
                        time.sleep(3)  # Brief wait after refresh
                    else:
                        self.driver.refresh()
                        time.sleep(3)
                except Exception as refresh_error:
                    self.logger.debug(f"Refresh error: {str(refresh_error)}")
            else:
                # All retries exhausted - final check
                total_elapsed = time.time() - start_time
                try:
                    page_source_final = self.driver.page_source
                    current_url_final = self.driver.current_url.lower()
                    # If page has content and not on Cloudflare URL, consider accessible
                    if len(page_source_final) > 5000 and 'challenges.cloudflare.com' not in current_url_final:
                        if "just a moment" not in page_source_final[:2000].lower() and "checking your browser" not in page_source_final[:2000].lower():
                            self.logger.info(f"✅ Page accessible (final check) - continuing... (took {total_elapsed:.1f}s)")
                            return True
                except:
                    pass
                
                self.logger.warning(f"⚠️ Cloudflare challenge timeout after {total_elapsed:.1f}s")
                return False
        
        return False
    
    def get_page(self, url, use_selenium=False, wait_time=1, max_retries=5):  # Increased retries to 5 for better reliability
        """Fetch page content with comprehensive error handling"""
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.health_status['total_requests'] += 1
                self.logger.info(f"Fetching: {url} (attempt {retry_count + 1}/{max_retries})")
                
                if use_selenium or self.use_selenium:
                    # Ensure driver is valid before use
                    self.ensure_driver()
                    
                    try:
                        # Load page with timeout protection (30s timeout set in setup_selenium, optimized for speed)
                        self.driver.get(url)
                        
                        # Quick error check - only for critical connection errors
                        current_url = self.driver.current_url.lower()
                        if any(err in current_url for err in ['chrome-error://', 'err_', 'dns_probe']):
                            raise Exception(f"Connection error detected: {current_url}")
                        
                        # Wait for page to start loading (increased for more human-like behavior)
                        import random
                        time.sleep(random.uniform(0.5, 1.5))  # Increased delay before accessing page_source
                        
                        # Quick Cloudflare check - only if on challenge URL or page is very small
                        current_url_check = current_url
                        page_preview = self.driver.page_source[:6000]
                        
                        # Only check Cloudflare if on challenge URL or page is suspiciously small
                        if ('challenges.cloudflare.com' in current_url_check or '/cdn-cgi/challenge' in current_url_check or len(page_preview) < 5000) and self.has_cloudflare_challenge():
                            self.logger.info("🛡️ Cloudflare challenge detected - waiting for bypass...")
                            cloudflare_bypassed = self.wait_for_cloudflare(timeout=30, target_url=url, max_retries=1)
                            if not cloudflare_bypassed:
                                # Quick final check - if page has content, continue anyway
                                if len(self.driver.page_source) > 5000 and 'challenges.cloudflare.com' not in self.driver.current_url.lower():
                                    self.logger.info("✓ Page accessible despite Cloudflare warning - continuing...")
                                else:
                                    retry_count += 1
                                    if retry_count < max_retries:
                                        import random
                                        delay = random.uniform(10, 15)
                                        self.logger.warning(f"Retrying page load in {delay:.1f}s...")
                                        time.sleep(delay)
                                        continue
                                    else:
                                        return None
                            time.sleep(1)  # Brief wait after bypass
                        
                        # Wait time for page to stabilize (increased for more human-like behavior)
                        import random
                        if wait_time > 0:
                            time.sleep(random.uniform(1.0, 2.0))  # Increased to 1-2s for more realistic timing
                        else:
                            time.sleep(random.uniform(1.0, 2.0))  # Increased to 1-2s
                        
                        # REMOVED: Second Cloudflare check after page load
                        # If page already loaded successfully with content, there's no Cloudflare challenge
                        # This was causing false positives and unnecessary delays
                        
                        # Add delay before accessing page_source (more human-like)
                        time.sleep(random.uniform(0.5, 1.0))
                        html = self.driver.page_source
                        
                        # Basic content check - only fail if completely empty
                        if not html or len(html) < 100:
                            raise Exception(f"Insufficient HTML content: {len(html) if html else 0} chars")
                        
                        # Success
                        self.health_status['successful_requests'] += 1
                        self.health_status['consecutive_failures'] = 0
                        self.health_status['last_success_time'] = datetime.now()
                        
                        return html
                    except TimeoutException as e:
                        # Handle timeout - wait longer for FULL content instead of accepting partial
                        recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                        
                        # Don't accept partial content - wait for full content
                        self.logger.warning(f"⚠️ Page load timeout - waiting longer for FULL content...")
                        
                        # Increase timeout temporarily
                        original_timeout = self.page_load_timeout  # Use stored value
                        self.page_load_timeout = 60
                        self.driver.set_page_load_timeout(60)  # Increase to 60 seconds
                        
                        try:
                            # Wait for content to fully load (optimized for speed)
                            wait_for_content = 8  # Reduced from 15s to 8s for faster response
                            waited = 0
                            content_ready = False
                            
                            while waited < wait_for_content:
                                html = self.driver.page_source
                                
                                # Require substantial content (not just partial)
                                if html and len(html) > 8000:  # Reduced from 10KB to 8KB for faster detection
                                    # Check if page seems complete
                                    soup = BeautifulSoup(html, 'lxml')
                                    has_body = soup.find('body')
                                    body_text = has_body.get_text(strip=True) if has_body else ''
                                    
                                    # Page is ready if it has substantial body content (reduced threshold)
                                    if len(body_text) > 500:  # Reduced from 1000 for faster detection
                                        content_ready = True
                                        self.logger.info(f"✓ Full content loaded after additional {waited}s wait ({len(html)} chars)")
                                        break
                                
                                if waited % 3 == 0:  # Log every 3s instead of 5s
                                    self.logger.info(f"Waiting for full content... ({waited}s/{wait_for_content}s)")
                                
                                time.sleep(0.5)  # Check every 0.5s instead of 1s for faster response
                                waited += 0.5
                            
                            if content_ready:
                                html = self.driver.page_source
                                self.page_load_timeout = original_timeout
                                self.driver.set_page_load_timeout(original_timeout)
                                return html
                            else:
                                raise Exception("Full content not loaded after extended wait")
                                
                        except Exception as wait_error:
                            self.logger.warning(f"Could not get full content: {str(wait_error)}")
                            self.page_load_timeout = original_timeout
                            self.driver.set_page_load_timeout(original_timeout)
                            
                            # Retry with increased timeout
                            if recovery['should_retry'] and retry_count < max_retries - 1:
                                wait_time = recovery['wait_time']
                                import random
                                delay = random.uniform(wait_time[0], wait_time[1])
                                self.logger.warning(f"Timeout error, retrying with longer timeout in {delay:.1f}s...")
                                time.sleep(delay)
                                retry_count += 1
                                continue
                            else:
                                self.health_status['consecutive_failures'] += 1
                                self.health_status['last_failure_time'] = datetime.now()
                                return None
                            
                    except Exception as e:
                        # Comprehensive error handling
                        recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                        
                        if not recovery['should_retry']:
                            error_msg = recovery.get('message', recovery.get('reason', 'Unknown error'))
                            self.logger.error(f"Unrecoverable error: {error_msg}")
                            self.health_status['consecutive_failures'] += 1
                            self.health_status['last_failure_time'] = datetime.now()
                            return None
                        
                        # Handle recovery actions
                        if recovery['action'] == 'reinitialize_driver' and recovery['requires_reinit']:
                            try:
                                self.ensure_driver()
                            except Exception as reinit_e:
                                self.logger.error(f"Failed to reinitialize: {str(reinit_e)}")
                                retry_count += 1
                                if retry_count < max_retries:
                                    continue
                                return None
                        
                        if retry_count < max_retries - 1:
                            wait_time = recovery['wait_time']
                            import random
                            delay = random.uniform(wait_time[0], wait_time[1])
                            error_msg = recovery.get('message', recovery.get('reason', 'Unknown error'))
                            self.logger.warning(f"{error_msg}, retrying in {delay:.1f}s...")
                            time.sleep(delay)
                            retry_count += 1
                            continue
                        else:
                            self.health_status['consecutive_failures'] += 1
                            self.health_status['last_failure_time'] = datetime.now()
                            self.logger.error(f"Failed after {max_retries} attempts: {recovery['message']}")
                            return None
                else:
                    # Use requests for non-Selenium fetching
                    try:
                        response = self.session.get(url, headers=self.headers, timeout=15)  # Optimized: reduced from 60 to 15 seconds
                        response.raise_for_status()
                        
                        # Success
                        self.health_status['successful_requests'] += 1
                        self.health_status['consecutive_failures'] = 0
                        self.health_status['last_success_time'] = datetime.now()
                        
                        return response.text
                    except Exception as e:
                        recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                        
                        if recovery['should_retry'] and retry_count < max_retries - 1:
                            wait_time = recovery['wait_time']
                            import random
                            delay = random.uniform(wait_time[0], wait_time[1])
                            self.logger.warning(f"Request error, retrying in {delay:.1f}s...")
                            time.sleep(delay)
                            retry_count += 1
                            continue
                        else:
                            self.health_status['consecutive_failures'] += 1
                            self.health_status['last_failure_time'] = datetime.now()
                            return None
                            
            except Exception as e:
                # Top-level error handling
                self.logger.error(f"Unexpected error fetching {url}: {str(e)}")
                self.logger.debug(traceback.format_exc())
                self.health_status['consecutive_failures'] += 1
                self.health_status['last_failure_time'] = datetime.now()
                return None
        
        # All retries exhausted
        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    # Wheel-related keywords - shared across all scrapers
    WHEEL_KEYWORDS = [
            # Cap keywords FIRST (more specific, should match first)
            'wheel cap',
            'wheel kit',
            'hub cap',
            'center cap',
            'hubcap',
            'wheel cover',
            # Then wheel types
            'alloy wheel',
            'steel wheel',
            'aluminum wheel',
            'chrome wheel',
            'spoke wheel',
            'forged wheel',
            'cast wheel',
            'custom wheel',
            'rim',
            'alloy',  # Standalone keyword for alloy wheels,
            'wheel',
            'AllWheel',
            'AllWheel Rim',
            'AllWheel Rim Assembly',
            'AllWheel Rim Assembly',
            'Disc Wheel',
            'Front Wheel',
            'Rear Wheel',
            'Wheel Set',
            'Wheel Assembly',
            'Wheel Rim',
            'wheel rim',  # Lowercase version
            'complete wheel',
            'OEM Kia wheel',
            'OEM wheel',
            '19 inch Kia wheel',
            'wheel disc',
            'rim cap',
            # Additional wheel-related terms found on AcuraPartsWarehouse
            'disk',  # For "Acura Disk (18X8J)" products
            'Disk',  # Capitalized version
            'wheel disk',  # Combined term
            'aluminum wheel rim',  # Full phrase
            'alloy wheel rim',  # Full phrase
            'wheel rim assembly',  # Assembly variants
            'wheel assembly',  # Lowercase version
            'spare wheel',  # Spare wheels
            'spare wheel rim',  # Spare wheel rim
            'berlina black',  # Specific wheel finish (often with wheels)
            'black alloy wheel',  # Black finish wheels
            'alloy wheels',  # Plural form
            'alloy wheel',  # Already there but ensure it's clear
            'wheel rim',  # Ensure lowercase version
            'rims',  # Plural form
            'wheels',  # Plural form
                       
    ]
    
    # Keywords to EXCLUDE (not wheels) - Be very specific to avoid false positives
    EXCLUDE_KEYWORDS = [
            'steering wheel',
            'Wheel Flange',
            'wheel bearing',
            'Wheel Spacer',
            'bearing hub',
            'hub bearing',
            'wheel hub',
            'wheel hub bearing',
            'hub bearing assembly',  # ← Changed from just 'hub assembly'
            'bearing assembly',      # ← More specific
            'wheel nut', 
            'wheel stud', 
            'wheel bolt', 
            'wheel valve',
            'wheel weight', 
            'wheel arch', 
            'wheel well', 
            'wheel sensor',
            'wheel speed sensor', 
            'wheel cylinder', 
            'wheel seal',
            'lug nut',
            'lug bolt',
            'tire pressure', 
            'tpms',
            'wheel lock nut',
            'wheel lock key', 
            'wheel alignment',
            'wheel opening', 
            'wheel house', 
            'wheel liner', 
            'wheel adapter',
            'wheel mounting kit'
    ]
    
    def is_wheel_product(self, title, description=''):
        """
        Check if product is a wheel or wheel cap (not other wheel parts)
        
        Args:
            title: Product title
            description: Product description (optional)
        
        Returns:
            bool: True if it's a wheel/wheel cap product, False otherwise
        """
        # Use class-level keywords
        wheel_keywords = self.WHEEL_KEYWORDS
        exclude_keywords = self.EXCLUDE_KEYWORDS
        
        text = f"{title} {description}".lower()
        
        # Normalize hyphens and underscores to spaces for better matching
        text = re.sub(r'[-_]', ' ', text)

        # DEBUG INFO
        self.logger.info(f"🔍 DEBUG: title='{title}'")
        self.logger.info(f"🔍 DEBUG: lowercased='{text}'")
        self.logger.info(f"🔍 DEBUG: 'wheel cap' in text? {('wheel cap' in text)}")
        self.logger.info(f"🔍 DEBUG: 'hub cap' in text? {('hub cap' in text)}")

        # DEBUG: Print what we're checking
        self.logger.info(f"🔍 Analyzing: '{title}'")
        self.logger.info(f"   Lowercased text: '{text}'")
        
        # Check exclusions first
        for exclude in exclude_keywords:
            if exclude in text:
                self.logger.info(f"❌ EXCLUDED '{title[:50]}' - matched '{exclude}'")
                return False
        
        # Check if it's a wheel product
        for keyword in wheel_keywords:
            # For single-word keywords like 'rim', match as whole word
            # For multi-word keywords like 'wheel cap', use substring match
            if len(keyword.split()) == 1:
                # Single word: match as whole word using word boundaries
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    self.logger.info(f"✅ INCLUDED '{title[:50]}' - matched '{keyword}'")
                    return True
            else:
                # Multi-word: use substring match (already safe)
                if keyword in text:
                    self.logger.info(f"✅ INCLUDED '{title[:50]}' - matched '{keyword}'")
                    return True
        
        self.logger.info(f"⚠️ NO MATCH '{title[:50]}' - no wheel keywords found")
        return False
    
    def clean_sku(self, sku):
        """
        Remove spaces, dashes, and special chars from SKU
        
        Args:
            sku: SKU string
        
        Returns:
            str: Cleaned SKU with only alphanumeric characters
        """
        if not sku:
            return ''
        return re.sub(r'[^a-zA-Z0-9]', '', str(sku))
    
    def extract_price(self, price_text):
        """
        Extract numeric price from text
        
        Args:
            price_text: Price text (e.g., "$787.31" or "787.31")
        
        Returns:
            str: Numeric price string
        """
        if not price_text:
            return ''
        
        # Remove currency symbols and extract numbers
        price = re.sub(r'[^\d.]', '', str(price_text))
        return price
    
    def safe_find_text(self, soup, selector, attribute=None, default=''):
        """
        Safely extract text from BeautifulSoup element
        
        Args:
            soup: BeautifulSoup object
            selector: CSS selector or element
            attribute: Attribute to extract (None for text)
            default: Default value if not found
        
        Returns:
            str: Extracted text or default value
        """
        try:
            if isinstance(selector, str):
                elem = soup.select_one(selector)
            else:
                elem = selector
            
            if not elem:
                return default
            
            if attribute:
                return elem.get(attribute, default)
            else:
                return elem.get_text(strip=True)
        except Exception as e:
            self.logger.warning(f"Error extracting text: {str(e)}")
            return default
    
    @abstractmethod
    def get_product_urls(self):
        """
        Get all product URLs - must be implemented by child class
        
        Returns:
            list: List of product URLs
        """
        pass
    
    @abstractmethod
    def scrape_product(self, url):
        """
        Scrape single product - must be implemented by child class
        
        Args:
            url: Product URL
        
        Returns:
            dict: Product data dictionary or None
        """
        pass
    
    def get_health_status(self) -> dict:
        """Get current health status of the scraper"""
        success_rate = 0
        if self.health_status['total_requests'] > 0:
            success_rate = (self.health_status['successful_requests'] / self.health_status['total_requests']) * 100
        
        return {
            **self.health_status,
            'success_rate': f"{success_rate:.1f}%",
            'is_healthy': self.health_status['consecutive_failures'] < 5
        }
    
    def check_health(self) -> bool:
        """Check if scraper is healthy enough to continue"""
        # Check consecutive failures
        if self.health_status['consecutive_failures'] >= 10:
            self.logger.error("Too many consecutive failures, scraper unhealthy")
            return False
        
        # Check success rate
        if self.health_status['total_requests'] > 20:
            success_rate = (self.health_status['successful_requests'] / self.health_status['total_requests']) * 100
            if success_rate < 20:  # Less than 20% success rate
                self.logger.error(f"Success rate too low ({success_rate:.1f}%), scraper unhealthy")
                return False
        
        return True
    
    def reset_health_status(self):
        """Reset health status (useful after recovery)"""
        self.health_status['consecutive_failures'] = 0
        self.logger.info("Health status reset")
    
    def safe_execute(self, func: Callable, *args, default=None, max_retries=2, **kwargs) -> Any:
        """Safely execute a function with error handling and retries"""
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                recovery = self.error_handler.handle_error(e, retry_count)
                
                if not recovery['should_retry'] or retry_count >= max_retries - 1:
                    self.logger.warning(f"Function {func.__name__} failed after {retry_count + 1} attempts: {str(e)}")
                    return default
                
                wait_time = recovery['wait_time']
                import random
                delay = random.uniform(wait_time[0], wait_time[1])
                self.logger.debug(f"Retrying {func.__name__} in {delay:.1f}s...")
                time.sleep(delay)
                retry_count += 1
        
        return default
    
    def close(self):
        """Cleanup ChromeDriver and resources with comprehensive error handling"""
        import gc  # For garbage collection
        
        cleanup_errors = []
        
        # Close driver
        if self.driver:
            driver_ref = self.driver  # Keep reference to avoid issues
            self.driver = None  # Set to None FIRST to prevent __del__ from running
            
            try:
                # Try to quit gracefully
                driver_ref.quit()
                self.logger.info(f"WebDriver closed for {self.site_name}")
            except (OSError, AttributeError) as e:
                # OSError [WinError 6] The handle is invalid - harmless during shutdown
                # AttributeError - driver already closed
                error_msg = str(e).lower()
                if 'handle is invalid' in error_msg or 'invalid' in error_msg:
                    # This is harmless - driver was already closed or handle invalid
                    self.logger.debug(f"Driver handle already invalid (harmless): {str(e)}")
                else:
                    cleanup_errors.append(f"Error closing driver: {str(e)}")
            except Exception as e:
                cleanup_errors.append(f"Error closing driver: {str(e)}")
                try:
                    # Try to force close if normal quit fails
                    import subprocess
                    import platform
                    if platform.system() == 'Windows':
                        subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], 
                                     capture_output=True, timeout=5)
                except:
                    pass
            finally:
                # Ensure driver reference is cleared and force garbage collection
                try:
                    # Explicitly delete the reference
                    del driver_ref
                except:
                    pass
                
                # Force multiple garbage collection passes to ensure cleanup
                for _ in range(3):
                    gc.collect()
                
                # Small delay to allow cleanup to complete
                time.sleep(0.1)
        
        # Close session
        if self.session:
            try:
                self.session.close()
            except Exception as e:
                cleanup_errors.append(f"Error closing session: {str(e)}")
        
        # Log any cleanup errors (excluding harmless handle errors)
        if cleanup_errors:
            self.logger.warning(f"Cleanup errors: {'; '.join(cleanup_errors)}")
        
        # Log final health status
        try:
            health = self.get_health_status()
            self.logger.info(f"Final health status: {health['success_rate']} success rate, {health['consecutive_failures']} consecutive failures")
        except:
            pass