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
        
        # Setup logging with UTF-8 encoding to handle emojis
        log_file = f'logs/{site_name}_{datetime.now().strftime("%Y%m%d")}.log'
        
        # Create logger
        self.logger = logging.getLogger(site_name)
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # File handler with UTF-8 encoding
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler with safe Unicode handling
        # Define SafeUnicodeHandler class outside to avoid redefinition issues
        class SafeUnicodeHandler(logging.StreamHandler):
            """StreamHandler that safely handles Unicode encoding errors"""
            def emit(self, record):
                try:
                    # Try normal emit first
                    msg = self.format(record)
                    stream = self.stream
                    # Try to write directly
                    try:
                        stream.write(msg + self.terminator)
                        self.flush()
                    except UnicodeEncodeError:
                        # Replace emojis with plain text alternatives
                        safe_msg = msg
                        safe_msg = safe_msg.replace('🔍', '[SEARCH]')
                        safe_msg = safe_msg.replace('✅', '[OK]')
                        safe_msg = safe_msg.replace('❌', '[X]')
                        safe_msg = safe_msg.replace('💰', '[PRICE]')
                        safe_msg = safe_msg.replace('🛡️', '[SHIELD]')
                        safe_msg = safe_msg.replace('⏳', '[WAIT]')
                        safe_msg = safe_msg.replace('🔄', '[RETRY]')
                        safe_msg = safe_msg.replace('📝', '[NOTE]')
                        safe_msg = safe_msg.replace('📦', '[BOX]')
                        safe_msg = safe_msg.replace('⚠️', '[WARN]')
                        safe_msg = safe_msg.replace('✓', '[OK]')
                        safe_msg = safe_msg.replace('⏭️', '[SKIP]')
                        try:
                            stream.write(safe_msg + self.terminator)
                            self.flush()
                        except UnicodeEncodeError:
                            # Last resort: encode with errors='replace'
                            encoding = getattr(stream, 'encoding', 'utf-8') or 'utf-8'
                            try:
                                safe_bytes = safe_msg.encode(encoding, errors='replace')
                                stream.write(safe_bytes.decode(encoding, errors='replace') + self.terminator)
                            except:
                                # Ultimate fallback: remove all non-ASCII
                                ascii_msg = ''.join(c if ord(c) < 128 else '?' for c in safe_msg)
                                stream.write(ascii_msg + self.terminator)
                            self.flush()
                except Exception:
                    # Silently ignore logging errors to prevent crashes
                    self.handleError(record)
        
        safe_console_handler = SafeUnicodeHandler(sys.stdout)
        safe_console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        safe_console_handler.setFormatter(console_formatter)
        self.logger.addHandler(safe_console_handler)
        
        # Prevent propagation to root logger to avoid duplicate messages
        self.logger.propagate = False
        
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
    
    def _detect_chrome_version(self):
        """
        Detect the installed Chrome browser version
        Returns the major version number (e.g., 142) or None if detection fails
        """
        try:
            import subprocess
            import platform
            
            if platform.system() == 'Windows':
                # Windows: Check registry or chrome.exe version
                chrome_paths = [
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
                ]
                
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        try:
                            # Get version using PowerShell
                            result = subprocess.run(
                                ['powershell', '-Command', 
                                 f'(Get-Item "{chrome_path}").VersionInfo.FileVersion'],
                                capture_output=True,
                                text=True,
                                timeout=5
                            )
                            if result.returncode == 0:
                                version_str = result.stdout.strip()
                                # Extract major version (e.g., "142.0.7444.176" -> 142)
                                version_match = re.search(r'^(\d+)\.', version_str)
                                if version_match:
                                    version = int(version_match.group(1))
                                    self.logger.info(f"Detected Chrome version: {version} (from {version_str})")
                                    return version
                        except Exception as e:
                            self.logger.debug(f"Error detecting Chrome version from {chrome_path}: {str(e)}")
                            continue
                
                # Fallback: Try to get version from Chrome's version file
                try:
                    version_file = os.path.expanduser(r"~\AppData\Local\Google\Chrome\User Data\Last Version")
                    if os.path.exists(version_file):
                        with open(version_file, 'r') as f:
                            version_str = f.read().strip()
                            version_match = re.search(r'^(\d+)\.', version_str)
                            if version_match:
                                version = int(version_match.group(1))
                                self.logger.info(f"Detected Chrome version: {version} (from version file)")
                                return version
                except Exception as e:
                    self.logger.debug(f"Error reading Chrome version file: {str(e)}")
            
            # Fallback: Try to run Chrome with --version flag
            try:
                chrome_cmd = 'chrome' if platform.system() != 'Windows' else 'chrome.exe'
                result = subprocess.run(
                    [chrome_cmd, '--version'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    version_str = result.stdout.strip()
                    version_match = re.search(r'(\d+)\.', version_str)
                    if version_match:
                        version = int(version_match.group(1))
                        self.logger.info(f"Detected Chrome version: {version} (from --version)")
                        return version
            except Exception as e:
                self.logger.debug(f"Error running Chrome --version: {str(e)}")
            
        except Exception as e:
            self.logger.debug(f"Error detecting Chrome version: {str(e)}")
        
        self.logger.warning("Could not detect Chrome version, will use auto-detection")
        return None
    
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
            # Detect Chrome version to ensure ChromeDriver compatibility
            chrome_version = self._detect_chrome_version()
            
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
            # undetected_chromedriver automatically handles Cloudflare challenges
            max_retries = 2
            retry_count = 0
            driver_initialized = False
            
            while retry_count < max_retries and not driver_initialized:
                try:
                    if driver_executable_path and os.path.exists(driver_executable_path):
                        # Use local ChromeDriver to avoid network timeout
                        self.logger.info(f"Using local ChromeDriver with undetected_chromedriver: {driver_executable_path}")
                        # uc.Chrome() automatically bypasses Cloudflare - let it handle challenges naturally
                        # Use detected Chrome version to ensure compatibility
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=chrome_version,  # Use detected Chrome version for compatibility
                            use_subprocess=True,  # Run in subprocess to avoid detection
                            driver_executable_path=driver_executable_path,  # Use local ChromeDriver
                            browser_executable_path=None  # Use system Chrome
                        )
                    else:
                        # No local ChromeDriver, try auto-download
                        self.logger.info("No local ChromeDriver found, attempting auto-download with undetected_chromedriver...")
                        # uc.Chrome() automatically bypasses Cloudflare - let it handle challenges naturally
                        # Use detected Chrome version to ensure compatibility
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=chrome_version,  # Use detected Chrome version for compatibility
                            use_subprocess=True,  # Run in subprocess to avoid detection
                            driver_executable_path=None,  # Let it auto-detect and download
                            browser_executable_path=None  # Use system Chrome
                        )
                    
                    # Maximize window if not headless
                    try:
                        self.driver.maximize_window()
                        # Verify window is still open after maximization
                        if not self.driver.window_handles:
                            raise Exception("Window closed immediately after maximization")
                    except Exception as window_error:
                        error_msg = str(window_error).lower()
                        if 'no such window' in error_msg or 'target window already closed' in error_msg:
                            self.logger.warning(f"Window closed during maximization: {str(window_error)}")
                            # Try to create a new window
                            try:
                                self.driver.switch_to.new_window('tab')
                                self.logger.info("Created new window after closure")
                            except:
                                raise Exception(f"Failed to recover from window closure: {str(window_error)}")
                        else:
                            # Window maximization is optional, but log the error
                            self.logger.debug(f"Window maximization failed (non-critical): {str(window_error)}")
                    
                    # Wait a moment for window to stabilize
                    time.sleep(1)
                    
                    # Final verification that driver and window are valid
                    if not self.driver:
                        raise Exception("Driver is None after initialization")
                    
                    # Check window handles multiple times to ensure stability
                    window_check_attempts = 3
                    window_stable = False
                    for attempt in range(window_check_attempts):
                        try:
                            handles = self.driver.window_handles
                            if handles:
                                window_stable = True
                                break
                        except Exception as check_error:
                            if attempt < window_check_attempts - 1:
                                time.sleep(0.5)  # Wait before retry
                                continue
                            else:
                                raise Exception(f"No window handles available after initialization: {str(check_error)}")
                    
                    if not window_stable:
                        raise Exception("Window is not stable after initialization")
                    
                    driver_initialized = True
                    self.logger.info(f"Browser initialized successfully for {self.site_name}")
                    
                except (urllib.error.URLError, TimeoutError, OSError, Exception) as e:
                    error_msg = str(e).lower()
                    
                    # Check if it's a version mismatch error
                    if 'version' in error_msg and ('chromedriver' in error_msg or 'chrome version' in error_msg):
                        self.logger.warning(f"ChromeDriver version mismatch detected: {str(e)}")
                        
                        retry_count += 1
                        if retry_count < max_retries:
                            # Strategy 1: If using local ChromeDriver and it has version mismatch, try auto-download
                            if driver_executable_path and os.path.exists(driver_executable_path):
                                self.logger.info(f"Local ChromeDriver version mismatch, retrying with auto-download (attempt {retry_count}/{max_retries})...")
                                driver_executable_path = None  # Force auto-download
                                chrome_version = None  # Let undetected_chromedriver auto-detect
                                time.sleep(2)
                                continue
                            # Strategy 2: If we specified a version and it failed, try auto-detection instead
                            elif chrome_version is not None:
                                self.logger.info(f"Retrying with auto-detection (attempt {retry_count}/{max_retries})...")
                                chrome_version = None  # Let undetected_chromedriver auto-detect
                                time.sleep(2)
                                continue
                            # Strategy 3: Try without any version specification
                            else:
                                self.logger.info(f"Retrying without version specification (attempt {retry_count}/{max_retries})...")
                                chrome_version = None
                                time.sleep(2)
                                continue
                        else:
                            self.logger.error(f"Failed to initialize ChromeDriver after version mismatch retries: {str(e)}")
                            self.logger.error("Please update Chrome browser or ChromeDriver to matching versions")
                            self.logger.error("You can also delete the local ChromeDriver to force auto-download of correct version")
                            raise
                    
                    # Check if it's a network/timeout error
                    elif any(keyword in error_msg for keyword in ['timeout', 'connection', 'failed', 'urlopen', '10060']):
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
            
            # Verify driver is still valid before proceeding
            if not self.driver:
                raise Exception("Driver initialization failed - driver is None")
            
            # Check if window is still open
            try:
                # Try to get window handles to verify window is open
                window_handles = self.driver.window_handles
                if not window_handles:
                    raise Exception("No window handles found - window may have closed")
            except Exception as window_check_error:
                self.logger.error(f"Window check failed: {str(window_check_error)}")
                raise Exception(f"Browser window is not available: {str(window_check_error)}")
            
            # Small delay to ensure window is fully ready
            time.sleep(0.5)
            
            # Set timeouts - increased to allow undetected_chromedriver to handle Cloudflare
            # undetected_chromedriver needs more time to automatically bypass Cloudflare challenges
            try:
                self.page_load_timeout = 60  # Increased from 30 to 60 seconds for Cloudflare bypass
                self.driver.set_page_load_timeout(60)  # Increased to allow Cloudflare challenge completion
                self.driver.implicitly_wait(5)  # Increased from 2 to 5 seconds for element finding
                self.driver.set_script_timeout(30)  # Increased from 10 to 30 seconds for JavaScript execution (Cloudflare uses JS)
            except Exception as timeout_error:
                self.logger.warning(f"Error setting timeouts (may be non-critical): {str(timeout_error)}")
                # Continue anyway - timeouts might already be set
            
            # Additional anti-detection scripts - Enhanced
            # Wrap in try-except to handle cases where window closes during execution
            try:
                # Verify window is still open before executing CDP command
                if self.driver and self.driver.window_handles:
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
                else:
                    self.logger.warning("Window closed before CDP command execution - skipping anti-detection scripts")
            except Exception as cdp_error:
                error_msg = str(cdp_error).lower()
                # Check if it's a window closed error
                if 'no such window' in error_msg or 'target window already closed' in error_msg or 'web view not found' in error_msg:
                    self.logger.warning(f"Window closed during CDP command execution (non-critical): {str(cdp_error)}")
                    # This is not critical - undetected_chromedriver already has built-in anti-detection
                    # Continue without the additional scripts
                else:
                    # Other CDP errors - log but don't fail
                    self.logger.warning(f"Error executing CDP command (non-critical): {str(cdp_error)}")
                    # Continue anyway - undetected_chromedriver has built-in anti-detection
            
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
                    "verifying you are human",  # New indicator from image
                    "review the security of your connection",  # New indicator from image
                    "verifying...",  # Loading state indicator
                    "this may take a few seconds",  # Time indicator
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
                        # Check for multiple Cloudflare challenge selectors
                        challenge_selectors = [
                            ".cf-browser-verification",
                            ".challenge-container",
                            "#challenge-form",
                            "form[action*='challenge']",
                            "[data-ray]",
                            ".cf-im-under-attack",
                            ".cf-wrapper",
                            "div:contains('Verifying')",
                            "div:contains('verifying')"
                        ]
                        
                        for selector in challenge_selectors:
                            try:
                                challenge_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                if challenge_elements:
                                    # Check if any are visible AND page is small
                                    for elem in challenge_elements:
                                        try:
                                            if elem.is_displayed() and len(page_source) < 3000:
                                                return True
                                        except:
                                            continue
                            except:
                                continue
                        
                        # Also check for text content that indicates verification
                        try:
                            page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
                            if any(indicator in page_text for indicator in ["verifying", "review the security", "this may take"]):
                                if len(page_source) < 5000:
                                    return True
                        except:
                            pass
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
        Wait for Cloudflare challenge to complete - ENHANCED VERSION
        
        Enhanced with better detection, human-like behavior, and more retry strategies.
        
        Args:
            timeout: Maximum time to wait in seconds per attempt (default: 30s, can be increased to 60s)
            target_url: Expected URL to verify we're on the correct site
            max_retries: Maximum number of retries if Cloudflare bypass fails (default: 1, can be increased to 3)
            
        Returns:
            bool: True if Cloudflare challenge was bypassed and we're on correct site, False otherwise
        """
        if not self.driver:
            return False
        
        self.logger.info(f"⏳ Cloudflare challenge detected - waiting up to {timeout}s per attempt (max {max_retries + 1} attempts)...")
        start_time = time.time()
        check_interval = 2.0  # Check every 2 seconds (more human-like)
        retry_count = 0
        import random
        
        # Initial wait for Cloudflare to start processing (increased for better success)
        # For "Verifying you are human" challenges, they can take longer
        initial_wait = random.uniform(5, 8)  # Increased from 3-5s to 5-8s
        self.logger.info(f"⏳ Initial wait: {initial_wait:.1f}s for Cloudflare to start...")
        time.sleep(initial_wait)
        
        # Simulate human behavior during initial wait
        try:
            self.simulate_human_behavior()
        except:
            pass
        
        while retry_count <= max_retries:
            attempt_start = time.time()
            
            while time.time() - attempt_start < timeout:
                try:
                    # Simulate human behavior while waiting (helps bypass detection)
                    if random.random() < 0.4:  # 40% chance to simulate activity (increased from 30%)
                        try:
                            # Small random scroll
                            self.driver.execute_script(f"window.scrollBy(0, {random.randint(10, 50)});")
                            time.sleep(random.uniform(0.3, 0.8))
                        except:
                            pass
                    
                    # Check for "Verifying..." element disappearing (specific to this challenge type)
                    try:
                        from selenium.webdriver.common.by import By
                        verifying_elements = self.driver.find_elements(By.XPATH, 
                            "//*[contains(text(), 'Verifying') or contains(text(), 'verifying')]")
                        if verifying_elements:
                            # Check if any are still visible
                            still_verifying = False
                            for elem in verifying_elements:
                                try:
                                    if elem.is_displayed():
                                        still_verifying = True
                                        break
                                except:
                                    continue
                            
                            # If "Verifying..." is gone, challenge might be complete
                            if not still_verifying:
                                self.logger.info("✓ 'Verifying...' element disappeared - challenge may be complete")
                                time.sleep(2)  # Wait a bit more for page to load
                    except:
                        pass
                    
                    # Check page source and URL
                    page_source = self.driver.page_source
                    current_url = self.driver.current_url.lower()
                    
                    # Check if we're still on Cloudflare challenge URL
                    if 'challenges.cloudflare.com' in current_url or '/cdn-cgi/challenge' in current_url:
                        # Still on challenge page, continue waiting
                        pass
                    else:
                        # Not on challenge URL - check if challenge indicators are gone
                        page_preview = page_source[:10000].lower()  # Check more content (increased from 5000)
                        has_challenge_text = (
                            "just a moment" in page_preview or 
                            "checking your browser" in page_preview or
                            "verifying you are human" in page_preview or
                            "review the security of your connection" in page_preview or
                            "verifying..." in page_preview or
                            "this may take a few seconds" in page_preview or
                            "ddos protection" in page_preview or
                            "ray id" in page_preview or
                            "cf-browser-verification" in page_preview
                        )
                        
                        # Also check for challenge elements in DOM
                        try:
                            from selenium.webdriver.common.by import By
                            challenge_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                                ".cf-browser-verification, .challenge-container, #challenge-form, [data-ray]")
                            if challenge_elements:
                                for elem in challenge_elements:
                                    try:
                                        if elem.is_displayed():
                                            has_challenge_text = True
                                            break
                                    except:
                                        continue
                        except:
                            pass
                        
                        # Enhanced check: If no challenge text AND we have substantial content, we're good
                        # Also check that "Verifying..." elements are gone
                        verifying_gone = True
                        try:
                            from selenium.webdriver.common.by import By
                            verifying_elements_check = self.driver.find_elements(By.XPATH, 
                                "//*[contains(text(), 'Verifying') or contains(text(), 'verifying')]")
                            for elem in verifying_elements_check:
                                try:
                                    if elem.is_displayed():
                                        verifying_gone = False
                                        break
                                except:
                                    continue
                        except:
                            pass
                        
                        if not has_challenge_text and len(page_source) > 8000 and verifying_gone:  # Increased threshold
                            # Domain check if target URL provided
                            on_target = True
                            if target_url:
                                try:
                                    target_domain = urlparse(target_url).netloc.split(':')[0].lower().replace('www.', '')
                                    current_domain = urlparse(current_url if '//' in current_url else 'http://' + current_url).netloc.split(':')[0].lower().replace('www.', '')
                                    on_target = target_domain == current_domain or target_domain in current_domain
                                except:
                                    # Simple fallback check
                                    try:
                                        target_domain = target_url.split('//')[1].split('/')[0].split(':')[0].lower().replace('www.', '')
                                        on_target = target_domain in current_url.replace('www.', '')
                                    except:
                                        on_target = True  # If we can't parse, assume we're on target
                            
                            if on_target:
                                elapsed = time.time() - start_time
                                self.logger.info(f"✅ Cloudflare bypassed successfully! (took {elapsed:.1f}s)")
                                # Additional wait for page to fully stabilize
                                time.sleep(random.uniform(2, 3))  # Increased from 1-2s to 2-3s
                                return True
                    
                    # Wait before next check (with some randomness)
                    time.sleep(check_interval + random.uniform(-0.3, 0.3))
                    
                except Exception as e:
                    self.logger.debug(f"Error while waiting for Cloudflare: {str(e)}")
                    time.sleep(check_interval)
            
            # Timeout reached for this attempt
            elapsed = time.time() - attempt_start
            retry_count += 1
            
            if retry_count <= max_retries:
                self.logger.warning(f"⚠️ Cloudflare timeout after {elapsed:.1f}s (attempt {retry_count}/{max_retries + 1})")
                
                # Enhanced retry strategy: Navigate to target URL instead of just refreshing
                try:
                    if target_url:
                        self.logger.info(f"🔄 Retrying: Navigating to {target_url}...")
                        # Try navigating to target URL again (sometimes works better than refresh)
                        self.driver.get(target_url)
                        time.sleep(random.uniform(3, 5))  # Wait after navigation
                        
                        # Simulate human behavior
                        self.simulate_human_behavior()
                        time.sleep(random.uniform(1, 2))
                    else:
                        self.logger.info("🔄 Refreshing page...")
                        self.driver.refresh()
                        time.sleep(random.uniform(3, 5))
                except Exception as retry_error:
                    self.logger.debug(f"Retry navigation error: {str(retry_error)}")
                    # Fallback to refresh
                    try:
                        self.driver.refresh()
                        time.sleep(random.uniform(3, 5))
                    except:
                        pass
            else:
                # All retries exhausted - final comprehensive check
                total_elapsed = time.time() - start_time
                try:
                    page_source_final = self.driver.page_source
                    current_url_final = self.driver.current_url.lower()
                    
                    # Comprehensive final check - include all challenge indicators
                    page_preview_final = page_source_final[:10000].lower()  # Check more content
                    has_challenge_final = (
                        'challenges.cloudflare.com' in current_url_final or
                        "just a moment" in page_preview_final or
                        "checking your browser" in page_preview_final or
                        "verifying you are human" in page_preview_final or
                        "review the security of your connection" in page_preview_final or
                        "verifying..." in page_preview_final or
                        "this may take a few seconds" in page_preview_final
                    )
                    
                    # Also check for challenge elements in DOM
                    try:
                        from selenium.webdriver.common.by import By
                        challenge_elements_final = self.driver.find_elements(By.CSS_SELECTOR, 
                            ".cf-browser-verification, .challenge-container, #challenge-form, [data-ray]")
                        if challenge_elements_final:
                            for elem in challenge_elements_final:
                                try:
                                    if elem.is_displayed():
                                        has_challenge_final = True
                                        break
                                except:
                                    continue
                    except:
                        pass
                    
                    # If page has substantial content and no challenge indicators, consider accessible
                    if len(page_source_final) > 8000 and not has_challenge_final:
                        self.logger.info(f"✅ Page accessible (final check) - continuing... (took {total_elapsed:.1f}s)")
                        return True
                except:
                    pass
                
                self.logger.warning(f"⚠️ Cloudflare challenge timeout after {total_elapsed:.1f}s (all {max_retries + 1} attempts exhausted)")
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
                        
                        # undetected_chromedriver handles Cloudflare automatically
                        # Wait a bit longer to let it complete the challenge
                        time.sleep(random.uniform(2, 4))  # Initial wait for page to start loading
                        
                        # Check if we're on a Cloudflare challenge page
                        current_url_after_wait = self.driver.current_url.lower()
                        page_preview_after_wait = self.driver.page_source[:10000] if len(self.driver.page_source) > 10000 else self.driver.page_source
                        
                        # Only check for Cloudflare if we're definitely on a challenge page
                        is_challenge_page = (
                            'challenges.cloudflare.com' in current_url_after_wait or 
                            '/cdn-cgi/challenge' in current_url_after_wait or
                            (len(page_preview_after_wait) < 5000 and self.has_cloudflare_challenge())
                        )
                        
                        if is_challenge_page:
                            self.logger.info("🛡️ Cloudflare challenge detected - waiting for undetected_chromedriver to handle it...")
                            
                            # undetected_chromedriver should handle this automatically, but we need to wait
                            # Give it plenty of time (up to 60 seconds total)
                            max_wait_time = 60
                            waited = 0
                            check_interval = 3
                            
                            while waited < max_wait_time:
                                time.sleep(check_interval)
                                waited += check_interval
                                
                                # Check current state
                                current_url_check = self.driver.current_url.lower()
                                page_source_check = self.driver.page_source
                                page_preview_check = page_source_check[:10000].lower() if len(page_source_check) > 10000 else page_source_check.lower()
                                
                                # Check if challenge is gone
                                still_on_challenge = (
                                    'challenges.cloudflare.com' in current_url_check or
                                    '/cdn-cgi/challenge' in current_url_check or
                                    "verifying you are human" in page_preview_check or
                                    "review the security of your connection" in page_preview_check or
                                    "verifying..." in page_preview_check or
                                    (len(page_source_check) < 5000 and self.has_cloudflare_challenge())
                                )
                                
                                if not still_on_challenge and len(page_source_check) > 8000:
                                    # Challenge passed!
                                    self.logger.info(f"✅ Cloudflare bypassed by undetected_chromedriver! (waited {waited}s)")
                                    time.sleep(random.uniform(1, 2))  # Brief stabilization
                                    break
                                
                                if waited % 10 == 0:  # Log every 10 seconds
                                    self.logger.info(f"⏳ Still waiting for Cloudflare bypass... ({waited}s/{max_wait_time}s)")
                            
                            # Final check - if still on challenge, use manual bypass
                            final_url = self.driver.current_url.lower()
                            final_page = self.driver.page_source
                            still_challenged = (
                                'challenges.cloudflare.com' in final_url or
                                '/cdn-cgi/challenge' in final_url or
                                (len(final_page) < 5000 and self.has_cloudflare_challenge())
                            )
                            
                            if still_challenged:
                                self.logger.warning("⚠️ Cloudflare still present after wait - using manual bypass...")
                                cloudflare_bypassed = self.wait_for_cloudflare(timeout=60, target_url=url, max_retries=2)
                                if not cloudflare_bypassed:
                                    retry_count += 1
                                    if retry_count < max_retries:
                                        delay = random.uniform(10, 15)
                                        self.logger.warning(f"Retrying page load in {delay:.1f}s...")
                                        time.sleep(delay)
                                        continue
                                    else:
                                        return None
                            else:
                                self.logger.info("✓ Cloudflare bypassed successfully!")
                        else:
                            # Not on challenge page, continue normally
                            pass
                        
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
                        # Handle timeout - check for Cloudflare first, then wait for content
                        recovery = self.error_handler.handle_error(e, retry_count, {'url': url})
                        
                        # IMPORTANT: Check for Cloudflare when timeout occurs (Cloudflare might be causing the timeout)
                        try:
                            current_url_check = self.driver.current_url.lower()
                            page_preview = self.driver.page_source[:6000] if self.driver.page_source else ''
                            
                            # Check if we're on Cloudflare challenge page
                            if ('challenges.cloudflare.com' in current_url_check or '/cdn-cgi/challenge' in current_url_check or len(page_preview) < 5000) and self.has_cloudflare_challenge():
                                self.logger.info("🛡️ Cloudflare challenge detected during timeout - waiting for bypass...")
                                # Increased timeout to 60s for "Verifying you are human" challenges
                                cloudflare_bypassed = self.wait_for_cloudflare(timeout=60, target_url=url, max_retries=2)
                                if cloudflare_bypassed:
                                    # Cloudflare bypassed, now get the page content
                                    time.sleep(random.uniform(1, 2))
                                    html = self.driver.page_source
                                    if html and len(html) > 5000:
                                        self.health_status['successful_requests'] += 1
                                        self.health_status['consecutive_failures'] = 0
                                        self.health_status['last_success_time'] = datetime.now()
                                        return html
                        except Exception as cf_check_error:
                            self.logger.debug(f"Error checking Cloudflare during timeout: {str(cf_check_error)}")
                        
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
    
    def convert_currency(self, amount, from_currency, to_currency):
        """
        Convert currency amount from one currency to another
        
        Args:
            amount: Numeric amount (as float or string)
            from_currency: Source currency code (e.g., 'EUR', 'USD')
            to_currency: Target currency code (e.g., 'EUR', 'USD')
        
        Returns:
            str: Converted amount as string, or original amount if conversion fails
        """
        try:
            # Convert amount to float
            if isinstance(amount, str):
                # Extract numeric value
                amount = re.sub(r'[^\d.]', '', amount)
            amount_float = float(amount)
            
            # If same currency, return as is
            if from_currency.upper() == to_currency.upper():
                return f"{amount_float:.2f}"
            
            # Exchange rates (approximate, can be updated)
            # Using approximate rates - for production, consider using an API
            # Updated rate: €870.63 * 1.16 = 1009.24 USD (as per user requirement)
            exchange_rates = {
                'EUR': {
                    'USD': 1.16,  # 1 EUR = 1.16 USD (updated rate)
                },
                'USD': {
                    'EUR': 0.862,  # 1 USD = 0.862 EUR (inverse of 1.16)
                },
            }
            
            # Get conversion rate
            if from_currency.upper() in exchange_rates:
                if to_currency.upper() in exchange_rates[from_currency.upper()]:
                    rate = exchange_rates[from_currency.upper()][to_currency.upper()]
                    converted = amount_float * rate
                    return f"{converted:.2f}"
            
            # If conversion not available, return original
            self.logger.warning(f"Currency conversion not available: {from_currency} -> {to_currency}, returning original amount")
            return f"{amount_float:.2f}"
        except Exception as e:
            self.logger.warning(f"Error converting currency: {str(e)}, returning original amount")
            return str(amount) if amount else ''
    
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
                text = elem.get_text(strip=True)
                # Ensure text is safe for logging and storage
                try:
                    # Try to encode/decode to ensure it's valid Unicode
                    text.encode('utf-8')
                    return text
                except UnicodeEncodeError:
                    # Replace problematic characters
                    return text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        except Exception as e:
            self.logger.warning(f"Error extracting text: {str(e)}")
            return default
    
    def safe_str(self, value, default=''):
        """
        Safely convert value to string, handling Unicode encoding issues
        
        Args:
            value: Value to convert to string
            default: Default value if conversion fails
        
        Returns:
            str: Safe string representation
        """
        try:
            if value is None:
                return default
            str_value = str(value)
            # Ensure it can be encoded
            str_value.encode('utf-8')
            return str_value
        except (UnicodeEncodeError, UnicodeDecodeError):
            # Replace problematic characters
            try:
                return str(value).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            except:
                return default
        except Exception:
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
                if 'handle is invalid' in error_msg or 'invalid' in error_msg or 'winerror 6' in error_msg:
                    # This is harmless - driver was already closed or handle invalid
                    # Suppress the error message to avoid cluttering output
                    pass  # Don't log this - it's expected during cleanup
                else:
                    cleanup_errors.append(f"Error closing driver: {str(e)}")
            except Exception as e:
                error_msg = str(e).lower()
                # Suppress common harmless cleanup errors
                if 'handle is invalid' in error_msg or 'winerror 6' in error_msg or 'invalid handle' in error_msg:
                    pass  # Suppress - harmless cleanup error
                else:
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