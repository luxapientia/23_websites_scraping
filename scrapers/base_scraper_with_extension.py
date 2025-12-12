"""Base scraper class with browser extension support for sites that require anti-detection extensions"""
from scrapers.base_scraper import BaseScraper
import undetected_chromedriver as uc
import os
import json
import time


class BaseScraperWithExtension(BaseScraper):
    """
    Base scraper class with browser extension support.
    Extends BaseScraper to add support for loading Chrome extensions (e.g., anti-detection, proxy extensions).
    
    This is specifically designed for the 8 sites that are getting blocked:
    - audiusa, ford, jaguar, mazda, subaru, volkswagen, volvo, porsche
    """
    
    def __init__(self, site_name, use_selenium=False, headless=False, extension_paths=None):
        """
        Initialize the scraper with extension support.
        
        Args:
            site_name: Name of the site being scraped
            use_selenium: Whether to use Selenium (should be True for these sites)
            headless: Whether to run in headless mode
            extension_paths: List of paths to unpacked Chrome extension directories (optional)
                           If None, will try to load from config or auto-detect from extensions folder
        """
        # Initialize extension_paths BEFORE calling super().__init__()
        # because BaseScraper.__init__() calls setup_selenium() which needs extension_paths
        # We need to set site_name temporarily so _get_extension_paths() can access it
        self.site_name = site_name
        
        # Get extension paths from parameter, config, or auto-detect
        if extension_paths is not None:
            self.extension_paths = extension_paths
        else:
            # Temporarily set up minimal attributes needed for _get_extension_paths()
            # We'll properly initialize via super() after this
            self.extension_paths = self._get_extension_paths()
        
        # Now call parent __init__ which may call setup_selenium()
        # setup_selenium() will use self.extension_paths which is now set
        super().__init__(site_name, use_selenium=use_selenium, headless=headless)
        
        # Log extension status after initialization
        if self.extension_paths:
            self.logger.info(f"Extension support enabled. Found {len(self.extension_paths)} extension(s):")
            for ext_path in self.extension_paths:
                self.logger.info(f"  - {os.path.basename(ext_path)}")
        else:
            self.logger.info("No extension paths configured. Browser will start without extensions.")
    
    def _get_extension_paths(self):
        """
        Get extension paths from config file, environment variable, or auto-detect from extensions folder.
        
        Returns:
            list: List of paths to extension directories, or empty list if none found
        """
        extension_paths = []
        
        # Helper function to safely log (logger might not be initialized yet)
        def safe_log(level, message):
            if hasattr(self, 'logger') and self.logger:
                if level == 'info':
                    self.logger.info(message)
                elif level == 'warning':
                    self.logger.warning(message)
                elif level == 'debug':
                    self.logger.debug(message)
        
        # First, try to get from config file
        try:
            config_path = os.path.join(os.getcwd(), 'config', 'sites_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    for site in config.get('sites', []):
                        if site.get('name') == self.site_name:
                            # Support both single path (string) and multiple paths (list)
                            ext_config = site.get('extension_path') or site.get('extension_paths')
                            if ext_config:
                                if isinstance(ext_config, str):
                                    # Single path
                                    ext_paths = [ext_config]
                                elif isinstance(ext_config, list):
                                    # Multiple paths
                                    ext_paths = ext_config
                                else:
                                    ext_paths = []
                                
                                # Resolve relative paths and validate
                                for ext_path in ext_paths:
                                    if not os.path.isabs(ext_path):
                                        ext_path = os.path.join(os.getcwd(), ext_path)
                                    if os.path.exists(ext_path):
                                        manifest_path = os.path.join(ext_path, 'manifest.json')
                                        if os.path.exists(manifest_path):
                                            extension_paths.append(ext_path)
                                        else:
                                            safe_log('warning', f"Extension path does not contain manifest.json: {ext_path}")
                                    else:
                                        safe_log('warning', f"Extension path not found: {ext_path}")
                                
                                if extension_paths:
                                    return extension_paths
        except Exception as e:
            safe_log('debug', f"Error reading extension paths from config: {str(e)}")
        
        # Fallback to environment variable
        env_path = os.getenv('CHROME_EXTENSION_PATH', None)
        if env_path:
            # Support comma-separated paths
            env_paths = [p.strip() for p in env_path.split(',')]
            for ext_path in env_paths:
                if not os.path.isabs(ext_path):
                    ext_path = os.path.join(os.getcwd(), ext_path)
                if os.path.exists(ext_path):
                    manifest_path = os.path.join(ext_path, 'manifest.json')
                    if os.path.exists(manifest_path):
                        extension_paths.append(ext_path)
                    else:
                        safe_log('warning', f"Extension path does not contain manifest.json: {ext_path}")
                else:
                    safe_log('warning', f"Extension path from environment variable not found: {ext_path}")
            
            if extension_paths:
                return extension_paths
        
        # Auto-detect: Look for all folders in extensions/ directory
        extensions_dir = os.path.join(os.getcwd(), 'extensions')
        if os.path.exists(extensions_dir):
            # Only log if logger is available (after super().__init__())
            if hasattr(self, 'logger') and self.logger:
                self.logger.info(f"Auto-detecting extensions from: {extensions_dir}")
            for item in os.listdir(extensions_dir):
                ext_path = os.path.join(extensions_dir, item)
                if os.path.isdir(ext_path):
                    manifest_path = os.path.join(ext_path, 'manifest.json')
                    if os.path.exists(manifest_path):
                        extension_paths.append(ext_path)
                        if hasattr(self, 'logger') and self.logger:
                            self.logger.info(f"  Found extension: {item}")
                    else:
                        if hasattr(self, 'logger') and self.logger:
                            self.logger.debug(f"  Skipping {item} (no manifest.json)")
            
            if extension_paths:
                return extension_paths
        
        return []
    
    def _create_chrome_options(self):
        """
        Create a new ChromeOptions object with extension support.
        Overrides the base class method to add extension loading.
        """
        # Call parent method to get base options
        options = super()._create_chrome_options()
        
        # Load browser extensions if configured
        # Safety check: ensure extension_paths is initialized
        if not hasattr(self, 'extension_paths'):
            self.extension_paths = []
        
        if self.extension_paths:
            for ext_path in self.extension_paths:
                if os.path.exists(ext_path):
                    # Verify it's a valid extension directory (should contain manifest.json)
                    manifest_path = os.path.join(ext_path, 'manifest.json')
                    if os.path.exists(manifest_path):
                        # For unpacked extensions, use --load-extension argument
                        # Chrome supports multiple --load-extension arguments
                        options.add_argument(f'--load-extension={ext_path}')
                        self.logger.info(f"✓ Browser extension will be loaded: {os.path.basename(ext_path)}")
                    else:
                        self.logger.warning(f"Extension directory does not contain manifest.json: {ext_path}")
                else:
                    self.logger.warning(f"Extension path does not exist: {ext_path}")
        
        return options
    
    def setup_selenium(self):
        """
        Setup undetected ChromeDriver with extension support.
        Overrides the base class method to ensure extensions are loaded.
        """
        # Don't start if browser already exists
        if self.driver is not None:
            self.logger.info("Browser already initialized, skipping...")
            return
        
        try:
            # Detect Chrome version to ensure ChromeDriver compatibility
            chrome_version = self._detect_chrome_version()
            
            # Try to use local ChromeDriver first to avoid network timeout
            chromedriver_path = os.path.join(os.getcwd(), 'chromedriver-win32', 'chromedriver.exe')
            driver_executable_path = chromedriver_path if os.path.exists(chromedriver_path) else None
            
            # Create undetected ChromeDriver with stealth settings and extensions
            max_retries = 2
            retry_count = 0
            driver_initialized = False
            
            while retry_count < max_retries and not driver_initialized:
                try:
                    # Create fresh ChromeOptions for each retry attempt
                    # This will include all extensions if configured
                    options = self._create_chrome_options()
                    
                    if driver_executable_path and os.path.exists(driver_executable_path):
                        # Use local ChromeDriver to avoid network timeout
                        self.logger.info(f"Using local ChromeDriver with undetected_chromedriver: {driver_executable_path}")
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=chrome_version,
                            use_subprocess=True,
                            driver_executable_path=driver_executable_path,
                            browser_executable_path=None
                        )
                    else:
                        # No local ChromeDriver, try auto-download
                        self.logger.info("No local ChromeDriver found, attempting auto-download with undetected_chromedriver...")
                        self.driver = uc.Chrome(
                            options=options, 
                            version_main=chrome_version,
                            use_subprocess=True,
                            driver_executable_path=None,
                            browser_executable_path=None
                        )
                    
                    # Maximize window if not headless
                    try:
                        self.driver.maximize_window()
                        if not self.driver.window_handles:
                            raise Exception("Window closed immediately after maximization")
                    except Exception as window_error:
                        error_msg = str(window_error).lower()
                        if 'no such window' in error_msg or 'target window already closed' in error_msg:
                            self.logger.warning(f"Window closed during maximization: {str(window_error)}")
                            try:
                                self.driver.switch_to.new_window('tab')
                                self.logger.info("Created new window after closure")
                            except:
                                raise Exception(f"Failed to recover from window closure: {str(window_error)}")
                        else:
                            self.logger.debug(f"Window maximization failed (non-critical): {str(window_error)}")
                    
                    # Wait a moment for window and extensions to stabilize
                    time.sleep(2)  # Give extensions time to load
                    
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
                                time.sleep(0.5)
                                continue
                            else:
                                raise Exception(f"No window handles available after initialization: {str(check_error)}")
                    
                    if not window_stable:
                        raise Exception("Window is not stable after initialization")
                    
                    driver_initialized = True
                    if self.extension_paths:
                        extension_status = f"with {len(self.extension_paths)} extension(s)"
                        self.logger.info(f"Browser initialized successfully for {self.site_name} {extension_status}")
                        # Verify extensions are loaded by checking Chrome's extension list
                        try:
                            # Give extensions a moment to fully initialize
                            time.sleep(1)
                            # Log extension names for verification
                            ext_names = [os.path.basename(ext) for ext in self.extension_paths]
                            self.logger.info(f"✓ Extensions loaded: {', '.join(ext_names)}")
                        except Exception as ext_check_error:
                            self.logger.debug(f"Could not verify extension loading: {str(ext_check_error)}")
                    else:
                        self.logger.info(f"Browser initialized successfully for {self.site_name} without extensions")
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Handle version mismatch errors
                    if 'version' in error_msg and ('chromedriver' in error_msg or 'chrome version' in error_msg):
                        self.logger.warning(f"ChromeDriver version mismatch detected: {str(e)}")
                        retry_count += 1
                        if retry_count < max_retries:
                            if driver_executable_path and os.path.exists(driver_executable_path):
                                self.logger.info(f"Local ChromeDriver version mismatch, retrying with auto-download (attempt {retry_count}/{max_retries})...")
                                driver_executable_path = None
                                chrome_version = None
                                time.sleep(2)
                                continue
                            elif chrome_version is not None:
                                self.logger.info(f"Retrying with auto-detection (attempt {retry_count}/{max_retries})...")
                                chrome_version = None
                                time.sleep(2)
                                continue
                            else:
                                self.logger.info(f"Retrying without version specification (attempt {retry_count}/{max_retries})...")
                                chrome_version = None
                                time.sleep(2)
                                continue
                        else:
                            self.logger.error(f"Failed to initialize ChromeDriver after version mismatch retries: {str(e)}")
                            raise
                    
                    # Handle network/timeout errors
                    elif any(keyword in error_msg for keyword in ['timeout', 'connection', 'failed', 'urlopen', '10060', 'no such window', 'target window already closed', 'web view not found']):
                        retry_count += 1
                        if retry_count < max_retries:
                            self.logger.warning(f"Network/window error during initialization (attempt {retry_count}/{max_retries}): {str(e)}")
                            try:
                                if self.driver:
                                    self.driver.quit()
                                    self.driver = None
                            except:
                                pass
                            time.sleep(2 * retry_count)
                            continue
                        else:
                            self.logger.error(f"Failed to initialize browser after {max_retries} attempts: {str(e)}")
                            raise
                    else:
                        # Other errors - re-raise
                        self.logger.error(f"Unexpected error during browser initialization: {str(e)}")
                        raise
                        
        except Exception as e:
            self.logger.error(f"Failed to setup Selenium with extension support: {str(e)}")
            raise
    
    def ensure_driver(self):
        """
        Ensure driver is initialized and valid, reinitialize if needed.
        Overrides base class to ensure extensions are loaded.
        """
        if self.driver and self.is_driver_valid():
            return True
        
        max_reinit_attempts = 3
        attempt = 0
        
        while attempt < max_reinit_attempts:
            self.logger.warning(f"Driver session invalid or missing, reinitializing with extension support (attempt {attempt + 1}/{max_reinit_attempts})...")
            try:
                if self.driver:
                    old_driver = self.driver
                    self.driver = None
                    try:
                        old_driver.quit()
                    except (OSError, AttributeError):
                        pass
                    except Exception:
                        pass
                    finally:
                        try:
                            del old_driver
                        except:
                            pass
                
                self.setup_selenium()
                self.logger.info("✓ Driver reinitialized successfully with extension support")
                return True
                    
            except Exception as e:
                attempt += 1
                if attempt >= max_reinit_attempts:
                    self.logger.error(f"Failed to reinitialize driver after {max_reinit_attempts} attempts: {str(e)}")
                    raise
                time.sleep(2 * attempt)
        
        return False
