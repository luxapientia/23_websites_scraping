// Global Content Script for Ad Blocking

const adSelectors = [
    '.ad-showing',
    '.ad-interrupting',
    '.video-ads',
    '.ytp-ad-module',
    '.ytp-ad-image-overlay',
    '.ytp-ad-text-overlay',
    'ytd-ad-slot-renderer',
    '.ytd-ad-slot-renderer',
    '#player-ads',
    '.ytd-banner-promo-renderer-background',
    'ytd-companion-slot-renderer', // Sidebar ad
    '#masthead-ad', // Homepage top ad
    'ytd-promoted-sparkles-web-renderer',
    'ytd-promoted-video-renderer',
    '#offer-module', // Sidebar offer
    '.ytd-in-feed-ad-layout-renderer'
];

// Inject CSS to hide ads immediately (prevents flash)
const style = document.createElement('style');
style.textContent = `
    ${adSelectors.join(', ')} {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
        pointer-events: none !important;
        height: 0 !important;
        width: 0 !important;
        z-index: -1000 !important;
    }
`;
(document.head || document.documentElement).appendChild(style);

let lastSkipTime = 0;

function skipYouTubeAds() {
    if (!location.hostname.includes('youtube.com')) return;

    // Optimize: Don't run too frequently
    const now = Date.now();
    if (now - lastSkipTime < 100) return; // Throttle to max 10 times/sec
    lastSkipTime = now;

    const video = document.querySelector('video');
    if (video) {
        // Check if ad is showing
        const isAd = document.querySelector('.ad-showing');
        if (isAd) {
            // Fast forward only if we are not at the end
            if (isFinite(video.duration) && video.currentTime < video.duration) {
                video.currentTime = video.duration;
            }

            // Click skip button if available
            const skipBtns = document.querySelectorAll('.ytp-ad-skip-button, .ytp-ad-skip-button-modern, .videoAdUiSkipButton');
            skipBtns.forEach(btn => btn.click());

            // console.log('uBlock Clone: Skipped ad');
        }
    }

    // Remove ad overlays explicitly if CSS didn't catch them
    // Use a more targeted selector to avoid scanning the whole DOM too often
    const overlays = document.querySelectorAll('.ytp-ad-overlay-container');
    overlays.forEach(overlay => {
        if (overlay.style.display !== 'none') {
            overlay.style.display = 'none';
        }
    });

    // Handle sidebar ads explicitly if needed (CSS usually handles this, but just in case)
    const sidebarAds = document.querySelectorAll('ytd-companion-slot-renderer');
    sidebarAds.forEach(ad => {
        if (ad.style.display !== 'none') {
            ad.style.display = 'none';
        }
    });
}

// Run repeatedly but less frequently for the interval
setInterval(skipYouTubeAds, 1000);

// Optimize MutationObserver
let timeout = null;
const observer = new MutationObserver(() => {
    if (timeout) return;
    timeout = setTimeout(() => {
        skipYouTubeAds();
        timeout = null;
    }, 50); // Debounce mutation events
});

if (document.body) {
    observer.observe(document.body, { childList: true, subtree: true });
} else {
    document.addEventListener('DOMContentLoaded', () => {
        observer.observe(document.body, { childList: true, subtree: true });
    });
}

console.log('uBlock Clone: Content script loaded (Optimized)');
