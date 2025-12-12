// Background service worker

chrome.runtime.onInstalled.addListener(() => {
  console.log("uBlock Origin Clone installed.");
  // Initialize storage
  chrome.storage.local.set({
    blockingEnabled: true,
    blockedStats: 0
  });
});

// Listen for messages from popup
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === "getStats") {
    chrome.storage.local.get(['blockedStats'], (result) => {
      sendResponse({ blockedStats: result.blockedStats || 0 });
    });
    return true; // Keep channel open
  }
});

// Note: In Manifest V3 with declarativeNetRequest, we don't count blocked requests in the background script
// the same way as webRequest. We would need to use the declarativeNetRequest.onRuleMatchedDebug listener
// which requires the "declarativeNetRequestFeedback" permission (only for unpacked extensions).
// For this clone, we will simulate stats or just show the static count for now if we can't get real-time feedback easily without extra permissions.
