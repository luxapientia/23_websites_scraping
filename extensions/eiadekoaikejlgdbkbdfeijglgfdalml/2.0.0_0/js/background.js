chrome.runtime.onInstalled.addListener(() => {
  // Open the options page upon install
  chrome.runtime.openOptionsPage();

  // Set default WebRTC handling policy if not already set
  chrome.storage.local.get("rtcIPHandling", (items) => {
    if (items.rtcIPHandling === undefined) {
      chrome.storage.local.set({ rtcIPHandling: "default_public_interface_only" }, () => {
        chrome.privacy.network.webRTCIPHandlingPolicy.set({
          value: "default_public_interface_only"
        });
      });
    }
  });
});

chrome.action.onClicked.addListener(() => {
  chrome.runtime.openOptionsPage();
})
