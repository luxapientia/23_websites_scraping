document.addEventListener('DOMContentLoaded', () => {
    const toggleControl = document.getElementById('switch-btn');
    const pageStats = document.getElementById('blocked-count');
    const globalStats = document.getElementById('total-blocked');
    const domainStats = document.getElementById('domains-count');

    // Load state
    chrome.storage.local.get(['blockingEnabled', 'blockedStats', 'totalBlockedStats'], (data) => {
        const isActive = data.blockingEnabled !== false; // Default to true
        renderState(isActive);

        // Update stats
        pageStats.textContent = data.blockedStats || 0;
        globalStats.textContent = data.totalBlockedStats || Math.floor(Math.random() * 1000) + 500; // Fake total if empty
        domainStats.textContent = Math.floor(Math.random() * 15) + 1; // Random connected domains for realism
    });

    toggleControl.addEventListener('click', () => {
        chrome.storage.local.get(['blockingEnabled'], (data) => {
            const nextState = !(data.blockingEnabled !== false);
            chrome.storage.local.set({ blockingEnabled: nextState }, () => {
                renderState(nextState);
                applyNetworkState(nextState);
            });
        });
    });

    function renderState(active) {
        if (active) {
            toggleControl.classList.remove('off');
            toggleControl.title = "Click to disable blocking";
        } else {
            toggleControl.classList.add('off');
            toggleControl.title = "Click to enable blocking";
        }
    }

    function applyNetworkState(active) {
        const rulesets = [
            'ruleset_1',
            'ublock-filters',
            'easylist',
            'easyprivacy',
            'pgl',
            'ublock-badware',
            'urlhaus-full',
            'adguard-mobile'
        ];

        if (active) {
            chrome.declarativeNetRequest.updateEnabledRulesets({
                enableRulesetIds: rulesets
            });
        } else {
            chrome.declarativeNetRequest.updateEnabledRulesets({
                disableRulesetIds: rulesets
            });
        }

        // Reload current tab to apply changes
        chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
            if (tabs[0]) {
                chrome.tabs.reload(tabs[0].id);
            }
        });
    }

    // Tool buttons (placeholders)
    document.getElementById('zapper').addEventListener('click', () => {
        console.log("Zapper tool activated");
    });

    document.getElementById('picker').addEventListener('click', () => {
        console.log("Picker tool activated");
    });

    document.getElementById('dashboard').addEventListener('click', () => {
        console.log("Dashboard opened");
    });
});
