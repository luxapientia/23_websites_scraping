var background = {
  "port": null,
  "message": {},
  "receive": function (id, callback) {
    if (id) {
      background.message[id] = callback;
    }
  },
  "send": function (id, data) {
    if (id) {
      chrome.runtime.sendMessage({
        "method": id,
        "data": data,
        "path": "popup-to-background"
      }, function () {
        return chrome.runtime.lastError;
      });
    }
  },
  "connect": function (port) {
    chrome.runtime.onMessage.addListener(background.listener); 
    /*  */
    if (port) {
      background.port = port;
      background.port.onMessage.addListener(background.listener);
      background.port.onDisconnect.addListener(function () {
        background.port = null;
      });
    }
  },
  "post": function (id, data) {
    if (id) {
      if (background.port) {
        background.port.postMessage({
          "method": id,
          "data": data,
          "path": "popup-to-background",
          "port": background.port.name
        });
      }
    }
  },
  "listener": function (e) {
    if (e) {
      for (let id in background.message) {
        if (background.message[id]) {
          if ((typeof background.message[id]) === "function") {
            if (e.path === "background-to-popup") {
              if (e.method === id) {
                background.message[id](e.data);
              }
            }
          }
        }
      }
    }
  }
};

var config = {
  "ids": [
    "support", 
    "donation", 
    "fingerprint", 
    "notifications"
  ],
  "render": function (e) {
    const name = document.querySelector(".name");
    const notifications = document.querySelector(".notifications");
    /*  */
    name.textContent = chrome.runtime.getManifest().name;
    notifications.textContent = e.notifications ? '☑' : '☐';
  },
  "load": function () {
    for (let i = 0; i < config.ids.length; i++) {
      const icon = document.querySelector("." + config.ids[i]);
      const button = document.querySelector("#" + config.ids[i]);
      /*  */
      button.addEventListener("click", function (e) {
        background.send(e.target.id);
      });
      /*  */
      icon.addEventListener("click", function (e) {
        background.send(e.target.className.replace("icon ", ''));
      });
    }
    /*  */
    if (navigator.userAgent.indexOf("Edg") !== -1) {
      document.getElementById("explore").style.display = "none";
    }
    /*  */
    background.send("load");
    window.removeEventListener("load", config.load, false);
  }
};

background.receive("storage", config.render);
background.connect(chrome.runtime.connect({"name": "popup"}));

window.addEventListener("load", config.load, false);
