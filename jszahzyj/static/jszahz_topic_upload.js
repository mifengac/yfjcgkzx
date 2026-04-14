(function (root) {
  var ns = root.JSZahzTopicApp = root.JSZahzTopicApp || {};

  ns.showUploadOverlay = function (title, detail) {
    var overlay = ns.$("jszahzTopicUploadOverlay");
    var titleEl = ns.$("jszahzTopicUploadOverlayTitle");
    var detailEl = ns.$("jszahzTopicUploadOverlayDetail");
    if (titleEl) titleEl.textContent = title || "正在处理上传...";
    if (detailEl) detailEl.textContent = detail || "";
    if (overlay) overlay.classList.remove("jszahz-topic-hidden");
  };

  ns.updateUploadOverlay = function (title, detail) {
    var titleEl = ns.$("jszahzTopicUploadOverlayTitle");
    var detailEl = ns.$("jszahzTopicUploadOverlayDetail");
    if (titleEl && title) titleEl.textContent = title;
    if (detailEl) detailEl.textContent = detail || "";
  };

  ns.hideUploadOverlay = function () {
    var overlay = ns.$("jszahzTopicUploadOverlay");
    if (overlay) overlay.classList.add("jszahz-topic-hidden");
  };

  ns.uploadExcel = function () {
    var input = ns.$("jszahzTopicFile");
    if (!input || !input.files || !input.files[0]) {
      ns.setError("请先选择 Excel 文件。");
      return Promise.resolve();
    }

    var formData = new FormData();
    formData.append("file", input.files[0]);
    ns.setError("");
    ns.setStatus("");
    ns.showUploadOverlay("正在上传文件...", "");

    return new Promise(function (resolve, reject) {
      var xhr = new XMLHttpRequest();
      var processedLength = 0;
      var pendingText = "";
      var lastPayload = null;
      var uploadVersion = "";
      var timer = root.setTimeout(function () {
        xhr.abort();
      }, 180000);

      function applyUploadVersion(version) {
        if (!version || version === uploadVersion) return;
        uploadVersion = version;
        if (root.console && root.console.info) {
          root.console.info("JSZAHZ upload api version:", uploadVersion);
        }
      }

      function buildOverlayDetail(detail) {
        var detailText = detail || "";
        if (!uploadVersion) return detailText;
        return detailText ? detailText + " | 接口版本: " + uploadVersion : "接口版本: " + uploadVersion;
      }

      function processText(text, isFinal) {
        var newText = text.substring(processedLength);
        var lines;
        processedLength = text.length;
        if (!newText) {
          if (!isFinal || !pendingText.trim()) {
            return;
          }
        }

        pendingText += newText;
        lines = pendingText.split("\n");
        pendingText = lines.pop() || "";

        lines.forEach(function (line) {
          var trimmed = line.trim();
          var data;
          if (!trimmed) return;
          try {
            data = JSON.parse(trimmed);
            applyUploadVersion(data.api_version || "");
            if (data.progress) {
              ns.updateUploadOverlay(data.title || "", buildOverlayDetail(data.detail || ""));
            } else {
              lastPayload = data;
            }
          } catch (error) {}
        });

        if (isFinal && pendingText.trim()) {
          try {
            var tail = JSON.parse(pendingText.trim());
            applyUploadVersion(tail.api_version || "");
            if (tail.progress) {
              ns.updateUploadOverlay(tail.title || "", buildOverlayDetail(tail.detail || ""));
            } else {
              lastPayload = tail;
            }
          } catch (error) {}
          pendingText = "";
        }
      }

      xhr.open("POST", "/jszahzyj/api/jszahzztk/upload");
      xhr.onprogress = function () {
        if (xhr.responseText) processText(xhr.responseText, false);
      };
      xhr.onload = function () {
        root.clearTimeout(timer);
        ns.hideUploadOverlay();
        applyUploadVersion(xhr.getResponseHeader("X-JSZAHZ-Upload-Version") || "");
        if (xhr.responseText) processText(xhr.responseText, true);
        if (xhr.status < 200 || xhr.status >= 300) {
          reject(new Error((lastPayload && lastPayload.message) || "上传失败"));
          return;
        }
        if (!lastPayload || !lastPayload.success) {
          reject(new Error((lastPayload && lastPayload.message) || "上传失败"));
          return;
        }
        input.value = "";
        ns.setBatchInfo(lastPayload.active_batch || null);
        ns.setStatus(
          "上传成功，已切换为最新生效批次。" +
          (uploadVersion ? " 接口版本: " + uploadVersion : "")
        );
        resolve();
      };
      xhr.onerror = function () {
        root.clearTimeout(timer);
        ns.hideUploadOverlay();
        reject(new Error("上传请求失败，请检查网络连接。"));
      };
      xhr.onabort = function () {
        root.clearTimeout(timer);
        ns.hideUploadOverlay();
        reject(new Error("上传超过 3 分钟仍未完成，后端可能正在等待数据库锁或执行长查询。"));
      };
      xhr.send(formData);
    });
  };
})(window);
