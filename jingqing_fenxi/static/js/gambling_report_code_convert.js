(function() {
    var initialized = false;

    function getEl(id) {
        return document.getElementById(id);
    }

    function setStatus(message, isError) {
        var status = getEl("gamblingCodeConvertStatus");
        if (!status) return;
        status.textContent = message || "";
        status.classList.toggle("error", Boolean(isError));
    }

    function openModal() {
        var modal = getEl("gamblingCodeConvertModal");
        var fileInput = getEl("gamblingCodeConvertFile");
        if (fileInput) fileInput.value = "";
        setStatus("", false);
        if (modal) modal.classList.remove("special-case-hidden");
    }

    function closeModal() {
        var modal = getEl("gamblingCodeConvertModal");
        if (modal) modal.classList.add("special-case-hidden");
    }

    function filenameFromDisposition(disposition) {
        var fallback = "赌博分析报告_派出所名称转换.docx";
        if (!disposition) return fallback;
        var utfMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i);
        if (utfMatch) {
            try {
                return decodeURIComponent(utfMatch[1]);
            } catch (error) {
                return fallback;
            }
        }
        var plainMatch = disposition.match(/filename="?([^";]+)"?/i);
        return plainMatch ? plainMatch[1] : fallback;
    }

    function triggerDownload(blob, filename) {
        var url = window.URL.createObjectURL(blob);
        var link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
    }

    function convertAndDownload(event) {
        event.preventDefault();
        var fileInput = getEl("gamblingCodeConvertFile");
        var confirmBtn = getEl("gamblingCodeConvertConfirmBtn");
        if (!fileInput || !fileInput.files || !fileInput.files.length) {
            setStatus("请先选择 markdown 文件", true);
            return;
        }
        var file = fileInput.files[0];
        if (!/\.md$/i.test(file.name)) {
            setStatus("只支持上传 .md 格式文件", true);
            return;
        }

        var formData = new FormData();
        formData.append("file", file);
        setStatus("正在转换，请稍候...", false);
        if (confirmBtn) confirmBtn.disabled = true;

        fetch("/jingqing_fenxi/download/gambling-topic/code-convert", {
            method: "POST",
            body: formData
        })
            .then(function(response) {
                if (!response.ok) {
                    return response.json().catch(function() {
                        return { message: "转换失败" };
                    }).then(function(payload) {
                        throw new Error(payload.message || "转换失败");
                    });
                }
                var filename = filenameFromDisposition(response.headers.get("Content-Disposition"));
                return response.blob().then(function(blob) {
                    return { blob: blob, filename: filename };
                });
            })
            .then(function(result) {
                triggerDownload(result.blob, result.filename);
                setStatus("转换完成，已开始下载。", false);
                closeModal();
            })
            .catch(function(error) {
                setStatus(error.message || "转换失败", true);
            })
            .finally(function() {
                if (confirmBtn) confirmBtn.disabled = false;
            });
    }

    function init() {
        if (initialized || !getEl("gamblingCodeConvertBtn")) return;
        initialized = true;
        var openBtn = getEl("gamblingCodeConvertBtn");
        var closeBtn = getEl("gamblingCodeConvertCloseBtn");
        var cancelBtn = getEl("gamblingCodeConvertCancelBtn");
        var form = getEl("gamblingCodeConvertForm");
        var modal = getEl("gamblingCodeConvertModal");
        if (openBtn) openBtn.addEventListener("click", openModal);
        if (closeBtn) closeBtn.addEventListener("click", closeModal);
        if (cancelBtn) cancelBtn.addEventListener("click", closeModal);
        if (form) form.addEventListener("submit", convertAndDownload);
        if (modal) {
            modal.addEventListener("click", function(event) {
                if (event.target === modal) closeModal();
            });
        }
    }

    window.GamblingReportCodeConvertPage = { init: init };
})();
