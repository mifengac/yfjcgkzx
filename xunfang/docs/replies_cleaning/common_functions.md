# Common Functions

## clean_replies_text

Source: `xunfang/service/jiemiansanlei_service.py`

Purpose: normalize the raw `replies` value from dispatch logs before keyword filtering. The raw field contains routing, delivery, sign-off, call, tag, and feedback entries. Street filtering should use operational feedback content, not system workflow noise.

```python
def clean_replies_text(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if text.startswith("暂无"):
        text = text[2:].strip()
    if not text:
        return ""

    parts = [part.strip() for part in _REPLIES_ENTRY_HEADER_RE.split(text) if part and part.strip()]
    kept: List[str] = []
    for part in parts:
        compact = " ".join(part.split()).strip()
        if compact.startswith("暂无"):
            compact = compact[2:].strip()
        if not compact or any(compact.startswith(prefix) for prefix in _REPLIES_DROP_PREFIXES):
            continue
        if any(marker in compact for marker in _REPLIES_KEEP_MARKERS):
            kept.append(_normalize_replies_feedback_text(compact))
    return " ".join(kept)
```

Keep entries when they contain feedback markers such as process feedback, arrival feedback, closing feedback, supplementary closing feedback, or result-description text.

Drop entries when they are only dispatch workflow events, including unit selection, dispatch assignment, terminal delivery, sign-off, save events, video-call records, source-type edits, party-info entry, linked-reply records, and tag edits.

Use the cleaned text for keyword matching. Keep the raw `replies` value for export display unless the user explicitly asks to export cleaned replies.
