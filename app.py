"""
Flask web UI for approving/editing email replies before sending.
Runs alongside the assistant polling loop.
"""

import os
import time
import threading
from flask import Flask, render_template_string, request, jsonify

from assistant import (
    pending_approvals, send_reply, create_calendar_event, poll_gmail,
    send_general_telegram, get_email_thread, draft_general_reply,
    extract_meeting_details_from_reply,
    save_pending_approvals, build_calendar_json_preview, resolve_proposed_date,
    edit_telegram_message, edit_general_telegram_message,
    _get_telegram_config,
)

app = Flask(__name__)

CSS = """
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f5f5f5; color: #333; padding: 24px;
}
.container { max-width: 800px; margin: 0 auto; }
h1 { font-size: 22px; font-weight: 600; margin-bottom: 24px; color: #1a1a1a; }
.card {
    background: white; border-radius: 12px; padding: 20px;
    margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.card h2 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em; color: #888; margin-bottom: 12px; }
.original-email {
    background: #f9f9f9; border-left: 3px solid #ddd; padding: 12px;
    font-size: 14px; line-height: 1.6; white-space: pre-wrap; border-radius: 4px;
    max-height: 200px; overflow-y: auto;
}
.meta { font-size: 13px; color: #666; margin-bottom: 6px; }
.meta strong { color: #333; }
.travel-box {
    background: #e8f4fd; border: 1px solid #b3d9f7;
    border-radius: 8px; padding: 12px; font-size: 14px; color: #1a6fa0;
}
.meeting-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 14px; }
.detail-item { padding: 8px; background: #f9f9f9; border-radius: 6px; }
.detail-item label { font-size: 11px; text-transform: uppercase; color: #888; display: block; margin-bottom: 2px; }
textarea {
    width: 100%; min-height: 200px; padding: 12px;
    border: 1px solid #ddd; border-radius: 8px; font-size: 14px;
    line-height: 1.6; font-family: inherit; resize: vertical;
}
textarea:focus { outline: none; border-color: #4a90e2; box-shadow: 0 0 0 2px rgba(74,144,226,0.2); }
.calendar-toggle { display: flex; align-items: center; gap: 8px; font-size: 14px; margin-top: 12px; }
.calendar-toggle input[type=checkbox] { width: 16px; height: 16px; }
.actions { display: flex; gap: 12px; margin-top: 16px; }
.btn {
    padding: 12px 24px; border: none; border-radius: 8px;
    font-size: 15px; font-weight: 500; cursor: pointer; transition: background 0.15s; text-decoration: none; text-align: center;
}
.btn-send { background: #2e7d32; color: white; flex: 1; }
.btn-send:hover { background: #1b5e20; }
.btn-discard { background: #f5f5f5; color: #666; }
.btn-discard:hover { background: #e0e0e0; }
.status-badge {
    display: inline-block; padding: 4px 10px; border-radius: 20px;
    font-size: 12px; font-weight: 600; text-transform: uppercase;
}
.status-pending { background: #fff3cd; color: #856404; }
.status-sent { background: #d4edda; color: #155724; }
.status-discarded { background: #f8d7da; color: #721c24; }
.empty { text-align: center; padding: 60px; color: #888; font-size: 16px; }
.list-item {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 16px; border-bottom: 1px solid #f0f0f0; font-size: 14px;
}
.list-item:last-child { border-bottom: none; }
.review-link { color: #4a90e2; text-decoration: none; font-weight: 500; }
.review-link:hover { text-decoration: underline; }
.back-link { display: inline-block; margin-top: 16px; color: #4a90e2; font-size: 14px; }
</style>
"""


def base_html(title, content):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} — Email Assistant</title>
    {CSS}
</head>
<body>
<div class="container">
{content}
</div>
</body>
</html>"""


@app.route("/")
def index():
    # Filter to only real approval items (skip tg: and gentg: index keys)
    real_items = {
        aid: item for aid, item in pending_approvals.items()
        if isinstance(item, dict) and "email" in item
    }
    if real_items:
        rows = ""
        for aid, item in real_items.items():
            status = item["status"]
            email_type = item.get("type", "meeting")
            type_badge = "📧" if email_type == "general" else "📅"
            review = f'<a class="review-link" href="/approve/{aid}">Review →</a>' if status == "pending" else ""
            rows += f"""
            <div class="list-item">
                <div>
                    <div><strong>{type_badge} {item['email']['subject']}</strong></div>
                    <div style="color:#888;font-size:13px">From: {item['email']['sender']}</div>
                </div>
                <div style="display:flex;align-items:center;gap:12px">
                    <span class="status-badge status-{status}">{status}</span>
                    {review}
                </div>
            </div>"""
        content = f'<h1>Email Assistant</h1><div class="card">{rows}</div>'
    else:
        content = '''<h1>Email Assistant</h1>
        <div class="card">
            <div class="empty">No pending approvals.<br>
            <span style="font-size:14px;margin-top:8px;display:block">Watching your inbox every 30 seconds.</span>
            </div>
        </div>'''
    return base_html("Dashboard", content)


@app.route("/approve/<approval_id>")
def approve(approval_id):
    item = pending_approvals.get(approval_id)
    if not item:
        return base_html("Not Found", '<div class="card"><p>This approval was not found.</p></div>'), 404

    email = item["email"]
    details = item.get("meeting_details", {})
    travel = item.get("travel_info")
    reply = item.get("reply_text", "")

    # Meeting details grid
    detail_grid = f"""
    <div class="meeting-grid">
        <div class="detail-item"><label>Date</label>{details.get('proposed_date') or 'Not specified'}</div>
        <div class="detail-item"><label>Time</label>{details.get('proposed_time') or 'Not specified'}</div>
        <div class="detail-item"><label>Location</label>{details.get('location') or 'Not specified'}</div>
        <div class="detail-item"><label>Type</label>{details.get('meeting_type') or 'Not specified'}</div>
        <div class="detail-item"><label>Topic</label>{details.get('topic') or 'Not specified'}</div>
        <div class="detail-item"><label>Duration</label>{details.get('duration_minutes', 60)} minutes</div>
    </div>""" if details else ""

    # Travel info
    travel_html = f"""
    <div class="card">
        <h2>Travel Time</h2>
        <div class="travel-box">
            🚆 From <strong>{travel['origin']}</strong> to <strong>{travel['destination']}</strong><br>
            Travel time by public transit: <strong>{travel['duration_text']}</strong> ({travel['distance_text']})
        </div>
    </div>""" if travel else ""

    content = f"""
    <h1>Review Email Reply</h1>

    <div class="card">
        <h2>Original Email</h2>
        <div class="meta"><strong>From:</strong> {email['sender']}</div>
        <div class="meta"><strong>Subject:</strong> {email['subject']}</div>
        <div class="meta"><strong>Date:</strong> {email['date']}</div>
        <div class="original-email">{email['body']}</div>
    </div>

    {"<div class='card'><h2>Meeting Details</h2>" + detail_grid + "</div>" if detail_grid else ""}

    {travel_html}

    <div class="card">
        <h2>Drafted Reply — Edit Before Sending</h2>
        <form method="POST" action="/send/{approval_id}">
            <textarea name="reply_text">{reply}</textarea>
            <div class="calendar-toggle">
                <input type="checkbox" name="create_calendar" id="cc" checked>
                <label for="cc">Create calendar event in Caraomics calendar</label>
            </div>
            <div class="calendar-toggle" style="margin-top:8px">
                <input type="checkbox" name="invite_sender" id="inv">
                <label for="inv">Also send calendar invite to sender</label>
            </div>
            <div class="actions">
                <button type="submit" class="btn btn-send">✉️ Send Reply</button>
                <a href="/discard/{approval_id}" class="btn btn-discard">🗑 Discard</a>
            </div>
        </form>
    </div>
    <a class="back-link" href="/">← Back to dashboard</a>
    """
    return base_html("Review Reply", content)


@app.route("/send/<approval_id>", methods=["POST"])
def send(approval_id):
    item = pending_approvals.get(approval_id)
    if not item:
        return base_html("Not Found", '<div class="card"><p>Approval not found.</p></div>'), 404

    reply_text = request.form.get("reply_text", "").strip()
    create_calendar = request.form.get("create_calendar") == "on"

    invite_sender = request.form.get("invite_sender") == "on"

    try:
        send_reply(item["email"], reply_text)

        calendar_msg = ""
        if create_calendar:
            calendar_link, meet_link, date_uncertain = create_calendar_event(item["meeting_details"], item["email"], invite_sender=invite_sender)
            invite_str = " (invite sent to sender)" if invite_sender else ""
            meet_str = f' | <a href="{meet_link}" target="_blank">Google Meet link</a>' if meet_link else ""
            date_warn_str = ' <strong style="color:#e65c00">⚠️ Date uncertain — please update the event</strong>' if date_uncertain else ""
            calendar_msg = f' and a <a href="{calendar_link}" target="_blank">calendar event</a> was created{invite_str}{meet_str}{date_warn_str}'
            print(f"Calendar event created: {calendar_link}")

        pending_approvals[approval_id]["status"] = "sent"
        save_pending_approvals()

        content = f"""<div class="card" style="text-align:center;padding:48px">
            <div style="font-size:48px;margin-bottom:16px">✅</div>
            <h1 style="margin-bottom:8px">Reply Sent!</h1>
            <p style="color:#888;margin-bottom:24px">Your reply has been sent{calendar_msg}.</p>
            <a href="/" class="back-link">← Back to dashboard</a>
        </div>"""
        return base_html("Sent", content)

    except Exception as e:
        content = f"""<div class="card" style="text-align:center;padding:48px">
            <div style="font-size:48px;margin-bottom:16px">❌</div>
            <h1 style="margin-bottom:8px">Error</h1>
            <p style="color:#888;margin-bottom:24px">Something went wrong: {str(e)}</p>
            <a href="/" class="back-link">← Back to dashboard</a>
        </div>"""
        return base_html("Error", content)


@app.route("/discard/<approval_id>")
def discard(approval_id):
    if approval_id in pending_approvals:
        pending_approvals[approval_id]["status"] = "discarded"
        save_pending_approvals()

    content = """<div class="card" style="text-align:center;padding:48px">
        <div style="font-size:48px;margin-bottom:16px">🗑️</div>
        <h1 style="margin-bottom:8px">Discarded</h1>
        <p style="color:#888;margin-bottom:24px">No email was sent.</p>
        <a href="/" class="back-link">← Back to dashboard</a>
    </div>"""
    return base_html("Discarded", content)


def poll_telegram():
    """
    Long-poll Telegram for updates (button presses + reply-edits).
    Runs in a background thread — no webhook or ngrok needed.
    """
    import requests as _requests
    from assistant import send_telegram, _get_telegram_config

    token, _ = _get_telegram_config()
    if not token:
        print("Telegram not configured, skipping polling.")
        return

    offset = None
    print("Telegram polling started.")

    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
            if offset:
                params["offset"] = offset

            resp = _requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params=params,
                timeout=35,
            )
            if not resp.ok:
                time.sleep(5)
                continue

            updates = resp.json().get("result", [])

            for update in updates:
                offset = update["update_id"] + 1
                _handle_telegram_update(update, token)

        except Exception as e:
            print(f"Telegram polling error: {e}")
            time.sleep(5)


def _send_telegram_plain(token, text, chat_id=None, keyboard=None):
    """Send a plain-text Telegram message (no Markdown parsing). Returns message_id or None."""
    import requests as _requests
    from assistant import _get_telegram_config
    if chat_id is None:
        _, chat_id = _get_telegram_config()
    if not chat_id:
        return None
    payload = {"chat_id": chat_id, "text": text}
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    resp = _requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload, timeout=10,
    )
    if resp.ok:
        return resp.json()["result"]["message_id"]
    print(f"  → _send_telegram_plain error: {resp.text}")
    return None


def _send_general_telegram_plain(token, text, chat_id=None, keyboard=None):
    """Send a plain-text message (no Markdown parsing). Returns message_id or None."""
    import requests as _requests
    if chat_id is None:
        _, chat_id = _get_telegram_config()
    if not chat_id:
        return None
    payload = {"chat_id": chat_id, "text": text}
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    resp = _requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload, timeout=10,
    )
    if resp.ok:
        return resp.json()["result"]["message_id"]
    print(f"  → _send_general_telegram_plain error: {resp.text}")
    return None


def _handle_telegram_update(update, token):
    """Process a single Telegram update."""
    import requests as _requests
    import json as _json
    from assistant import send_telegram

    def ack(callback_id):
        _requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id}, timeout=5
        )

    # ── Button press ──────────────────────────────────────────────────────
    callback = update.get("callback_query")
    if callback:
        callback_data = callback.get("data", "")
        ack(callback["id"])

        if callback_data.startswith("send:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] == "pending":
                try:
                    send_reply(item["email"], item["reply_text"])
                    pending_approvals[approval_id]["status"] = "sent"
                    save_pending_approvals()
                    sender = item["email"]["sender"].split("<")[0].strip()
                    # Edit the draft message (the one with the buttons) to show sent
                    draft_msg_id = item.get("draft_telegram_message_id")
                    if draft_msg_id:
                        edit_telegram_message(draft_msg_id, f"✅ Sent to {sender}", keyboard=[])
                    cal_keyboard = [[{"text": "📅 Calendar Invite", "callback_data": f"cal:{approval_id}"}]]
                    send_telegram(f"✅ Reply sent to *{sender}*!", keyboard=cal_keyboard)
                except Exception as e:
                    send_telegram(f"❌ Error: {str(e)}")
            else:
                send_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("edit:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] == "pending":
                draft = item["reply_text"]
                sent_id = send_telegram(f"`{draft}`")
                if sent_id:
                    pending_approvals[f"tg:{sent_id}"] = approval_id
                    save_pending_approvals()
            else:
                send_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("discard:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] in ("awaiting_instructions", "pending"):
                pending_approvals[approval_id]["status"] = "discarded"
                save_pending_approvals()
                sender = item["email"]["sender"].split("<")[0].strip()
                # Edit whichever message has the Discard button
                button_msg_id = callback["message"]["message_id"]
                edit_telegram_message(button_msg_id, f"🗑️ Discarded ({sender})", keyboard=[])
            else:
                send_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("cal:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item is None:
                send_telegram(f"⚠️ Item not found (id: {approval_id})")
            else:
                original_details = item.get("meeting_details") or {}
                confirmed = extract_meeting_details_from_reply(
                    item.get("reply_text", ""), original_details
                )
                preview = build_calendar_json_preview(confirmed, item["email"])
                msg_text = (
                    "📅 Calendar invite — reply 'ok' to confirm or paste edited JSON:\n\n"
                    f"{preview}\n\n"
                    'Set "invite" to "me_only" or "all"'
                )
                sent_id = _send_telegram_plain(token, msg_text)
                if sent_id:
                    pending_approvals[approval_id]["calendar_json_preview"] = preview
                    pending_approvals[f"tgcal:{sent_id}"] = approval_id
                    save_pending_approvals()
        return

    # ── Incoming text message ─────────────────────────────────────────────
    message = update.get("message", {})
    text = message.get("text", "").strip()
    reply_to = message.get("reply_to_message", {})
    replied_msg_id = reply_to.get("message_id")

    if not (text and replied_msg_id):
        return

    # ── Reply to original notification → draft from instructions ──────────
    approval_id = pending_approvals.get(f"tgorig:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] in ("awaiting_instructions", "pending"):
            try:
                from assistant import draft_from_instructions, build_meeting_tg_message
                new_draft = draft_from_instructions(item, text)
                pending_approvals[approval_id]["reply_text"] = new_draft
                pending_approvals[approval_id]["status"] = "pending"
                save_pending_approvals()
                message, keyboard = build_meeting_tg_message(approval_id)
                draft_msg_id = send_telegram(message, keyboard)
                if draft_msg_id:
                    pending_approvals[approval_id]["draft_telegram_message_id"] = draft_msg_id
                    pending_approvals[f"tgdraft:{draft_msg_id}"] = approval_id
                    save_pending_approvals()
            except Exception as e:
                send_telegram(f"❌ Draft error: {str(e)}")
        else:
            send_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to draft message → re-draft with new instructions ───────────
    approval_id = pending_approvals.get(f"tgdraft:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] == "pending":
            try:
                from assistant import redraft_with_notes, build_meeting_tg_message
                new_draft = redraft_with_notes(item, text)
                pending_approvals[approval_id]["reply_text"] = new_draft
                save_pending_approvals()
                message, keyboard = build_meeting_tg_message(approval_id)
                edit_telegram_message(replied_msg_id, message, keyboard)
            except Exception as e:
                send_telegram(f"❌ Re-draft error: {str(e)}")
        else:
            send_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to Edit & Send message → send edited text as email ──────────
    approval_id = pending_approvals.get(f"tg:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] == "pending":
            try:
                send_reply(item["email"], text)
                pending_approvals[approval_id]["status"] = "sent"
                save_pending_approvals()
                sender = item["email"]["sender"].split("<")[0].strip()
                draft_msg_id = item.get("draft_telegram_message_id")
                if draft_msg_id:
                    edit_telegram_message(draft_msg_id, f"✅ Sent to {sender}", keyboard=[])
                cal_keyboard = [[{"text": "📅 Calendar Invite", "callback_data": f"cal:{approval_id}"}]]
                send_telegram(f"✅ Reply sent to *{sender}*!", keyboard=cal_keyboard)
            except Exception as e:
                send_telegram(f"❌ Error: {str(e)}")
        else:
            send_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to calendar JSON preview → create calendar event ────────────
    approval_id = pending_approvals.get(f"tgcal:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item:
            if text.strip().lower() == "ok":
                stored = item.get("calendar_json_preview")
                if not stored:
                    send_telegram("❌ No calendar preview found — please paste the JSON directly.")
                    return
                try:
                    cal_data = _json.loads(stored)
                except _json.JSONDecodeError:
                    send_telegram("❌ Stored preview is invalid — please paste the JSON directly.")
                    return
            else:
                import re as _re
                json_match = _re.search(r'\{.*\}', text, _re.DOTALL)
                if not json_match:
                    send_telegram("❌ Could not find JSON in your reply — please paste just the JSON block.")
                    return
                try:
                    cal_data = _json.loads(json_match.group())
                except _json.JSONDecodeError:
                    send_telegram("❌ Invalid JSON — please check the format and try again.")
                    return
            try:
                confirmed_details = {
                    "proposed_date": resolve_proposed_date(cal_data.get("date", "")),
                    "proposed_time": cal_data.get("time"),
                    "duration_minutes": int(cal_data.get("duration_minutes", 60)),
                    "location": cal_data.get("location", ""),
                    "meeting_type": item.get("meeting_details", {}).get("meeting_type", "in-person"),
                    "topic": cal_data.get("title", ""),
                    "attendees": item.get("meeting_details", {}).get("attendees", []),
                }
                invite_sender = cal_data.get("invite") in ("everyone", "all")
                cal_link, meet_link, date_uncertain = create_calendar_event(
                    confirmed_details, item["email"], invite_sender=invite_sender
                )
                meet_str = f"\n🎥 Meet link: {meet_link}" if meet_link else ""
                invite_str = " + invite sent to sender" if invite_sender else ""
                date_warn = "\n⚠️ *Date uncertain — please update the event!*" if date_uncertain else ""
                send_telegram(f"📅 Calendar event created{invite_str}!{meet_str}{date_warn}\n[Open event]({cal_link})")
            except Exception as e:
                send_telegram(f"❌ Calendar error: {str(e)}")

    # ── Also handle gen_* callbacks and gentg/gentgorig/gentgdraft messages ─
    _handle_general_telegram_update(update, token)


@app.route("/status")
def status():
    return jsonify({
        "pending": sum(1 for v in pending_approvals.values() if v["status"] == "pending"),
        "total": len(pending_approvals),
    })


def _handle_general_telegram_update(update, token):
    """Handle button presses and reply-edits for the general email bot."""
    import requests as _requests
    import json as _json

    def ack(callback_id):
        _requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id}, timeout=5
        )

    # ── Button press ──────────────────────────────────────────────────────
    callback = update.get("callback_query")
    if callback:
        callback_data = callback.get("data", "")
        ack(callback["id"])

        if callback_data.startswith("gen_send:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] == "pending":
                try:
                    send_reply(item["email"], item["reply_text"])
                    pending_approvals[approval_id]["status"] = "sent"
                    save_pending_approvals()
                    sender = item["email"]["sender"].split("<")[0].strip()
                    draft_msg_id = item.get("draft_telegram_message_id")
                    if draft_msg_id:
                        edit_general_telegram_message(draft_msg_id, f"✅ Sent to {sender}", keyboard=[])
                    cal_keyboard = [[{"text": "📅 Calendar Invite", "callback_data": f"gen_cal:{approval_id}"}]]
                    send_general_telegram(f"✅ Reply sent to *{sender}*!", keyboard=cal_keyboard)
                except Exception as e:
                    send_general_telegram(f"❌ Error: {str(e)}")
            else:
                send_general_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("gen_edit:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] == "pending":
                draft = item["reply_text"]
                sent_id = send_general_telegram(f"`{draft}`")
                if sent_id:
                    pending_approvals[f"gentg:{sent_id}"] = approval_id
                    save_pending_approvals()
            else:
                send_general_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("gen_discard:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item and item["status"] in ("awaiting_instructions", "pending"):
                pending_approvals[approval_id]["status"] = "discarded"
                save_pending_approvals()
                sender = item["email"]["sender"].split("<")[0].strip()
                button_msg_id = callback["message"]["message_id"]
                edit_general_telegram_message(button_msg_id, f"🗑️ Discarded ({sender})", keyboard=[])
            else:
                send_general_telegram("⚠️ Already sent or discarded.")

        elif callback_data.startswith("gen_cal:"):
            approval_id = callback_data.split(":", 1)[1]
            item = pending_approvals.get(approval_id)
            if item is None:
                send_general_telegram(f"⚠️ Item not found (id: {approval_id})")
            else:
                original_details = item.get("meeting_details") or {}
                confirmed = extract_meeting_details_from_reply(
                    item.get("reply_text", ""), original_details
                )
                preview = build_calendar_json_preview(confirmed, item["email"])
                msg_text = (
                    "📅 Calendar invite — reply 'ok' to confirm or paste edited JSON:\n\n"
                    f"{preview}\n\n"
                    'Set "invite" to "me_only" or "all"'
                )
                sent_id = _send_general_telegram_plain(token, msg_text)
                if sent_id:
                    pending_approvals[approval_id]["calendar_json_preview"] = preview
                    pending_approvals[f"gentgcal:{sent_id}"] = approval_id
                    save_pending_approvals()
        return

    # ── Incoming text message ─────────────────────────────────────────────
    message = update.get("message", {})
    text = message.get("text", "").strip()
    reply_to = message.get("reply_to_message", {})
    replied_msg_id = reply_to.get("message_id")

    if not (text and replied_msg_id):
        return

    # ── Reply to original notification → draft from instructions ──────────
    approval_id = pending_approvals.get(f"gentgorig:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] in ("awaiting_instructions", "pending"):
            try:
                from assistant import draft_from_instructions, build_general_tg_message
                new_draft = draft_from_instructions(item, text)
                pending_approvals[approval_id]["reply_text"] = new_draft
                pending_approvals[approval_id]["status"] = "pending"
                save_pending_approvals()
                message, keyboard = build_general_tg_message(approval_id)
                draft_msg_id = send_general_telegram(message, keyboard)
                if draft_msg_id:
                    pending_approvals[approval_id]["draft_telegram_message_id"] = draft_msg_id
                    pending_approvals[f"gentgdraft:{draft_msg_id}"] = approval_id
                    save_pending_approvals()
            except Exception as e:
                send_general_telegram(f"❌ Draft error: {str(e)}")
        else:
            send_general_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to draft message → re-draft with new instructions ───────────
    approval_id = pending_approvals.get(f"gentgdraft:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] == "pending":
            try:
                from assistant import redraft_with_notes, build_general_tg_message
                new_draft = redraft_with_notes(item, text)
                pending_approvals[approval_id]["reply_text"] = new_draft
                save_pending_approvals()
                message, keyboard = build_general_tg_message(approval_id)
                edit_general_telegram_message(replied_msg_id, message, keyboard)
            except Exception as e:
                send_general_telegram(f"❌ Re-draft error: {str(e)}")
        else:
            send_general_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to Edit & Send message → send edited text as email ──────────
    approval_id = pending_approvals.get(f"gentg:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item and item["status"] == "pending":
            try:
                send_reply(item["email"], text)
                pending_approvals[approval_id]["status"] = "sent"
                save_pending_approvals()
                sender = item["email"]["sender"].split("<")[0].strip()
                draft_msg_id = item.get("draft_telegram_message_id")
                if draft_msg_id:
                    edit_general_telegram_message(draft_msg_id, f"✅ Sent to {sender}", keyboard=[])
                cal_keyboard = [[{"text": "📅 Calendar Invite", "callback_data": f"gen_cal:{approval_id}"}]]
                send_general_telegram(f"✅ Reply sent to *{sender}*!", keyboard=cal_keyboard)
            except Exception as e:
                send_general_telegram(f"❌ Error: {str(e)}")
        else:
            send_general_telegram("⚠️ This email was already handled.")
        return

    # ── Reply to calendar JSON preview → create calendar event ────────────
    approval_id = pending_approvals.get(f"gentgcal:{replied_msg_id}")
    if approval_id:
        item = pending_approvals.get(approval_id)
        if item:
            if text.strip().lower() == "ok":
                stored = item.get("calendar_json_preview")
                if not stored:
                    send_general_telegram("❌ No calendar preview found — please paste the JSON directly.")
                    return
                try:
                    cal_data = _json.loads(stored)
                except _json.JSONDecodeError:
                    send_general_telegram("❌ Stored preview is invalid — please paste the JSON directly.")
                    return
            else:
                import re as _re
                json_match = _re.search(r'\{.*\}', text, _re.DOTALL)
                if not json_match:
                    send_general_telegram("❌ Could not find JSON in your reply — please paste just the JSON block.")
                    return
                try:
                    cal_data = _json.loads(json_match.group())
                except _json.JSONDecodeError:
                    send_general_telegram("❌ Invalid JSON — please check the format and try again.")
                    return
            try:
                confirmed_details = {
                    "proposed_date": resolve_proposed_date(cal_data.get("date", "")),
                    "proposed_time": cal_data.get("time"),
                    "duration_minutes": int(cal_data.get("duration_minutes", 60)),
                    "location": cal_data.get("location", ""),
                    "meeting_type": item.get("meeting_details", {}).get("meeting_type", "in-person"),
                    "topic": cal_data.get("title", ""),
                    "attendees": item.get("meeting_details", {}).get("attendees", []),
                }
                invite_sender = cal_data.get("invite") in ("everyone", "all")
                cal_link, meet_link, date_uncertain = create_calendar_event(
                    confirmed_details, item["email"], invite_sender=invite_sender
                )
                meet_str = f"\n🎥 Meet link: {meet_link}" if meet_link else ""
                invite_str = " + invite sent to sender" if invite_sender else ""
                date_warn = "\n⚠️ *Date uncertain — please update the event!*" if date_uncertain else ""
                send_general_telegram(f"📅 Calendar event created{invite_str}!{meet_str}{date_warn}\n[Open event]({cal_link})")
            except Exception as e:
                send_general_telegram(f"❌ Calendar error: {str(e)}")


def start_polling():
    threading.Thread(target=poll_gmail, daemon=True).start()
    threading.Thread(target=poll_telegram, daemon=True).start()


if __name__ == "__main__":
    start_polling()
    print("Starting approval UI at http://localhost:5002")
    app.run(host="0.0.0.0", port=5002, debug=False)
