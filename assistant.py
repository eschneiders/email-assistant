"""
Email Assistant - Main script
Watches Gmail for meeting requests, drafts replies using Claude,
checks travel times, and asks for approval before sending.
"""

import os
import time
import base64
import json
import re
import sqlite3
import threading
import webbrowser
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import anthropic
import requests
from googleapiclient.discovery import build
from dotenv import load_dotenv

from auth import get_credentials

# Always load .env from the directory this file lives in
_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_DIR, ".env"), override=True)

MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
BASE_LOCATION = os.getenv("BASE_LOCATION", "Amsterdam Zuid station, Amsterdam, Netherlands")
WORK_EMAIL = os.getenv("WORK_EMAIL")
CALENDAR_ID = os.getenv("CALENDAR_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 60))
DB_FILE = os.path.join(_DIR, "email_assistant.db")
_LEGACY_JSON = os.path.join(_DIR, "pending_approvals.json")

def get_anthropic_client():
    """Create Anthropic client fresh, always reading from .env."""
    key = open(os.path.join(_DIR, ".env")).read()
    for line in key.splitlines():
        if line.startswith("EMAIL_ASSISTANT_ANTHROPIC_KEY="):
            api_key = line.split("=", 1)[1].strip()
            return anthropic.Anthropic(api_key=api_key)
    raise ValueError("EMAIL_ASSISTANT_ANTHROPIC_KEY not found in .env")

# ── Persistence (SQLite) ──────────────────────────────────────────────────────
_db_lock = threading.Lock()

def _db_init():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS approvals (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filtered_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sender      TEXT NOT NULL,
                subject     TEXT NOT NULL,
                stage       TEXT NOT NULL,
                filtered_at REAL NOT NULL
            )
        """)
        conn.commit()


def _load_pending_approvals():
    _db_init()
    # One-time migration from legacy JSON
    if os.path.exists(_LEGACY_JSON):
        try:
            with open(_LEGACY_JSON) as f:
                legacy = json.load(f)
            with _db_lock:
                with sqlite3.connect(DB_FILE) as conn:
                    for k, v in legacy.items():
                        conn.execute(
                            "INSERT OR IGNORE INTO approvals (key, value) VALUES (?, ?)",
                            (k, json.dumps(v, default=str)),
                        )
                    conn.commit()
            os.rename(_LEGACY_JSON, _LEGACY_JSON + ".migrated")
            print(f"  → Migrated {len(legacy)} items from JSON to SQLite.")
            return legacy
        except Exception as e:
            print(f"  → JSON migration error (continuing): {e}")
    try:
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute("SELECT key, value FROM approvals").fetchall()
        result = {}
        for k, v in rows:
            try:
                result[k] = json.loads(v)
            except Exception:
                result[k] = v
        return result
    except Exception as e:
        print(f"  → Could not load from SQLite: {e}")
        return {}


def save_pending_approvals():
    """Persist the in-memory pending_approvals dict to SQLite."""
    try:
        now = time.time()
        with _db_lock:
            with sqlite3.connect(DB_FILE) as conn:
                for key, value in pending_approvals.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO approvals (key, value) VALUES (?, ?)",
                        (key, json.dumps(value, default=str)),
                    )
                conn.commit()
    except Exception as e:
        print(f"  → Could not save to SQLite: {e}")


def log_filtered_email(email, stage):
    """Log a skipped email to filtered_log. stage: 'automated_sender' or 'llm_filter'."""
    try:
        with _db_lock:
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute(
                    "INSERT INTO filtered_log (sender, subject, stage, filtered_at) VALUES (?, ?, ?, ?)",
                    (email.get("sender", ""), email.get("subject", ""), stage, time.time()),
                )
                conn.commit()
    except Exception as e:
        print(f"  → log_filtered_email error: {e}")


def send_daily_digest():
    """Send a Telegram summary of emails filtered in the last 24 hours."""
    try:
        since = time.time() - 86400
        with sqlite3.connect(DB_FILE) as conn:
            rows = conn.execute(
                "SELECT sender, subject, stage FROM filtered_log "
                "WHERE filtered_at > ? ORDER BY filtered_at DESC",
                (since,),
            ).fetchall()
        if not rows:
            send_telegram("📋 *Daily digest:* No emails were filtered in the last 24 hours.")
            return
        lines = [f"📋 *Daily digest* — {len(rows)} email(s) filtered in the last 24h:\n"]
        for sender, subject, stage in rows[:30]:
            tag = "🤖 auto" if stage == "automated_sender" else "🧠 LLM"
            s = _tg_escape(sender.split("<")[0].strip()[:30] or sender[:30])
            subj = _tg_escape(subject[:50])
            lines.append(f"{tag} — *{s}*: _{subj}_")
        send_telegram("\n".join(lines))
    except Exception as e:
        print(f"  → send_daily_digest error: {e}")


pending_approvals = _load_pending_approvals()

# Cached style examples loaded once at startup
_style_examples_cache = None


def _strip_quoted_reply(body):
    """Remove the quoted 'On ... wrote:' section and everything after from a reply body."""
    # Remove lines starting with > (quoted text)
    lines = body.splitlines()
    clean = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(">"):
            break
        # Stop at 'On ... wrote:' even if split across lines (match start of pattern)
        if re.match(r"^On .{5,100}wrote:?$", stripped):
            break
        # Also stop at the standalone "On [date]" line that precedes multi-line 'wrote:'
        if re.match(r"^On (Mon|Tue|Wed|Thu|Fri|Sat|Sun|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d)", stripped):
            break
        clean.append(line)
    return "\n".join(clean).strip()


def _llm_filter_and_clean(email):
    """
    Uses Claude Haiku to:
    1. Decide if this email needs Edouard's personal response (YES/NO)
    2. If YES, extract just the main message body — strip footers, disclaimers,
       hyperlink-only lines, legal text, and other noise.

    Returns (should_notify: bool, clean_body: str).
    On any error, defaults to (True, raw_body) so nothing is silently dropped.
    """
    raw_body = _strip_quoted_reply(email.get("body", "")).strip()
    body_snippet = raw_body[:2000]

    prompt = f"""You are filtering emails for Edouard Schneiders, CEO of Caraomics (biotech startup).

Decide if this email needs Edouard's personal response.

Reply NO for: newsletters, automated notifications, calendar invites, order confirmations, receipts, cold sales/marketing outreach, LinkedIn/social notifications, group announcements, automated welcome/onboarding emails triggered by a signup, or anything that does not expect a direct personal reply from Edouard.
Reply YES for: emails from real people asking a question, requesting a meeting, following up on a conversation, personally inviting Edouard or CaraOmics to a program, event, or partnership, or clearly expecting a personal response.

If YES, also extract only the main human-written message — remove email footers, legal disclaimers, confidentiality notices, unsubscribe text, lines that are only hyperlinks, excessive blank lines, and signature boilerplate.

From: {email['sender']}
Subject: {email['subject']}
Body:
{body_snippet}

Respond in EXACTLY this format (no extra text):
DECISION: YES
CLEAN_BODY: <main message only, max 600 chars>

or:
DECISION: NO"""

    try:
        response = get_anthropic_client().messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        if text.startswith("DECISION: NO"):
            return False, ""
        # Extract clean body
        if "CLEAN_BODY:" in text:
            clean_body = text.split("CLEAN_BODY:", 1)[1].strip()
        else:
            clean_body = raw_body
        return True, clean_body
    except Exception as e:
        print(f"  → LLM filter error (defaulting to notify): {e}")
        return True, raw_body


def get_style_examples(force_refresh=False):
    """
    Fetch up to 10 best style examples from Edouard's sent emails.
    Results are cached in memory so the Gmail API is only called once per run.
    Returns a formatted string ready to inject into Claude prompts.
    """
    global _style_examples_cache
    if _style_examples_cache and not force_refresh:
        return _style_examples_cache

    print("  → Loading writing style examples from sent emails...")
    try:
        service = get_gmail_service()
        results = service.users().messages().list(
            userId="me", labelIds=["SENT"], maxResults=80
        ).execute()
        messages = results.get("messages", [])

        # IDs of the manually selected best examples (from index output above):
        # [02] Kak Khee confirm, [03] Hilco reschedule, [04] Kak Khee reschedule,
        # [05] Henk morning, [06] Christine Dutch Rabo, [07] Kak Khee alternatives,
        # [08] Harm Dutch delay, [09] Marjolein Dutch confirm, [12] Henk Tuesday,
        # [13] Rob March, [17] Rob Monday, [19] Pieter Dutch slots,
        # [22] Arne follow-up, [26] Sander PLAY confirm
        SKIP_SUBJECTS = {
            "edouard@caraomics.ai is now connected to hubspot!",
            "re: f", "re: v", "screenshot", "re: final pitch after feedback",
            "re: caraomics deck",
        }
        SKIP_BODY_PREFIXES = [
            "✏️",           # our own edit-prompt messages
            "---------- Forwarded",
        ]

        collected = []
        for m in messages:
            if len(collected) >= 10:
                break
            try:
                full = service.users().messages().get(
                    userId="me", id=m["id"], format="full"
                ).execute()
                headers = {h["name"]: h["value"] for h in full["payload"]["headers"]}
                subject = headers.get("Subject", "")
                to = headers.get("To", "")

                # Skip noise
                if subject.lower() in SKIP_SUBJECTS:
                    continue
                # Skip internal / forwarded
                if "colm@caraomics.ai" in to and "@" not in to.replace("colm@caraomics.ai", ""):
                    continue

                body = extract_body(full["payload"]).strip()
                if len(body) < 40:
                    continue
                if any(body.startswith(p) for p in SKIP_BODY_PREFIXES):
                    continue

                clean_body = _strip_quoted_reply(body)
                if len(clean_body) < 40:
                    continue

                collected.append({
                    "subject": subject,
                    "body": clean_body[:600],
                })
            except Exception:
                continue

        if not collected:
            _style_examples_cache = ""
            return ""

        lines = [
            "\n--- EDOUARD'S WRITING STYLE EXAMPLES (from his actual sent emails) ---",
            "Use these as a reference for tone, length, greeting style, sign-off, and language switching.",
            "",
        ]
        for i, ex in enumerate(collected, 1):
            lines.append(f"Example {i} (Re: {ex['subject']}):")
            lines.append(ex["body"])
            lines.append("")
        lines.append("--- END OF STYLE EXAMPLES ---\n")

        _style_examples_cache = "\n".join(lines)
        print(f"  → Loaded {len(collected)} style examples.")
        return _style_examples_cache

    except Exception as e:
        print(f"  → Could not load style examples: {e}")
        _style_examples_cache = ""
        return ""


def get_gmail_service():
    creds = get_credentials()
    return build("gmail", "v1", credentials=creds)


def get_calendar_service():
    creds = get_credentials()
    return build("calendar", "v3", credentials=creds)


def get_unread_meeting_emails(service, last_check_time):
    """
    Fetch unread emails received after last_check_time.
    Skips emails whose thread already has a recent reply from Edouard.
    Attaches thread context if someone else replied after the triggering email.
    """
    after_timestamp = int(last_check_time.timestamp())
    query = f"is:unread after:{after_timestamp} -from:{WORK_EMAIL}"

    results = service.users().messages().list(userId="me", q=query, maxResults=20).execute()
    messages = results.get("messages", [])

    emails = []
    seen_thread_ids = set()

    for msg in messages:
        full_msg = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
        email_data = parse_email(full_msg)
        if not email_data:
            continue

        thread_id = email_data["thread_id"]

        # Don't process the same thread twice in one poll cycle
        if thread_id in seen_thread_ids:
            continue
        seen_thread_ids.add(thread_id)

        # Fetch full thread to check for recent activity
        try:
            thread = service.users().threads().get(
                userId="me", id=thread_id, format="metadata",
                metadataHeaders=["From", "Date"]
            ).execute()
            thread_msgs = thread.get("messages", [])
        except Exception:
            emails.append(email_data)
            continue

        # Build a timeline of thread messages: (timestamp, from, message_id)
        timeline = []
        for tm in thread_msgs:
            headers = {h["name"]: h["value"] for h in tm.get("payload", {}).get("headers", [])}
            from_addr = headers.get("From", "")
            date_str = headers.get("Date", "")
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(date_str)
                ts = dt.timestamp()
            except Exception:
                ts = 0
            timeline.append({"ts": ts, "from": from_addr, "id": tm["id"]})

        timeline.sort(key=lambda x: x["ts"])

        # Find the position of the triggering email in the thread
        trigger_ts = next((t["ts"] for t in timeline if t["id"] == email_data["id"]), 0)

        # ── Skip if Edouard already replied after the triggering email ──
        already_replied = any(
            t["ts"] > trigger_ts and WORK_EMAIL.lower() in t["from"].lower()
            for t in timeline
        )
        if already_replied:
            print(f"  → Skipping '{email_data['subject']}': already replied in thread.")
            continue

        # ── Attach thread context if someone else replied recently after trigger ──
        # "recently" = within 4 hours (covers cases where a 3rd party replied before you)
        RECENT_WINDOW = 4 * 3600
        now_ts = datetime.now(timezone.utc).timestamp()
        later_msgs = [
            t for t in timeline
            if t["ts"] > trigger_ts and WORK_EMAIL.lower() not in t["from"].lower()
        ]
        if later_msgs:
            # Fetch bodies of later messages for thread context
            thread_context_msgs = []
            for tm in thread_msgs:
                headers = {h["name"]: h["value"] for h in tm.get("payload", {}).get("headers", [])}
                date_str = headers.get("Date", "")
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(date_str)
                    ts = dt.timestamp()
                except Exception:
                    ts = 0
                # Get full body for context messages
                try:
                    full_tm = service.users().messages().get(
                        userId="me", id=tm["id"], format="full"
                    ).execute()
                    body = extract_body(full_tm["payload"])
                except Exception:
                    body = ""
                thread_context_msgs.append({
                    "from": headers.get("From", ""),
                    "date": date_str,
                    "body": body[:1000],
                    "ts": ts,
                })
            email_data["thread_context"] = thread_context_msgs
            print(f"  → Thread has {len(later_msgs)} later message(s) — context attached.")

        emails.append(email_data)

    return emails


def parse_email(full_msg):
    """Extract relevant fields from a Gmail message."""
    headers = {h["name"]: h["value"] for h in full_msg["payload"]["headers"]}
    subject = headers.get("Subject", "")
    sender = headers.get("From", "")
    date = headers.get("Date", "")
    message_id = headers.get("Message-ID", "")
    to_header = headers.get("To", "")
    cc_header = headers.get("Cc", "")

    body = extract_body(full_msg["payload"])

    # Determine if Edouard is directly addressed (To:) vs only CC'd
    work_email_lower = (WORK_EMAIL or "").lower()
    in_to = work_email_lower in to_header.lower()
    in_cc = work_email_lower in cc_header.lower()
    is_direct = in_to  # False if only in CC

    return {
        "id": full_msg["id"],
        "thread_id": full_msg["threadId"],
        "subject": subject,
        "sender": sender,
        "date": date,
        "message_id": message_id,
        "body": body,
        "to": to_header,
        "cc": cc_header,
        "is_direct": is_direct,
        "cc_only": in_cc and not in_to,
    }


def extract_body(payload):
    """Recursively extract plain text body from email payload."""
    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")

    if "parts" in payload:
        for part in payload["parts"]:
            result = extract_body(part)
            if result:
                return result

    return ""


_AUTOMATED_SENDER_PATTERNS = re.compile(
    r"(noreply|no-reply|no\.reply|donotreply|do-not-reply|"
    r"mailer-daemon|postmaster|bounce|notifications?@|"
    r"alerts?@|newsletter|@mailchimp|@sendgrid|@hubspot|"
    r"@klaviyo|@substack|@beehiiv|@constantcontact|@campaignmonitor|"
    r"@mail\.(masterclass|medium|substack)|"
    r"@linkedin\.com|@twitter\.com|@facebook\.com|"
    r"automatisch|automated|support-noreply)",
    re.IGNORECASE,
)

TG_MAX_LEN = 4096  # Telegram hard limit per message

_TG_ESCAPE_RE = re.compile(r"([_*`\[])")

def _tg_escape(text: str) -> str:
    """Escape Telegram Markdown v1 special chars in untrusted content."""
    return _TG_ESCAPE_RE.sub(r"\\\1", text)


def _fit_to_telegram(header: str, email_body: str, reply: str) -> str:
    """
    Build the final Telegram message string, trimming email_body first if needed
    to stay within Telegram's 4096-char limit.
    """
    # Try full lengths first
    body_part = f"*Original message:*\n_{email_body}_\n\n"
    reply_part = f"*Draft reply:*\n_{reply}_"
    msg = header + body_part + reply_part
    if len(msg) <= TG_MAX_LEN:
        return msg

    # Trim email body to fit, keeping reply intact
    overhead = len(header) + len(reply_part) + len("*Original message:*\n__\n\n") + len("...\n\n")
    available = TG_MAX_LEN - overhead
    if available > 100:
        trimmed_body = email_body[:available]
        body_part = f"*Original message:*\n_{trimmed_body}..._\n\n"
    else:
        # Extreme edge case: no room for body at all
        body_part = ""

    return header + body_part + reply_part


def is_automated_sender(email):
    """Return True if the email looks like it came from an automated system or is a newsletter."""
    sender = email.get("sender", "")
    if _AUTOMATED_SENDER_PATTERNS.search(sender):
        return True
    # Newsletters and marketing emails always contain an unsubscribe link
    body = email.get("body", "").lower()
    if "unsubscribe" in body:
        return True
    return False


_NL_STOPWORDS = {"de", "het", "een", "van", "en", "dat", "is", "op", "te", "in", "je", "ik",
                 "we", "ze", "hij", "niet", "met", "voor", "aan", "zijn", "om", "maar", "ook",
                 "als", "dan", "nog", "al", "er", "dit", "die", "hoe", "wat", "goed", "dag"}
_EN_STOPWORDS = {"the", "a", "an", "of", "and", "that", "is", "on", "to", "in", "you", "i",
                 "we", "they", "he", "not", "with", "for", "at", "be", "are", "was", "but",
                 "also", "if", "then", "still", "already", "there", "this", "those", "how",
                 "what", "good", "hi", "hello", "dear", "please", "thanks", "thank"}

def _detect_language(text):
    """Return 'nl' if Dutch, 'en' if English, based on stopword frequency."""
    words = set(re.findall(r"\b[a-z]{2,}\b", text.lower()[:1000]))
    nl_score = len(words & _NL_STOPWORDS)
    en_score = len(words & _EN_STOPWORDS)
    if nl_score > en_score:
        return "nl"
    if en_score > nl_score:
        return "en"
    return "en"  # default to English


_MEETING_KEYWORDS = re.compile(
    r"\b(meet(ing)?|call|afspraak|vergadering|overleg|agenda|schedule|appointment|"
    r"introduct|intro|catch[- ]?up|sync|bellen|spreken|bespreking|bijeenkomst|"
    r"demo|discovery|onboarding|kick[- ]?off|follow[- ]?up)\b",
    re.IGNORECASE,
)

def is_meeting_related(email):
    """Determine if the email is about scheduling a meeting.
    Fast keyword pre-filter first; Claude only if ambiguous."""
    text = f"{email.get('subject', '')} {email.get('body', '')[:500]}"

    # Fast path: strong keyword match → definitely meeting-related
    if _MEETING_KEYWORDS.search(text):
        return True

    # Slow path: ask Claude for ambiguous cases
    try:
        prompt = f"""Analyze this email and respond with only "yes" or "no".
Is this email requesting, proposing, or discussing scheduling a meeting, call, or appointment?

Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:1000]}
"""
        response = get_anthropic_client().messages.create(
            model="claude-haiku-4-5",
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = response.content[0].text.strip().lower()
        return answer == "yes"
    except Exception as e:
        print(f"  → is_meeting_related Claude call failed ({e}), defaulting to False")
        return False


def extract_meeting_details(email):
    """Use Claude to extract structured meeting details from the email."""
    prompt = f"""Extract meeting details from this email. Respond with a JSON object only, no other text.

Fields to extract:
- proposed_date: (string, e.g. "2024-03-15" or "next Tuesday" or null if not specified)
- proposed_time: (string, e.g. "14:00" or "2pm" or null if not specified)
- duration_minutes: (integer, estimated duration, default 60 if not specified)
- location: (string, city or address in the Netherlands, or "video call" or null)
- meeting_type: (string, e.g. "in-person", "video call", "phone call")
- attendees: (list of email addresses found in the email)
- topic: (string, brief description of meeting purpose)

Email:
Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:2000]}
"""
    response = get_anthropic_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    # Strip markdown code blocks if present
    raw = re.sub(r"^```(?:json)?\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def extract_meeting_details_from_reply(reply_text, original_meeting_details=None):
    """
    Extract confirmed meeting date/time/location from Edouard's outgoing reply text.
    This is the ground truth — what was actually confirmed, not what was proposed.
    Falls back to original_meeting_details for fields not found in the reply.
    Returns a meeting_details dict compatible with create_calendar_event.
    """
    fallback = original_meeting_details or {}
    now_str = datetime.now().strftime("%A %B %d, %Y")

    prompt = f"""Extract the confirmed meeting details from this outgoing email reply.
Respond with a JSON object only, no other text.

TODAY is {now_str}. Use this to resolve relative dates like "monday", "next week", "tomorrow" into ISO dates.

Fields to extract:
- confirmed_date: (ISO date string e.g. "2026-02-25" — resolve relative dates using today's date above, or null if truly unclear)
- confirmed_time: (string e.g. "14:00", or null if not clearly stated)
- location: (string, city or address, or "video call", or null)
- meeting_type: (string: "in-person", "video call", or "phone call")
- duration_minutes: (integer, default 60)
- topic: (string, brief description)
- single_time_confirmed: (boolean: true ONLY if exactly ONE specific date+time is confirmed in this reply, false if multiple options are proposed or no time is stated)

Reply text:
{reply_text}
"""
    response = get_anthropic_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {}

    # Resolve relative/named dates to ISO format
    confirmed_date = parsed.get("confirmed_date")
    if confirmed_date:
        confirmed_date = resolve_proposed_date(confirmed_date)

    # Build a meeting_details dict, falling back to original where needed
    return {
        "proposed_date": confirmed_date or fallback.get("proposed_date"),
        "proposed_time": parsed.get("confirmed_time") or fallback.get("proposed_time"),
        "location": parsed.get("location") or fallback.get("location"),
        "meeting_type": parsed.get("meeting_type") or fallback.get("meeting_type", "in-person"),
        "duration_minutes": parsed.get("duration_minutes") or fallback.get("duration_minutes", 60),
        "topic": parsed.get("topic") or fallback.get("topic", ""),
        "attendees": fallback.get("attendees", []),
        "single_time_confirmed": bool(parsed.get("single_time_confirmed", False)),
    }


def build_calendar_json_preview(meeting_details, email):
    """
    Build a pre-filled JSON string for the Telegram calendar-invite editing flow.
    The user edits this JSON and replies to create a calendar event.
    'invite' field: "me_only" = only add to Edouard's calendar,
                    "everyone" = also send invite to sender
    """
    sender_name = email["sender"].split("<")[0].strip() or email["sender"]
    title = meeting_details.get("topic") or f"Meeting with {sender_name}"

    date_default = datetime.now().strftime("%Y-%m-DD")
    return json.dumps({
        "title": title,
        "date": meeting_details.get("proposed_date") or date_default,
        "time": meeting_details.get("proposed_time") or "HH:MM",
        "duration_minutes": meeting_details.get("duration_minutes", 60),
        "location": meeting_details.get("location") or "",
        "invite": "me_only",   # or "all" to send invite to sender too
    }, indent=2, ensure_ascii=False)


def resolve_proposed_date(proposed_date_str):
    """
    Convert a relative date string like 'next Wednesday' or 'next week Wednesday'
    into an ISO date string like '2026-02-25'.
    Returns the original string unchanged if it's already an ISO date or can't be resolved.
    """
    if not proposed_date_str:
        return proposed_date_str

    # Already ISO format — nothing to do
    if re.match(r"^\d{4}-\d{2}-\d{2}$", proposed_date_str.strip()):
        return proposed_date_str

    from datetime import timedelta
    from zoneinfo import ZoneInfo
    AMS = ZoneInfo("Europe/Amsterdam")
    today = datetime.now(AMS).date()

    text = proposed_date_str.lower().strip()

    WEEKDAYS = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }

    # Detect "next week X" or "next X"
    is_next_week = "next week" in text or text.startswith("next ")
    target_weekday = None
    for name, num in WEEKDAYS.items():
        if name in text:
            target_weekday = num
            break

    if target_weekday is not None:
        days_ahead = (target_weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # same day of week → next occurrence
        if is_next_week and days_ahead < 7:
            days_ahead += 7  # force into next calendar week
        resolved = today + timedelta(days=days_ahead)
        print(f"  → Resolved '{proposed_date_str}' → {resolved.isoformat()}")
        return resolved.isoformat()

    # "tomorrow"
    if "tomorrow" in text:
        return (today + timedelta(days=1)).isoformat()

    # "today"
    if "today" in text:
        return today.isoformat()

    return proposed_date_str


def _fetch_calendar_events(days=21):
    """
    Fetch calendar events for the next `days` days.
    Returns a list of (start_dt, end_dt) tuples in Amsterdam time.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    AMS = ZoneInfo("Europe/Amsterdam")

    service = get_calendar_service()
    now = datetime.now(AMS)
    end = now + timedelta(days=days)

    try:
        result = service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=now.isoformat(),
            timeMax=end.isoformat(),
            maxResults=100,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        events = []
        for ev in result.get("items", []):
            start_raw = ev["start"].get("dateTime")
            end_raw = ev["end"].get("dateTime")
            if not start_raw or not end_raw:
                continue  # skip all-day events
            try:
                start_dt = datetime.fromisoformat(start_raw).astimezone(AMS)
                end_dt = datetime.fromisoformat(end_raw).astimezone(AMS)
                events.append((start_dt, end_dt))
            except Exception:
                continue
        return events
    except Exception as e:
        print(f"Calendar fetch error: {e}")
        return []


def _slot_is_free(events, slot_start, duration_minutes):
    """
    Returns True if no calendar event overlaps with the proposed slot.
    slot_start: timezone-aware datetime
    duration_minutes: length of the meeting
    """
    from datetime import timedelta
    slot_end = slot_start + timedelta(minutes=duration_minutes)
    for ev_start, ev_end in events:
        # Overlap if slot starts before event ends AND slot ends after event starts
        if slot_start < ev_end and slot_end > ev_start:
            return False
    return True


def get_free_slots(duration_minutes=60, travel_minutes=0, is_long_distance=False, days_ahead=14):
    """
    Returns a plain list of free weekday time slots for the next `days_ahead` days.
    Python does all the work — no Claude reasoning about the calendar.

    Rules:
    - Weekdays only (Mon–Fri)
    - Skip today (too short notice)
    - For long-distance in-person (travel > 60 min): skip next 3 days too
    - Business hours: 09:00–17:00
    - For in-person with travel: only include slots where departure from Amsterdam Zuid
      is after 09:00 (NS off-peak)
    - Each slot is 1 hour apart to avoid clutter
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    AMS = ZoneInfo("Europe/Amsterdam")

    today = datetime.now(AMS).date()
    min_days = 3 if is_long_distance else 1
    events = _fetch_calendar_events(days=days_ahead + 7)

    # Generate candidate slots: every hour from 09:00–16:00 on each weekday
    candidate_hours = [9, 10, 11, 14, 15, 16]
    free_slots = []

    for i in range(min_days, days_ahead + 1):
        day = today + timedelta(days=i)
        if day.weekday() >= 5:  # skip weekends
            continue
        for hour in candidate_hours:
            slot_start = datetime(day.year, day.month, day.day, hour, 0, tzinfo=AMS)
            if not _slot_is_free(events, slot_start, duration_minutes):
                continue
            # NS off-peak check for in-person travel
            if travel_minutes > 0:
                departure = slot_start - timedelta(minutes=travel_minutes)
                if departure.hour < 9 or departure.hour >= 16:
                    continue
            free_slots.append(slot_start.strftime("%A %B %d at %H:%M"))

    print(f"  → Free slots found: {len(free_slots)} over next {days_ahead} days")
    return free_slots


def get_travel_time(destination):
    """
    Get travel time from BASE_LOCATION to destination using Google Maps.
    Also fetches walking time from the destination to its nearest train station.
    """
    if not destination or destination.lower() in ["video call", "phone call", "online", "teams", "zoom"]:
        return None

    destination_nl = f"{destination}, Netherlands"

    try:
        # 1. Transit time from home base to destination
        transit_resp = requests.get(
            "https://maps.googleapis.com/maps/api/distancematrix/json",
            params={
                "origins": BASE_LOCATION,
                "destinations": destination_nl,
                "mode": "transit",
                "language": "en",
                "key": MAPS_API_KEY,
            },
            timeout=10,
        ).json()

        transit_duration_text = None
        transit_duration_minutes = None
        distance_text = None

        if transit_resp["status"] == "OK":
            el = transit_resp["rows"][0]["elements"][0]
            if el["status"] == "OK":
                transit_duration_text = el["duration"]["text"]
                transit_duration_minutes = el["duration"]["value"] // 60
                distance_text = el["distance"]["text"]

        # 2. Find nearest train station to destination using Places API
        places_resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={
                "query": f"train station near {destination_nl}",
                "key": MAPS_API_KEY,
            },
            timeout=10,
        ).json()

        nearest_station = None
        walk_to_station_text = None
        walk_to_station_minutes = None

        if places_resp.get("results"):
            nearest_station = places_resp["results"][0]["name"]

            # 3. Walking time from destination to nearest station
            walk_resp = requests.get(
                "https://maps.googleapis.com/maps/api/distancematrix/json",
                params={
                    "origins": destination_nl,
                    "destinations": f"{nearest_station}, Netherlands",
                    "mode": "walking",
                    "language": "en",
                    "key": MAPS_API_KEY,
                },
                timeout=10,
            ).json()

            if walk_resp["status"] == "OK":
                wl = walk_resp["rows"][0]["elements"][0]
                if wl["status"] == "OK":
                    walk_to_station_text = wl["duration"]["text"]
                    walk_to_station_minutes = wl["duration"]["value"] // 60

        if not transit_duration_text:
            return None

        return {
            "duration_text": transit_duration_text,
            "duration_minutes": transit_duration_minutes,
            "distance_text": distance_text,
            "origin": BASE_LOCATION,
            "destination": destination,
            "nearest_station": nearest_station,
            "walk_to_station_text": walk_to_station_text,
            "walk_to_station_minutes": walk_to_station_minutes,
        }

    except Exception as e:
        print(f"Maps API error: {e}")

    return None


def draft_reply(email, meeting_details, travel_info, free_slots, thread_messages=None):
    """
    Use Claude to draft a meeting reply in Edouard's style.
    Python provides the verified free slots — Claude just writes the email.
    """
    style_examples = get_style_examples()
    lang = _detect_language(email.get("body", ""))
    lang_hint = "The email is in Dutch — reply in Dutch and prioritise Dutch style examples above.\n" if lang == "nl" else ""

    # Thread context: only the most recent reply sent by Edouard (if any)
    # This prevents repeating what was already agreed, without bloating the prompt
    thread_context = ""
    if thread_messages and len(thread_messages) > 1:
        # Find the last message sent by Edouard (excluding the triggering email)
        for m in reversed(thread_messages[:-1]):
            if WORK_EMAIL.lower() in m.get("from", "").lower():
                thread_context = f"\nYour last reply in this thread:\n{m['body'][:800]}\n"
                break

    # Travel context (informational only)
    travel_context = ""
    if travel_info:
        walk_line = ""
        if travel_info.get("walk_to_station_text") and travel_info.get("nearest_station"):
            walk_line = f"\n- Walk from venue to {travel_info['nearest_station']}: {travel_info['walk_to_station_text']}"
        travel_context = (
            f"\nTravel: {travel_info['duration_text']} by public transit from Amsterdam Zuid "
            f"to {travel_info['destination']} ({travel_info['distance_text']}){walk_line}"
            f"\nNS off-peak tip: departure after 09:00 and before 16:00 from Amsterdam Zuid is free."
        )

    # Format free slots as a simple list
    if free_slots:
        slots_str = "\n".join(f"- {s}" for s in free_slots[:20])  # cap at 20 to keep prompt short
        slots_block = f"EDOUARD'S FREE SLOTS (verified against his calendar):\n{slots_str}"
    else:
        slots_block = "EDOUARD'S FREE SLOTS: No free slots found in the next 2 weeks."

    now_str = datetime.now().strftime("%A %B %d, %Y")

    prompt = f"""You are drafting an email reply on behalf of Edouard Schneiders, Co-founder of CaraOmics.
{style_examples}
{lang_hint}Write in Edouard's voice: match his tone, greeting style, sign-off, language, and conciseness.

TODAY is {now_str}.
{thread_context}
Original email:
Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:2000]}
{travel_context}

{slots_block}

INSTRUCTIONS:
- Reply in the SAME LANGUAGE as the original email
- Match Edouard's greeting style from the examples
- ONLY use dates/times from the FREE SLOTS list above — never invent or assume other times are free
- If the sender proposed a specific time that appears in the free slots list: confirm it
- If the sender proposed a specific time NOT in the free slots: apologise briefly and suggest 2-3 slots from the list that match their preferences (e.g. morning only, no Wednesdays, etc.)
- If the sender asked for options (no specific time proposed): suggest 3 slots from the list that best match any preferences they mentioned
- For virtual meetings: mention video call (link to follow)
- End with Edouard's sign-off style
- Do NOT include a subject line
- Keep it concise — max 120 words
"""

    response = get_anthropic_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def create_calendar_event(meeting_details, email, invite_sender=False):
    """
    Create a Google Calendar event.
    - invite_sender=False: only adds to Edouard's calendar (default)
    - invite_sender=True: sends a calendar invite to the other person too
    """
    service = get_calendar_service()

    summary = meeting_details.get("topic") or email["subject"]
    location = meeting_details.get("location", "")
    duration = meeting_details.get("duration_minutes", 60)
    meeting_type = meeting_details.get("meeting_type", "")
    is_virtual = meeting_type in ["video call", "phone call"] or \
                 (location or "").lower() in ["video call", "online", "zoom", "teams", "google meet"]

    sender_name = email["sender"].split("<")[0].strip() or email["sender"]
    sender_email = email["sender"].split("<")[-1].strip(">")

    # Try to parse proposed date + time from meeting_details
    from zoneinfo import ZoneInfo
    AMS = ZoneInfo("Europe/Amsterdam")
    date_uncertain = False

    proposed_date = meeting_details.get("proposed_date")
    proposed_time = meeting_details.get("proposed_time")
    start_dt = None

    if proposed_date:
        try:
            # Try ISO format first (e.g. "2026-03-15")
            from datetime import date as _date
            import re as _re
            # Normalise common formats: "March 15", "15 March 2026", "next Tuesday", etc.
            # Try direct ISO parse
            parsed = datetime.strptime(proposed_date, "%Y-%m-%d")
            # Add time if available
            hour, minute = 10, 0
            if proposed_time:
                t = _re.sub(r"[^\d:]", "", proposed_time.replace(".", ":"))
                parts = t.split(":")
                if len(parts) >= 1 and parts[0].isdigit():
                    hour = int(parts[0])
                if len(parts) >= 2 and parts[1].isdigit():
                    minute = int(parts[1])
            start_dt = parsed.replace(hour=hour, minute=minute, tzinfo=AMS)
        except ValueError:
            pass

    if start_dt is None:
        # Couldn't parse date — default to tomorrow at 10:00 and flag it
        tomorrow = datetime.now(AMS).replace(hour=10, minute=0, second=0, microsecond=0)
        from datetime import timedelta
        start_dt = tomorrow + timedelta(days=1)
        date_uncertain = True

    end_dt = start_dt.replace(
        hour=start_dt.hour + duration // 60,
        minute=start_dt.minute + duration % 60,
    )
    start_str = start_dt.isoformat()
    end_str = end_dt.isoformat()

    date_note = "\n⚠️ DATE UNCERTAIN — could not parse from email, please update." if date_uncertain else ""
    event = {
        "summary": summary + (" ⚠️ DATE TBC" if date_uncertain else ""),
        "description": (
            f"Meeting arranged via email assistant.\n"
            f"Contact: {sender_name} <{sender_email}>\n"
            f"Original subject: {email['subject']}"
            f"{date_note}"
        ),
        "start": {"dateTime": start_str, "timeZone": "Europe/Amsterdam"},
        "end": {"dateTime": end_str, "timeZone": "Europe/Amsterdam"},
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "email", "minutes": 60},
                {"method": "popup", "minutes": 30},
            ],
        },
    }

    # Store location
    if location and not is_virtual:
        event["location"] = location

    # Only add Google Meet when explicitly sending invite to sender
    if invite_sender and sender_email:
        event["attendees"] = [{"email": sender_email}]
        send_updates = "all"
        if is_virtual:
            event["conferenceData"] = {
                "createRequest": {
                    "requestId": f"meet-{email['id']}",
                    "conferenceSolutionKey": {"type": "hangoutsMeet"},
                }
            }
    else:
        send_updates = "none"

    conference_version = 1 if (invite_sender and is_virtual) else 0

    created = service.events().insert(
        calendarId=CALENDAR_ID,
        body=event,
        sendUpdates=send_updates,
        conferenceDataVersion=conference_version,
    ).execute()

    meet_link = ""
    if invite_sender and is_virtual:
        meet_link = created.get("hangoutLink", "")

    return created.get("htmlLink"), meet_link, date_uncertain


def send_reply(email, reply_text):
    """Send the approved reply via Gmail."""
    service = get_gmail_service()

    # Extract plain email address from "Name <email>" format
    sender_email = email["sender"]
    match = re.search(r"<(.+?)>", sender_email)
    if match:
        to_address = match.group(1)
    else:
        to_address = sender_email

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Re: {email['subject']}"
    msg["From"] = WORK_EMAIL
    msg["To"] = to_address
    msg["Cc"] = "colm@caraomics.ai"
    msg["In-Reply-To"] = email.get("message_id", "")
    msg["References"] = email.get("message_id", "")

    msg.attach(MIMEText(reply_text, "plain"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(
        userId="me",
        body={"raw": raw, "threadId": email["thread_id"]},
    ).execute()

    print(f"Reply sent to {to_address}")


def _get_telegram_config():
    """Read Telegram token and chat_id from .env."""
    env = open(os.path.join(_DIR, ".env")).read()
    token, chat_id = None, None
    for line in env.splitlines():
        if line.startswith("TELEGRAM_BOT_TOKEN="):
            token = line.split("=", 1)[1].strip()
        if line.startswith("TELEGRAM_CHAT_ID="):
            chat_id = line.split("=", 1)[1].strip()
    return token, chat_id


def edit_telegram_message(message_id, message, keyboard=None):
    """Edit an existing Telegram message (meeting bot). Returns True on success.
    Pass keyboard=[] to remove all buttons."""
    token, chat_id = _get_telegram_config()
    if not token or not chat_id:
        return False
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    if keyboard is not None:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    resp = requests.post(
        f"https://api.telegram.org/bot{token}/editMessageText",
        json=payload, timeout=10,
    )
    return resp.ok


def edit_general_telegram_message(message_id, message, keyboard=None):
    """Alias for edit_telegram_message — single bot now handles all emails."""
    return edit_telegram_message(message_id, message, keyboard)


def send_telegram(message, keyboard=None):
    """Send a message to Telegram. Returns the Telegram message_id or None."""
    token, chat_id = _get_telegram_config()

    if not token or not chat_id:
        print("  → Telegram not configured, skipping notification.")
        return None

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}

    resp = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload,
        timeout=10,
    )
    if resp.ok:
        msg_id = resp.json()["result"]["message_id"]
        print(f"  → Telegram notification sent (msg_id={msg_id}).")
        return msg_id
    else:
        print(f"  → Telegram error: {resp.text}")
        return None


def build_tg_message(approval_id, is_redraft=False):
    """Build the Telegram message text and keyboard for any email approval item."""
    item = pending_approvals.get(approval_id)
    if not item:
        return None, None
    email = item["email"]
    reply_text = item["reply_text"]
    meeting_details = item.get("meeting_details") or {}
    travel_info = item.get("travel_info")
    is_meeting = bool(meeting_details)

    sender_name = _tg_escape(email["sender"].split("<")[0].strip() or email["sender"])
    tg_subject = _tg_escape(email["subject"])
    reply_preview = _tg_escape(reply_text[:1500] + ("..." if len(reply_text) > 1500 else ""))
    _email_body = _strip_quoted_reply(email["body"]).strip()
    email_preview = _tg_escape(_email_body[:2000] + ("..." if len(_email_body) > 2000 else ""))

    to_header = email.get("to", "")
    group_note = "👥 *Group thread — you may not need to reply*\n\n" if to_header.count("@") > 1 else ""
    draft_label = "↻ *Re\\-drafted*" if is_redraft else "✏️ *Draft*"

    if is_meeting:
        tg_location = _tg_escape(meeting_details.get("location") or "Not specified")
        proposed_date = _tg_escape(meeting_details.get("proposed_date") or "Not specified")
        proposed_time = _tg_escape(meeting_details.get("proposed_time") or "")
        travel_line = f"\n🚆 *Travel:* {travel_info['duration_text']} from Amsterdam Zuid" if travel_info else ""
        header = (
            f"{draft_label}\n\n"
            f"{group_note}"
            f"📬 *New meeting request*\n"
            f"*From:* {sender_name}\n"
            f"*Subject:* {tg_subject}\n"
            f"*Date:* {proposed_date} {proposed_time}\n"
            f"*Location:* {tg_location}"
            f"{travel_line}\n\n"
        )
    else:
        header = (
            f"{draft_label}\n\n"
            f"{group_note}"
            f"📧 *New email*\n"
            f"*From:* {sender_name}\n"
            f"*Subject:* {tg_subject}\n\n"
        )

    message = _fit_to_telegram(header, email_preview, reply_preview)
    keyboard = [
        [
            {"text": "✅ Send", "callback_data": f"send:{approval_id}"},
            {"text": "✏️ Edit & Send", "callback_data": f"edit:{approval_id}"},
            {"text": "🗑️ Discard", "callback_data": f"discard:{approval_id}"},
        ],
    ]
    return message, keyboard


# Backward-compat aliases (kept for any external callers)
def build_meeting_tg_message(approval_id, is_redraft=False):
    return build_tg_message(approval_id, is_redraft)

def build_general_tg_message(approval_id, is_redraft=False):
    return build_tg_message(approval_id, is_redraft)


def process_email(email):
    """Notify about incoming email — no auto-draft. User replies with instructions to draft."""
    print(f"\nProcessing email: {email['subject']} from {email['sender']}")

    if is_automated_sender(email):
        print(f"  → Automated sender detected, skipping: {email['sender']}")
        log_filtered_email(email, "automated_sender")
        return

    should_notify, clean_body = _llm_filter_and_clean(email)
    if not should_notify:
        print(f"  → LLM filter: skipping (not worth notifying)")
        log_filtered_email(email, "llm_filter")
        return

    meeting_related = is_meeting_related(email)
    icon = "📬" if meeting_related else "📧"
    email_type = "meeting email" if meeting_related else "email"

    thread_id = email["thread_id"]
    existing_id = pending_approvals.get(f"thread:{thread_id}")
    existing_item = pending_approvals.get(existing_id) if existing_id else None

    if existing_item and existing_item.get("status") in ("awaiting_instructions", "pending"):
        existing_item["email"] = email
        approval_id = existing_id
    else:
        approval_id = email["id"]
        pending_approvals[approval_id] = {
            "email": email,
            "status": "awaiting_instructions",
            "type": "meeting" if meeting_related else "general",
        }
        pending_approvals[f"thread:{thread_id}"] = approval_id
        save_pending_approvals()

    sender_name = _tg_escape(email["sender"].split("<")[0].strip() or email["sender"])
    tg_subject = _tg_escape(email["subject"])
    email_preview = _tg_escape(clean_body[:3000] + ("..." if len(clean_body) > 3000 else ""))
    to_header = email.get("to", "")
    group_note = "👥 *Group thread — you may not need to reply*\n\n" if to_header.count("@") > 1 else ""

    message = (
        f"{group_note}"
        f"{icon} *New {email_type}*\n"
        f"*From:* {sender_name}\n"
        f"*Subject:* {tg_subject}\n\n"
        f"{email_preview}\n\n"
        f"_Reply with instructions to draft a reply_"
    )
    keyboard = [[{"text": "🗑️ Discard", "callback_data": f"discard:{approval_id}"}]]

    existing_tg_msg_id = pending_approvals.get(approval_id, {}).get("telegram_message_id") if existing_item else None
    if existing_tg_msg_id:
        edit_telegram_message(existing_tg_msg_id, message, keyboard)
    else:
        tg_msg_id = send_telegram(message, keyboard)
        if tg_msg_id:
            pending_approvals[approval_id]["telegram_message_id"] = tg_msg_id
            pending_approvals[f"orig:{tg_msg_id}"] = approval_id
            save_pending_approvals()


def send_general_telegram(message, keyboard=None):
    """Alias for send_telegram — single bot now handles all emails."""
    return send_telegram(message, keyboard)


def get_email_thread(thread_id):
    """Fetch all messages in a Gmail thread and return as a list of dicts."""
    service = get_gmail_service()
    try:
        thread = service.users().threads().get(
            userId="me", id=thread_id, format="full"
        ).execute()
        messages = thread.get("messages", [])
        thread_messages = []
        for msg in messages:
            headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
            body = extract_body(msg["payload"])
            thread_messages.append({
                "from": headers.get("From", ""),
                "date": headers.get("Date", ""),
                "body": body[:3000],  # cap per thread message
            })
        return thread_messages
    except Exception as e:
        print(f"Thread fetch error: {e}")
        return []


def draft_general_reply(email, model="claude-haiku-4-5", thread_messages=None, style_examples=None):
    """
    Draft a reply for a non-meeting email in Edouard's voice.
    Uses haiku by default. Pass thread_messages for full context (Sonnet path).
    style_examples is ignored (always fetched fresh from cache).
    """
    fetched_examples = get_style_examples()
    lang = _detect_language(email.get("body", ""))
    lang_hint = "The email is in Dutch — reply in Dutch and prioritise Dutch style examples above.\n" if lang == "nl" else ""

    thread_context = ""
    if thread_messages and len(thread_messages) > 1:
        parts = []
        for m in thread_messages[:-1]:  # exclude the triggering email itself
            parts.append(f"[{m['date']}] From {m['from']}:\n{m['body']}")
        thread_context = "\n\n---\n".join(parts)
        thread_context = f"\nEmail thread history (most recent is at the bottom):\n{thread_context}\n"

    from datetime import timedelta
    now_local = datetime.now()
    min_date = now_local + timedelta(days=3)
    min_date_str = min_date.strftime("%A %B %d, %Y")

    prompt = f"""You are drafting an email reply on behalf of Edouard Schneiders, Co-founder of CaraOmics.
{fetched_examples}
{lang_hint}Write in Edouard's voice exactly as shown in the examples above: match his tone, greeting style, sign-off, language choice, and conciseness.
{thread_context}
Email to reply to:
Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:2000]}

TODAY is {now_local.strftime("%A %B %d, %Y")}.

INSTRUCTIONS:
- Reply in the SAME LANGUAGE as the original email (if they write Dutch, reply in Dutch)
- Match Edouard's greeting style from the examples (e.g. "Hi X," for informal, "Dear X," for formal, "Hallo X," for Dutch)
- Be helpful and concise — answer or acknowledge their email directly
- Do NOT invent facts or commitments not grounded in the email
- If this email involves scheduling or suggesting times: NEVER suggest a date earlier than {min_date_str}
- End with Edouard's sign-off style as shown in the examples
- Do NOT include a subject line, only the email body
- Keep it concise — max 150 words
"""

    response = get_anthropic_client().messages.create(
        model=model,
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def redraft_with_notes(item, notes):
    """
    Re-draft a reply based on user feedback notes.
    Works for both meeting and general emails.
    item: the pending_approvals entry (has 'email', 'reply_text', optionally 'meeting_details')
    notes: plain-text instructions from the user (e.g. "make time 8:00, more professional tone")
    """
    email = item["email"]
    current_draft = item.get("reply_text", "")
    fetched_examples = get_style_examples()
    lang = _detect_language(email.get("body", ""))
    lang_hint = "The email is in Dutch — reply in Dutch and prioritise Dutch style examples above.\n" if lang == "nl" else ""

    prompt = f"""You are drafting an email reply on behalf of Edouard Schneiders, Co-founder of CaraOmics.
{fetched_examples}
{lang_hint}Write in Edouard's voice exactly as shown in the examples above.

Original email:
Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:2000]}

Current draft:
{current_draft}

Edouard's feedback / revision instructions:
{notes}

INSTRUCTIONS:
- Apply Edouard's feedback to revise the draft
- Keep his voice, tone, and sign-off style from the examples
- Reply in the SAME LANGUAGE as the original email
- Do NOT include a subject line, only the email body
- Keep it concise — max 150 words
"""

    response = get_anthropic_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def draft_from_instructions(item, instructions):
    """
    Draft a reply from scratch based on user instructions.
    Called when the user replies to the initial email notification.
    item: pending_approvals entry with 'email'
    instructions: user's plain-text instructions
    """
    email = item["email"]
    fetched_examples = get_style_examples()
    lang = _detect_language(email.get("body", ""))
    lang_hint = "The email is in Dutch — reply in Dutch and prioritise Dutch style examples above.\n" if lang == "nl" else ""
    now_str = datetime.now().strftime("%A %B %d, %Y")

    # Fetch thread context: last 3 prior messages
    thread_context = ""
    thread_id = email.get("thread_id")
    if thread_id:
        thread_messages = get_email_thread(thread_id)
        if len(thread_messages) > 1:
            prior = thread_messages[:-1][-3:]  # last 3 before the current email
            parts = []
            for m in prior:
                role = "You" if WORK_EMAIL and WORK_EMAIL.lower() in m.get("from", "").lower() else (m.get("from", "").split("<")[0].strip() or m.get("from", ""))
                parts.append(f"[{m['date']}] {role}:\n{m['body'][:600]}")
            thread_context = "\nThread history (earlier messages, most recent last):\n" + "\n\n---\n".join(parts) + "\n"

    prompt = f"""You are drafting an email reply on behalf of Edouard Schneiders, Co-founder of CaraOmics.
{fetched_examples}
{lang_hint}Write in Edouard's voice exactly as shown in the examples above.

TODAY is {now_str}.
{thread_context}
Original email:
Subject: {email['subject']}
From: {email['sender']}
Body:
{email['body'][:2000]}

Edouard's instructions for this reply:
{instructions}

INSTRUCTIONS:
- Follow Edouard's instructions above precisely
- Write in his voice, tone, greeting style, and sign-off as shown in the examples
- Reply in the SAME LANGUAGE as the original email
- Do NOT include a subject line, only the email body
- Keep it concise — max 150 words
"""

    response = get_anthropic_client().messages.create(
        model="claude-haiku-4-5",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def poll_gmail():
    """Main polling loop."""
    from zoneinfo import ZoneInfo
    import google.auth.exceptions as _gauth_exc

    AMS = ZoneInfo("Europe/Amsterdam")
    print(f"Email assistant started. Polling every {POLL_INTERVAL} seconds...")
    print(f"Base location: {BASE_LOCATION}")
    print(f"Work email: {WORK_EMAIL}")

    last_check = datetime.now(timezone.utc)
    last_digest_date = None

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking for new emails...")

            try:
                service = get_gmail_service()
                emails = get_unread_meeting_emails(service, last_check)
            except _gauth_exc.RefreshError as auth_err:
                print(f"  → Google OAuth token expired: {auth_err}")
                send_telegram(
                    "⚠️ *Google OAuth token expired!*\n"
                    "Re-run auth on your laptop and copy `token.json` to the server."
                )
                time.sleep(3600)
                continue

            last_check = datetime.now(timezone.utc)

            if not emails:
                print("  No new emails.")
            else:
                print(f"  Found {len(emails)} new email(s).")
                for email in emails:
                    process_email(email)

            # Daily digest at 8am Amsterdam time
            now_ams = datetime.now(AMS)
            if now_ams.hour == 8 and last_digest_date != now_ams.date():
                last_digest_date = now_ams.date()
                try:
                    send_daily_digest()
                except Exception as de:
                    print(f"  → Digest error: {de}")

        except Exception as e:
            print(f"Error during poll: {e}")

        time.sleep(POLL_INTERVAL)
