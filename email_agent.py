#!/usr/bin/env python3
"""
Safety-first Gmail/OpenAI agent for Buzz reporting workflows.

The agent watches Gmail, classifies messages, labels/moves them, creates draft
replies, and can trigger existing reporting scripts when explicitly allowed.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import email.utils
import html
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "email_agent_config.json"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
]

INTENTS = [
    "deal_report_request",
    "inventory_report_request",
    "headset",
    "important_human",
    "routine_answerable",
    "ignore",
]

ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {"type": "string", "enum": INTENTS},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "summary": {"type": "string"},
        "requested_brand": {"type": ["string", "null"]},
        "requested_report": {
            "type": "string",
            "enum": ["deals", "inventory", "aging_inventory", "both", "unknown"],
        },
        "age_days": {"type": ["integer", "null"], "minimum": 1},
        "move_to": {
            "type": "string",
            "enum": ["inbox", "needs_human", "headset", "report_requests", "ignore"],
        },
        "should_archive": {"type": "boolean"},
        "should_create_draft": {"type": "boolean"},
        "draft_reply": {"type": "string"},
        "should_run_job": {"type": "boolean"},
        "job_key": {"type": "string", "enum": ["weekly_deals", "inventory", "aged_710_flower", "none"]},
        "reason": {"type": "string"},
    },
    "required": [
        "intent",
        "confidence",
        "summary",
        "requested_brand",
        "requested_report",
        "age_days",
        "move_to",
        "should_archive",
        "should_create_draft",
        "draft_reply",
        "should_run_job",
        "job_key",
        "reason",
    ],
}


DEFAULT_CONFIG: dict[str, Any] = {
    "state_file": ".email_agent_state.json",
    "timezone": "America/Los_Angeles",
    "gmail": {
        "credentials_file": "credentials.json",
        "token_file": "token_gmail_agent.json",
        "poll_seconds": 120,
        "search_query": "in:inbox newer_than:14d -from:help@nabis.com -from:info@headset.io -from:buzz-office@buzz -from:noreply@leaflogix.com -from:sc-noreply@google.com -from:noreply -from:no-reply -from:donotreply -from:do-not-reply",
        "max_messages_per_poll": 10,
        "skip_senders": [
            "help@nabis.com",
            "info@headset.io",
            "buzz-office@buzz",
            "noreply@leaflogix.com",
            "sc-noreply@google.com",
        ],
        "skip_noreply_senders": True,
        "processed_label": "AI Agent/Processed",
        "review_label": "AI Agent/Needs Human",
        "report_label": "AI Agent/Report Requests",
        "headset_label": "AI Agent/Headset",
        "ignore_label": "AI Agent/Ignore",
        "low_confidence_label": "AI Agent/Review",
        "draft_replies": True,
        "archive_processed": False,
    },
    "openai": {"env_file": ".env", "model": "gpt-5.5", "reasoning_effort": "low"},
    "safety": {
        "dry_run": True,
        "auto_run_reports": False,
        "create_drafts": True,
        "archive_low_risk": False,
        "allowed_requesters": [],
        "never_auto_archive_from": [],
        "human_review_confidence_below": 0.72,
    },
    "review": {
        "queue_file": ".email_agent_review_queue.jsonl",
        "log_all_actions": True,
        "popup_before_draft": False,
    },
    "report_drafts": {
        "auto_generate": False,
        "default_age_days": 90,
        "inventory_full_update": True,
        "create_failure_drafts": True,
    },
    "routing": {
        "headset_keywords": ["headset", "head set", "headphones", "earpiece", "radio", "walkie"],
        "important_keywords": ["urgent", "legal", "complaint", "refund", "invoice", "metrc", "dutchie", "owner"],
        "report_keywords": ["deal report", "deals report", "kickback report", "inventory report", "brand report"],
    },
    "jobs": {
        "weekly_deals_command": [".venv/bin/python", "autoJob.py"],
        "inventory_command": [".venv/bin/python", "BrandINVEmailer.py"],
        "aged_710_flower_command": [
            ".venv/bin/python",
            "aged_flower_inventory_report.py",
            "--brand",
            "710 Labs",
            "--brand-alias",
            "710",
            "--age-days",
            "90",
        ],
        "weekly_schedule": [],
    },
}


@dataclass
class EmailRecord:
    message_id: str
    thread_id: str
    gmail_message_id_header: str
    references_header: str
    sender: str
    sender_email: str
    subject: str
    date: str
    snippet: str
    body: str
    label_ids: list[str]


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Config not found: {path}. Copy email_agent_config.example.json to this path first."
        )
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return deep_merge(DEFAULT_CONFIG, raw)


def load_env_file(path_value: str | os.PathLike[str] | None) -> None:
    if not path_value:
        return
    path = resolved_path(path_value)
    if not path.exists():
        return
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv(path, override=False)


def resolved_path(value: str | os.PathLike[str]) -> Path:
    path = Path(value)
    return path if path.is_absolute() else BASE_DIR / path


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"job_runs": {}, "drafted_threads": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"job_runs": {}, "drafted_threads": []}


def save_state(path: Path, state: dict[str, Any]) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def b64url_decode(data: str | None) -> str:
    if not data:
        return ""
    padding = "=" * (-len(data) % 4)
    raw = base64.urlsafe_b64decode((data + padding).encode("utf-8"))
    return raw.decode("utf-8", errors="replace")


def html_to_text(value: str) -> str:
    value = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p\s*>", "\n", value)
    value = re.sub(r"(?s)<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"[ \t]+", " ", value).strip()


def compact_text(value: str, max_chars: int = 12000) -> str:
    value = re.sub(r"\r\n?", "\n", value or "")
    value = re.sub(r"\n{4,}", "\n\n\n", value)
    value = value.strip()
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n\n[trimmed]"


def extract_payload_text(payload: dict[str, Any]) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    def walk(part: dict[str, Any]) -> None:
        mime_type = part.get("mimeType", "")
        body_data = (part.get("body") or {}).get("data")
        if mime_type == "text/plain" and body_data:
            plain_parts.append(b64url_decode(body_data))
        elif mime_type == "text/html" and body_data:
            html_parts.append(html_to_text(b64url_decode(body_data)))
        for child in part.get("parts") or []:
            walk(child)

    walk(payload)
    text = "\n\n".join(part for part in plain_parts if part.strip())
    if not text:
        text = "\n\n".join(part for part in html_parts if part.strip())
    return compact_text(text)


def header_value(headers: list[dict[str, str]], name: str) -> str:
    for header in headers:
        if header.get("name", "").lower() == name.lower():
            return header.get("value", "")
    return ""


class GmailClient:
    def __init__(self, cfg: dict[str, Any], dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.service = self._authenticate()
        self._label_cache: dict[str, str] = {}

    def _authenticate(self):
        import google.auth.transport.requests
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build

        credentials_file = resolved_path(self.cfg["credentials_file"])
        token_file = resolved_path(self.cfg["token_file"])
        creds = None

        if token_file.exists():
            creds = Credentials.from_authorized_user_file(str(token_file), GMAIL_SCOPES)

        needs_new_scopes = bool(creds and hasattr(creds, "has_scopes") and not creds.has_scopes(GMAIL_SCOPES))
        if not creds or not creds.valid or needs_new_scopes:
            if creds and creds.expired and creds.refresh_token and not needs_new_scopes:
                creds.refresh(google.auth.transport.requests.Request())
            else:
                if not credentials_file.exists():
                    raise FileNotFoundError(f"Missing Gmail OAuth credentials file: {credentials_file}")
                flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), GMAIL_SCOPES)
                creds = flow.run_local_server(port=0)
            token_file.write_text(creds.to_json(), encoding="utf-8")

        return build("gmail", "v1", credentials=creds)

    def ensure_label(self, label_name: str) -> str:
        if label_name in self._label_cache:
            return self._label_cache[label_name]

        labels = self.service.users().labels().list(userId="me").execute().get("labels", [])
        for label in labels:
            if label.get("name") == label_name:
                self._label_cache[label_name] = label["id"]
                return label["id"]

        body = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        }
        if self.dry_run:
            fake_id = f"dry-run:{label_name}"
            self._label_cache[label_name] = fake_id
            print(f"[DRY-RUN] Would create Gmail label: {label_name}")
            return fake_id

        created = self.service.users().labels().create(userId="me", body=body).execute()
        self._label_cache[label_name] = created["id"]
        return created["id"]

    def list_message_ids(self, query: str, max_results: int) -> list[str]:
        response = (
            self.service.users()
            .messages()
            .list(userId="me", q=query, maxResults=max_results)
            .execute()
        )
        return [item["id"] for item in response.get("messages", [])]

    def get_message(self, message_id: str) -> EmailRecord:
        message = (
            self.service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute()
        )
        payload = message.get("payload") or {}
        headers = payload.get("headers") or []
        sender = header_value(headers, "From")
        _, sender_email = email.utils.parseaddr(sender)
        return EmailRecord(
            message_id=message_id,
            thread_id=message.get("threadId", ""),
            gmail_message_id_header=header_value(headers, "Message-ID"),
            references_header=header_value(headers, "References"),
            sender=sender,
            sender_email=sender_email.lower(),
            subject=header_value(headers, "Subject"),
            date=header_value(headers, "Date"),
            snippet=message.get("snippet", ""),
            body=extract_payload_text(payload),
            label_ids=message.get("labelIds", []),
        )

    def modify_message(self, message_id: str, add_labels: list[str], remove_labels: list[str] | None = None) -> None:
        add_label_ids = [self.ensure_label(label) for label in add_labels]
        remove_label_ids = remove_labels or []
        if self.dry_run:
            print(f"[DRY-RUN] Would modify {message_id}: add={add_labels}, remove={remove_label_ids}")
            return

        self.service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"addLabelIds": add_label_ids, "removeLabelIds": remove_label_ids},
        ).execute()

    def create_reply_draft(self, email_record: EmailRecord, body_text: str) -> str | None:
        if not body_text.strip():
            return None

        message = EmailMessage()
        message["To"] = email_record.sender
        subject = email_record.subject or "(no subject)"
        message["Subject"] = subject if subject.lower().startswith("re:") else f"Re: {subject}"
        if email_record.gmail_message_id_header:
            message["In-Reply-To"] = email_record.gmail_message_id_header
            references = email_record.references_header.strip()
            message["References"] = f"{references} {email_record.gmail_message_id_header}".strip()
        message.set_content(body_text.strip())

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        body = {"message": {"raw": raw, "threadId": email_record.thread_id}}
        if self.dry_run:
            print(f"[DRY-RUN] Would create draft reply for {email_record.message_id}:\n{body_text.strip()}\n")
            return None

        draft = self.service.users().drafts().create(userId="me", body=body).execute()
        return draft.get("id")


class EmailClassifier:
    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.client = None
        self.disabled_reason = ""
        try:
            from openai import OpenAI

            self.client = OpenAI()
        except Exception as exc:
            self.disabled_reason = str(exc)

    def classify(self, email_record: EmailRecord) -> dict[str, Any]:
        if self.client is None:
            return self.rule_based_classify(email_record, f"OpenAI unavailable: {self.disabled_reason}")

        prompt = (
            "Classify an inbound business email for a cannabis retail reporting assistant. "
            "Return only the structured action. The assistant may label messages, create drafts, "
            "and recommend report jobs, but it must not send final emails or delete anything. "
            "Use important_human for sensitive, urgent, legal, complaint, refund, vendor conflict, "
            "payment, ownership, security, or unclear operational messages. Use headset for headset, "
            "radio, earpiece, or staff equipment messages. Use deal_report_request when the sender "
            "asks for brand deal, kickback, or weekly deals reports. Use inventory_report_request "
            "when the sender asks for brand inventory, reorder, availability, stock, or inventory links. "
            "Extract requested_brand when the sender asks for a report for a specific brand. "
            "Use requested_report='aging_inventory' for aged/aging/old flower inventory requests, "
            "requested_report='inventory' for standard brand inventory, and requested_report='both' "
            "when both standard inventory and aged inventory are requested. "
            "If the age window is stated, convert it to days in age_days, for example three months is 90. "
            "If uncertain, choose important_human with low confidence."
        )
        payload = {
            "from": email_record.sender,
            "subject": email_record.subject,
            "date": email_record.date,
            "snippet": email_record.snippet,
            "body": compact_text(email_record.body, max_chars=8000),
        }

        try:
            response = self.client.responses.create(
                model=self.cfg["model"],
                input=[
                    {"role": "developer", "content": prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                reasoning={"effort": self.cfg.get("reasoning_effort", "low")},
                text={
                    "verbosity": "low",
                    "format": {
                        "type": "json_schema",
                        "name": "email_agent_action",
                        "strict": True,
                        "schema": ACTION_SCHEMA,
                    },
                },
            )
            raw_text = getattr(response, "output_text", "") or self._extract_output_text(response)
            return self._normalize_action(json.loads(raw_text))
        except Exception as exc:
            return self.rule_based_classify(email_record, f"OpenAI classification failed: {exc}")

    def _extract_output_text(self, response: Any) -> str:
        chunks: list[str] = []
        for output in getattr(response, "output", []) or []:
            for item in getattr(output, "content", []) or []:
                text = getattr(item, "text", None)
                if text:
                    chunks.append(text)
        return "\n".join(chunks)

    def _normalize_action(self, action: dict[str, Any]) -> dict[str, Any]:
        action = {key: action.get(key) for key in ACTION_SCHEMA["properties"]}
        action["intent"] = action["intent"] if action["intent"] in INTENTS else "important_human"
        action["confidence"] = max(0.0, min(1.0, float(action.get("confidence") or 0)))
        action["requested_brand"] = action.get("requested_brand") or None
        action["requested_report"] = action.get("requested_report") or "unknown"
        try:
            action["age_days"] = int(action["age_days"]) if action.get("age_days") else None
        except (TypeError, ValueError):
            action["age_days"] = None
        action["move_to"] = action.get("move_to") or "needs_human"
        action["should_archive"] = bool(action.get("should_archive"))
        action["should_create_draft"] = bool(action.get("should_create_draft"))
        action["draft_reply"] = str(action.get("draft_reply") or "")
        action["should_run_job"] = bool(action.get("should_run_job"))
        action["job_key"] = action.get("job_key") or "none"
        action["summary"] = str(action.get("summary") or "")
        action["reason"] = str(action.get("reason") or "")
        return action

    def rule_based_classify(self, email_record: EmailRecord, reason: str = "rule fallback") -> dict[str, Any]:
        routing = self.cfg.get("routing", {})
        text = f"{email_record.sender} {email_record.subject} {email_record.snippet} {email_record.body}".lower()

        def has_any(values: list[str]) -> bool:
            return any(str(value).lower() in text for value in values)

        if has_any(routing.get("headset_keywords", [])):
            intent = "headset"
            move_to = "headset"
            requested_report = "unknown"
            job_key = "none"
            should_create_draft = False
        elif ("710" in text and ("aged" in text or "older than" in text or "old flower" in text)):
            intent = "inventory_report_request"
            move_to = "report_requests"
            requested_report = "aging_inventory"
            job_key = "aged_710_flower"
            should_create_draft = True
        elif "inventory" in text or "stock" in text or "availability" in text:
            intent = "inventory_report_request"
            move_to = "report_requests"
            requested_report = "inventory"
            job_key = "inventory"
            should_create_draft = True
        elif "deal report" in text or "deals report" in text or "kickback" in text:
            intent = "deal_report_request"
            move_to = "report_requests"
            requested_report = "deals"
            job_key = "weekly_deals"
            should_create_draft = True
        elif has_any(routing.get("important_keywords", [])):
            intent = "important_human"
            move_to = "needs_human"
            requested_report = "unknown"
            job_key = "none"
            should_create_draft = False
        else:
            intent = "routine_answerable"
            move_to = "needs_human"
            requested_report = "unknown"
            job_key = "none"
            should_create_draft = False

        draft = ""
        if should_create_draft:
            draft = (
                "Got it. I am checking the requested report and will follow up with the correct link/file shortly.\n\n"
                "Thanks,\nAnthony"
            )

        return {
            "intent": intent,
            "confidence": 0.64,
            "summary": compact_text(email_record.snippet or email_record.subject, max_chars=240),
            "requested_brand": None,
            "requested_report": requested_report,
            "age_days": None,
            "move_to": move_to,
            "should_archive": False,
            "should_create_draft": should_create_draft,
            "draft_reply": draft,
            "should_run_job": job_key != "none",
            "job_key": job_key,
            "reason": reason,
        }


class ReportRunner:
    def __init__(self, cfg: dict[str, Any], dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run

    def run_job(self, job_key: str) -> int:
        command_key = {
            "weekly_deals": "weekly_deals_command",
            "inventory": "inventory_command",
            "aged_710_flower": "aged_710_flower_command",
        }.get(job_key)
        if not command_key:
            print(f"[JOB] Unknown job key: {job_key}")
            return 2

        command = self.cfg.get(command_key)
        if not command:
            print(f"[JOB] No command configured for {job_key}")
            return 2

        print(f"[JOB] {job_key}: {' '.join(command)}")
        if self.dry_run:
            print("[DRY-RUN] Would run report job.")
            return 0

        completed = subprocess.run(command, cwd=BASE_DIR)
        return completed.returncode


def safe_report_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return slug or "brand"


def normalize_for_alias(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def brand_aliases_for_report(brand: str) -> list[str]:
    normalized = normalize_for_alias(brand)
    aliases: list[str] = []
    if normalized in {"710", "710labs"}:
        aliases.extend(["710 Labs", "710Labs", "710"])
    return [alias for alias in dict.fromkeys(aliases) if normalize_for_alias(alias) != normalized]


def local_today(config: dict[str, Any]) -> str:
    tz = ZoneInfo(config.get("timezone", "America/Los_Angeles"))
    return dt.datetime.now(tz).date().isoformat()


def detect_age_days_from_text(text: str, default_age_days: int) -> int:
    lowered = text.casefold()
    match = re.search(r"older than\s+(\d+)\s+month", lowered)
    if match:
        return int(match.group(1)) * 30
    match = re.search(r"(\d+)\s+month", lowered)
    if match and any(token in lowered for token in ("aged", "aging", "older", "old flower")):
        return int(match.group(1)) * 30
    match = re.search(r"older than\s+(\d+)\s+day", lowered)
    if match:
        return int(match.group(1))
    match = re.search(r"(\d+)\s+day", lowered)
    if match and any(token in lowered for token in ("aged", "aging", "older", "old flower")):
        return int(match.group(1))
    if "three month" in lowered or "3 month" in lowered:
        return 90
    return int(default_age_days)


def read_colon_links(path: Path) -> dict[str, str]:
    links: dict[str, str] = {}
    if not path.exists():
        return links
    for line in path.read_text(encoding="utf-8").splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        if key.strip() and value.strip():
            links[key.strip()] = value.strip()
    return links


def inventory_manifest_path(brand: str, today: str) -> Path:
    return BASE_DIR / "reports" / "brand_inventory_requests" / today / safe_report_slug(brand) / "brand_inventory_manifest.json"


def aged_inventory_links_path(today: str) -> Path:
    return BASE_DIR / "reports" / "aged_inventory" / today / "drive_links.txt"


class ReviewQueue:
    def __init__(self, path: Path):
        self.path = path

    def append(self, email_record: EmailRecord, action: dict[str, Any], extra: dict[str, Any] | None = None) -> None:
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "message_id": email_record.message_id,
            "thread_id": email_record.thread_id,
            "from": email_record.sender,
            "from_email": email_record.sender_email,
            "subject": email_record.subject,
            "date": email_record.date,
            "intent": action.get("intent"),
            "confidence": action.get("confidence"),
            "summary": action.get("summary"),
            "requested_brand": action.get("requested_brand"),
            "requested_report": action.get("requested_report"),
            "move_to": action.get("move_to"),
            "should_create_draft": action.get("should_create_draft"),
            "draft_reply": action.get("draft_reply"),
            "should_run_job": action.get("should_run_job"),
            "job_key": action.get("job_key"),
            "reason": action.get("reason"),
        }
        if extra:
            payload.update(extra)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def popup_approve_draft(email_record: EmailRecord, action: dict[str, Any]) -> bool | None:
    """
    Return True to create the draft, False to skip it, None when a popup cannot be shown.
    """
    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY") or sys.platform == "darwin"):
        return None

    try:
        import tkinter as tk
        from tkinter.scrolledtext import ScrolledText
    except Exception:
        return None

    result: dict[str, bool | None] = {"approved": None}

    root = tk.Tk()
    root.title("Email Agent Review")
    root.geometry("780x620")
    root.minsize(620, 480)

    container = tk.Frame(root, padx=14, pady=14)
    container.pack(fill="both", expand=True)

    tk.Label(container, text="Email Agent Review", font=("TkDefaultFont", 15, "bold")).pack(anchor="w")
    tk.Label(container, text=f"From: {email_record.sender}", anchor="w", justify="left").pack(fill="x", pady=(12, 0))
    tk.Label(container, text=f"Subject: {email_record.subject}", anchor="w", justify="left", wraplength=720).pack(fill="x")
    tk.Label(
        container,
        text=f"Intent: {action.get('intent')}  |  Confidence: {float(action.get('confidence') or 0):.2f}",
        anchor="w",
        justify="left",
    ).pack(fill="x", pady=(4, 8))

    tk.Label(container, text="Summary", font=("TkDefaultFont", 10, "bold")).pack(anchor="w")
    summary = tk.Message(container, text=action.get("summary") or "", width=720)
    summary.pack(fill="x", anchor="w", pady=(0, 10))

    tk.Label(container, text="Draft Reply", font=("TkDefaultFont", 10, "bold")).pack(anchor="w")
    text = ScrolledText(container, wrap="word", height=18)
    text.insert("1.0", action.get("draft_reply") or "")
    text.pack(fill="both", expand=True)

    def approve() -> None:
        action["draft_reply"] = text.get("1.0", "end").strip()
        result["approved"] = True
        root.destroy()

    def skip() -> None:
        result["approved"] = False
        root.destroy()

    buttons = tk.Frame(container)
    buttons.pack(fill="x", pady=(12, 0))
    tk.Button(buttons, text="Create Gmail Draft", command=approve, width=18).pack(side="left")
    tk.Button(buttons, text="Skip Draft", command=skip, width=12).pack(side="left", padx=(8, 0))

    root.protocol("WM_DELETE_WINDOW", skip)
    root.mainloop()
    return result["approved"]


def show_review_queue(cfg: dict[str, Any], limit: int) -> int:
    path = resolved_path(cfg.get("review", {}).get("queue_file", ".email_agent_review_queue.jsonl"))
    if not path.exists():
        print(f"No review queue found yet: {path}")
        return 0

    lines = path.read_text(encoding="utf-8").splitlines()
    entries = []
    for line in lines[-max(1, limit):]:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not entries:
        print(f"Review queue is empty: {path}")
        return 0

    for entry in entries:
        print("=" * 72)
        print(f"Time: {entry.get('created_at')}")
        print(f"From: {entry.get('from')}")
        print(f"Subject: {entry.get('subject')}")
        print(f"Intent: {entry.get('intent')} ({float(entry.get('confidence') or 0):.2f})")
        print(f"Move to: {entry.get('move_to')} | Job: {entry.get('job_key')}")
        print(f"Summary: {entry.get('summary')}")
        draft_reply = str(entry.get("draft_reply") or "").strip()
        if draft_reply:
            print("\nDraft reply:")
            print(draft_reply)
        print()
    return 0


class EmailAgent:
    def __init__(self, cfg: dict[str, Any], dry_run_override: bool | None = None):
        self.cfg = cfg
        self.gmail_cfg = cfg["gmail"]
        self.safety_cfg = cfg["safety"]
        self.review_cfg = cfg.get("review", {})
        self.report_drafts_cfg = cfg.get("report_drafts", {})
        self.dry_run = self.safety_cfg.get("dry_run", True) if dry_run_override is None else dry_run_override
        self.state_path = resolved_path(cfg["state_file"])
        self.state = load_state(self.state_path)
        self.gmail = GmailClient(self.gmail_cfg, dry_run=self.dry_run)
        classifier_cfg = dict(cfg.get("openai", {}))
        classifier_cfg["routing"] = cfg.get("routing", {})
        self.classifier = EmailClassifier(classifier_cfg)
        self.runner = ReportRunner(cfg.get("jobs", {}), dry_run=self.dry_run)
        self.review_queue = ReviewQueue(resolved_path(self.review_cfg.get("queue_file", ".email_agent_review_queue.jsonl")))
        self.processed_label_id = self.gmail.ensure_label(self.gmail_cfg["processed_label"])

    def process_once(self) -> int:
        query = self.gmail_cfg["search_query"]
        max_results = int(self.gmail_cfg.get("max_messages_per_poll", 10))
        message_ids = self.gmail.list_message_ids(query, max_results=max_results)
        processed = 0
        skipped = 0
        print(f"[SCAN] Found {len(message_ids)} candidate message(s).")

        for message_id in message_ids:
            email_record = self.gmail.get_message(message_id)
            if self.processed_label_id in email_record.label_ids:
                continue
            if self.should_skip_email(email_record):
                print(f"[SKIP] {email_record.sender_email or email_record.sender} | {email_record.subject!r}")
                skipped += 1
                continue
            self.process_message(email_record)
            processed += 1

        print(f"[SCAN] Processed {processed} new message(s), skipped {skipped}.")
        return processed

    def should_skip_email(self, email_record: EmailRecord) -> bool:
        sender_email = (email_record.sender_email or "").casefold()
        sender_text = f"{email_record.sender} {sender_email}".casefold()
        skip_senders = [str(value).strip().casefold() for value in self.gmail_cfg.get("skip_senders", [])]

        for value in skip_senders:
            if not value:
                continue
            if "@" in value and value.endswith((".com", ".net", ".org", ".io", ".co")):
                if sender_email == value:
                    return True
            elif value in sender_text:
                return True

        if self.gmail_cfg.get("skip_noreply_senders", True):
            local_part = sender_email.split("@", 1)[0]
            compact_local = re.sub(r"[^a-z0-9]+", "", local_part)
            if "noreply" in compact_local or "donotreply" in compact_local:
                return True

        return False

    def maybe_prepare_report_draft(self, email_record: EmailRecord, action: dict[str, Any]) -> dict[str, Any]:
        if not self.report_drafts_cfg.get("auto_generate", False):
            return action
        if self.dry_run:
            return action
        if action.get("intent") != "inventory_report_request":
            return action
        confidence_floor = float(self.safety_cfg.get("human_review_confidence_below", 0.72))
        if float(action.get("confidence") or 0) < confidence_floor:
            return action
        allowed_requesters = {str(x).casefold() for x in self.safety_cfg.get("allowed_requesters", [])}
        if allowed_requesters and email_record.sender_email.casefold() not in allowed_requesters:
            return action
        if not self.safety_cfg.get("create_drafts", True):
            return action
        if not self.should_create_draft(email_record):
            return action

        brand = str(action.get("requested_brand") or "").strip()
        if not brand:
            action = dict(action)
            action["should_create_draft"] = True
            action["draft_reply"] = (
                "Hi,\n\n"
                "I can pull this, but I need the brand name first. "
                "Send me the brand and I will generate the inventory report.\n\n"
                "Best,\nAnthony"
            )
            return action

        try:
            draft_body = self.generate_inventory_report_reply(email_record, action, brand)
        except Exception as exc:
            action = dict(action)
            action["move_to"] = "needs_human"
            action["should_create_draft"] = bool(self.report_drafts_cfg.get("create_failure_drafts", True))
            action["draft_reply"] = (
                f"Hi,\n\nI tried to generate the {brand} inventory report, but the automation hit an error:\n\n"
                f"{exc}\n\n"
                "I am going to review it and follow up shortly.\n\n"
                "Best,\nAnthony"
            )
            action["reason"] = f"{action.get('reason', '')} Report generation failed: {exc}".strip()
            return action

        action = dict(action)
        action["move_to"] = "report_requests"
        action["should_create_draft"] = True
        action["draft_reply"] = draft_body
        action["should_run_job"] = False
        return action

    def generate_inventory_report_reply(self, email_record: EmailRecord, action: dict[str, Any], brand: str) -> str:
        text = f"{email_record.subject}\n{email_record.snippet}\n{email_record.body}"
        requested_report = str(action.get("requested_report") or "unknown")
        default_age_days = int(self.report_drafts_cfg.get("default_age_days", 90))
        age_days = int(action.get("age_days") or detect_age_days_from_text(text, default_age_days))
        lowered = text.casefold()

        wants_aging = requested_report in {"aging_inventory", "both"} or any(
            token in lowered for token in ("aged", "aging", "older than", "old flower")
        )
        wants_inventory = requested_report in {"inventory", "both"} or (
            "inventory" in lowered and not wants_aging
        )
        if requested_report == "both":
            wants_inventory = True
            wants_aging = True
        if not wants_inventory and not wants_aging:
            wants_inventory = True

        today = local_today(self.cfg)
        sections: list[str] = []

        if wants_inventory:
            sections.append(self.run_full_inventory_report(brand, today))
        if wants_aging:
            sections.append(self.run_aged_inventory_report(brand, age_days, today))

        section_text = "\n\n".join(section for section in sections if section.strip())
        if not section_text:
            section_text = "I generated the report, but no Drive link was returned. I will review it."

        return (
            f"Hi,\n\n"
            f"I pulled the requested {brand} report(s). Links are below:\n\n"
            f"{section_text}\n\n"
            "Best,\nAnthony"
        )

    def run_full_inventory_report(self, brand: str, today: str) -> str:
        cmd = [sys.executable, "brand_inventory_report_job.py", "--brand", brand]
        for alias in brand_aliases_for_report(brand):
            cmd.extend(["--brand-alias", alias])
        if not self.report_drafts_cfg.get("inventory_full_update", True):
            cmd.append("--no-refresh")
        self.run_report_command(cmd, "full inventory")

        manifest_path = inventory_manifest_path(brand, today)
        if not manifest_path.exists():
            return f"Full inventory report: generated, but manifest was not found at {manifest_path}"

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        links = manifest.get("links") or {}
        if not links:
            return f"Full inventory report: generated locally at {manifest_path.parent}, but no Drive link was returned."

        lines = ["Full inventory report:"]
        for brand_key, link in sorted(links.items()):
            lines.append(f"- {brand_key}: {link}")
        return "\n".join(lines)

    def run_aged_inventory_report(self, brand: str, age_days: int, today: str) -> str:
        cmd = [sys.executable, "aged_flower_inventory_report.py", "--brand", brand, "--age-days", str(age_days)]
        for alias in brand_aliases_for_report(brand):
            cmd.extend(["--brand-alias", alias])
        self.run_report_command(cmd, "aged inventory")

        links = read_colon_links(aged_inventory_links_path(today))
        folder_link = links.get("folder")
        report_link = next((value for key, value in links.items() if key.endswith(".xlsx")), folder_link)
        if not report_link and not folder_link:
            return f"Aged inventory report ({age_days}+ days): generated locally, but no Drive link was returned."

        lines = [f"Aged flower inventory report ({age_days}+ days):"]
        if report_link:
            lines.append(f"- Report: {report_link}")
        if folder_link:
            lines.append(f"- Folder: {folder_link}")
        return "\n".join(lines)

    def run_report_command(self, cmd: list[str], label: str) -> None:
        print(f"[REPORT] Running {label}: {' '.join(cmd)}")
        completed = subprocess.run(cmd, cwd=BASE_DIR, text=True, capture_output=True)
        if completed.stdout:
            print(completed.stdout.strip())
        if completed.stderr:
            print(completed.stderr.strip(), file=sys.stderr)
        if completed.returncode != 0:
            raise RuntimeError(f"{label} command failed with exit code {completed.returncode}")

    def process_message(self, email_record: EmailRecord) -> None:
        action = self.classifier.classify(email_record)
        action = self.apply_safety(email_record, action)
        action = self.maybe_prepare_report_draft(email_record, action)
        labels = self.labels_for_action(action)
        remove_labels = ["INBOX"] if action.get("should_archive") else []

        print(
            "[EMAIL] "
            f"{email_record.sender_email or email_record.sender} | {email_record.subject!r} "
            f"=> {action['intent']} ({action['confidence']:.2f})"
        )
        print(f"[EMAIL] Summary: {action.get('summary')}")
        print(f"[EMAIL] Reason: {action.get('reason')}")

        self.gmail.modify_message(email_record.message_id, labels, remove_labels=remove_labels)

        if self.review_cfg.get("log_all_actions", True):
            self.review_queue.append(email_record, action, extra={"labels": labels, "dry_run": self.dry_run})

        if action.get("should_create_draft") and action.get("draft_reply"):
            if self.should_create_draft(email_record):
                skip_draft = False
                if self.review_cfg.get("popup_before_draft", False):
                    approved = popup_approve_draft(email_record, action)
                    if approved is None:
                        print("[REVIEW] Popup unavailable; proposal was written to the review queue.")
                        skip_draft = True
                    elif not approved:
                        print("[REVIEW] Draft skipped from popup.")
                        self.review_queue.append(email_record, action, extra={"draft_skipped_by_popup": True})
                        skip_draft = True

                if not skip_draft:
                    draft_id = self.gmail.create_reply_draft(email_record, action["draft_reply"])
                    if draft_id:
                        self.remember_drafted_thread(email_record.thread_id)
                        print(f"[DRAFT] Created Gmail draft {draft_id}")
                        self.review_queue.append(email_record, action, extra={"draft_id": draft_id})
                    elif self.dry_run:
                        self.review_queue.append(email_record, action, extra={"draft_previewed": True})

        if action.get("should_run_job"):
            self.maybe_run_requested_job(email_record, action)

    def apply_safety(self, email_record: EmailRecord, action: dict[str, Any]) -> dict[str, Any]:
        action = dict(action)
        confidence_floor = float(self.safety_cfg.get("human_review_confidence_below", 0.72))
        sender_email = email_record.sender_email
        allowed_requesters = {str(x).lower() for x in self.safety_cfg.get("allowed_requesters", [])}
        never_archive = {str(x).lower() for x in self.safety_cfg.get("never_auto_archive_from", [])}

        if action["confidence"] < confidence_floor:
            action["move_to"] = "needs_human"
            action["should_archive"] = False

        if sender_email in never_archive:
            action["should_archive"] = False

        if allowed_requesters and sender_email not in allowed_requesters:
            action["should_run_job"] = False

        if not self.safety_cfg.get("auto_run_reports", False):
            action["should_run_job"] = False

        if not self.safety_cfg.get("create_drafts", True):
            action["should_create_draft"] = False

        if not self.safety_cfg.get("archive_low_risk", False):
            action["should_archive"] = False

        return action

    def labels_for_action(self, action: dict[str, Any]) -> list[str]:
        labels = [self.gmail_cfg["processed_label"]]

        if action["confidence"] < float(self.safety_cfg.get("human_review_confidence_below", 0.72)):
            labels.append(self.gmail_cfg["low_confidence_label"])

        move_to = action.get("move_to")
        if move_to == "needs_human":
            labels.append(self.gmail_cfg["review_label"])
        elif move_to == "headset":
            labels.append(self.gmail_cfg["headset_label"])
        elif move_to == "report_requests":
            labels.append(self.gmail_cfg["report_label"])
        elif move_to == "ignore":
            labels.append(self.gmail_cfg["ignore_label"])

        return list(dict.fromkeys(labels))

    def should_create_draft(self, email_record: EmailRecord) -> bool:
        if not self.gmail_cfg.get("draft_replies", True):
            return False
        return email_record.thread_id not in set(self.state.get("drafted_threads", []))

    def remember_drafted_thread(self, thread_id: str) -> None:
        drafted = list(self.state.get("drafted_threads", []))
        if thread_id and thread_id not in drafted:
            drafted.append(thread_id)
            self.state["drafted_threads"] = drafted[-500:]
            save_state(self.state_path, self.state)

    def maybe_run_requested_job(self, email_record: EmailRecord, action: dict[str, Any]) -> None:
        job_key = action.get("job_key")
        if job_key in ("weekly_deals", "inventory", "aged_710_flower"):
            rc = self.runner.run_job(job_key)
            if rc != 0:
                print(f"[JOB] {job_key} failed with exit code {rc}; message kept labeled for review.")

    def run_due_jobs(self) -> None:
        schedules = self.cfg.get("jobs", {}).get("weekly_schedule", [])
        if not schedules:
            return

        tz = ZoneInfo(self.cfg.get("timezone", "America/Los_Angeles"))
        now = dt.datetime.now(tz)
        weekday = now.strftime("%A").lower()
        job_runs = self.state.setdefault("job_runs", {})

        for item in schedules:
            if not item.get("enabled", False):
                continue
            if str(item.get("weekday", "")).lower() != weekday:
                continue

            try:
                scheduled_time = dt.time.fromisoformat(str(item.get("time", "")))
            except ValueError:
                print(f"[JOB] Invalid schedule time for {item.get('name')}: {item.get('time')}")
                continue
            if now.time().replace(second=0, microsecond=0) < scheduled_time:
                continue

            run_key = f"{item.get('name')}:{now.date().isoformat()}"
            if job_runs.get(run_key):
                continue
            command_key = item.get("command_key")
            if command_key == "weekly_deals_command":
                job_key = "weekly_deals"
            elif command_key == "aged_710_flower_command":
                job_key = "aged_710_flower"
            else:
                job_key = "inventory"
            rc = self.runner.run_job(job_key)
            job_runs[run_key] = {"ran_at": now.isoformat(), "returncode": rc}
            save_state(self.state_path, self.state)

    def watch(self) -> None:
        poll_seconds = int(self.gmail_cfg.get("poll_seconds", 120))
        print(f"[WATCH] Email agent running. dry_run={self.dry_run}, poll_seconds={poll_seconds}")
        while True:
            try:
                self.process_once()
                self.run_due_jobs()
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[ERROR] Watch loop error: {exc}", file=sys.stderr)
            time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Buzz Gmail/OpenAI email agent.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to email agent config JSON.")
    parser.add_argument("--once", action="store_true", help="Process one scan and exit.")
    parser.add_argument("--watch", action="store_true", help="Run forever and poll Gmail.")
    parser.add_argument(
        "--run-job",
        choices=["weekly_deals", "inventory", "aged_710_flower"],
        help="Manually run a configured report job.",
    )
    parser.add_argument(
        "--show-review-queue",
        type=int,
        metavar="N",
        help="Print the last N proposed actions/drafts from the review queue.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode for this invocation.")
    parser.add_argument("--live", action="store_true", help="Force live mode for this invocation.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = resolved_path(args.config)
    cfg = load_config(config_path)
    load_env_file(cfg.get("openai", {}).get("env_file", ".env"))

    dry_run_override = None
    if args.dry_run and args.live:
        print("[ERROR] Use only one of --dry-run or --live.", file=sys.stderr)
        return 2
    if args.dry_run:
        dry_run_override = True
    if args.live:
        dry_run_override = False

    if args.show_review_queue is not None:
        return show_review_queue(cfg, args.show_review_queue)

    if args.run_job:
        dry_run = cfg.get("safety", {}).get("dry_run", True) if dry_run_override is None else dry_run_override
        return ReportRunner(cfg.get("jobs", {}), dry_run=dry_run).run_job(args.run_job)

    agent = EmailAgent(cfg, dry_run_override=dry_run_override)
    if args.watch:
        agent.watch()
        return 0
    if args.once or not args.watch:
        agent.process_once()
        agent.run_due_jobs()
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
