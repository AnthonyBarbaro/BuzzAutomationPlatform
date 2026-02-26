import os
import base64
import html
from io import BytesIO
from pathlib import Path
from email.message import EmailMessage
from datetime import date
from typing import List, Optional, Dict, Any, Tuple, Union

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# =============================================================================
# CONFIG (easy to tweak)
# =============================================================================

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
GMAIL_TOKEN = "token_gmail.json"

# Buzz theme
BUZZ = {
    "yellow": "#FFF200",
    "green": "#00AE6F",
    "black": "#000000",
    "white": "#FFFFFF",
    "bg": "#F3F4F6",
    "muted": "#374151",
    "muted2": "#6B7280",
    "border": "#E5E7EB",
    "soft": "#F7F7F7",
}

# -----------------------------------------------------------------------------
# Inline preview settings (PDF -> image)
# Legacy options kept for compatibility; current email flow uses HTML-only previews.
# -----------------------------------------------------------------------------
ENABLE_INLINE_PREVIEWS = False

# Cap how many PDFs we render previews for (PDFs are still attached regardless)
MAX_PREVIEWS = 12

# Render zoom; higher = sharper but larger
PREVIEW_ZOOM = 2.2

# Downscale if rendered image width is too large
PREVIEW_MAX_WIDTH_PX = 980

# ✅ Crop preview to focus on top KPIs
# Show the top X% of page 1 (0.50 = top half)
PREVIEW_CROP_TOP_FRACTION = 0.00          # usually 0.00
PREVIEW_CROP_HEIGHT_FRACTION = 0.55       # ✅ top 55% feels great for KPIs

# Prefer JPEG to keep email size down (requires Pillow for conversion)
PREFER_JPEG = True
JPEG_QUALITY = 72

# -----------------------------------------------------------------------------
# Store images/icons (optional)
# -----------------------------------------------------------------------------
ENABLE_STORE_ICONS = False

# Put store images here (recommended):
#   store_images/MV.jpg
#   store_images/LM.jpg
#   store_images/SV.jpg
#   store_images/LG.jpg
#   store_images/NC.jpg
#   store_images/WP.jpg
STORE_IMAGE_DIR = Path("store_images")

# Optional overrides if your filenames don't match abbr exactly
STORE_IMAGE_OVERRIDES: Dict[str, str] = {
    # "MV": "mission_viejo.jpg",
}

# Icon render size (px). Bigger looks more “premium”
STORE_ICON_SIZE_PX = 44

# -----------------------------------------------------------------------------
# “Color wave” header banner (generated as inline PNG) (requires Pillow)
# -----------------------------------------------------------------------------
ENABLE_WAVE_BANNER = False
WAVE_BANNER_WIDTH_PX = 760
WAVE_BANNER_HEIGHT_PX = 26

# -----------------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------------
# If you want “one store per row” (big readable KPI preview), set this to 1
# If you want a 2-column grid, set this to 2
CARDS_PER_ROW = 1

STORE_EMAIL_ORDER = ["MV", "LM", "SV", "LG", "NC", "WP"]
STORE_ORDER_INDEX = {abbr: i for i, abbr in enumerate(STORE_EMAIL_ORDER)}


# =============================================================================
# Helpers: parsing filenames
# =============================================================================

def _store_sort_key(abbr: Any) -> Tuple[int, str]:
    key = str(abbr or "").strip().upper()
    return (STORE_ORDER_INDEX.get(key, 999), key)

def _parse_pdf_identity(pdf_path: str) -> Dict[str, Any]:
    """
    Filenames:
      - "ALL STORES - Owner Snapshot - 2026-02-08.pdf"
      - "MV - Owner Snapshot - MISSION VIEJO - 2026-02-08.pdf"
    """
    p = Path(pdf_path)
    stem = p.stem
    parts = stem.split(" - ")

    if parts and parts[0].strip().upper() == "ALL STORES":
        return {
            "is_all": True,
            "abbr": "ALL",
            "store_name": "All Stores",
            "display_title": "All Stores Summary",
            "sort_key": (0, "ALL"),
        }

    abbr = (parts[0].strip() if parts else "STORE").upper()
    store_name = parts[2].strip() if len(parts) >= 3 else abbr
    display_title = f"{abbr} • {store_name.title()}"

    return {
        "is_all": False,
        "abbr": abbr,
        "store_name": store_name,
        "display_title": display_title,
        "sort_key": (1, *_store_sort_key(abbr)),
    }


def _chunk(items: List[Any], n: int) -> List[List[Any]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def _human_file_size(num_bytes: int) -> str:
    if num_bytes is None or num_bytes < 0:
        return "0 B"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    kb = num_bytes / 1024.0
    if kb < 1024:
        return f"{kb:.1f} KB"
    mb = kb / 1024.0
    return f"{mb:.2f} MB"


def _fmt_money(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "$0"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:,.1f}%"
    except Exception:
        return "0.0%"


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except Exception:
        return "0"


# =============================================================================
# PDF preview rendering (top-of-page crop)
# =============================================================================

def _try_render_pdf_first_page(pdf_path: str) -> Optional[bytes]:
    """
    Render first page (cropped to top section) to PNG bytes using PyMuPDF (fitz).
    Returns None if rendering isn't possible.

    Crop behavior is controlled by:
      PREVIEW_CROP_TOP_FRACTION
      PREVIEW_CROP_HEIGHT_FRACTION
    """
    if not ENABLE_INLINE_PREVIEWS:
        return None

    try:
        import fitz  # PyMuPDF
    except Exception:
        return None

    doc = None
    try:
        doc = fitz.open(pdf_path)
        if doc.page_count < 1:
            return None

        page = doc.load_page(0)
        rect = page.rect  # (x0, y0, x1, y1)

        # ✅ Clip to top portion for KPI readability
        clip = None
        if PREVIEW_CROP_HEIGHT_FRACTION and PREVIEW_CROP_HEIGHT_FRACTION < 0.999:
            y0 = rect.y0 + rect.height * float(PREVIEW_CROP_TOP_FRACTION)
            y1 = y0 + rect.height * float(PREVIEW_CROP_HEIGHT_FRACTION)
            clip = fitz.Rect(rect.x0, y0, rect.x1, y1)

        # First render
        mat = fitz.Matrix(PREVIEW_ZOOM, PREVIEW_ZOOM)
        pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)

        # Downscale by re-rendering with a smaller zoom (better than resampling)
        if PREVIEW_MAX_WIDTH_PX and pix.width > PREVIEW_MAX_WIDTH_PX:
            scale = PREVIEW_MAX_WIDTH_PX / float(pix.width)
            mat2 = fitz.Matrix(PREVIEW_ZOOM * scale, PREVIEW_ZOOM * scale)
            pix = page.get_pixmap(matrix=mat2, clip=clip, alpha=False)

        return pix.tobytes("png")
    except Exception:
        return None
    finally:
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass


def _maybe_convert_png_to_jpeg(png_bytes: bytes) -> Tuple[bytes, str]:
    """
    Convert PNG -> JPEG to reduce size (if Pillow is available).
    Returns (bytes, subtype) where subtype is 'jpeg' or 'png'.
    """
    if not PREFER_JPEG:
        return png_bytes, "png"

    try:
        from PIL import Image
    except Exception:
        return png_bytes, "png"

    try:
        img = Image.open(BytesIO(png_bytes)).convert("RGB")
        out = BytesIO()
        img.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        return out.getvalue(), "jpeg"
    except Exception:
        return png_bytes, "png"


# =============================================================================
# Store icon loading (optional)
# =============================================================================

def _try_load_store_icon_bytes(abbr: str) -> Optional[Tuple[bytes, str]]:
    """
    Loads a store image from STORE_IMAGE_DIR and returns (bytes, subtype).
    If Pillow is installed, it will:
      - resize to STORE_ICON_SIZE_PX
      - crop to a circle
      - output PNG (best for icons)
    """
    if not ENABLE_STORE_ICONS:
        return None

    abbr = (abbr or "").strip().upper()
    if not abbr or abbr == "ALL":
        return None

    STORE_IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    candidates: List[Path] = []
    override = STORE_IMAGE_OVERRIDES.get(abbr)
    if override:
        candidates.append(STORE_IMAGE_DIR / override)
    else:
        for ext in (".png", ".jpg", ".jpeg", ".webp"):
            candidates.append(STORE_IMAGE_DIR / f"{abbr}{ext}")
            candidates.append(STORE_IMAGE_DIR / f"{abbr.lower()}{ext}")

    img_path = next((p for p in candidates if p.exists()), None)
    if not img_path:
        return None

    raw = img_path.read_bytes()

    # Best: resize + circle mask via Pillow
    try:
        from PIL import Image, ImageDraw

        im = Image.open(BytesIO(raw)).convert("RGBA")
        im = im.resize((STORE_ICON_SIZE_PX, STORE_ICON_SIZE_PX), Image.LANCZOS)

        mask = Image.new("L", (STORE_ICON_SIZE_PX, STORE_ICON_SIZE_PX), 0)
        draw = ImageDraw.Draw(mask)
        draw.ellipse((0, 0, STORE_ICON_SIZE_PX - 1, STORE_ICON_SIZE_PX - 1), fill=255)

        out = Image.new("RGBA", (STORE_ICON_SIZE_PX, STORE_ICON_SIZE_PX), (0, 0, 0, 0))
        out.paste(im, (0, 0), mask=mask)

        buf = BytesIO()
        out.save(buf, format="PNG", optimize=True)
        return buf.getvalue(), "png"
    except Exception:
        # Fallback: embed original bytes (may be larger)
        ext = img_path.suffix.lower().lstrip(".")
        if ext == "jpg":
            ext = "jpeg"
        if ext not in ("png", "jpeg", "webp", "gif"):
            ext = "png"
        return raw, ext


# =============================================================================
# Wave banner (optional)
# =============================================================================

def _try_make_wave_banner_png(width_px: int, height_px: int) -> Optional[bytes]:
    """
    Creates a small “color wave” banner as PNG bytes using Pillow.
    If Pillow isn't installed, returns None.
    """
    if not ENABLE_WAVE_BANNER:
        return None

    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    import math

    img = Image.new("RGB", (width_px, height_px), BUZZ["white"])
    draw = ImageDraw.Draw(img)

    # Wave parameters
    amp1 = max(2, int(height_px * 0.18))
    amp2 = max(2, int(height_px * 0.14))
    base1 = int(height_px * 0.45)
    base2 = int(height_px * 0.62)
    cycles = 1.30  # number of sine cycles across width

    # Yellow wave (upper)
    pts_yellow = []
    for x in range(width_px + 1):
        y = base1 + int(amp1 * math.sin((2 * math.pi * cycles * x) / width_px))
        pts_yellow.append((x, y))
    poly_yellow = [(0, height_px)] + pts_yellow + [(width_px, height_px)]
    draw.polygon(poly_yellow, fill=BUZZ["yellow"])

    # Green wave (lower, phase shifted)
    pts_green = []
    for x in range(width_px + 1):
        y = base2 + int(amp2 * math.sin((2 * math.pi * cycles * x) / width_px + 1.4))
        pts_green.append((x, y))
    poly_green = [(0, height_px)] + pts_green + [(width_px, height_px)]
    draw.polygon(poly_green, fill=BUZZ["green"])

    # Thin top border line for crispness
    draw.line([(0, 0), (width_px, 0)], fill=BUZZ["border"], width=1)

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# =============================================================================
# HTML builder
# =============================================================================

def _build_plain_text_email(
    report_day: date,
    data_start: date,
    data_end: date,
    executive_summary: Optional[Dict[str, Any]] = None,
    store_summaries: Optional[List[Dict[str, Any]]] = None,
) -> str:
    lines = [
        "Buzz Cannabis — Owner Snapshot",
        "",
        f"Report Day: {report_day.strftime('%A, %B %d, %Y')} ({report_day.isoformat()})",
        f"Data Window: {data_start.isoformat()} → {data_end.isoformat()}",
        "",
    ]
    if executive_summary:
        lines += [
            "Executive Snapshot:",
            f"- Today Net: {_fmt_money(executive_summary.get('today_net'))}",
            f"- Today Tickets: {_fmt_int(executive_summary.get('today_tickets'))}",
            f"- MTD Net: {_fmt_money(executive_summary.get('mtd_net'))}",
            f"- MTD Tickets: {_fmt_int(executive_summary.get('mtd_tickets'))}",
            f"- Projected Month Net: {_fmt_money(executive_summary.get('proj_month_net'))}",
            f"- Projected Month Profit: {_fmt_money(executive_summary.get('proj_month_profit'))}",
            f"- Projected Margin: {_fmt_pct(executive_summary.get('proj_margin'))}",
            f"- Remaining Days: {_fmt_int(executive_summary.get('remaining_days'))}",
            "",
        ]
    if store_summaries:
        lines += [
            "Store KPI Preview:",
        ]
        for s in sorted(store_summaries, key=lambda x: _store_sort_key(x.get("abbr", ""))):
            lines.append(f"- {s.get('abbr','')}")
            lines.append(
                f"  Today: Net {_fmt_money(s.get('today_net'))} | "
                f"Tix {_fmt_int(s.get('today_tickets'))} | "
                f"Avg {_fmt_money(s.get('today_basket'))}"
            )
            lines.append(
                f"  MTD: Net {_fmt_money(s.get('mtd_net'))} | "
                f"Tix {_fmt_int(s.get('mtd_tickets'))} | "
                f"Avg {_fmt_money(s.get('mtd_basket'))} | "
                f"Margin {_fmt_pct(s.get('mtd_margin'))} | "
                f"Proj {_fmt_money(s.get('proj_month_net'))}"
            )
    lines += ["", "This email was generated automatically."]
    return "\n".join(lines)


def _build_html_email(
    report_day: date,
    data_start: date,
    data_end: date,
    executive_summary: Optional[Dict[str, Any]] = None,
    store_summaries: Optional[List[Dict[str, Any]]] = None,
) -> str:
    header_date = report_day.strftime("%A, %B %d, %Y")

    def _esc(text: Any) -> str:
        return html.escape(str(text or ""))

    perf_block = ""
    if executive_summary:
        perf_cells: List[str] = []

        def _add_perf_cell(label: str, value: str) -> None:
            perf_cells.append(
                f"<td width=\"50%\" style=\"padding:10px;border:1px solid {BUZZ['border']};background:#FFFFFF;\">"
                f"<div style=\"font-size:11px;color:{BUZZ['muted2']};font-weight:700;\">{_esc(label)}</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(value)}</div>"
                f"</td>"
            )

        _add_perf_cell("Today Net", _fmt_money(executive_summary.get("today_net")))
        _add_perf_cell("Today Tickets", _fmt_int(executive_summary.get("today_tickets")))
        _add_perf_cell("Today Avg Ticket", _fmt_money(executive_summary.get("today_basket")))
        _add_perf_cell("Today Discount Rate", _fmt_pct(executive_summary.get("today_discount_rate")))
        _add_perf_cell("MTD Net", _fmt_money(executive_summary.get("mtd_net")))
        _add_perf_cell("MTD Tickets", _fmt_int(executive_summary.get("mtd_tickets")))
        _add_perf_cell("MTD Avg Ticket", _fmt_money(executive_summary.get("mtd_basket")))
        _add_perf_cell("MTD Margin", _fmt_pct(executive_summary.get("mtd_margin")))
        _add_perf_cell("Projected Month Net", _fmt_money(executive_summary.get("proj_month_net")))
        _add_perf_cell("Projected Month Profit", _fmt_money(executive_summary.get("proj_month_profit")))
        _add_perf_cell("Projected Margin", _fmt_pct(executive_summary.get("proj_margin")))
        _add_perf_cell("Remaining Days", _fmt_int(executive_summary.get("remaining_days")))

        perf_rows = ""
        for i in range(0, len(perf_cells), 2):
            row_cells = perf_cells[i:i + 2]
            if len(row_cells) < 2:
                row_cells.append(
                    f"<td width=\"50%\" style=\"padding:10px;border:1px solid {BUZZ['border']};background:#FFFFFF;\"></td>"
                )
            perf_rows += f"<tr>{''.join(row_cells)}</tr>"

        perf_block = (
            f"<tr><td style=\"padding:0 20px 6px 20px;\">"
            f"<div style=\"font-size:14px;font-weight:900;color:#111827;\">Performance Snapshot</div>"
            f"<div style=\"margin-top:3px;font-size:12px;color:{BUZZ['muted2']};\">All-store day, MTD, avg-ticket, and month-end projection highlights.</div>"
            f"</td></tr>"
            f"<tr><td style=\"padding:8px 20px 12px 20px;\">"
            f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;\">{perf_rows}</table>"
            f"</td></tr>"
        )

    store_kpi_block = ""
    if store_summaries:
        cards = ""
        sorted_stores = sorted(store_summaries, key=lambda s: _store_sort_key(s.get("abbr", "")))
        for idx, s in enumerate(sorted_stores):
            card_bg = "#FFFFFF" if idx % 2 == 0 else BUZZ["soft"]
            cards += (
                f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" "
                f"style=\"margin:0 0 10px 0;border:1px solid {BUZZ['border']};border-radius:10px;overflow:hidden;"
                f"border-collapse:separate;border-spacing:0;background:{card_bg};\">"
                f"<tr><td colspan=\"2\" style=\"padding:8px 10px;border-bottom:1px solid {BUZZ['border']};\">"
                f"<span style=\"display:inline-block;padding:3px 9px;border-radius:999px;background:{BUZZ['green']};"
                f"color:#FFFFFF;font-size:11px;font-weight:900;letter-spacing:0.3px;\">{_esc(s.get('abbr',''))}</span>"
                f"</td></tr>"
                f"<tr>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-right:1px solid {BUZZ['border']};border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">Today Net</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_money(s.get('today_net')))}</div>"
                f"</td>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">Today Tickets</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_int(s.get('today_tickets')))}</div>"
                f"</td>"
                f"</tr>"
                f"<tr>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-right:1px solid {BUZZ['border']};border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">Today Avg Ticket</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_money(s.get('today_basket')))}</div>"
                f"</td>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">MTD Net</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_money(s.get('mtd_net')))}</div>"
                f"</td>"
                f"</tr>"
                f"<tr>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-right:1px solid {BUZZ['border']};border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">MTD Tickets</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_int(s.get('mtd_tickets')))}</div>"
                f"</td>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-bottom:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">MTD Avg Ticket</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_money(s.get('mtd_basket')))}</div>"
                f"</td>"
                f"</tr>"
                f"<tr>"
                f"<td width=\"50%\" style=\"padding:9px 10px;border-right:1px solid {BUZZ['border']};\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">MTD Margin</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_pct(s.get('mtd_margin')))}</div>"
                f"</td>"
                f"<td width=\"50%\" style=\"padding:9px 10px;\">"
                f"<div style=\"font-size:10px;color:{BUZZ['muted2']};font-weight:700;\">Projected Month Net</div>"
                f"<div style=\"margin-top:3px;font-size:13px;color:#111827;font-weight:900;\">{_esc(_fmt_money(s.get('proj_month_net')))}</div>"
                f"</td>"
                f"</tr>"
                f"</table>"
            )

        store_kpi_block = (
            f"<tr><td style=\"padding:0 20px 6px 20px;\">"
            f"<div style=\"font-size:14px;font-weight:900;color:#111827;\">Store KPI Preview</div>"
            f"<div style=\"margin-top:3px;font-size:12px;color:{BUZZ['muted2']};\">Mobile-friendly KPI cards by store.</div>"
            f"</td></tr>"
            f"<tr><td style=\"padding:8px 20px 14px 20px;\">"
            f"{cards}</td></tr>"
        )

    html_body = f"""
    <div style="margin:0;padding:0;background:{BUZZ['bg']};">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:{BUZZ['bg']};padding:20px 0;">
        <tr>
          <td align="center" style="padding:0 10px;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
                   style="max-width:780px;background:{BUZZ['white']};border:1px solid {BUZZ['border']};border-radius:14px;overflow:hidden;">
              <tr>
                <td style="background:{BUZZ['black']};padding:18px 20px;">
                  <div style="color:#FFFFFF;font-size:19px;font-weight:900;letter-spacing:0.5px;">BUZZ CANNABIS</div>
                  <div style="color:#E5E7EB;font-size:13px;margin-top:4px;">Owner Snapshot • {_esc(header_date)}</div>
                </td>
              </tr>
              <tr>
                <td style="height:5px;background:linear-gradient(90deg,{BUZZ['yellow']} 0%,{BUZZ['green']} 55%,{BUZZ['yellow']} 100%);"></td>
              </tr>
              {perf_block}
              {store_kpi_block}

              <tr>
                <td style="padding:12px 20px;background:#111827;color:#9CA3AF;font-size:11px;line-height:1.5;">
                  Auto-generated by Buzz Automation • Reply to this email if any value looks off.
                  <span style="color:#FFFFFF;font-weight:800;"> #Buzz</span>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </div>
    """
    return html_body


# =============================================================================
# Main sender
# =============================================================================

def send_owner_snapshot_email(
    pdf_paths: List[str],
    report_day: date,
    data_start: date,
    data_end: date,
    to_email: Union[str, List[str]] = "anthony@buzzcannabis.com",
    executive_summary: Optional[Dict[str, Any]] = None,
    store_summaries: Optional[List[Dict[str, Any]]] = None,
):
    """
    Sends Owner Snapshot PDFs via Gmail API using JSON token (cron-safe),
    with a lightweight executive HTML body and PDF attachments.

    Notes:
      - No image previews are embedded (faster load, smaller email payload)
      - HTML body is KPI-focused for quick executive scanning
      - Full details remain in attached PDFs
    """

    if not os.path.exists(GMAIL_TOKEN):
        raise RuntimeError("❌ token_gmail.json not found — run Gmail auth first")

    creds = Credentials.from_authorized_user_file(GMAIL_TOKEN, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(GMAIL_TOKEN, "w") as f:
            f.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)

    # ---------- Sort + identify PDFs ----------
    existing_pdfs = [p for p in pdf_paths if p and os.path.exists(p)]
    pdf_identities = [_parse_pdf_identity(p) for p in existing_pdfs]
    pdf_sorted = sorted(zip(existing_pdfs, pdf_identities), key=lambda x: x[1]["sort_key"])
    sorted_pdfs: List[str] = []
    total_size_bytes = 0
    for pdf_path, _ in pdf_sorted:
        try:
            size_bytes = int(os.path.getsize(pdf_path))
        except Exception:
            size_bytes = 0
        sorted_pdfs.append(pdf_path)
        total_size_bytes += size_bytes

    # ---------- Email ----------
    subject = f"Buzz Cannabis Owner Snapshot — {report_day.isoformat()}"
    to_header = ", ".join(to_email) if isinstance(to_email, list) else to_email

    msg = EmailMessage()
    msg["To"] = to_header
    msg["From"] = "me"
    msg["Subject"] = subject

    # Plain text fallback
    msg.set_content(_build_plain_text_email(
        report_day,
        data_start,
        data_end,
        executive_summary,
        store_summaries,
    ))

    # HTML body
    html_body = _build_html_email(
        report_day=report_day,
        data_start=data_start,
        data_end=data_end,
        executive_summary=executive_summary,
        store_summaries=store_summaries,
    )
    msg.add_alternative(html_body, subtype="html")

    # Attach PDFs
    for path in sorted_pdfs:
        with open(path, "rb") as f:
            data = f.read()

        msg.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=os.path.basename(path),
        )

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    service.users().messages().send(
        userId="me",
        body={"raw": raw},
    ).execute()

    print(
        f"📧 Owner Snapshot emailed to {to_email} "
        f"(PDFs: {len(existing_pdfs)}, Total size: {_human_file_size(total_size_bytes)}, Inline previews: disabled)"
    )
