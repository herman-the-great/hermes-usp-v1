#!/usr/bin/env python3
"""
Generate USP one-pager images for each vertical.
Uses PIL — no external API calls, no internet required.
Output: 989×1280 portrait JPG, saved to /home/cortana/.hermes/image_cache/
"""
import json, os, textwrap
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.expanduser("~/.hermes/Hermes-USP-v1")
OUT_DIR = os.path.expanduser("~/.home/cortana/.hermes/image_cache")
OUT_DIR = "/home/cortana/.hermes/image_cache"
os.makedirs(OUT_DIR, exist_ok=True)

W, H = 989, 1280

VERTICALS = {
    "accounting_bookkeeping": {
        "name": "Accounting & Bookkeeping",
        "accent": "#d4af37",       # gold
        "accent_rgb": (212, 175, 55),
        "tagline": "Practical workflow systems\nfor accounting & bookkeeping firms",
        "pain_header": "Common Friction Points",
        "pains": [
            ("Document Chasing", "Clients send docs late, staff spend hours following up"),
            ("Inbox Overload", "Requests pile up in email — nothing gets systematized"),
            ("Admin Leakage", "Senior staff doing work that could be delegated"),
        ],
        "process_header": "How We Work",
        "process": [
            "Audit your current intake and workflow gaps",
            "Design a simple system that fits your practice",
            "Implement with your team — no disruption",
            "Hand off a working system you can run yourself",
        ],
        "offer": "Free workflow audit\n+ 30-day follow-up",
    },
    # home_services is EXCLUDED — one-pager is user-provided via upload
    # and must never be overwritten by this auto-generator script.
    "estate_planning_probate": {
        "name": "Estate Planning & Probate",
        "accent": "#a78bfa",       # purple
        "accent_rgb": (167, 139, 250),
        "tagline": "Practical workflow systems\nfor estate planning & probate law firms",
        "pain_header": "Common Friction Points",
        "pains": [
            ("Client Intake Friction", "Initial consultations stall before engagement is signed"),
            ("Document Collection", "Gathering client documents takes weeks of back-and-forth"),
            ("Probate Coordination", "Multiple touchpoints — executors, beneficiaries, attorneys"),
        ],
        "process_header": "How We Work",
        "process": [
            "Map your current engagement and document flow",
            "Design a client momentum system that keeps matters moving",
            "Implement without disrupting active cases",
            "Hand off a system your staff can own",
        ],
        "offer": "Free workflow audit\n+ 30-day follow-up",
    },
}

def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

def load_font(size, path=None):
    """Try to load a nice font, fall back to default."""
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
    return ImageFont.load_default()

def load_font_regular(size):
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
    return ImageFont.load_default()

def draw_rounded_rectangle(draw, xy, radius, fill, outline=None, width=1):
    x1, y1, x2, y2 = xy
    # Main rectangle
    draw.rectangle([x1+radius, y1, x2-radius, y2], fill=fill, outline=outline, width=width)
    draw.rectangle([x1, y1+radius, x2, y2-radius], fill=fill, outline=outline, width=width)
    # Corners
    draw.pieslice([x1, y1, x1+2*radius, y1+2*radius], 180, 270, fill=fill, outline=outline, width=width)
    draw.pieslice([x2-2*radius, y1, x2, y1+2*radius], 270, 360, fill=fill, outline=outline, width=width)
    draw.pieslice([x1, y2-2*radius, x1+2*radius, y2], 90, 180, fill=fill, outline=outline, width=width)
    draw.pieslice([x2-2*radius, y2-2*radius, x2, y2], 0, 90, fill=fill, outline=outline, width=width)
    if outline:
        # Top edge
        draw.line([(x1+radius, y1), (x2-radius, y1)], fill=outline, width=width)
        # Bottom edge
        draw.line([(x1+radius, y2), (x2-radius, y2)], fill=outline, width=width)
        # Left edge
        draw.line([(x1, y1+radius), (x1, y2-radius)], fill=outline, width=width)
        # Right edge
        draw.line([(x2, y1+radius), (x2, y2-radius)], fill=outline, width=width)

def generate_one_pager(config, output_path):
    accent = hex_to_rgb(config["accent"])
    bg_r, bg_g, bg_b = 16, 16, 32  # dark navy

    img = Image.new("RGB", (W, H), (bg_r, bg_g, bg_b))
    draw = ImageDraw.Draw(img)

    font_bold_large = load_font(42)
    font_bold_med   = load_font(30)
    font_bold_small = load_font(26)
    font_regular    = load_font_regular(22)
    font_small      = load_font_regular(18)
    font_tiny       = load_font_regular(15)

    margin = 60
    content_w = W - 2 * margin

    y = 60

    # ── Header accent bar ────────────────────────────────────────────────
    draw.rectangle([0, 0, W, 8], fill=accent)

    # ── USP logo / name ─────────────────────────────────────────────────
    y += 40
    draw.text((margin, y), "USP", font=font_bold_large, fill=accent)
    font_bbox = draw.textbbox((0, 0), "USP", font=font_bold_large)
    logo_w = font_bbox[2] - font_bbox[0]
    draw.text((margin + logo_w + 12, y + 6), "workflow improvement", font=font_bold_small, fill=(160, 160, 180))
    y += 70

    # ── Divider ─────────────────────────────────────────────────────────
    draw.rectangle([margin, y, W - margin, y + 2], fill=accent)
    y += 30

    # ── Vertical name ────────────────────────────────────────────────────
    draw.text((margin, y), config["name"].upper(), font=font_bold_med, fill=(240, 240, 250))
    y += 60

    # ── Tagline ──────────────────────────────────────────────────────────
    tagline_lines = config["tagline"].split("\n")
    for line in tagline_lines:
        draw.text((margin, y), line, font=font_regular, fill=(200, 200, 220))
        y += 36
    y += 20

    # ── Pain Points section ───────────────────────────────────────────────
    draw.text((margin, y), config["pain_header"], font=font_bold_small, fill=accent)
    y += 45

    for title, desc in config["pains"]:
        # Bullet circle
        cx = margin + 12
        cy = y + 10
        draw.ellipse([cx-6, cy-6, cx+6, cy+6], fill=accent)
        # Title
        draw.text((margin + 30, y), title, font=font_bold_small, fill=(240, 240, 250))
        y += 32
        # Desc — wrap at content_w - 30
        wrapped = textwrap.wrap(desc, width=50)
        for wl in wrapped:
            draw.text((margin + 30, y), wl, font=font_small, fill=(150, 150, 170))
            y += 26
        y += 12

    y += 10

    # ── Process section ─────────────────────────────────────────────────
    draw.text((margin, y), config["process_header"], font=font_bold_small, fill=accent)
    y += 45

    for i, step in enumerate(config["process"], 1):
        step_str = f"{i}.  {step}"
        wrapped = textwrap.wrap(step_str, width=48)
        for wl in wrapped:
            draw.text((margin, y), wl, font=font_small, fill=(200, 200, 220))
            y += 26
        y += 6

    y += 20

    # ── Offer box ────────────────────────────────────────────────────────
    box_padding = 24
    box_top = y
    box_bottom = y + 130
    box_color = tuple(min(255, max(0, c + 20)) for c in accent)  # lighter accent
    draw_rounded_rectangle(draw, [margin, box_top, W - margin, box_bottom],
                           radius=16, fill=(30, 30, 50), outline=accent, width=2)

    inner_x = margin + box_padding
    inner_y = box_top + 20

    draw.text((inner_x, inner_y), "OUR OFFER", font=font_small, fill=accent)
    inner_y += 30

    offer_lines = config["offer"].split("\n")
    for line in offer_lines:
        draw.text((inner_x, inner_y), line, font=font_bold_med, fill=(240, 240, 250))
        inner_y += 40

    y = box_bottom + 30

    # ── CTA ─────────────────────────────────────────────────────────────
    cta_text = "Reply to this email to schedule your free consultation."
    draw.text((margin, y), cta_text, font=font_regular, fill=(180, 180, 200))
    y += 45

    # ── Footer ──────────────────────────────────────────────────────────
    draw.rectangle([0, H - 60, W, H], fill=(10, 10, 20))
    draw.text((margin, H - 45), "uspai.io  |  No spam. No commitment. Just a conversation.", font=font_tiny, fill=(100, 100, 130))

    img.save(output_path, "JPEG", quality=88)
    size_kb = os.path.getsize(output_path) // 1024
    print(f"  Saved: {output_path} ({size_kb}KB, {W}x{H})")


for vert_key, config in VERTICALS.items():
    out_name = f"{vert_key}_one_pager.jpg"
    out_path = os.path.join(OUT_DIR, out_name)
    print(f"Generating: {vert_key}")
    generate_one_pager(config, out_path)

print("\nDone.")
