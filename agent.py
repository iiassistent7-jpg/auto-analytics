import os
import json
import time
import tempfile
import threading
import schedule
import telebot
import anthropic
import gspread
import requests
from datetime import datetime, timedelta, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ============================================================
# CONFIGURATION
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
MY_CHAT_ID = int(os.environ.get("MY_CHAT_ID", "0"))
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

# Google Sheets IDs
MAYA_LEADS_SHEET = os.environ.get("MAYA_LEADS_SHEET", "1ZM-rN0dlUdeQhgFltodlPW9KIuvtdsIb2QkcjzZtvKI")
CARCITY_LEADS_SHEET = os.environ.get("CARCITY_LEADS_SHEET", "1Y2X_nDDnyfQiadHO-B07IhaqZPCHVr7XU583_gaNlYM")
SALES_SHEET = os.environ.get("SALES_SHEET", "12GZ6qg_t9lPlwp0p4dN6-JQ281VGa_nsDzN_NwRD1TE")

# Google Calendar
CALENDAR_ID = os.environ.get("CALENDAR_ID", "6adb497d70d6f51fb1bfee8d5fda6661b9c61f79d88069ac4b0b843f2f9f4358@group.calendar.google.com")

ISRAEL_UTC_OFFSET = 2

bot = telebot.TeleBot(TELEGRAM_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ============================================================
# GOOGLE AUTH
# ============================================================
def get_google_creds():
    """Get Google credentials from env var or file."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/calendar.readonly"
    ]
    json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if json_str:
        info = json.loads(json_str)
        return Credentials.from_service_account_info(info, scopes=scopes)
    json_path = os.environ.get("GOOGLE_SA_KEY_PATH", "service_account.json")
    if os.path.exists(json_path):
        return Credentials.from_service_account_file(json_path, scopes=scopes)
    raise Exception("No Google credentials found. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SA_KEY_PATH")

# ============================================================
# HELPERS
# ============================================================
def get_israel_now():
    return datetime.now(timezone.utc) + timedelta(hours=ISRAEL_UTC_OFFSET)

def parse_date(date_str):
    """Parse various date formats from sheets."""
    if not date_str:
        return None
    date_str = str(date_str).strip()
    formats = [
        "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
        "%d.%m.%y", "%d/%m/%Y", "%d/%m/%y",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def safe_send(chat_id, text, max_len=4000):
    if not text:
        text = "Данные обрабатываются..."
    if len(text) <= max_len:
        try:
            bot.send_message(chat_id, text)
        except Exception as e:
            print(f"Send error: {e}")
        return
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        split_at = text.rfind("\n\n", 0, max_len)
        if split_at == -1:
            split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        parts.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    for part in parts:
        try:
            bot.send_message(chat_id, part)
            time.sleep(0.3)
        except Exception as e:
            print(f"Send error: {e}")

# ============================================================
# DATA: GOOGLE SHEETS
# ============================================================
def read_maya_leads(since=None, until=None):
    """Read MayaCars leads sheet."""
    try:
        creds = get_google_creds()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(MAYA_LEADS_SHEET)
        ws = sh.get_worksheet(0)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []
        headers = rows[0]
        leads = []
        for row in rows[1:]:
            if len(row) < 3:
                continue
            date_str = row[0] if len(row) > 0 else ""
            name = row[1] if len(row) > 1 else ""
            phone = row[2] if len(row) > 2 else ""
            source = row[3] if len(row) > 3 else ""
            comment = row[-1] if row[-1] else ""
            dt = parse_date(date_str)
            if dt and since and dt < since:
                continue
            if dt and until and dt > until:
                continue
            is_lead_form = "Лид" in source or "лид" in source or "#" in source
            leads.append({
                "date": dt,
                "name": name,
                "phone": phone,
                "source": "MayaCars",
                "type": "lead_form" if is_lead_form else "message",
                "comment": comment
            })
        return leads
    except Exception as e:
        print(f"Error reading MayaLeads: {e}")
        return []

def read_carcity_leads(since=None, until=None):
    """Read CarCity/AutoMotors leads sheet."""
    try:
        creds = get_google_creds()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(CARCITY_LEADS_SHEET)
        ws = sh.get_worksheet(0)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []
        leads = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            page = row[0].strip() if len(row) > 0 else ""
            date_str = row[1].strip() if len(row) > 1 else ""
            time_str = row[2].strip() if len(row) > 2 else ""
            name = row[3].strip() if len(row) > 3 else ""
            phone = row[4].strip() if len(row) > 4 else ""
            placement = row[5].strip() if len(row) > 5 else ""
            city = row[6].strip() if len(row) > 6 else ""
            comment = row[7].strip() if len(row) > 7 else ""
            full_date = f"{date_str} {time_str}".strip()
            dt = parse_date(full_date) or parse_date(date_str)
            if dt and since and dt < since:
                continue
            if dt and until and dt > until:
                continue
            # Normalize source
            page_lower = page.lower()
            if "auto" in page_lower:
                src = "AutoMotors"
            elif "car city" in page_lower or "carcity" in page_lower:
                src = "CarCity"
            elif "сам обратился" in page_lower:
                src = "Organic"
            elif "marketplace" in page_lower:
                src = "Marketplace"
            elif "tik" in page_lower:
                src = "TikTok"
            else:
                src = page if page else "Unknown"
            # Determine type
            is_lead_form = bool(page) and "сам" not in page_lower and "marketplace" not in page_lower
            leads.append({
                "date": dt,
                "name": name,
                "phone": phone,
                "source": src,
                "type": "lead_form" if is_lead_form else "message",
                "placement": placement,
                "city": city,
                "comment": comment
            })
        return leads
    except Exception as e:
        print(f"Error reading CarCity leads: {e}")
        return []

def read_sales(since=None, until=None):
    """Read sales table."""
    try:
        creds = get_google_creds()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SALES_SHEET)
        ws = sh.get_worksheet(0)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []
        sales = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            no = row[0].strip() if len(row) > 0 else ""
            lid = row[1].strip() if len(row) > 1 else ""
            name = row[2].strip() if len(row) > 2 else ""
            phone = row[3].strip() if len(row) > 3 else ""
            city = row[4].strip() if len(row) > 4 else ""
            notes = row[5].strip() if len(row) > 5 else ""
            status = row[6].strip() if len(row) > 6 else ""
            if not status or "УЕХАЛ" not in status.upper():
                continue
            # Normalize source
            lid_upper = lid.upper()
            if "CAR CITY" in lid_upper:
                src = "CarCity"
            elif "AUTO" in lid_upper:
                src = "AutoMotors"
            elif "MAYA" in lid_upper or "ARTEM" in lid_upper:
                src = "MayaCars"
            elif "TIK" in lid_upper:
                src = "TikTok"
            elif "КЛИЕНТ" in lid_upper or "ДРУГ" in lid_upper:
                src = "Organic"
            else:
                src = lid if lid else "Unknown"
            # Extract car info from notes
            car_info = notes.split(")")[0] + ")" if ")" in notes else notes[:50]
            sales.append({
                "name": name,
                "phone": phone,
                "city": city,
                "source": src,
                "car": car_info,
                "notes": notes
            })
        return sales
    except Exception as e:
        print(f"Error reading sales: {e}")
        return []

# ============================================================
# DATA: GOOGLE CALENDAR
# ============================================================
def read_calendar_meetings(since=None, until=None):
    """Read meetings from Google Calendar.
    30 min = cancelled/no-show, 60 min = completed."""
    try:
        creds = get_google_creds()
        service = build("calendar", "v3", credentials=creds)
        if not since:
            since = get_israel_now() - timedelta(days=30)
        if not until:
            until = get_israel_now()
        time_min = since.strftime("%Y-%m-%dT00:00:00+02:00")
        time_max = until.strftime("%Y-%m-%dT23:59:59+02:00")
        events = []
        page_token = None
        while True:
            result = service.events().list(
                calendarId=CALENDAR_ID,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=500,
                pageToken=page_token
            ).execute()
            events.extend(result.get("items", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                break
        meetings = []
        for ev in events:
            start = ev.get("start", {})
            end = ev.get("end", {})
            start_dt = start.get("dateTime", start.get("date", ""))
            end_dt = end.get("dateTime", end.get("date", ""))
            summary = ev.get("summary", "")
            if not start_dt or not end_dt:
                continue
            try:
                from dateutil import parser as dtparser
                s = dtparser.parse(start_dt)
                e = dtparser.parse(end_dt)
                duration_min = (e - s).total_seconds() / 60
            except:
                duration_min = 0
            # 30 min = no-show, 60 min = completed
            if duration_min <= 0:
                continue
            status = "completed" if duration_min >= 50 else "no_show"
            meetings.append({
                "date": start_dt,
                "summary": summary,
                "duration_min": duration_min,
                "status": status
            })
        return meetings
    except Exception as e:
        print(f"Error reading calendar: {e}")
        return []

# ============================================================
# ANALYTICS
# ============================================================
def full_analytics(since=None, until=None):
    """Gather all data and compute analytics."""
    now = get_israel_now()
    if not since:
        since = now - timedelta(days=30)
    if not until:
        until = now
    if isinstance(since, str):
        since = parse_date(since) or (now - timedelta(days=30))
    if isinstance(until, str):
        until = parse_date(until) or now
    # Make dates timezone-naive for comparison
    if hasattr(since, 'tzinfo') and since.tzinfo:
        since = since.replace(tzinfo=None)
    if hasattr(until, 'tzinfo') and until.tzinfo:
        until = until.replace(tzinfo=None)

    # Read all data
    maya_leads = read_maya_leads(since, until)
    cc_leads = read_carcity_leads(since, until)
    all_leads = maya_leads + cc_leads
    sales = read_sales()
    meetings = read_calendar_meetings(since, until)

    # Leads by source
    leads_by_source = {}
    for lead in all_leads:
        src = lead["source"]
        if src not in leads_by_source:
            leads_by_source[src] = {"total": 0, "lead_form": 0, "message": 0}
        leads_by_source[src]["total"] += 1
        leads_by_source[src][lead["type"]] += 1

    # Sales by source
    sales_by_source = {}
    for sale in sales:
        src = sale["source"]
        if src not in sales_by_source:
            sales_by_source[src] = 0
        sales_by_source[src] += 1

    # Meetings stats
    total_meetings = len(meetings)
    completed_meetings = sum(1 for m in meetings if m["status"] == "completed")
    no_show_meetings = sum(1 for m in meetings if m["status"] == "no_show")
    no_show_rate = round(no_show_meetings / total_meetings * 100, 1) if total_meetings > 0 else 0

    # Funnel
    total_leads = len(all_leads)
    total_sales = len(sales)

    # Lead form vs Message comparison
    lead_form_count = sum(1 for l in all_leads if l["type"] == "lead_form")
    message_count = sum(1 for l in all_leads if l["type"] == "message")

    # Match leads to sales by phone
    lead_phones = set()
    for l in all_leads:
        p = l.get("phone", "").replace("-", "").replace(" ", "").strip()
        if p:
            lead_phones.add(p[-7:])  # last 7 digits for matching

    sales_from_leads = 0
    sales_from_other = 0
    for s in sales:
        p = s.get("phone", "").replace("-", "").replace(" ", "").strip()
        if p and p[-7:] in lead_phones:
            sales_from_leads += 1
        else:
            sales_from_other += 1

    # Conversion rates
    lead_to_meeting = round(total_meetings / total_leads * 100, 1) if total_leads > 0 else 0
    meeting_to_sale = round(total_sales / completed_meetings * 100, 1) if completed_meetings > 0 else 0
    lead_to_sale = round(total_sales / total_leads * 100, 1) if total_leads > 0 else 0

    data = {
        "period": {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")},
        "leads": {
            "total": total_leads,
            "maya_cars": len(maya_leads),
            "car_city_auto_motors": len(cc_leads),
            "by_source": leads_by_source,
            "lead_form": lead_form_count,
            "message": message_count
        },
        "meetings": {
            "total": total_meetings,
            "completed": completed_meetings,
            "no_show": no_show_meetings,
            "no_show_rate": no_show_rate
        },
        "sales": {
            "total": total_sales,
            "by_source": sales_by_source,
            "from_leads": sales_from_leads,
            "from_other": sales_from_other
        },
        "funnel": {
            "leads": total_leads,
            "meetings": total_meetings,
            "completed_meetings": completed_meetings,
            "sales": total_sales,
            "lead_to_meeting": lead_to_meeting,
            "meeting_to_sale": meeting_to_sale,
            "lead_to_sale": lead_to_sale,
            "no_show_rate": no_show_rate
        },
        "comparison": {
            "lead_form_count": lead_form_count,
            "message_count": message_count,
            "lead_form_pct": round(lead_form_count / total_leads * 100, 1) if total_leads > 0 else 0,
            "message_pct": round(message_count / total_leads * 100, 1) if total_leads > 0 else 0
        }
    }
    return data

# ============================================================
# CLAUDE API
# ============================================================
def call_claude(system_prompt, user_content, max_tokens=4000, retries=3):
    for attempt in range(retries):
        try:
            response = claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_content}]
            )
            return response.content[0].text
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < retries - 1:
                time.sleep((attempt + 1) * 10)
                continue
            print(f"Claude error: {e.status_code}")
            return None
        except Exception as e:
            print(f"Claude exception: {e}")
            return None

# ============================================================
# REPORT GENERATION
# ============================================================
ANALYST_PROMPT = """Ты — аналитик автомобильного бизнеса. Анализируешь данные продаж б/у автомобилей в Израиле.

Бизнес: 3 страницы (MayaCars в Instagram, Car City и Auto Motors в Facebook). Два кабинета в Meta Ads.
Воронка: Лид (форма/переписка) → Звонок → Встреча (Google Calendar) → Продажа.
В календаре: 30 мин = клиент не дошёл, 60 мин = встреча состоялась.

АБСОЛЮТНЫЙ ЗАПРЕТ НА ФОРМАТИРОВАНИЕ:
Не используй звёздочки (*), двойные звёздочки (**), подчёркивания (_), решётки (#), обратные кавычки (`) или любую Markdown-разметку. ТОЛЬКО чистый текст и эмодзи.

СТИЛЬ:
- Конкретные цифры, проценты, сравнения
- Рекомендации с конкретными действиями
- Русский язык
- 3-6 эмодзи к месту
- Обращайся к Михаилу на "ты"
"""

def generate_report(data, report_type="full"):
    """Generate analytical report using Claude."""
    prompt = f"Вот данные аналитики автобизнеса:\n\n{json.dumps(data, ensure_ascii=False, indent=2, default=str)}\n\n"

    if report_type == "full":
        prompt += """Сделай полный отчёт:
1. Общая картина — сколько лидов, встреч, продаж
2. Конверсия на каждом этапе воронки
3. Сравнение источников (MayaCars vs CarCity vs AutoMotors vs Organic vs Marketplace vs TikTok)
4. Процент недошедших на встречу
5. Сравнение лид-формы vs переписки — что эффективнее
6. Конкретные рекомендации (3-5 штук)"""
    elif report_type == "funnel":
        prompt += "Проанализируй воронку продаж. Где теряются клиенты? Какой этап слабый?"
    elif report_type == "sources":
        prompt += "Сравни все источники лидов. Какой самый эффективный? Где стоит увеличить бюджет?"
    elif report_type == "meetings":
        prompt += "Проанализируй встречи. Процент недошедших. Как снизить отмены?"

    return call_claude(ANALYST_PROMPT, prompt)

# ============================================================
# DASHBOARD PNG
# ============================================================
def generate_dashboard_png(data):
    """Generate dashboard PNG using chromium headless."""
    import subprocess

    leads = data.get("leads", {})
    meetings = data.get("meetings", {})
    sales = data.get("sales", {})
    funnel = data.get("funnel", {})
    comparison = data.get("comparison", {})
    period = data.get("period", {})

    total_leads = leads.get("total", 0)
    total_meetings = meetings.get("total", 0)
    completed = meetings.get("completed", 0)
    no_show = meetings.get("no_show", 0)
    no_show_rate = meetings.get("no_show_rate", 0)
    total_sales = sales.get("total", 0)
    lead_to_sale = funnel.get("lead_to_sale", 0)

    by_source = leads.get("by_source", {})
    sales_by_src = sales.get("by_source", {})

    period_label = f'{period.get("since", "")} — {period.get("until", "")}'
    now = get_israel_now()
    date_str = now.strftime("%d.%m.%Y %H:%M")

    # Source cards
    source_cards = ""
    sources = ["MayaCars", "CarCity", "AutoMotors", "TikTok", "Marketplace", "Organic"]
    colors = {"MayaCars": "#f0c040", "CarCity": "#3b82f6", "AutoMotors": "#a855f7", "TikTok": "#ef4444", "Marketplace": "#22c55e", "Organic": "#6b7280"}
    for src in sources:
        src_data = by_source.get(src, {})
        cnt = src_data.get("total", 0) if isinstance(src_data, dict) else 0
        s_cnt = sales_by_src.get(src, 0)
        conv = round(s_cnt / cnt * 100, 1) if cnt > 0 else 0
        col = colors.get(src, "#6b7280")
        if cnt > 0 or s_cnt > 0:
            source_cards += f'<div class="src-card"><div class="src-name" style="color:{col}">{src}</div><div class="src-row"><span class="src-l">Лиды</span><span class="src-v">{cnt}</span></div><div class="src-row"><span class="src-l">Продажи</span><span class="src-v">{s_cnt}</span></div><div class="src-row"><span class="src-l">Конверсия</span><span class="src-v" style="color:{col}">{conv}%</span></div></div>'

    # Funnel bars
    funnel_steps = []
    if total_leads > 0:
        funnel_steps.append(("Лиды", total_leads, 100, "#3b82f6"))
    if total_meetings > 0:
        funnel_steps.append(("Встречи", total_meetings, max(15, total_meetings/max(total_leads,1)*100), "#a855f7"))
    if completed > 0:
        funnel_steps.append(("Состоялись", completed, max(12, completed/max(total_leads,1)*100), "#22c55e"))
    if no_show > 0:
        funnel_steps.append(("Не дошли", no_show, max(10, no_show/max(total_leads,1)*100), "#ef4444"))
    if total_sales > 0:
        funnel_steps.append(("Продажи", total_sales, max(8, total_sales/max(total_leads,1)*100), "#f0c040"))

    funnel_html = ""
    for i, (label, val, width, color) in enumerate(funnel_steps):
        pct = ""
        if i > 0 and funnel_steps[i-1][1] > 0:
            pct = f"{round(val / funnel_steps[i-1][1] * 100, 1)}%"
        funnel_html += f'<div class="fs"><div class="fv">{pct}</div><div class="fw"><div class="fb" style="width:{width}%;background:{color}"><span class="ft">{val}</span></div></div><div class="fl">{label}</div></div>'

    lf_count = comparison.get("lead_form_count", 0)
    msg_count = comparison.get("message_count", 0)
    lf_pct = comparison.get("lead_form_pct", 0)
    msg_pct = comparison.get("message_pct", 0)

    html = f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#06060c;color:#e8e8f0;font-family:Arial,Helvetica,sans-serif;width:1280px;overflow:hidden}}
.db{{max-width:1240px;margin:0 auto;padding:32px 20px 24px}}
.hd{{text-align:center;margin-bottom:36px}}
.lg{{font-size:44px;font-weight:900;color:#f0c040}}
.badge{{display:inline-block;padding:12px 28px;border:1px solid rgba(255,255,255,.1);border-radius:24px;font-size:20px;font-weight:700;color:#a0a0b8;background:rgba(18,18,28,.85);margin-top:12px}}
.sb{{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:18px}}
.sd{{width:10px;height:10px;border-radius:50%}}
.stx{{font-size:20px;font-weight:800;letter-spacing:2px}}
.sec{{font-size:22px;font-weight:700;color:#e8e8f0;letter-spacing:4px;text-transform:uppercase;margin:36px 0 18px;text-align:center}}
.g4{{display:flex;gap:14px;margin-bottom:14px}}
.g4 .card{{flex:1}}
.g3{{display:flex;gap:14px;margin-bottom:14px}}
.g3 .card{{flex:1}}
.card{{background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:24px;text-align:center}}
.cl{{font-size:18px;color:#a0a0b8;font-weight:700;text-transform:uppercase;letter-spacing:2px;margin-bottom:10px}}
.cv{{font-size:48px;font-weight:800;line-height:1}}
.fcard{{background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:28px 24px;margin-bottom:14px}}
.fn{{max-width:700px;margin:0 auto}}
.fs{{display:flex;align-items:center;width:100%;gap:12px;margin-bottom:6px}}
.fw{{flex:1}}
.fb{{height:56px;border-radius:12px;display:flex;align-items:center;justify-content:center}}
.ft{{font-size:28px;font-weight:800;color:#fff}}
.fl{{font-size:20px;color:#e0e0f0;font-weight:700;text-transform:uppercase;width:180px}}
.fv{{font-size:20px;font-weight:700;width:80px;text-align:right;color:#22c55e}}
.src-grid{{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:14px}}
.src-card{{flex:1;min-width:200px;background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:24px}}
.src-name{{font-size:22px;font-weight:800;margin-bottom:14px;text-align:center}}
.src-row{{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04)}}
.src-l{{font-size:16px;color:#a0a0b8}}
.src-v{{font-size:18px;font-weight:700}}
.comp-bar{{display:flex;height:56px;border-radius:14px;overflow:hidden;margin:12px 0}}
.comp-seg{{display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#fff}}
.pill-grid{{display:flex;gap:14px}}
.pill{{flex:1;background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:20px;text-align:center}}
.pill-l{{font-size:18px;color:#e0e0f0;font-weight:700;text-transform:uppercase;letter-spacing:2px;margin-bottom:10px}}
.pill-v{{font-size:44px;font-weight:900}}
.footer{{text-align:center;padding:24px 0 8px;font-size:12px;color:#3a3a50;letter-spacing:2px;text-transform:uppercase}}
</style></head><body><div class="db">
<div class="hd">
<div class="lg">AutoAnalytics</div>
<div class="badge">{period_label}</div>
<div class="sb"><div class="sd" style="background:#22c55e;box-shadow:0 0 12px rgba(34,197,94,.5)"></div><div class="stx" style="color:#22c55e">АВТОБИЗНЕС</div></div></div>
<div class="sec">Общая воронка</div>
<div class="g4">
<div class="card"><div class="cl">Лиды</div><div class="cv" style="color:#3b82f6">{total_leads}</div></div>
<div class="card"><div class="cl">Встречи</div><div class="cv" style="color:#a855f7">{total_meetings}</div></div>
<div class="card"><div class="cl">Продажи</div><div class="cv" style="color:#22c55e">{total_sales}</div></div>
<div class="card"><div class="cl">Конверсия</div><div class="cv" style="color:#f0c040">{lead_to_sale}%</div></div></div>
<div class="sec">Воронка продаж</div>
<div class="fcard"><div class="fn">{funnel_html}</div>
<div style="display:flex;justify-content:center;gap:40px;margin-top:24px;font-size:24px;font-weight:800">
<span style="color:#22c55e">Конверсия: {lead_to_sale}%</span>
<span style="color:#ef4444">Недошедшие: {no_show_rate}%</span></div></div>
<div class="sec">Источники</div>
<div class="src-grid">{source_cards}</div>
<div class="sec">Лид-форма vs Переписка</div>
<div class="fcard"><div class="comp-bar">
<div class="comp-seg" style="width:{max(lf_pct,5)}%;background:#3b82f6">Формы: {lf_count} ({lf_pct}%)</div>
<div class="comp-seg" style="width:{max(msg_pct,5)}%;background:#a855f7">Переписка: {msg_count} ({msg_pct}%)</div></div></div>
<div class="sec">Встречи</div>
<div class="g3">
<div class="card"><div class="cl">Всего встреч</div><div class="cv" style="color:#a855f7">{total_meetings}</div></div>
<div class="card"><div class="cl">Состоялись</div><div class="cv" style="color:#22c55e">{completed}</div></div>
<div class="card"><div class="cl">Не дошли</div><div class="cv" style="color:#ef4444">{no_show}</div></div></div>
<div class="sec">Ключевые показатели</div>
<div class="pill-grid">
<div class="pill"><div class="pill-l">Лид-Встреча</div><div class="pill-v" style="color:#a855f7">{funnel.get("lead_to_meeting", 0)}%</div></div>
<div class="pill"><div class="pill-l">Встреча-Продажа</div><div class="pill-v" style="color:#22c55e">{funnel.get("meeting_to_sale", 0)}%</div></div>
<div class="pill"><div class="pill-l">Лид-Продажа</div><div class="pill-v" style="color:#f0c040">{lead_to_sale}%</div></div>
<div class="pill"><div class="pill-l">Не дошли</div><div class="pill-v" style="color:#ef4444">{no_show_rate}%</div></div></div>
<div class="footer">AutoAnalytics Dashboard · {date_str}</div>
</div></body></html>'''

    html_path = tempfile.mktemp(suffix=".html", prefix="dash_")
    png_path = tempfile.mktemp(suffix=".png", prefix="dashboard_")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    try:
        subprocess.run([
            "chromium", "--headless", "--disable-gpu", "--no-sandbox",
            "--window-size=1280,2000", "--screenshot=" + png_path,
            "--default-background-color=00000000",
            "file://" + html_path
        ], check=True, timeout=30, capture_output=True)
    finally:
        try:
            os.unlink(html_path)
        except:
            pass
    return png_path

# ============================================================
# INTENT DETECTION
# ============================================================
INTENT_PROMPT = """Ты определяешь намерение пользователя. Ответь ТОЛЬКО одним словом:
- "dashboard" — хочет визуальный дашборд/картинку
- "full_report" — хочет полный текстовый отчёт
- "funnel" — спрашивает про воронку продаж
- "sources" — сравнение источников (какая страница лучше)
- "meetings" — про встречи и недошедших
- "comparison" — лид-формы vs переписка
- "chat" — обычный разговор, вопрос

Период (ответь вторым словом):
- "today" / "yesterday" / "week" / "month" / "3months" / "all"
По умолчанию "month"

Формат ответа: INTENT PERIOD
Пример: full_report month
Пример: dashboard week"""

def detect_intent(text):
    """Detect user intent using Claude."""
    text_lower = text.lower()

    # Quick keyword detection
    if any(w in text_lower for w in ["дашборд", "dashboard", "картинк", "png", "визуал"]):
        intent = "dashboard"
    elif any(w in text_lower for w in ["воронк", "funnel", "конверси"]):
        intent = "funnel"
    elif any(w in text_lower for w in ["источник", "страниц", "source", "майя", "карсити", "автомотор"]):
        intent = "sources"
    elif any(w in text_lower for w in ["встреч", "недошед", "не дошёл", "не пришёл", "meeting"]):
        intent = "meetings"
    elif any(w in text_lower for w in ["лид-форм", "переписк", "сравн", "форма vs", "что лучше"]):
        intent = "comparison"
    elif any(w in text_lower for w in ["отчёт", "отчет", "report", "аналитик", "покажи", "статистик"]):
        intent = "full_report"
    else:
        # Use Claude for complex intents
        result = call_claude(INTENT_PROMPT, text, max_tokens=50)
        if result:
            parts = result.strip().lower().split()
            intent = parts[0] if parts else "chat"
        else:
            intent = "chat"

    # Detect period
    if "сегодн" in text_lower or "today" in text_lower:
        period = "today"
    elif "вчера" in text_lower or "yesterday" in text_lower:
        period = "yesterday"
    elif "недел" in text_lower or "week" in text_lower:
        period = "week"
    elif "месяц" in text_lower or "month" in text_lower:
        period = "month"
    elif "квартал" in text_lower or "3 месяц" in text_lower:
        period = "3months"
    elif "всё время" in text_lower or "all" in text_lower or "за всё" in text_lower:
        period = "all"
    else:
        period = "month"

    return intent, period

def get_period_dates(period):
    """Convert period name to since/until dates."""
    now = get_israel_now()
    if period == "today":
        since = now.replace(hour=0, minute=0, second=0)
    elif period == "yesterday":
        since = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0)
        now = since.replace(hour=23, minute=59, second=59)
    elif period == "week":
        since = now - timedelta(days=7)
    elif period == "month":
        since = now - timedelta(days=30)
    elif period == "3months":
        since = now - timedelta(days=90)
    elif period == "all":
        since = now - timedelta(days=365 * 3)
    else:
        since = now - timedelta(days=30)
    return since, now

# ============================================================
# COMMANDS
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID,
        "🚗 АвтоАналитик на связи!\n\n"
        "Я анализирую данные из:\n"
        "  Google Sheets (лиды MayaCars, CarCity, AutoMotors)\n"
        "  Google Calendar (встречи)\n"
        "  Таблица продаж\n\n"
        "Команды:\n"
        "/report — полный отчёт за 30 дней\n"
        "/dashboard — визуальный дашборд\n"
        "/funnel — анализ воронки\n"
        "/sources — сравнение источников\n"
        "/meetings — анализ встреч\n\n"
        "Или просто спроси — например:\n"
        "  'Покажи аналитику за неделю'\n"
        "  'Какой источник лучше?'\n"
        "  'Сколько недошедших?'"
    )

@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Собираю данные за 30 дней...")
    try:
        data = full_analytics()
        report = generate_report(data, "full")
        if report:
            safe_send(MY_CHAT_ID, report)
        else:
            safe_send(MY_CHAT_ID, "❌ Ошибка генерации отчёта")
    except Exception as e:
        print(f"Report error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

@bot.message_handler(commands=["dashboard"])
def cmd_dashboard(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Генерирую дашборд...\n⏳ 10-15 секунд")
    try:
        data = full_analytics()
        png_path = generate_dashboard_png(data)
        with open(png_path, 'rb') as photo:
            bot.send_photo(MY_CHAT_ID, photo, caption="📊 AutoAnalytics Dashboard")
        os.unlink(png_path)
        # Send text summary too
        report = generate_report(data, "full")
        if report:
            safe_send(MY_CHAT_ID, report)
    except Exception as e:
        print(f"Dashboard error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

@bot.message_handler(commands=["funnel"])
def cmd_funnel(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "🔄 Анализирую воронку...")
    try:
        data = full_analytics()
        report = generate_report(data, "funnel")
        if report:
            safe_send(MY_CHAT_ID, report)
    except Exception as e:
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

@bot.message_handler(commands=["sources"])
def cmd_sources(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📈 Сравниваю источники...")
    try:
        data = full_analytics()
        report = generate_report(data, "sources")
        if report:
            safe_send(MY_CHAT_ID, report)
    except Exception as e:
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

@bot.message_handler(commands=["meetings"])
def cmd_meetings(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📅 Анализирую встречи...")
    try:
        data = full_analytics()
        report = generate_report(data, "meetings")
        if report:
            safe_send(MY_CHAT_ID, report)
    except Exception as e:
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

# ============================================================
# FREE TEXT
# ============================================================
@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    intent, period = detect_intent(user_text)
    since, until = get_period_dates(period)

    period_names = {"today": "Сегодня", "yesterday": "Вчера", "week": "Неделя", "month": "Месяц", "3months": "3 месяца", "all": "Всё время"}
    plabel = period_names.get(period, "Месяц")

    if intent == "dashboard":
        safe_send(MY_CHAT_ID, f"📊 Генерирую дашборд ({plabel})...\n⏳")
        try:
            data = full_analytics(since, until)
            png_path = generate_dashboard_png(data)
            with open(png_path, 'rb') as photo:
                bot.send_photo(MY_CHAT_ID, photo, caption=f"📊 Dashboard · {plabel}")
            os.unlink(png_path)
            report = generate_report(data, "full")
            if report:
                safe_send(MY_CHAT_ID, report)
        except Exception as e:
            safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

    elif intent in ("full_report", "funnel", "sources", "meetings", "comparison"):
        safe_send(MY_CHAT_ID, f"📊 Анализирую ({plabel})...")
        try:
            data = full_analytics(since, until)
            rtype = intent if intent != "comparison" else "sources"
            report = generate_report(data, rtype)
            if report:
                safe_send(MY_CHAT_ID, report)
        except Exception as e:
            safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

    else:
        # Chat mode — still try to answer with data context
        try:
            data = full_analytics()
            summary = json.dumps(data, ensure_ascii=False, default=str)[:2000]
            prompt = f"Контекст данных бизнеса:\n{summary}\n\nМихаил спросил: «{user_text}»\n\nОтветь полезно и конкретно."
            response = call_claude(ANALYST_PROMPT, prompt, max_tokens=1500)
            if response:
                safe_send(MY_CHAT_ID, response)
        except Exception as e:
            safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

# ============================================================
# VOICE MESSAGES
# ============================================================
@bot.message_handler(content_types=["voice"])
def handle_voice(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "🎤 Голосовые пока не поддерживаются. Напиши текстом!")

# ============================================================
# FORCE STOP OTHER POLLING & START
# ============================================================
def force_drop_polling():
    """Call Telegram API directly to kill any existing polling sessions."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook"
    try:
        resp = requests.post(url, json={"drop_pending_updates": True}, timeout=10)
        print(f"deleteWebhook: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"deleteWebhook error: {e}")

    # Also call getUpdates with a short timeout to "steal" the polling lock
    url2 = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        resp2 = requests.post(url2, json={"offset": -1, "timeout": 0}, timeout=10)
        print(f"getUpdates reset: {resp2.status_code}")
    except Exception as e:
        print(f"getUpdates reset error: {e}")

if __name__ == "__main__":
    print("🚗 АВТОАНАЛИТИК НА ПОСТУ!")
    print(f"📅 {get_israel_now().strftime('%Y-%m-%d %H:%M')}")

    # Step 1: Force kill any existing polling
    print("🔄 Сбрасываю предыдущие polling-сессии...")
    force_drop_polling()
    time.sleep(3)
    force_drop_polling()
    time.sleep(2)

    # Step 2: Remove webhook just in case
    bot.delete_webhook(drop_pending_updates=True)
    time.sleep(2)

    # Step 3: Start polling with retry
    print("📱 Запускаю polling...")
    max_retries = 5
    for attempt in range(max_retries):
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
            break
        except telebot.apihelper.ApiTelegramException as e:
            if "409" in str(e) and attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                print(f"⚠️ Конфликт 409, попытка {attempt+1}/{max_retries}. Жду {wait} сек...")
                force_drop_polling()
                time.sleep(wait)
            else:
                print(f"❌ Критическая ошибка: {e}")
                raise
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise
