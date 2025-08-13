from notion_client import Client
from dotenv import load_dotenv
import os
from datetime import date

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

if not NOTION_TOKEN or not DATABASE_ID:
    raise SystemExit("Missing NOTION_TOKEN or NOTION_DATABASE_ID in .env")

notion = Client(auth=NOTION_TOKEN)

# 1) Inspect DB to find the title property and print properties (helps mapping)
db = notion.databases.retrieve(DATABASE_ID)
props = db.get("properties", {})
title_prop_name = None
for name, meta in props.items():
    if meta.get("type") == "title":
        title_prop_name = name
        break

if not title_prop_name:
    raise SystemExit("Could not find a title property in the database.")

print("Database properties:")
for name, meta in props.items():
    print(f"- {name}: {meta.get('type')}")

print(f"\nTitle property detected: {title_prop_name}")

# 2) Build a page payload that respects Notion types
def rich(text):  # helper for rich_text fields
    return {"rich_text": [{"type": "text", "text": {"content": text}}]}

def title(text):  # helper for title field
    return {"title": [{"type": "text", "text": {"content": text}}]}

def number(val):
    return {"number": float(val)} if val is not None else {"number": None}

def date_prop(iso_date):
    return {"date": {"start": iso_date}}

def select(name):
    return {"select": {"name": name}} if name else {"select": None}

def url(link):
    return {"url": link if link else None}

# === EXAMPLE: Create a single test page ===
payload = {
    # Title column (use whatever your DB’s title property actually is)
    title_prop_name: title("AAA @ BBB"),

    # Dates / text / numbers — adjust names to match your DB exactly
    "Game Date": date_prop(str(date.today())),
    "Away Team": rich("AAA"),
    "Home Team": rich("BBB"),
    "Start Time (ET)": rich("7:05 PM"),
    "Away Pitcher": rich("John Doe"),
    "Home Pitcher": rich("Max Sample"),
    "ML - Market Home": number(-150),
    "ML - Market Away": number(130),
    "Total (Market)": number(8.5),

    "Prediction - Moneyline": select("BBB ML"),
    "Confidence (ML)": number(7),

    "Prediction - Total": select("Over 8.5"),
    "Confidence (Total)": number(6),

    "Prediction - Run Line": select("BBB -1.5"),
    "Confidence (Run Line)": number(8),

    "Box Score Link": url("https://www.mlb.com/gameday/sample"),
    "Final Score": rich("5-3"),
    "Result (ML)": select("Win"),
    "Result (Total)": select("Loss"),
    "Result (Run Line)": select("Win"),
    "Closing Line (ML)": number(-155),
    "CLV (Closing Line Value)": number(0.02),
    "Notes / Angle": rich("Sample placeholder row."),
    "Data Source(s)": rich("Action Network, MLB.com"),
}

# Only include keys that actually exist in your DB (ignore typos/mismatches)
payload = {k: v for k, v in payload.items() if k in props}

created = notion.pages.create(parent={"database_id": DATABASE_ID}, properties=payload)
print("Created page:", created.get("id"))
