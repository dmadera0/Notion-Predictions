# app.py
# MLB -> Notion pipeline
# Modes:
#   1) python3 app.py
#        - Fetch today's REAL MLB schedule (probables, ET times, box links)
#        - Upsert rows to Notion (Key: YYYY-MM-DD|AWAY|HOME)
#        - Write slate_YYYY-MM-DD.csv locally
#   2) python3 app.py odds odds.csv
#        - Join odds to today's slate by Key (no stray rows)
#        - Compute ML/Total/Run Line picks + 1–10 confidence (no-vig)
#        - Upsert to Notion & write predictions_YYYY-MM-DD.csv

import os
import csv
from pathlib import Path
from datetime import date
import requests
from dateutil import parser as dtparser
import pytz

from notion_client import Client
from dotenv import load_dotenv

# -------------------- env & client --------------------
load_dotenv()
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
if not NOTION_TOKEN or not DATABASE_ID:
    raise SystemExit("Missing NOTION_TOKEN or NOTION_DATABASE_ID in .env")

notion = Client(auth=NOTION_TOKEN)

# -------------------- Notion property builders --------------------
def rich(text):
    return {"rich_text": [{"type": "text", "text": {"content": str(text)}}]}

def title_val(text):
    return {"title": [{"type": "text", "text": {"content": str(text)}}]}

def number(val):
    if val is None or val == "" or str(val).lower() == "none":
        return {"number": None}
    try:
        return {"number": float(val)}
    except Exception:
        return {"number": None}

def date_prop(iso_date):
    return {"date": {"start": iso_date}}

def url(link):
    return {"url": link if link else None}

# -------------------- files: CSV writer --------------------
def write_csv(rows, path):
    """Write a consistent CSV of rows (dicts) to `path`."""
    if not rows:
        return
    fields = [
        "game_date","away","home","start_et","away_p","home_p",
        "ml_home","ml_away","total",
        "pick_ml","conf_ml","pick_tot","conf_tot","pick_rl","conf_rl",
        "box_link","notes","sources"
    ]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

# -------------------- schema discovery & ensure "Key" --------------------
db = notion.databases.retrieve(DATABASE_ID)
props = db.get("properties", {})

# Ensure a "Key" text property exists (Notion "Text" == API "rich_text")
if "Key" not in props:
    notion.databases.update(
        database_id=DATABASE_ID,
        properties={"Key": {"rich_text": {}}}
    )
    db = notion.databases.retrieve(DATABASE_ID)
    props = db.get("properties", {})

TITLE_PROP = next((n for n, m in props.items() if m.get("type") == "title"), None)
if not TITLE_PROP:
    raise SystemExit("No title property found; the database must have one title column.")

# -------------------- team code normalization --------------------
def normalize_team(t):
    """Normalize team codes so odds CSV and MLB schedule share the same keys."""
    if not t:
        return t
    t = t.strip().upper()
    alias = {
        # Common cross-site variants
        "ARI": "AZ",
        "D-BACKS": "AZ",
        "SFG": "SF",
        "SDP": "SD",
        "TBR": "TB",
        "CWS": "CHW",
        "WAS": "WSH",
        "KCR": "KC",
        "OAK": "ATH",   # some sources use OAK; slate may use ATH
        # pass-throughs
        "AZ": "AZ", "SF": "SF", "SD": "SD", "TB": "TB", "CHW": "CHW",
        "NYY": "NYY", "NYM": "NYM", "LAA": "LAA", "LAD": "LAD",
        "CHC": "CHC", "MIL": "MIL", "PIT": "PIT", "CIN": "CIN",
        "STL": "STL", "MIA": "MIA", "BOS": "BOS", "BAL": "BAL", "HOU": "HOU",
        "SEA": "SEA", "TEX": "TEX", "PHI": "PHI", "WSH": "WSH", "TOR": "TOR",
        "ATL": "ATL", "CLE": "CLE", "DET": "DET", "MIN": "MIN", "KC": "KC",
        "COL": "COL", "ATH": "ATH", "OAK A'S": "ATH"
    }
    return alias.get(t, t)

# -------------------- Key + Notion query helpers --------------------
def make_key(game_date_iso, away, home):
    return f"{game_date_iso}|{away}|{home}"

def find_page_by_key(key_val):
    resp = notion.databases.query(
        database_id=DATABASE_ID,
        filter={"property": "Key", "rich_text": {"equals": key_val}},
        page_size=1,
    )
    results = resp.get("results", [])
    return results[0]["id"] if results else None

# -------------------- Upsert prediction row --------------------
def upsert_prediction_row(row):
    """
    row expects:
      game_date, away, home, start_et, away_p, home_p,
      ml_home, ml_away, total,
      pick_ml, conf_ml, pick_tot, conf_tot, pick_rl, conf_rl,
      box_link, notes, sources
    """
    key_val = make_key(row["game_date"], row["away"], row["home"])
    page_id = find_page_by_key(key_val)

    payload = {
        TITLE_PROP: title_val(row["away"]),  # your title property (likely "Away Team")
        "Key": rich(key_val),
        "Game Date": date_prop(row["game_date"]),

        **({"Away Team": rich(row["away"])} if (TITLE_PROP != "Away Team" and "Away Team" in props) else {}),
        "Home Team": rich(row.get("home", "")) if "Home Team" in props else None,
        "Start Time (ET)": rich(row.get("start_et", "")) if "Start Time (ET)" in props else None,
        "Away Pitcher": rich(row.get("away_p", "")) if "Away Pitcher" in props else None,
        "Home Pitcher": rich(row.get("home_p", "")) if "Home Pitcher" in props else None,

        "ML - Market Home": number(row.get("ml_home")) if "ML - Market Home" in props else None,
        "ML - Market Away": number(row.get("ml_away")) if "ML - Market Away" in props else None,
        "Total (Market)": number(row.get("total")) if "Total (Market)" in props else None,

        "Prediction - Moneyline": rich(row.get("pick_ml", "")) if "Prediction - Moneyline" in props else None,
        "Confidence (ML)": number(row.get("conf_ml")) if "Confidence (ML)" in props else None,
        "Prediction - Total": rich(row.get("pick_tot", "")) if "Prediction - Total" in props else None,
        "Confidence (Total)": number(row.get("conf_tot")) if "Confidence (Total)" in props else None,
        "Prediction - Run Line": rich(row.get("pick_rl", "")) if "Prediction - Run Line" in props else None,
        "Confidence (Run Line)": number(row.get("conf_rl")) if "Confidence (Run Line)" in props else None,

        "Box Score Link": url(row.get("box_link", "")) if "Box Score Link" in props else None,
        "Notes / Angle": rich(row.get("notes", "")) if "Notes / Angle" in props else None,
        "Data Source(s)": rich(row.get("sources", "")) if "Data Source(s)" in props else None,
    }
    payload = {k: v for k, v in payload.items() if v is not None and k in props}

    if page_id:
        notion.pages.update(page_id=page_id, properties=payload)
        return page_id, "updated", key_val
    else:
        created = notion.pages.create(parent={"database_id": DATABASE_ID}, properties=payload)
        return created["id"], "created", key_val

# -------------------- MLB schedule + probables (REAL) --------------------
US_EASTERN = pytz.timezone("US/Eastern")

def to_et_time_str(game_datetime_iso):
    if not game_datetime_iso:
        return ""
    dt_utc = dtparser.isoparse(game_datetime_iso)
    dt_et = dt_utc.astimezone(US_EASTERN)
    try:
        return dt_et.strftime("%-I:%M %p")  # mac/linux
    except ValueError:
        return dt_et.strftime("%#I:%M %p")  # windows

def fetch_todays_slate_from_mlb(game_date_iso: str):
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={game_date_iso}"
        "&hydrate=probablePitchers,team,linescore&language=en"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    out = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            game_pk = g.get("gamePk")
            game_dt = g.get("gameDate")
            away_team = g.get("teams", {}).get("away", {}).get("team", {})
            home_team = g.get("teams", {}).get("home", {}).get("team", {})
            away_abbr = away_team.get("abbreviation") or away_team.get("teamCode") or away_team.get("name")
            home_abbr = home_team.get("abbreviation") or home_team.get("teamCode") or home_team.get("name")
            away_abbr = normalize_team(away_abbr)
            home_abbr = normalize_team(home_abbr)

            away_pp = g.get("teams", {}).get("away", {}).get("probablePitcher", {})
            home_pp = g.get("teams", {}).get("home", {}).get("probablePitcher", {})
            away_p = away_pp.get("fullName") or ""
            home_p = home_pp.get("fullName") or ""

            out.append({
                "game_date": game_date_iso,
                "away": away_abbr,
                "home": home_abbr,
                "start_et": to_et_time_str(game_dt),
                "away_p": away_p,
                "home_p": home_p,
                "box_link": f"https://www.mlb.com/gameday/{game_pk}" if game_pk else "",
                # placeholders; will be filled by odds ingest
                "ml_home": None, "ml_away": None, "total": None,
                "pick_ml": "", "conf_ml": None,
                "pick_tot": "", "conf_tot": None,
                "pick_rl": "", "conf_rl": None,
                "notes": "Auto from MLB schedule — awaiting odds/model.",
                "sources": "MLB Stats API",
            })
    return out

# -------------------- odds math --------------------
def american_to_implied(ml):
    ml = float(ml)
    if ml < 0:
        return (-ml) / ((-ml) + 100.0)
    else:
        return 100.0 / (ml + 100.0)

def remove_vig(p_a, p_b):
    s = p_a + p_b
    if s == 0:
        return 0.5, 0.5
    return p_a / s, p_b / s

def edge_to_conf_1_10(edge_prob):
    """Map absolute edge to 1..10 (3 baseline, ~6 at 5% edge, ~8 at 10%, 10 at 15%+)."""
    if edge_prob < 0:
        edge_prob = 0
    score = 3 + (edge_prob / 0.05) * 1.5
    return max(1, min(10, round(score)))

def pick_moneyline_from_market(ml_home, ml_away):
    p_home = american_to_implied(ml_home)
    p_away = american_to_implied(ml_away)
    p_home_nv, p_away_nv = remove_vig(p_home, p_away)
    if p_home_nv >= p_away_nv:
        edge = p_home_nv - 0.5
        fav_prob = p_home_nv
        return "HOME ML", edge_to_conf_1_10(abs(edge)), fav_prob
    else:
        edge = p_away_nv - 0.5
        fav_prob = p_away_nv
        return "AWAY ML", edge_to_conf_1_10(abs(edge)), fav_prob

def pick_total_from_market(total_num, over_price, under_price):
    p_over = american_to_implied(over_price)
    p_under = american_to_implied(under_price)
    p_over_nv, p_under_nv = remove_vig(p_over, p_under)
    edge = abs(p_over_nv - 0.5)
    if edge < 0.02:
        return "No Edge", 2
    if p_over_nv > p_under_nv:
        return f"Over {total_num}", edge_to_conf_1_10(edge)
    else:
        return f"Under {total_num}", edge_to_conf_1_10(edge)

def pick_runline_from_market(fav_prob_nv):
    if fav_prob_nv >= 0.62:
        return "FAV -1.5", edge_to_conf_1_10(fav_prob_nv - 0.5)
    else:
        return "DOG +1.5", max(2, edge_to_conf_1_10(0.62 - fav_prob_nv) - 1)

# -------------------- JOIN: odds -> today's slate --------------------
def ingest_odds_and_compute_picks(csv_path):
    """
    Join odds onto today's slate by Key (YYYY-MM-DD|AWAY|HOME).
    Only slate games are processed -> avoids stray rows.
    """
    today_iso = date.today().isoformat()
    slate_path = f"slate_{today_iso}.csv"

    if not os.path.exists(slate_path):
        raise SystemExit(f"Slate file not found: {slate_path}. Run `python3 app.py` first.")

    # --- load slate (authoritative) ---
    slate_rows = []
    with open(slate_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            away = normalize_team((r.get("away") or "").strip())
            home = normalize_team((r.get("home") or "").strip())
            gd = (r.get("game_date") or "").strip()
            slate_rows.append({
                "game_date": gd,
                "away": away,
                "home": home,
                "start_et": r.get("start_et") or "",
                "away_p": r.get("away_p") or "",
                "home_p": r.get("home_p") or "",
                "box_link": r.get("box_link") or "",
                # placeholders; will be filled from odds if present
                "ml_home": None, "ml_away": None, "total": None,
                "pick_ml": "", "conf_ml": None,
                "pick_tot": "", "conf_tot": None,
                "pick_rl": "", "conf_rl": None,
                "notes": "Joined slate + odds",
                "sources": "MLB Stats API + Odds CSV",
            })

    slate_by_key = { make_key(r["game_date"], r["away"], r["home"]): r for r in slate_rows }

    # --- load odds and map by Key ---
    odds_by_key = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            gd = (r.get("Game Date") or "").strip()
            away = normalize_team((r.get("Away Team") or "").strip())
            home = normalize_team((r.get("Home Team") or "").strip())
            if not (gd and away and home):
                continue
            key = make_key(gd, away, home)
            try:
                ml_home = float(r.get("ML - Market Home"))
                ml_away = float(r.get("ML - Market Away"))
            except Exception:
                continue
            try:
                total_num = float(r.get("Total (Market)"))
            except Exception:
                total_num = None
            try:
                over_price = float(r.get("Over Price")) if r.get("Over Price") else -110.0
                under_price = float(r.get("Under Price")) if r.get("Under Price") else -110.0
            except Exception:
                over_price = under_price = -110.0

            odds_by_key[key] = {
                "ml_home": ml_home, "ml_away": ml_away,
                "total": total_num, "over_price": over_price, "under_price": under_price,
            }

    updated_rows = []

    # --- drive updates from the slate only ---
    for key, base in slate_by_key.items():
        odds = odds_by_key.get(key)

        if not odds:
            # No odds for this slate game -> keep lines empty, no picks
            row = { **base }
            pid, status, _ = upsert_prediction_row(row)
            print(f"[odds->picks] (no odds) {base['away']} @ {base['home']} {status} {pid} Key: {key}")
            updated_rows.append(row)
            continue

        # moneyline pick
        ml_side, conf_ml, fav_prob_nv = pick_moneyline_from_market(odds["ml_home"], odds["ml_away"])
        pick_ml = f"{base['home']} ML" if ml_side == "HOME ML" else f"{base['away']} ML"

        # totals pick
        if odds["total"] is not None:
            pick_tot, conf_tot = pick_total_from_market(odds["total"], odds["over_price"], odds["under_price"])
        else:
            pick_tot, conf_tot = "No Edge", 2

        # run line pick
        p_home = american_to_implied(odds["ml_home"])
        p_away = american_to_implied(odds["ml_away"])
        p_home_nv, p_away_nv = remove_vig(p_home, p_away)
        rl_side, conf_rl = pick_runline_from_market(max(p_home_nv, p_away_nv))
        rl_str = (
            f"{base['home']} -1.5" if (rl_side == "FAV -1.5" and p_home_nv >= p_away_nv)
            else f"{base['away']} -1.5" if (rl_side == "FAV -1.5" and p_away_nv > p_home_nv)
            else f"{base['away']} +1.5" if p_home_nv >= p_away_nv
            else f"{base['home']} +1.5"
        )

        row = {
            **base,
            "ml_home": odds["ml_home"],
            "ml_away": odds["ml_away"],
            "total": odds["total"],
            "pick_ml": pick_ml, "conf_ml": conf_ml,
            "pick_tot": pick_tot, "conf_tot": conf_tot,
            "pick_rl": rl_str, "conf_rl": conf_rl,
        }
        pid, status, _ = upsert_prediction_row(row)
        print(f"[odds->picks] {base['away']} @ {base['home']} {status} {pid} Key: {key}")
        updated_rows.append(row)

    # write local predictions CSV (joined)
    out_csv = f"predictions_{today_iso}.csv"
    write_csv(updated_rows, out_csv)
    print(f"[csv] Wrote {out_csv}")

# -------------------- today's slate (REAL) --------------------
def get_todays_predictions():
    today = date.today().isoformat()
    slate = fetch_todays_slate_from_mlb(today)
    return slate

# -------------------- daily runner --------------------
def run_daily():
    games = get_todays_predictions()
    if not games:
        print("[predictions] No MLB games found for today.")
        return
    # write local slate CSV
    out_csv = f"slate_{date.today().isoformat()}.csv"
    write_csv(games, out_csv)
    print(f"[csv] Wrote {out_csv}")

    for row in games:
        pid, status, key_val = upsert_prediction_row(row)
        print("[predictions]", row["away"], "@", row["home"], status, pid, "Key:", key_val)

# -------------------- CLI --------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2 and sys.argv[1].lower() == "odds":
        odds_file = sys.argv[2] if len(sys.argv) >= 3 else "odds.csv"
        ingest_odds_and_compute_picks(odds_file)
    else:
        run_daily()
