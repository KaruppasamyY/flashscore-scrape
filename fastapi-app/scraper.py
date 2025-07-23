'''-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC5iLutz0Tjf/G8
/+ly46xeVS0BI+65CG+Hzc//P0vMJpoAzwCY47K23Pk26zUttFtaW/M/bdAvlIpk
oq+KV7KnPv1FvUdW1HBK4Ed5T+SAqvkDEyR79NqggkZVeEaOP7B8mPnDo79Jf9R+
+MzNcLzn9sKtXipoLmqxAllemQd0nicNXwdCuhnbH9m/BJ9jqeL8iR7PSOvIZg2n
yJ9rvUbQ9EDPKhE+BtHjOLv+thqtbdkAdv5P0awlQRtj6UyLFf0YFn9ZDRE5z1zT
AuRXGMB48M8vCMs4WXk24eC8IOaG8EyAbRubyCklzKlKUdxVI/zmJzbnldJpMTs2
k9ieAjP7AgMBAAECggEAVhN2pxOjLTzdpUbBQtkXzggMQhwuk8+zFrlIh24RN1yt
VDS1XTk+X5IzWORUnqivPUOtnb7PuFNcxYWolTFMpEUX8fP3lfQnSMW2wzRqmbs4
ndGSuqOFjrQB94M+kJ5Bvfym27orc+kHaAT711A3hBK6jP5EBGOMgiTJ25Z5nAM8
vHZDAtx+sBp6BX3AtxsuV7FojIjIUkSQx1P1zLSk84Qs2WUm7EBKqNOofSwl2Tcn
TYQv1YQEccN0RvKfaDqy0OyL3bNIJpcs/+6RP1BCqlTm+DC8+Z1sOwlSDAdOVJfE
xqINCdLri8rq5GNrRLGLPTtWYpBZbPJeqhLoeD55KQKBgQDtT8Ive7UDG0eaIGou
cYa6ZvegNzkOMmnYZHkN8YT/9EUGlYGDMMSb5Rmw33RHUY3e+9GHq3wTsr0noS0u
g4k+t9H2nFvktukhnaCTkCqrIfHR7I16toCEPIGe0DQ6/x9OCKYJi2EfcEbDoFM6
ysMoeR7lDByjnr9lAZS+ItQkrQKBgQDIJSHrkAbaNPpOFbW19GLiGrGdDoyWWuWb
+bECDRsJxqhWSqY4T5hrrxMiqBAdoC8EH+E9eZJ2LDZJ8OaVR0tQZKJsceYgXOG9
FuRDX7v6ZFP5kjOaT6EV2DRQhtoRZqRd3iU42JsPWCDQMPnkxUioUTE1ebcABF2D
00O8alUoRwKBgQDhgcpQnuiK1DB9aNId4imaFmC3ceKY+BUW2ZsjQeAV7cRuFkIl
LYGXduSr7But7fS38WnR14YnndEbyJCLw9j2rW0rQLgeJiZxmD1QHE4/oxkNWBG1
i5mij3DJPqZzbZ4M5DELntQjMOk92qtckfNUEYznMVRBs4fC7vaum6AmXQKBgQC5
IcffiokcHHcP9XrwxQ8WkJSMb6HRjmDnRy7Zc9/3yf96WnCok8+sg/RSUV0e2kXD
Le3+HE9C6iCJgsD3gkwDICMx5qV0eBYC/4LDY3GQ6m7W4srCyTd66lNxzVs3T1do
0IxjqdC8f8tSaIsuduJ/dyLqkRyacm9B+hCk30LIDwKBgQCPgWhX1Ur+u6/IxWeb
bCYZ+74CDKvSSJhDtMLbvoQ4MteWD4LjYslr7+kuWtdyEdi6u/dfp1a7zVXQo8G9
aQlSrfH6vgiljoxNVMcaAMX7QxAmSV2lM+f65TSJb3bgfcg6fzRVCOWmM55+tvEY
Qdv1Q6OBm6F9EcRpP2tkbg8/bA==
-----END PRIVATE KEY-----
'''

import os, re, json, requests, unicodedata, uvicorn
from datetime import datetime, timedelta, timezone

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from fastapi import FastAPI
from tqdm import tqdm
import pandas as pd
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from zoneinfo import ZoneInfo

load_dotenv()

HEADERS      = {"User-Agent": "Mozilla/5.0"}
COUNTRY_URL  = "https://www.flashscore.co.uk/x/req/m_1_{}"
MATCH_URL    = "https://www.flashscore.co.uk/match/{}/#/match-summary"
MAX_ID       = 210
MAX_THREADS  = 8
MATCH_THREADS_PER_ROUND = 8

COUNTRY_RX   = re.compile(r"ML÷(.*?)¬")
LEAGUE_RX    = re.compile(r"MN÷(.*?)¬MU÷(.*?)¬")
MATCH_ID_RX  = re.compile(r"AA÷(.*?)¬AD")
ROUND_RX     = re.compile(r"ER÷(.*?)¬RW")
LEAGUE_ID_RX = re.compile(r"ZC÷(.*?)¬ZD")
ENV_RX       = re.compile(r"window\.environment\s*=\s*(\{.*?\});", re.DOTALL)

def is_finished(match_date: str, match_time: str) -> bool:
    try:
        uk_tz = ZoneInfo("Europe/London")
        match_dt = datetime.strptime(f"{match_date} {match_time}", "%d/%m/%Y %H:%M")
        match_dt = match_dt.replace(tzinfo=uk_tz).astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)
        return match_dt < now_utc
    except ValueError:
        return False

def export_to_google_sheet_with_update(
    new_data: list[dict],
    sheet_name_main="Flashscore Matches",
    sheet_name_finished="Finished Matches",
    creds_file="flashscore-scraper-i7-0e2298df8e96.json"
):
    FLAG_COLUMN = "is_published"
    DEFAULT_FLAG_VALUE = False

    new_df = pd.json_normalize(new_data)

    if FLAG_COLUMN not in new_df.columns:
        new_df[FLAG_COLUMN] = DEFAULT_FLAG_VALUE

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(credentials)

    try:
        sheet_main = client.open(sheet_name_main).sheet1
        sheet_finished = client.open(sheet_name_finished).sheet1
    except gspread.SpreadsheetNotFound as e:
        print(f"Error: Sheet not found: {e}")
        return

    existing_main_df = pd.DataFrame(sheet_main.get_all_records())

    if existing_main_df.empty:
        existing_main_df = pd.DataFrame(columns=new_df.columns)

    if FLAG_COLUMN not in existing_main_df.columns:
        existing_main_df[FLAG_COLUMN] = DEFAULT_FLAG_VALUE

    # Backup old flag for merge
    existing_main_df.rename(columns={FLAG_COLUMN: f"{FLAG_COLUMN}_old"}, inplace=True)

    combined_df = pd.concat([existing_main_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=["fixture.match_id"], keep="last", inplace=True)

    # Ensure both columns exist
    if f"{FLAG_COLUMN}_old" not in combined_df.columns:
        combined_df[f"{FLAG_COLUMN}_old"] = DEFAULT_FLAG_VALUE
    if FLAG_COLUMN not in combined_df.columns:
        combined_df[FLAG_COLUMN] = DEFAULT_FLAG_VALUE

    # Resolve is_published flag from old or new
    combined_df[FLAG_COLUMN] = combined_df[[f"{FLAG_COLUMN}_old", FLAG_COLUMN]].bfill(axis=1).iloc[:, 0]
    combined_df.drop(columns=[f"{FLAG_COLUMN}_old"], inplace=True)

    if {"fixture.start_date", "fixture.start_time"}.issubset(combined_df.columns):
        combined_df["fixture.start_date"] = combined_df["fixture.start_date"].astype(str)
        combined_df["fixture.start_time"] = combined_df["fixture.start_time"].astype(str)
        combined_df["is_finished_calc"] = combined_df.apply(
            lambda row: is_finished(row["fixture.start_date"], row["fixture.start_time"]), axis=1
        )
        finished_df = combined_df[combined_df["is_finished_calc"] == True].drop(columns=["is_finished_calc"])
        upcoming_df = combined_df[combined_df["is_finished_calc"] == False].drop(columns=["is_finished_calc"])
    else:
        finished_df = pd.DataFrame(columns=combined_df.columns)
        upcoming_df = combined_df

    existing_finished_df = pd.DataFrame(sheet_finished.get_all_records())
    if existing_finished_df.empty:
        existing_finished_df = pd.DataFrame(columns=finished_df.columns)

    if FLAG_COLUMN not in existing_finished_df.columns:
        existing_finished_df[FLAG_COLUMN] = DEFAULT_FLAG_VALUE

    combined_finished_df = pd.concat([existing_finished_df, finished_df], ignore_index=True)
    combined_finished_df.drop_duplicates(subset=["fixture.match_id"], keep="last", inplace=True)
    combined_finished_df[FLAG_COLUMN].fillna(DEFAULT_FLAG_VALUE, inplace=True)

    # Update finished sheet
    sheet_finished.clear()
    if not combined_finished_df.empty:
        sheet_finished.append_row(combined_finished_df.columns.tolist())
        sheet_finished.append_rows(combined_finished_df.fillna("").astype(str).values.tolist())
    print(f"Success: Finished matches updated: {sheet_name_finished}")

    # Update main sheet
    sheet_main.clear()
    if not upcoming_df.empty:
        sheet_main.append_row(upcoming_df.columns.tolist())
        sheet_main.append_rows(upcoming_df.fillna("").astype(str).values.tolist())
    print(f"Success: Upcoming matches updated: {sheet_name_main}")

def fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.text
    except requests.RequestException:
        print(f"Error: Failed to fetch: {url}")
        return None

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")

def fetch_match_details(match_id: str) -> dict | None:
    html = fetch(MATCH_URL.format(match_id))
    if not html:
        return None

    m_env = ENV_RX.search(html)
    if not m_env:
        print(f"Warning: Environment not found for match ID {match_id}")
        return None

    try:
        env = json.loads(m_env.group(1))
    except json.JSONDecodeError:
        print(f"Warning: Failed to parse JSON for match ID {match_id}")
        return None

    p_data = env.get("participantsData", {})
    home_data = p_data.get("home", [])
    away_data = p_data.get("away", [])

    home = home_data[0] if home_data else {}
    away = away_data[0] if away_data else {}

    def extract_slug(detail_link):
        return detail_link.split("/")[2] if detail_link else "unknown"

    home_slug = extract_slug(home.get("detail_link", ""))
    away_slug = extract_slug(away.get("detail_link", ""))

    ts = env.get("eventStageStartTime")
    if ts is not None:
        dt = datetime.utcfromtimestamp(ts) + timedelta(hours=1)
        start_date = dt.strftime("%d/%m/%Y")
        start_time = dt.strftime("%H:%M")
    else:
        start_date, start_time = "Unknown Date", "00:00"

    return {
        "match_id": match_id,
        "home_id": home.get("id", "N/A"),
        "home_name": home.get("name", "N/A"),
        "home_slug": home_slug,
        "away_id": away.get("id", "N/A"),
        "away_name": away.get("name", "N/A"),
        "away_slug": away_slug,
        "start_date": start_date,
        "start_time": start_time,
    }

def scrape_fixtures(country_slug: str, league_slug: str) -> dict | None:
    url  = f"https://www.flashscore.co.uk/football/{country_slug}/{league_slug}/fixtures/"
    html = fetch(url)
    if not html:
        return None

    m_lid = LEAGUE_ID_RX.search(html)
    if not m_lid:
        return None
    league_id = m_lid.group(1)

    match_ids     = MATCH_ID_RX.findall(html)
    round_numbers = ROUND_RX.findall(html)
    if len(match_ids) != len(round_numbers):
        length = min(len(match_ids), len(round_numbers))
        match_ids, round_numbers = match_ids[:length], round_numbers[:length]

    round_map: "OrderedDict[str, list[str]]" = OrderedDict()
    for rnd, mid in zip(round_numbers, match_ids):
        round_map.setdefault(rnd, []).append(mid)

    rounds = []
    for rnd, mids in round_map.items():
        with ThreadPoolExecutor(MATCH_THREADS_PER_ROUND) as mpool:
            futs = {mpool.submit(fetch_match_details, mid): mid for mid in mids}
            match_objs = [f.result() for f in as_completed(futs) if f.result()]
        rounds.append({"round": rnd, "matches": match_objs})

    return {"league_id": league_id, "rounds": rounds}

def scrape_country(cid: int) -> dict | None:
    html = fetch(COUNTRY_URL.format(cid))
    if not html:
        return None

    mc = COUNTRY_RX.search(html)
    if not mc:
        return None
    country_name = mc.group(1)
    country_slug = slugify(country_name)

    leagues_raw = LEAGUE_RX.findall(html)
    if not leagues_raw:
        return None

    leagues = []
    for league_name, league_slug in leagues_raw:
        fixtures = scrape_fixtures(country_slug, league_slug)
        if fixtures:
            leagues.append({
                "league_id": fixtures["league_id"],
                "name": league_name,
                "slug": f"{country_slug}/{league_slug}",
                "fixtures": fixtures
            })

    return {"country": country_name, "leagues": leagues} if leagues else None

def scrape_all_countries() -> list[dict]:
    dataset: list[dict] = []
    with ThreadPoolExecutor(MAX_THREADS) as pool:
        futs = {pool.submit(scrape_country, cid): cid for cid in range(1, MAX_ID + 1)}
        for fut in tqdm(as_completed(futs), total=MAX_ID, desc="Countries", leave=False):
            data = fut.result()
            if data:
                dataset.append(data)
    return dataset

def flatten_for_pinecone(nested_data: list[dict]) -> list[dict]:
    flat_list = []
    for country_obj in nested_data:
        country = country_obj.get("country")
        for league_obj in country_obj.get("leagues", []):
            league_id = league_obj.get("league_id")
            league_name = league_obj.get("name")
            league_slug = league_obj.get("slug")
            fixtures = league_obj.get("fixtures", {})
            rounds = fixtures.get("rounds", [])
            for round_obj in rounds:
                round_name = round_obj.get("round")
                for match in round_obj.get("matches", []):
                    flat_record = {
                        "country": country,
                        "league_id": league_id,
                        "league_name": league_name,
                        "league_slug": league_slug,
                        "fixture": {
                            "round": round_name,
                            "match_id": match.get("match_id"),
                            "home_id": match.get("home_id"),
                            "home_name": match.get("home_name"),
                            "home_slug": match.get("home_slug"),
                            "away_id": match.get("away_id"),
                            "away_name": match.get("away_name"),
                            "away_slug": match.get("away_slug"),
                            "start_date": match.get("start_date"),
                            "start_time": match.get("start_time"),
                        }
                    }
                    flat_list.append(flat_record)
    return flat_list

app = FastAPI(title="Flashscore Countries‑Leagues‑Fixtures API")

@app.get("/countries")
def get_countries():
    nested_data = scrape_all_countries()
    flat_data = flatten_for_pinecone(nested_data)
    return flat_data

if __name__ == "__main__":
    nested_data = scrape_all_countries()
    flat_data = flatten_for_pinecone(nested_data)
    export_to_google_sheet_with_update(
        flat_data,
        sheet_name_main="Flashscore Matches",
        sheet_name_finished="Finished Matches",
        creds_file="flashscore-scraper-i7-0e2298df8e96.json"
    )