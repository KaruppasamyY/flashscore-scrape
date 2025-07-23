import pandas as pd
import requests
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

MATCH_SHEET = "Flashscore Matches"
LINEUP_SHEET = "Flashscore Lineups"
FINISHED_SHEET = "Finished Matches"
CREDENTIALS_FILE = "flashscore-scraper-i7-0e2298df8e96.json"

API_TEMPLATE = "https://5.ds.lsapp.eu/pq_graphql?_hash=dlie2&eventId={}&projectId=5"

POLL_INTERVAL = 300
TIMEOUT = 1800

def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(credentials)

def load_matches_from_sheet():
    client = get_gspread_client()
    sheet = client.open(MATCH_SHEET).sheet1
    records = sheet.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        return df

    df['start_datetime'] = pd.to_datetime(
        df['fixture.start_date'] + ' ' + df['fixture.start_time'], format='%d/%m/%Y %H:%M'
    )
    df['match_id'] = df['fixture.match_id'].astype(str)

    today = datetime.now().date()
    df = df[df['start_datetime'].dt.date == today]

    if 'is_published' in df.columns:
        df = df[df['is_published'].astype(str).str.lower() != 'true']

    return df

def fetch_lineup(match_id):
    url = API_TEMPLATE.format(match_id)
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"[{match_id}] Failed API request: {response.status_code}")
            return None
        data = response.json()
        return data.get("data", {}).get("findEventById", {}).get("eventParticipants", [])
    except Exception as e:
        print(f"[{match_id}] Exception during fetch: {e}")
        return None

def parse_lineup(match_id, participants, start_date, fixture_start_time):
    rows = []
    for team in participants:
        team_name = team.get("name", "Unknown Team")
        players = team.get("lineup", {}).get("players", [])
        if not players:
            continue

        for i, player in enumerate(players):
            is_starter = i < 11
            rating = player.get("rating") or {}
            incidents = player.get("incidents", [])
            minute = "—"
            if isinstance(incidents, list) and incidents:
                minute = incidents[0].get("incident", {}).get("minute", "—")

            row = {
                "match_id": match_id,
                "start_date": start_date,
                "start_time": fixture_start_time,
                "team": team_name,
                "type": "Starting" if is_starter else "Substitute",
                "number": player.get("number", "N/A"),
                "list_name": player.get("listName", "N/A"),
                "field_name": player.get("fieldName", "N/A"),
                "player_id": player.get("id", "N/A"),
            }
            rows.append(row)
    return rows

def save_lineup_to_google_sheet(lineup_rows):
    if not lineup_rows:
        return

    client = get_gspread_client()
    sheet = client.open(LINEUP_SHEET).sheet1

    df_new = pd.DataFrame(lineup_rows)
    df_new["match_id"] = df_new["match_id"].astype(str)
    df_new["player_id"] = df_new["player_id"].astype(str)

    existing_values = sheet.get_all_values()
    if existing_values:
        df_existing = pd.DataFrame(existing_values[1:], columns=existing_values[0])
        df_existing["match_id"] = df_existing["match_id"].astype(str)
        df_existing["player_id"] = df_existing["player_id"].astype(str)

        merged = df_new.merge(df_existing[["match_id", "player_id"]], on=["match_id", "player_id"], how="left", indicator=True)
        df_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    if df_new.empty:
        print("Success: No new lineup data to append (all entries already exist).")
        return

    values_to_append = [df_new.columns.tolist()] + df_new.values.tolist()
    
    if not existing_values:
        sheet.append_rows(values_to_append, value_input_option="RAW")
    else:
        sheet.append_rows(values_to_append[1:], value_input_option="RAW")

    print(f"Success: Lineup data saved to Google Sheet ({len(df_new)} new rows).")

def mark_match_as_published(match_id):
    client = get_gspread_client()
    sheet = client.open(MATCH_SHEET).sheet1
    records = sheet.get_all_records()
    headers = sheet.row_values(1)

    if 'is_published' not in headers:
        sheet.update_cell(1, len(headers) + 1, 'is_published')
        headers.append('is_published')

    is_published_col_idx = headers.index('is_published') + 1

    for i, row in enumerate(records):
        if str(row.get('fixture.match_id')) == str(match_id):
            row_number = i + 2
            sheet.update_cell(row_number, is_published_col_idx, 'TRUE')
            print(f"Success: Marked match_id {match_id} as published.")
            break

def move_published_match_to_finished(match_id):
    client = get_gspread_client()
    sheet_main = client.open(MATCH_SHEET).sheet1
    sheet_finished = client.open(FINISHED_SHEET).sheet1

    df_main = pd.DataFrame(sheet_main.get_all_records())
    df_finished = pd.DataFrame(sheet_finished.get_all_records())

    if "is_published" not in df_main.columns:
        print("Alert: 'is_published' column missing in Flashscore Matches sheet.")
        return

    match_to_move = df_main[df_main["fixture.match_id"].astype(str) == str(match_id)]
    if match_to_move.empty:
        print(f"Alert: Match ID {match_id} not found in main sheet.")
        return

    df_main = df_main[df_main["fixture.match_id"].astype(str) != str(match_id)]

    df_finished = pd.concat([df_finished, match_to_move], ignore_index=True)
    df_finished.drop_duplicates(subset=["fixture.match_id"], inplace=True)

    sheet_main.clear()
    sheet_main.append_row(df_main.columns.tolist())
    if not df_main.empty:
        sheet_main.append_rows(df_main.fillna("").astype(str).values.tolist())

    sheet_finished.clear()
    sheet_finished.append_row(df_finished.columns.tolist())
    if not df_finished.empty:
        sheet_finished.append_rows(df_finished.fillna("").astype(str).values.tolist())

    print(f"Success: Moved match_id {match_id} ➜ Finished Matches sheet.")

def remove_duplicate_lineups_from_sheet():
    print("Cleaning duplicates from Flashscore Lineups sheet...")
    client = get_gspread_client()
    sheet = client.open(LINEUP_SHEET).sheet1

    existing_values = sheet.get_all_values()
    if not existing_values:
        print("Lineup sheet is empty. Nothing to clean.")
        return

    headers = existing_values[0]
    data = existing_values[1:]

    df = pd.DataFrame(data, columns=headers)

    if "match_id" not in df.columns or "player_id" not in df.columns:
        print("Alert: Missing 'match_id' or 'player_id' column. Cannot de-duplicate.")
        return

    df["match_id"] = df["match_id"].astype(str)
    df["player_id"] = df["player_id"].astype(str)

    before = len(df)
    df = df.drop_duplicates(subset=["match_id", "player_id"], keep="first")
    after = len(df)

    if before == after:
        print("Success: No duplicates found.")
        return

    print(f"Removed {before - after} duplicate rows.")

    # Clear and rewrite the cleaned sheet
    sheet.clear()
    sheet.append_row(headers)
    sheet.append_rows(df.fillna("").astype(str).values.tolist())
    
def sync_is_published_in_finished_sheet():
    print("Syncing is_published status in Finished Matches sheet...")

    client = get_gspread_client()
    sheet_finished = client.open(FINISHED_SHEET).sheet1
    sheet_lineup = client.open(LINEUP_SHEET).sheet1

    finished_values = sheet_finished.get_all_values()
    if not finished_values or len(finished_values) < 2:
        print("Alert: Finished Matches sheet is empty.")
        return

    finished_headers = finished_values[0]
    finished_data = finished_values[1:]
    df_finished = pd.DataFrame(finished_data, columns=finished_headers)

    # Add column if missing
    if 'is_published' not in df_finished.columns:
        df_finished['is_published'] = 'FALSE'
        finished_headers.append('is_published')

    df_finished["fixture.match_id"] = df_finished["fixture.match_id"].astype(str)

    lineup_values = sheet_lineup.get_all_values()
    if not lineup_values or len(lineup_values) < 2:
        print("Alert: Lineup sheet is empty.")
        return

    lineup_headers = lineup_values[0]
    lineup_data = lineup_values[1:]
    df_lineup = pd.DataFrame(lineup_data, columns=lineup_headers)
    df_lineup["match_id"] = df_lineup["match_id"].astype(str)

    # Match presence
    lineup_ids = set(df_lineup["match_id"].unique())

    df_finished["is_published"] = df_finished["fixture.match_id"].apply(
        lambda x: "TRUE" if x in lineup_ids else "FALSE"
    )

    # Write back to sheet
    sheet_finished.clear()
    sheet_finished.append_row(finished_headers)
    sheet_finished.append_rows(df_finished.fillna("").astype(str).values.tolist())

    print(f"Success: Synced is_published for {len(df_finished)} finished matches.")

def main():
    print("Starting: Starting Lineup Scraper...")
    script_start_time = time.time()
    processed_matches = set()

    while True:
        matches = load_matches_from_sheet()
        
        if matches.empty:
            print("Not found: No matches to process.")
        else:
            print(f"Found: Found {len(matches)} unprocessed matches.")
            
        for _, match in matches.iterrows():
            match_id = match['match_id']
            if match_id in processed_matches:
                continue

            print(f"Checking lineup for match_id: {match_id}")
            lineup = fetch_lineup(match_id)
            if not lineup:
                continue

            has_players = any(p.get("lineup", {}).get("players") for p in lineup)
            if not has_players:
                print(f"[{match_id}] No lineup yet. Will retry.")
                continue

            start_date = match['fixture.start_date']
            fixture_start_time = match['fixture.start_time']
            lineup_rows = parse_lineup(match_id, lineup, start_date, fixture_start_time)

            if lineup_rows:
                save_lineup_to_google_sheet(lineup_rows)
                mark_match_as_published(match_id)
                move_published_match_to_finished(match_id)
                processed_matches.add(match_id)
                print(f"Success: Lineup fetched and stored for match {match_id}.")
            else:
                print(f"[{match_id}] Lineup structure returned but no players found.")

        if len(processed_matches) == len(matches):
            print("Success: All lineups fetched.")
            break

        if time.time() - script_start_time > TIMEOUT:
            print("Timeout reached. Stopping polling.")
            break
        
        remove_duplicate_lineups_from_sheet()
        sync_is_published_in_finished_sheet()

        print(f"Waiting {POLL_INTERVAL//60} minutes before next poll...")
        time.sleep(POLL_INTERVAL)
        
if __name__ == "__main__":
    main()