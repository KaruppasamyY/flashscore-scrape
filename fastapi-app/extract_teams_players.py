import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from selectolax.parser import HTMLParser
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_squad_data_with_slug(team_id, team_name, team_slug):
    url = f"https://www.flashscore.co.uk/team/{team_slug}/{team_id}/squad/"
    print(f"Fetching: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Fail: Failed to fetch {url}: {e}")
        return []

    html = HTMLParser(response.text)

    filter_buttons = html.css(".filter__filter")
    total_button = next(
        (btn for btn in filter_buttons if "Total" in btn.text(strip=True)),
        None
    )

    if not total_button:
        print(f"Not found: No 'Total' button for {team_name} ({team_id})")
        return []

    total_id = total_button.attributes.get("id")
    squad_container = html.css_first(f"#{total_id}-table")
    if not squad_container:
        print(f"Not found: No squad container for {team_name} ({team_id})")
        return []

    players = []

    for section in squad_container.css(".lineupTable"):
        position_div = section.css_first(".lineupTable__title")
        position = position_div.text(strip=True) if position_div else "Unknown"

        for row in section.css(".lineupTable__row"):
            name_tag = row.css_first("a.lineupTable__cell--name")
            href = name_tag.attributes.get("href") if name_tag else ""
            match = re.search(r"/player/[^/]+/([^/]+)/", href or "")
            player_id = match.group(1) if match else ""

            players.append({
                "team_id": team_id,
                "team_name": team_name,
                "team_slug": team_slug,
                "position": position,
                "player_id": player_id,
                "player_name": name_tag.text(strip=True) if name_tag else "",
                "jersey_number": row.css_first(".lineupTable__cell--jersey").text(strip=True) if row.css_first(".lineupTable__cell--jersey") else "",
                "age": row.css_first(".lineupTable__cell--age").text(strip=True) if row.css_first(".lineupTable__cell--age") else "",
                "matches_played": row.css_first(".lineupTable__cell--matchesPlayed").text(strip=True) if row.css_first(".lineupTable__cell--matchesPlayed") else "0",
                "total_minutes_played": row.css_first(".lineupTable__cell--minutesPlayed").text(strip=True) if row.css_first(".lineupTable__cell--minutesPlayed") else "0",
                "goals": row.css_first(".lineupTable__cell--goal").text(strip=True) if row.css_first(".lineupTable__cell--goal") else "0",
                "assists": row.css_first(".lineupTable__cell--assist").text(strip=True) if row.css_first(".lineupTable__cell--assist") else "0",
                "yellow_cards": row.css_first(".lineupTable__cell--yellowCard").text(strip=True) if row.css_first(".lineupTable__cell--yellowCard") else "0",
                "red_cards": row.css_first(".lineupTable__cell--redCard").text(strip=True) if row.css_first(".lineupTable__cell--redCard") else "0",
            })

    print(f"Success: {len(players)} players found for {team_name} ({team_id})")
    return players

def fetch_team_players_wrapper(team):
    team_id, team_name, team_slug = team
    players = fetch_squad_data_with_slug(team_id, team_name, team_slug)
    time.sleep(0.2)
    return team_id, players

def extract_unique_teams_concurrent(
    sheet_name="Flashscore Matches",
    creds_file="flashscore-scraper-i7-0e2298df8e96.json",
    output_sheet_name="Flashscore Teams",
    max_workers=10
):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(credentials)

    match_sheet = client.open(sheet_name).sheet1
    df = pd.DataFrame(match_sheet.get_all_records())
    if df.empty:
        print("Not found: No match data found.")
        return

    home_teams = df[["fixture.home_id", "fixture.home_name", "fixture.home_slug"]].rename(columns={
        "fixture.home_id": "team_id", "fixture.home_name": "team_name", "fixture.home_slug": "team_slug"
    })
    away_teams = df[["fixture.away_id", "fixture.away_name", "fixture.away_slug"]].rename(columns={
        "fixture.away_id": "team_id", "fixture.away_name": "team_name", "fixture.away_slug": "team_slug"
    })
    all_teams = pd.concat([home_teams, away_teams], ignore_index=True)
    unique_teams = all_teams.drop_duplicates("team_id").sort_values("team_name").reset_index(drop=True)

    try:
        team_sheet = client.open(output_sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        team_sheet = client.create(output_sheet_name).sheet1
        team_sheet.append_row([
            "team_id", "team_name", "team_slug", "position", "player_id", "player_name",
            "jersey_number", "age", "matches_played", "total_minutes_played",
            "goals", "assists", "yellow_cards", "red_cards"
        ])

    sheet_data = team_sheet.get_all_values()
    headers = sheet_data[0]
    existing_df = pd.DataFrame(sheet_data[1:], columns=headers)

    teams = [(str(row["team_id"]), row["team_name"], row["team_slug"]) for _, row in unique_teams.iterrows()]

    print(f"Starting concurrent fetch for {len(teams)} teams...")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_team = {executor.submit(fetch_team_players_wrapper, team): team for team in teams}
        for future in as_completed(future_to_team):
            team_id, players = future.result()
            results.append((team_id, players))
            print(f"Fetched team {team_id}, players count: {len(players)}")

    print("All teams fetched. Starting batch update...")

    all_new_players = []
    for team_id, players in results:
        all_new_players.extend(players)

    if not all_new_players:
        print("Not found: No players fetched. Exiting.")
        return

    new_df = pd.DataFrame(all_new_players).fillna("").astype(str)

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    combined_df.drop_duplicates(subset=["team_id", "player_id"], keep="last", inplace=True)

    combined_df.sort_values(by=["team_name", "player_name"], inplace=True)

    print(f"Writing {len(combined_df)} player rows to Google Sheets in batch...")
    team_sheet.clear()
    team_sheet.append_row(headers)
    team_sheet.append_rows(combined_df.values.tolist())

    print("\nSuccess: All team squads updated concurrently and batch-written!")

if __name__ == "__main__":
    extract_unique_teams_concurrent()