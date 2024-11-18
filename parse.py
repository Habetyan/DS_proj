import asyncio
from playwright.async_api import async_playwright
import json
import pandas as pd
import re

async def parse_team_statistics_and_matches(team_url):
    async with async_playwright() as p:
        # Launch the browser
        browser = await p.chromium.launch(headless=False)  # Use headless=True for silent execution
        page = await browser.new_page()

        # Navigate to the team's page
        print(f"Navigating to {team_url}...")
        await page.goto(team_url, timeout=180000)

        # Wait for the page to load
        print("Waiting for the page to load...")
        await asyncio.sleep(5)

        # Extract all script tags from the page content
        content = await page.content()
        statistics_data_script = None
        for line in content.splitlines():
            if "var statisticsData" in line:
                statistics_data_script = line
                break

        if not statistics_data_script:
            print("Could not find the statistics data script.")
            await browser.close()
            return None, None

        # Extract JSON data from the JavaScript variable
        print("Extracting statistics data...")
        json_text = re.search(r'var statisticsData = JSON\.parse\((.*)\);', statistics_data_script).group(1)
        decoded_json = bytes(json_text.strip("'"), "utf-8").decode("unicode_escape")
        statistics_data = json.loads(decoded_json)

        # Process statistics
        print("Processing statistics...")
        data_frames = {}
        for category, stats in statistics_data.items():
            rows = []
            for stat_name, stat_values in stats.items():
                row = {"Statistic": stat_name, **stat_values, **stat_values.get("against", {})}
                del row["against"]  # Remove nested dictionary for simplicity
                rows.append(row)
            df = pd.DataFrame(rows)
            data_frames[category] = df

        # Extract match data
        print("Extracting match data...")
        matches = []
        calendar_selector = ".calendar-container .calendar-date-container"
        match_containers = await page.query_selector_all(calendar_selector)

        for container in match_containers:
            date_element = await container.query_selector(".calendar-date")
            date_text = await date_element.inner_text() if date_element else None

            game_element = await container.query_selector(".calendar-game")
            if not game_element:
                continue

            home_score = await game_element.query_selector(".team-home")
            away_score = await game_element.query_selector(".team-away")
            opponent_element = await game_element.query_selector(".team-title a")

            if home_score and away_score and opponent_element:
                home_score_text = await home_score.inner_text()
                away_score_text = await away_score.inner_text()
                opponent = await opponent_element.inner_text()
                matches.append({
                    "Date": date_text,
                    "Opponent": opponent,
                    "Home Score": home_score_text,
                    "Away Score": away_score_text,
                })

        match_df = pd.DataFrame(matches)

        # Close the browser
        await browser.close()

        # Return processed data
        return data_frames, match_df


async def scrape_team_links_and_statistics():
    league_url = 'https://understat.com/league/EPL'

    async with async_playwright() as p:
        # Launch the browser
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # Navigate to the main EPL page
        print(f"Navigating to {league_url}...")
        await page.goto(league_url, timeout=180000)

        # Wait for the table to load
        print("Waiting for the table to load...")
        await page.wait_for_selector("table tbody tr")

        # Locate all team links
        team_links = page.locator("table tbody tr td:nth-child(2) a")
        count = await team_links.count()
        print(f"Found {count} links.")

        # Process the first 20 links
        for i in range(min(20, count)):  # Process only 20 teams
            team_link = team_links.nth(i)
            team_name = await team_link.inner_text()
            team_href = await team_link.get_attribute("href")
            team_url = f"https://understat.com/{team_href}"

            print(f"Processing team: {team_name.strip()} - {team_url}")
            team_data, match_data = await parse_team_statistics_and_matches(team_url)

            # Save statistics data
            if team_data:
                for category, df in team_data.items():
                    file_name = f"{team_name.strip()}_{category}.csv"
                    df.to_csv(file_name, index=False)
                    print(f"Saved {file_name}")

            # Save match data
            if match_data is not None and not match_data.empty:
                match_file_name = f"{team_name.strip()}_matches.csv"
                match_data.to_csv(match_file_name, index=False)
                print(f"Saved {match_file_name}")

        # Close the browser
        await browser.close()
        print("Scraping complete.")

if __name__ == "__main__":
    asyncio.run(scrape_team_links_and_statistics())
