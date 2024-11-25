import asyncio
import os
from playwright.async_api import async_playwright
import json
import pandas as pd
import re


async def parse_team_statistics_and_matches(team_url, team_name):
    """
    Parse team statistics, match data, and table sections from the given team URL.

    This function navigates to the team's page, waits for the page to load,
    extracts script data for JSON statistics, parses the table sections,
    and extracts match data.

    :param team_url: The URL of the team's page
    :param team_name: The name of the team
    :return: A tuple of two dictionaries: team statistics by category and match data.
    """
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

        # Extract JSON statistics data
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

        print("Extracting statistics data...")
        json_text = re.search(r'var statisticsData = JSON\.parse\((.*)\);', statistics_data_script).group(1)
        decoded_json = bytes(json_text.strip("'"), "utf-8").decode("unicode_escape")
        statistics_data = json.loads(decoded_json)

        # Process JSON statistics
        print("Processing statistics...")
        data_frames = {}
        for category, stats in statistics_data.items():
            rows = []
            for stat_name, stat_values in stats.items():
                row = {"Statistic": stat_name, **stat_values, **stat_values.get("against", {})}
                del row["against"]
                rows.append(row)
            df = pd.DataFrame(rows)
            data_frames[category] = df

        # Parse the table sections
        print("Parsing table sections...")
        table_sections = await parse_team_statistics_table(page)

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

        # Combine data
        combined_data = {
            "json_statistics": data_frames,
            "table_sections": table_sections
        }

        # Return processed data
        return combined_data, match_df


async def parse_team_statistics_table(page):
    """
    Parse distinct sections of the statistics table from the current page.

    :param page: The Playwright page object.
    :return: A dictionary of Pandas DataFrames for each table section.
    """
    print("Parsing statistics table...")

    # Wait for the table to load
    await page.wait_for_selector("table tbody tr", timeout=10000)

    # Locate table headers and rows
    header_selector = "table thead tr"
    headers_list = await page.locator(header_selector).all_inner_texts()
    print(f"Headers list: {headers_list}")

    # Split headers into individual column names
    headers_split = [header.split("\t") for header in headers_list]
    print(f"Split headers: {headers_split}")

    # Locate all rows
    row_selector = "table tbody tr"
    rows = page.locator(row_selector)
    row_count = await rows.count()
    print(f"Number of rows: {row_count}")

    # Process each table section
    sections = {}
    for section_idx, headers in enumerate(headers_split):
        print(f"Processing section {section_idx + 1} with headers: {headers}")

        valid_rows = []
        for i in range(row_count):
            row = rows.nth(i)
            cells = await row.locator("td").all_inner_texts()

            # Match rows only if the number of cells matches the headers in this section
            if len(cells) == len(headers):
                valid_rows.append(cells)

        # Create a DataFrame for the section
        if valid_rows:
            section_df = pd.DataFrame(valid_rows, columns=headers)
            sections[f"section_{section_idx + 1}"] = section_df
        else:
            print(f"No valid rows found for section {section_idx + 1}.")

    return sections


async def scrape_team_links_and_statistics():
    """
    Scrape team links and statistics from the Understat EPL page.

    This function navigates to the Understat EPL page, waits for the table to load,
    locates all team links, and processes the first 20 links. For each team, it
    extracts team statistics, table sections, and match data, saves the data to CSV files.

    :return: None
    """
    league_url = 'https://understat.com/league/EPL'

    async with async_playwright() as p:
        # Launch the browser
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

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
            combined_data, match_data = await parse_team_statistics_and_matches(team_url, team_name.strip())

            # Create a directory for the team
            team_dir = team_name.strip().replace(" ", "_")
            if not os.path.exists(team_dir):
                os.makedirs(team_dir)

            # Save JSON statistics data
            json_statistics = combined_data["json_statistics"]
            if json_statistics:
                for category, df in json_statistics.items():
                    file_name = os.path.join(team_dir, f"{category}.csv")
                    df.to_csv(file_name, index=False)
                    print(f"Saved {file_name}")

            # Save table sections
            table_sections = combined_data["table_sections"]
            if table_sections:
                for section_name, section_df in table_sections.items():
                    section_file = os.path.join(team_dir, f"{section_name}.csv")
                    section_df.to_csv(section_file, index=False)
                    print(f"Saved {section_file}")

            # Save match data
            if match_data is not None and not match_data.empty:
                match_file_name = os.path.join(team_dir, "matches.csv")
                match_data.to_csv(match_file_name, index=False)
                print(f"Saved {match_file_name}")

        # Close the browser
        await browser.close()
        print("Scraping complete.")


if __name__ == "__main__":
    asyncio.run(scrape_team_links_and_statistics())
