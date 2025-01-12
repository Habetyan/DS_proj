from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import csv
import time

def scrape_table():
    url = 'https://www.whoscored.com/Regions/252/Tournaments/2/Seasons/10316/Stages/23400/TeamStatistics/England-Premier-League-2024-2025'

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("Navigating to the URL...")
        page.goto(url, timeout=18000)

        print("Waiting for the page to load...")
        time.sleep(10)
        try:
            print("Waiting for the table to load...")
            page.wait_for_selector('#top-team-stats-summary-grid', timeout=6000)
        except Exception as e:
            print(f"Error: {e}")
            print("Table did not load. Exiting...")
            browser.close()
            return

        # Get the page content
        content = page.content()

        # Close the browser
        browser.close()

        # Parse the page content with BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')

        # Find the table by its ID
        table = soup.find('table', {'id': 'top-team-stats-summary-grid'})

        if table:
            print('Table found! Extracting data...')

            # Extract table headers
            headers = [th.text.strip() for th in table.find('tr').find_all('th')]
            print(headers)

            # Extract table rows
            data_rows = [
                [td.text.strip() for td in row.find_all('td')]
                for row in table.find_all('tr')[1:]  # Skip the header row
                if row.find_all('td')
            ]

            for row in data_rows:
                print(row)

            # Save the data to a CSV file
            with open('premier_league_stats.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data_rows)

            print('Data saved to premier_league_stats.csv')
        else:
            print('Table not found.')


scrape_table()