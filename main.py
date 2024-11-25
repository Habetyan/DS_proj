import asyncio
from parse_understat import scrape_team_links_and_statistics
from team_points import process_team_data
import os

if __name__ == "__main__":
     asyncio.run(scrape_team_links_and_statistics())

csv_file = './pl-tables-1993-2024.csv'
current_dir = os.getcwd()

# Process the data
team_data = process_team_data(csv_file, current_dir)

# Print the resulting DataFrame
print(team_data)
