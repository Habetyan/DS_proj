import os
import pandas as pd
import asyncio
from team_points import process_team_data
from parse_understat import scrape_team_links_and_statistics

def get_team_dirs(base_dir):
    """
    Return a list of team directory names in the base directory.

    The function iterates through the base directory, filters out non-directory
    items and unwanted directories (e.g., '__pycache__'), and returns
    a list of valid team directory names.

    :param base_dir: The base directory containing team subdirectories.
    :return: A list of team directory names.
    """
    return [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d)) and not d.startswith('.') and not d.startswith('_')
    ]



def process_attack_speed(base_dir):

    """
    Process attackSpeed.csv for all teams to extract the total shots, goals and expected goals
    for each type of attack speed.

    This function iterates through each team's directory within the base directory,
    reads the attackSpeed.csv file, and calculates the total shots, goals, and expected
    goals for each type of attack speed. If the file is not found or an error occurs,
    the respective values default to 0 for the team.

    :param base_dir: The base directory containing team subdirectories.
    :return: A DataFrame with the team names and their respective attack speed statistics.
    """
    all_teams_data = []

    for team in get_team_dirs(base_dir):
        team_path = os.path.join(base_dir, team, "attackSpeed.csv")

        try:
            attack_speed_data = pd.read_csv(team_path)
            team_stats = {'team': team}
            for speed in ['Normal', 'Standard', 'Slow', 'Fast']:
                speed_data = attack_speed_data[attack_speed_data['stat'] == speed]
                if not speed_data.empty:
                    team_stats[f'{speed.lower()}_shots'] = speed_data['shots'].values[0]
                    team_stats[f'{speed.lower()}_goals'] = speed_data['goals'].values[0]
                    team_stats[f'{speed.lower()}_xg'] = speed_data['xG'].values[0]
                else:
                    team_stats[f'{speed.lower()}_shots'] = 0
                    team_stats[f'{speed.lower()}_goals'] = 0
                    team_stats[f'{speed.lower()}_xg'] = 0
            all_teams_data.append(team_stats)
        except FileNotFoundError:
            print(f"attackSpeed.csv not found for team: {team}")

    return pd.DataFrame(all_teams_data)


def process_team_formation(base_dir):
    """
    Process formation.csv for all teams to determine their favorite tactics.

    This function iterates through each team's directory within the base directory,
    reads the formation.csv file, and determines the favorite tactics based on the
    maximum 'time' value. If the file is not found or an error occurs, the favorite
    tactics default to None for the respective team.

    :param base_dir: The base directory containing team subdirectories.
    :return: A DataFrame with the team names and their respective favorite tactics.
    """
    all_teams_data = []

    for team in get_team_dirs(base_dir):
        formation_path = os.path.join(base_dir, team, "formation.csv")

        try:
            formation_data = pd.read_csv(formation_path)
            favorite_tactics = formation_data.loc[formation_data['time'].idxmax(), 'stat']
            all_teams_data.append({'team': team, 'favorite_tactics': favorite_tactics})
        except (FileNotFoundError, ValueError):
            print(f"formation.csv not found or empty for team: {team}")
            all_teams_data.append({'team': team, 'favorite_tactics': None})

    return pd.DataFrame(all_teams_data)


def process_team_game_state(base_dir):

    """
    Process gameState.csv for all teams to extract the total time spent
    in a winning, losing, or drawing position.

    This function iterates through each team's directory within the base directory,
    reads the gameState.csv file, and calculates the total time spent in each
    game state. The total time is calculated by summing up the 'time' column
    for each game state. If the file is not found or an error occurs, the
    respective times default to 0 for the team.

    :param base_dir: The base directory containing team subdirectories.
    :return: A DataFrame with the team names and their respective times.
    """
    all_teams_data = []

    for team in get_team_dirs(base_dir):
        game_state_path = os.path.join(base_dir, team, "gameState.csv")

        try:
            game_state_data = pd.read_csv(game_state_path)
            winning_time = game_state_data.loc[
                game_state_data['stat'].isin(['Goal diff +1', 'Goal diff > +1']), 'time'
            ].sum()
            losing_time = game_state_data.loc[
                game_state_data['stat'].isin(['Goal diff -1', 'Goal diff < -1']), 'time'
            ].sum()
            draw_time = game_state_data.loc[game_state_data['stat'] == 'Goal diff 0', 'time'].sum()
            all_teams_data.append({
                'team': team,
                'winning_time': winning_time,
                'losing_time': losing_time,
                'draw_time': draw_time
            })
        except (FileNotFoundError, ValueError):
            print(f"gameState.csv not found or empty for team: {team}")
            all_teams_data.append({'team': team, 'winning_time': 0, 'losing_time': 0, 'draw_time': 0})

    return pd.DataFrame(all_teams_data)

def process_team_form(base_dir):
    """
    Process matches.csv for all teams to calculate the rolling form.

    This function iterates through each team's directory within the base directory,
    reads the matches.csv file, and calculates the form based on the points earned
    in the last 5 matches. The form is defined as the sum of points from the last 5
    matches, where a win is 3 points, a draw is 1 point, and a loss is 0 points.
    If the file is not found or an error occurs, the form defaults to 0.

    :param base_dir: The base directory containing team subdirectories.
    :return: A DataFrame with the team names and their respective forms.
    """
    all_teams_data = []

    for team in get_team_dirs(base_dir):
        matches_path = os.path.join(base_dir, team, "matches.csv")

        try:
            matches_data = pd.read_csv(matches_path)
            matches_data['Date'] = pd.to_datetime(matches_data['Date'], format='%b %d, %Y')
            matches_data = matches_data.sort_values('Date')
            matches_data['points'] = matches_data.apply(
                lambda row: 3 if row['Home Score'] - row['Away Score'] > 0 else
                1 if row['Home Score'] - row['Away Score'] == 0 else 0,
                axis=1
            )
            matches_data['form'] = matches_data['points'].rolling(window=5).sum()
            latest_form = matches_data.iloc[-1]['form'] if not matches_data.empty else 0
            all_teams_data.append({'team': team, 'form': latest_form})
        except (FileNotFoundError, ValueError):
            print(f"matches.csv not found or empty for team: {team}")
            all_teams_data.append({'team': team, 'form': 0})

    return pd.DataFrame(all_teams_data)

def process_team_squad_size(base_dir):
    """
    Process section_2.csv for all teams to determine the squad size.

    This function iterates through each team's directory within the base directory,
    reads the section_2.csv file, and calculates the squad size based on the
    threshold of 30% of the maximum 'Min' value in the dataset. The squad size
    is defined as the number of entries where the 'Min' value exceeds this
    threshold. If the file is not found or an error occurs, the squad size
    defaults to 0 for the respective team.

    :param base_dir: The base directory containing team subdirectories.
    :return: A DataFrame containing each team and its calculated squad size.
    """

    all_teams_data = []

    for team in get_team_dirs(base_dir):
        section_path = os.path.join(base_dir, team, "section_2.csv")

        try:
            section_data = pd.read_csv(section_path)
            max_min = section_data['Min'].max()
            threshold = 0.3 * max_min
            squad_size = section_data[section_data['Min'] > threshold].shape[0]
            all_teams_data.append({'team': team, 'squad_size': squad_size})
        except (FileNotFoundError, ValueError, KeyError):
            print(f"section_2.csv not found or empty for team: {team}")
            all_teams_data.append({'team': team, 'squad_size': 0})

    return pd.DataFrame(all_teams_data)

def process_all_data(base_dir):
    """
    Process all team data and merge into a single DataFrame.

    This function calls all processing functions and merges the resulting DataFrames
    into a single DataFrame. The merge is done using an outer join on the 'team'
    column, which ensures that all teams are included even if there are missing values.

    :param base_dir: The base directory containing team subdirectories.
    :return: A single DataFrame containing all processed data.
    """
    attack_speed_df = process_attack_speed(base_dir)
    formation_df = process_team_formation(base_dir)
    game_state_df = process_team_game_state(base_dir)
    form_df = process_team_form(base_dir)
    squad_size_df = process_team_squad_size(base_dir)

    # Merge all DataFrames
    final_df = attack_speed_df.merge(formation_df, on='team', how='outer') \
        .merge(game_state_df, on='team', how='outer') \
        .merge(form_df, on='team', how='outer') \
        .merge(squad_size_df, on='team', how='outer')

    return final_df

asyncio.run(scrape_team_links_and_statistics())

base_dir = "./"

df_tp = process_team_data(csv_file='./pl-tables-1993-2024.csv', current_dir=os.getcwd())
final_team_data = process_all_data(base_dir)
final_team_data['team'] = final_team_data['team'].str.lower()
df_tp['teams'] = df_tp['teams'].str.lower()

# Merge the two DataFrames on the normalized 'team'/'teams' column
merged_df = final_team_data.merge(df_tp, left_on='team', right_on='teams', how='outer')

# Drop the duplicate 'teams' column if it exists
merged_df.drop(columns=['teams'], inplace=True)
merged_df.to_csv('final_output.csv', index=False)
