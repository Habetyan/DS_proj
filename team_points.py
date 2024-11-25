import pandas as pd
import os

def load_table(file_path):
    """
    Load the Premier League table data from a CSV file.
    """
    return pd.read_csv(file_path)

def get_directory_names(current_dir):
    """
    List all directories in the current working directory, excluding hidden ones.
    """
    return [
        d for d in os.listdir(current_dir)
        if os.path.isdir(os.path.join(current_dir, d)) and not d.startswith('.')
    ]

def normalize_team_names(df, team_column='team'):
    """
    Normalize team names by replacing spaces with underscores and converting to lowercase.
    """
    df[team_column] = df[team_column].str.replace(' ', '_').str.lower()
    return df

def compute_average_points(table_df, season_cutoff):
    """
    Compute the average points for teams after a specified season cutoff.
    """
    return (
        table_df.loc[table_df['season_end_year'] > season_cutoff]
        .groupby('team')['points']
        .mean()
    )

def apply_team_name_corrections(points_series, corrections):
    """
    Apply corrections to team names in a points series.
    """
    return points_series.rename(index=corrections)

def map_points_to_teams(directory_names, points_2014, points_2019):
    """
    Create a DataFrame with directory names and map points from 2014 and 2019 to teams.
    """
    # Create a DataFrame for team directories
    df = pd.DataFrame({'teams': directory_names})

    # Normalize team names
    df['teams'] = df['teams'].str.replace(' ', '_').str.lower()

    # Map points to teams
    df['points_last_5'] = df['teams'].map(points_2019)
    df['points_last_10'] = df['teams'].map(points_2014)

    return df

def process_team_data(csv_file, current_dir):
    """
    Main function to process team data and return a DataFrame with mapped points.
    """
    # Load table data
    table_df = load_table(csv_file)

    # Normalize team names in the table
    table_df = normalize_team_names(table_df)

    # Compute points for seasons after 2014 and 2019
    points_2014 = compute_average_points(table_df, 2014)
    points_2019 = compute_average_points(table_df, 2019)

    # Apply team name corrections
    team_name_corrections = {
        'manchester_utd': 'manchester_united',
        'newcastle_utd': 'newcastle_united',
        'wolves': 'wolverhampton_wanderers',
        'leicester_city': 'leicester',
        'ipswich_town': 'ipswich',
    }
    points_2014 = apply_team_name_corrections(points_2014, team_name_corrections)
    points_2019 = apply_team_name_corrections(points_2019, team_name_corrections)

    # Get directory names
    directory_names = get_directory_names(current_dir)

    # Map points to teams
    return map_points_to_teams(directory_names, points_2014, points_2019)

