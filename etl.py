import os
import pandas as pd
import asyncio
from team_points import process_team_data
from parse_understat import scrape_team_links_and_statistics
from parse_whoscored import scrape_table

# Run WhoScored
scrape_table
def get_team_dirs(base_dir):
    """
    Return a list of team directory names in the base directory.
    """
    return [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
           and not d.startswith('.') and not d.startswith('_')
    ]

def process_attack_speed(base_dir):
    """
    Process attackSpeed.csv for all teams to extract the total shots, goals,
    and expected goals for each type of attack speed.
    """
    all_teams_data = []
    for team in get_team_dirs(base_dir):
        team_path = os.path.join(base_dir, team, "attackSpeed.csv")
        try:
            attack_speed_data = pd.read_csv(team_path)
            team_stats = {"team": team}
            for speed in ["Normal", "Standard", "Slow", "Fast"]:
                speed_data = attack_speed_data[attack_speed_data["stat"] == speed]
                if not speed_data.empty:
                    team_stats[f"{speed.lower()}_shots"] = speed_data["shots"].values[0]
                    team_stats[f"{speed.lower()}_goals"] = speed_data["goals"].values[0]
                    team_stats[f"{speed.lower()}_xg"] = speed_data["xG"].values[0]
                else:
                    team_stats[f"{speed.lower()}_shots"] = 0
                    team_stats[f"{speed.lower()}_goals"] = 0
                    team_stats[f"{speed.lower()}_xg"] = 0
            all_teams_data.append(team_stats)
        except FileNotFoundError:
            print(f"attackSpeed.csv not found for team: {team}")
    return pd.DataFrame(all_teams_data)

def process_team_formation(base_dir):
    """
    Process formation.csv for all teams to determine their favorite tactics.
    """
    all_teams_data = []
    for team in get_team_dirs(base_dir):
        formation_path = os.path.join(base_dir, team, "formation.csv")
        try:
            formation_data = pd.read_csv(formation_path)
            favorite_tactics = formation_data.loc[formation_data["time"].idxmax(), "stat"]
            all_teams_data.append({"team": team, "favorite_tactics": favorite_tactics})
        except (FileNotFoundError, ValueError):
            print(f"formation.csv not found or empty for team: {team}")
            all_teams_data.append({"team": team, "favorite_tactics": None})
    return pd.DataFrame(all_teams_data)

def process_team_game_state(base_dir):
    """
    Process gameState.csv for all teams to extract total time winning, losing, drawing.
    """
    all_teams_data = []
    for team in get_team_dirs(base_dir):
        game_state_path = os.path.join(base_dir, team, "gameState.csv")
        try:
            game_state_data = pd.read_csv(game_state_path)
            winning_time = game_state_data.loc[
                game_state_data["stat"].isin(["Goal diff +1", "Goal diff > +1"]),
                "time"
            ].sum()
            losing_time = game_state_data.loc[
                game_state_data["stat"].isin(["Goal diff -1", "Goal diff < -1"]),
                "time"
            ].sum()
            draw_time = game_state_data.loc[
                game_state_data["stat"] == "Goal diff 0",
                "time"
            ].sum()
            all_teams_data.append({
                "team": team,
                "winning_time": winning_time,
                "losing_time": losing_time,
                "draw_time": draw_time
            })
        except (FileNotFoundError, ValueError):
            print(f"gameState.csv not found or empty for team: {team}")
            all_teams_data.append({
                "team": team,
                "winning_time": 0,
                "losing_time": 0,
                "draw_time": 0
            })
    return pd.DataFrame(all_teams_data)

def process_team_form(base_dir):
    """
    Process matches.csv for all teams to calculate rolling form for the last 5 matches.
    """
    all_teams_data = []
    for team in get_team_dirs(base_dir):
        matches_path = os.path.join(base_dir, team, "matches.csv")
        try:
            matches_data = pd.read_csv(matches_path)
            matches_data["Date"] = pd.to_datetime(matches_data["Date"], format="%b %d, %Y")
            matches_data = matches_data.sort_values("Date")
            # Assign points
            matches_data["points"] = matches_data.apply(
                lambda row: 3 if (row["Home Score"] - row["Away Score"]) > 0
                else 1 if (row["Home Score"] - row["Away Score"]) == 0
                else 0,
                axis=1
            )
            matches_data["form"] = matches_data["points"].rolling(window=5).sum()
            latest_form = matches_data.iloc[-1]["form"] if not matches_data.empty else 0
            all_teams_data.append({"team": team, "form": latest_form})
        except (FileNotFoundError, ValueError):
            print(f"matches.csv not found or empty for team: {team}")
            all_teams_data.append({"team": team, "form": 0})
    return pd.DataFrame(all_teams_data)

def process_team_squad_size(base_dir):
    """
    Process section_2.csv for all teams to determine the squad size.
    """
    all_teams_data = []
    for team in get_team_dirs(base_dir):
        section_path = os.path.join(base_dir, team, "section_2.csv")
        try:
            section_data = pd.read_csv(section_path)
            max_min = section_data["Min"].max()
            threshold = 0.3 * max_min
            squad_size = section_data[section_data["Min"] > threshold].shape[0]
            all_teams_data.append({"team": team, "squad_size": squad_size})
        except (FileNotFoundError, ValueError, KeyError):
            print(f"section_2.csv not found or empty for team: {team}")
            all_teams_data.append({"team": team, "squad_size": 0})
    return pd.DataFrame(all_teams_data)

def process_all_data(base_dir):
    """
    Process all team data and merge into a single DataFrame.
    """
    attack_speed_df = process_attack_speed(base_dir)
    formation_df = process_team_formation(base_dir)
    game_state_df = process_team_game_state(base_dir)
    form_df = process_team_form(base_dir)
    squad_size_df = process_team_squad_size(base_dir)

    # Merge all DataFrames on 'team' using outer join
    final_df = (
        attack_speed_df
        .merge(formation_df, on="team", how="outer")
        .merge(game_state_df, on="team", how="outer")
        .merge(form_df, on="team", how="outer")
        .merge(squad_size_df, on="team", how="outer")
    )
    return final_df

def get_final_merged_df(
        base_dir="./",
        pl_tables_csv="./pl-tables-1993-2024.csv",
        who_scored_csv="./premier_league_stats.csv"
):
    """
    Scrape or parse data, then process and merge everything into a final DataFrame.
    Returns final_merged_df.
    """
    # Run Understat scraping (async)
    asyncio.run(scrape_team_links_and_statistics())
    # Process team points data
    df_tp = process_team_data(csv_file=pl_tables_csv, current_dir=os.getcwd())

    # Process the other team data
    final_team_data = process_all_data(base_dir)

    team_mapping = {
        "newcastle": "newcastle_united",
        "wolves": "wolverhampton_wanderers",
        "man_utd": "manchester_united",
        "man_city": "manchester_city",
        "spurs": "tottenham"
    }

    # Normalize team names in final_team_data
    final_team_data["team"] = (
        final_team_data["team"]
        .str.lower()
        .replace(team_mapping)
    )

    # Normalize team names in df_tp
    df_tp["teams"] = (
        df_tp["teams"]
        .str.lower()
        .replace(team_mapping)
    )

    # Merge final_team_data with df_tp
    merged_df = final_team_data.merge(df_tp, left_on="team", right_on="teams", how="outer")
    merged_df.drop(columns=["teams"], inplace=True)

    # Read WhoScored data
    who_scored = pd.read_csv(who_scored_csv, encoding="utf-8", skiprows=1)
    who_scored.columns = [
        "team", "goals", "shots pg", "discipline",
        "possession", "pass%", "aerialswon", "rating"
    ]
    who_scored["team"] = (
        who_scored["team"]
        .str.replace(r"^\d+\.\s*", "", regex=True)
        .str.lower()
        .replace(team_mapping)
        .str.replace(" ", "_")
    )

    # Merge with WhoScored data
    final_merged_df = merged_df.merge(who_scored, on="team", how="outer")

    # Drop rows where 'team' is NaN and reindex
    final_merged_df = final_merged_df.dropna(subset=["team"]).reset_index(drop=True)
    # Fill some missing data
    final_merged_df.fillna(
        {
            "normal_shots": 0,
            "aerialswon": 0,
            # For 'rating', fill with the overall mean rating
            "rating": final_merged_df["rating"].mean()
        },
        inplace=True
    )

    return final_merged_df
