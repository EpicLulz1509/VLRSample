from run_requests import *
from configure_tables import *
import pandas as pd

def get_results():
    for i in events:
        vlr_match_results(i)
        vlr_stats_events(i)


# def rename_columns_in_csv():

    file_path = 'full_player_stats.csv'
    column_mapping = {
        "player": "player",
        "org": "org",
        "region": "region",
        "event": "event",
        "rounds": "rounds",
        "rating": "rating",
        "average_combat_score": "ACS",
        "kill_deaths": "KD",
        "kill_assists_survived_traded": "KAST",
        "average_damage_per_round": "ADR",
        "kills_per_round": "KPR",
        "assists_per_round": "APR",
        "first_kills_per_round": "FKPR",
        "first_deaths_per_round": "FDPR",
        "headshot_percentage": "HS",
        "clutch_success_percentage": "CS",
        "kmax": "KMAX",
        "kills": "KILLS",
        "deaths": "DEATHS",
        "assists": "ASSISTS",
        "fk": "FK",
        "fd": "FD",
        "agents_played": "agents_played"
    }
    df = pd.read_csv(file_path)
    
    df.rename(columns=column_mapping, inplace=True)
    
    # Save the updated DataFrame back to the CSV file
    df.to_csv(file_path, index=False)


get_results()
