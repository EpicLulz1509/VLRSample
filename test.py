import pandas as pd


df = pd.read_csv('player_stats.csv', usecols=["player", "org", "region", "event", "rounds", "rating", "ACS", "KD", "KAST", "ADR", "KPR", "APR",	"FKPR",	"FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD"])

print(df)