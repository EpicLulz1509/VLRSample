import streamlit as st
import altair as alt
import plotly.express as px
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from utils import * 
import plotly.express as px
# from configure_tables import *

st.set_page_config(
    page_title="Valorant Champions 2024",
    layout="wide",
    initial_sidebar_state="collapsed")

# alt.themes.enable("dark")
alt.renderers.enable("mimetype")
# champion_stats()
df = pd.read_csv('player_stats_champions.csv', usecols=["player", "org", "region", "rounds", "rating", "ACS", "KD", "KAST", "ADR", "KPR", "APR",	"FKPR",	"FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD"])

df = df.style.set_properties(**{'font-size': '500pt'}).background_gradient(cmap='viridis').highlight_max(axis=0, subset=['rounds', 'rating', "ACS", "KD", "KAST", "ADR", "KPR", "APR",	"FKPR",	"FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD"])

st.subheader("Stats for Valorant Champions 2024")
st.dataframe(df, height=800, width=1800)


# st.subheader("Choose the parameters you will be needing")

# col1, col2 = st.columns(2)
# with col1:
#     regions = st.multiselect(
#         "Choose event: ",
#         regions_select,
#         ["NA"]
#     )

# with col2:
#     events = st.multiselect(
#         "Choose event: ",
#         events_select,
#         ["KICKOFF"]
#     )

# print(events, regions)

# regions_str = (", ".join(regions))
# events_str = (", ".join(events))

# st.subheader(f"Stats for {regions_str} region and {events_str} event")

# stats = specific_param_stats(events, regions)
stats = pd.read_csv('player_stats.csv', usecols=["player", "org", "rounds", "rating", "ACS", "KD", "KAST", "ADR", "KPR", "APR", "FKPR", "FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD", "region", "event"])

# stats = stats.rename(columns={'assists' : 'ASSISTS', 'assists_per_round' : 'APR', 'average_combat_score' : 'ACS', 'average_damage_per_round' : 'ADR', 'clutch_success_percentage' : 'CS', 'deaths' : 'DEATHS', 'fd' : 'FD', 'kills' : 'KILLS', 'fk' : 'FK', 'headshot_percentage': 'HS', 'first_kills_per_round': 'FKPR', 'kill_assists_survived_traded': 'KAST', 'kill_deaths': 'KD', 'kmax': 'KMAX', 'kills_per_round': 'KPR', 'first_deaths_per_round': 'FDPR' }, errors="raise")

# cols = ["player", "org", "region", "event", "rounds", "rating", "ACS", "KD", "KAST", "ADR", "KPR", "APR","FKPR","FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD"]
# stats = stats.reindex(columns=cols)

# print(stats.columns, "HERE")
st.subheader("Stats for Valorant Champions Tour 2024")
stats = stats.style.set_properties(**{'font-size': '500pt'}).background_gradient(cmap='viridis').highlight_max(axis=0, subset=['rounds', 'rating', "ACS", "KD", "KAST", "ADR", "KPR", "APR","FKPR",	"FDPR",	"HS", "CS",	"KMAX",	"KILLS", "DEATHS", "ASSISTS", "FK",	"FD"])
st.dataframe(stats, height=800, width=1800)

# print(stats) 

print(type(stats))

def bar_graph(df):

    fig = px.bar(stats, x = "player", y = "ACS", title = "Sorting players based upon ACS")
    return fig


def render_graphs():
    col = st.columns((1), gap='medium')

    with col[0]:
        plot = bar_graph(stats)
        st.plotly_chart(plot)

render_graphs()