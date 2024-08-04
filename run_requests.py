import json
import requests
import pandas as pd
from prev_match_results import vlr_match_results, vlr_stats_events
from utils import events, regions1, regions2

def get_news(file_name):

    url = 'https://vlrggapi.vercel.app/news'
    response = requests.get(url)

    print(response)

    data = response.json()
    with open(f"json_files/{file_name}.json", "w", encoding='utf-8') as f:
        json.dump(data["data"]["segments"], f, ensure_ascii=False)



def get_player_stats(region, timespan):

    url = f'https://vlrggapi.vercel.app/stats/{region}/{timespan}'
    response = requests.get(url)

    print(response)

    data = response.json()
    with open(f"json_files/{region}_player_stats.json", "w", encoding='utf-8') as f:
        json.dump(data["data"]["segments"], f)
    


def get_region_stats(region):

    url = f'https://vlrggapi.vercel.app/rankings/{region}'
    response = requests.get(url)

    print(response)

    data = response.json()
    with open(f"json_files/{region}_rankings_stats.json", "w", encoding='utf-8') as f:
        json.dump(data["data"], f)
    


def get_match_stats(file_name, query):

    url = f'https://vlrggapi.vercel.app/match?q={query}'
    response = requests.get(url)

    print(response)

    data = response.json()
    with open(f"json_files/{file_name}.json", "w", encoding='utf-8') as f:
        json.dump(data["data"]["segments"], f)
    


for i in regions1:
    get_region_stats(i)

for i in regions2:
    get_player_stats(i, 90)

get_news('news_data')

get_match_stats('match_data', 'results')

for i in events:
    vlr_match_results(i)
    vlr_stats_events(i)