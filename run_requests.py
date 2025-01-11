import json
import requests
import pandas as pd
from prev_match_results import vlr_match_results, vlr_stats_events
from utils_events import events
from bs4 import BeautifulSoup

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
        json.dump(data, f)
    


def get_match_ids(event):
    url = f"https://www.vlr.gg/event/matches/{event}/?series_id=all"
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    match_links = soup.find_all('a', href=True)
    match_ids = []
    for link in match_links:
        str = link['href'][1:7]
        if str.isnumeric():
            # match_id = link['href'].split('/')[2]
            match_id = str
            match_ids.append(match_id)
    return match_ids


# for i in regions1:
#     get_region_stats(i)

# for i in regions2:
#     get_player_stats(i, 90)

# get_news('news_data')

# get_match_stats('match_data', 'results')

for i in events:
    print(i)
    print(get_match_ids(i))
#     vlr_match_results(i)
#     vlr_stats_events(i)

