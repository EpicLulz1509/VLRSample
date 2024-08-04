import re
from datetime import datetime, timezone
import json
import requests
from selectolax.parser import HTMLParser
import utils as res
from utils import headers, agents_dic, events

def vlr_match_results(event):
    url = f"https://www.vlr.gg/event/matches/{event}/?series_id=all"
    resp = requests.get(url, headers=headers)
    html = HTMLParser(resp.text)
    status = resp.status_code

    result = []
    for item in html.css("a.wf-module-item"):
        url_path = item.attributes["href"]

        try:
            eta = item.css_first("div.ml-eta").text() + " ago"
        except AttributeError:
            eta = "0 ago"

        rounds = item.css_first("div.match-item-event-series").text()
        rounds = rounds.replace("\u2013", "-")
        rounds = rounds.replace("\n", " ").replace("\t", "")

        # tourney = item.css_first("div.match-item-event").text()
        # tourney = tourney.replace("\t", " ")
        # tourney = tourney.strip().split("\n")[1]
        # tourney = tourney.strip()

        # tourney_icon_url = item.css_first("img").attributes["src"]
        # tourney_icon_url = f"https:{tourney_icon_url}"

        try:
            team_array = (
                item.css_first("div.match-item-vs")
                .css_first("div:nth-child(2)")
                .text()
            )
        except Exception:  # Replace bare except with except Exception
            team_array = "TBD"
        team_array = team_array.replace("\t", " ").replace("\n", " ")
        team_array = team_array.strip().split("                                  ")
            # 1st item in team_array is first team
        team1 = team_array[0]
            # 2nd item in team_array is first team score
        score1 = team_array[1].replace(" ", "").strip()
            # 3rd item in team_array is second team
        team2 = team_array[4].strip()
            # 4th item in team_array is second team score
        score2 = team_array[-1].replace(" ", "").strip()

            # Creating a list of the classes of the flag elements.
        flag_list = [
            flag_parent.attributes["class"].replace(" mod-", "_")
            for flag_parent in item.css(".flag")
        ]
        flag1 = flag_list[0]
        flag2 = flag_list[1]

        result.append(
        {
                    "team1": team1,
                    "team2": team2,
                    "score1": score1,
                    "score2": score2,
                    "flag1": flag1,
                    "flag2": flag2,
                    "time_completed": eta,
                    "round_info": rounds,
                    # "tournament_name": tourney,
                    "match_page": url_path,
                    # "tournament_icon": tourney_icon_url,
        }
        )
    segments = {"status": status, "segments": result}

    data = {"data": segments}

    names = event.split('/')
    with open(f"json_files/{names[1]}_match_stats.json", "w", encoding='utf-8') as f:
        json.dump(result, f)

    if status != 200:
        raise Exception("API response: {}".format(status))
    return data



def vlr_stats_events(event):
        url = (f"https://www.vlr.gg/event/stats/{event}")

        resp = requests.get(url, headers=headers)
        html = HTMLParser(resp.text)
        status = resp.status_code

        result = []
        for item in html.css("tbody tr"):
            player = item.text().replace("\t", "").replace("\n", " ").strip()
            player = player.split()
            player_name = player[0]
            agents_played = []

            # get org name abbreviation via player variable
            try:
                org = player[1]
            except Exception:
                org = "N/A"

            for agn in item.css("td.mod-agents"):
                for i in agn.css("img"):
                    agents_played.append(agents_dic[i.attributes['src']])
    

            color_sq = [stats.text() for stats in item.css("td.mod-color-sq")]
            rat = color_sq[0]
            acs = color_sq[1]
            kd = color_sq[2]
            kast = color_sq[3]
            adr = color_sq[4]
            kpr = color_sq[5]
            apr = color_sq[6]
            fkpr = color_sq[7]
            fdpr = color_sq[8]
            hs = color_sq[9]
            cl = color_sq[10]

            result.append(
                {
                    "player": player_name,
                    "org": org,
                    "rating": rat,
                    "average_combat_score": acs,
                    "kill_deaths": kd,
                    "kill_assists_survived_traded": kast,
                    "average_damage_per_round": adr,
                    "kills_per_round": kpr,
                    "assists_per_round": apr,
                    "first_kills_per_round": fkpr,
                    "first_deaths_per_round": fdpr,
                    "headshot_percentage": hs,
                    "clutch_success_percentage": cl,
                    "agents_played": agents_played,
                }
            )
        segments = {"status": status, "segments": result}

        data = {"data": segments}

        # print(result)
        names = event.split('/')
        with open(f"json_files/{names[1]}_player_stats.json", "w", encoding='utf-8') as f:
            json.dump(result, f)

        if status != 200:
            raise Exception("API response: {}".format(status))
        return data

