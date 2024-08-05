import re
from datetime import datetime, timezone
import json
import requests
from selectolax.parser import HTMLParser
import utils as res
from utils import headers, agents_dic, events

def vlr_stats_events(event):
        url = (f"https://www.vlr.gg/event/stats/{event}")

        resp = requests.get(url, headers=headers)
        html = HTMLParser(resp.text)
        status = resp.status_code

        result = []
        for item in html.css("tbody tr"):
            player = item.text().replace("\t", "").replace("\n", " ").strip().split(" ")
            
            player_name = player[0]
            agents_played = []
            try:
                org = player[1]
            except Exception:
                org = "N/A"

            player = list(filter(None, player))
            # player = [item for item in player if ")" not in item]
            print(player)
            print(len(player))
            # print(type(player))

            # player = item.text().replace("\t", "").replace("\n", " ").strip()
            # player = player.split()
            

            for agn in item.css("td.mod-agents"):
                for i in agn.css("img"):
                    agents_played.append(agents_dic[i.attributes['src']])
    

            color_sq = [stats.text() for stats in item.css("td.mod-color-sq")]
            rat = color_sq[0]
            acs = color_sq[1]
            kd = color_sq[2]
            kast = color_sq[3].replace("%", "")
            adr = color_sq[4]
            kpr = color_sq[5]
            apr = color_sq[6]
            fkpr = color_sq[7]
            fdpr = color_sq[8]
            hs = color_sq[9].replace("%", "")
            cl = color_sq[10].replace("%", "")
            kmax = player[len(player)-6]
            kills = player[len(player)-5]
            deaths = player[len(player)-4]
            assists = player[len(player)-3]
            fk = player[len(player)-2]
            fd = player[len(player)-1]

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
                    "kamx" : kmax,
                    "kills" : kills,
                    "deaths" : deaths,
                    "assists" : assists,
                    "fk" : fk,
                    "fd" : fd
                }
            )
            # print(result)
        segments = {"status": status, "segments": result}

        data = {"data": segments}

        # print(result)
        names = event.split('/')
        with open(f"json_files/{names[1]}_player_stats.json", "w", encoding='utf-8') as f:
            json.dump(result, f)

        if status != 200:
            raise Exception("API response: {}".format(status))
        return data

vlr_stats_events("2095/champions-tour-2024-americas-stage-2")