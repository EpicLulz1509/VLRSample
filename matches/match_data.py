from match_funcs import *
import json
import requests
from bs4 import BeautifulSoup

events = {
    "2097/valorant-champions-2024",
    "2095/champions-tour-2024-americas-stage-2",
    "2096/champions-tour-2024-china-stage-2",
    "2005/champions-tour-2024-pacific-stage-2",
    "2094/champions-tour-2024-emea-stage-2",
    "1999/champions-tour-2024-masters-shanghai",
    "2006/champions-tour-2024-china-stage-1",
    "2004/champions-tour-2024-americas-stage-1",
    "1998/champions-tour-2024-emea-stage-1",
    "2002/champions-tour-2024-pacific-stage-1",
    "1921/champions-tour-2024-masters-madrid",
    "1923/champions-tour-2024-americas-kickoff",
    "1926/champions-tour-2024-china-kickoff",
    "1925/champions-tour-2024-emea-kickoff",
    "1924/champions-tour-2024-pacific-kickoff"
}

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



async def all_matches():
    respone = await match_list()
    print(respone)
    

async def main():
    for i in events:
        match_ids = get_match_ids(i)
        print(i)
        event_name = i[5:].replace('champions-tour-', '')
        for j in match_ids:
            print(j)
            response = await match_by_id(f"{str(j)}")
            response = response.model_dump()
            with open(f'/Users/riker/Python/VLRValorant/jason_files_matches/{j}-{event_name}.json', 'w') as json_file:
                json.dump(response, json_file, indent=4)

    # print(response)

# Run the main function
# if __name__ == "__main__":
#     asyncio.run(main())

if __name__ == "__main__":
    asyncio.run(main())