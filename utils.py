from asyncio import events


headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
}

events_select = ['KICKOFF', 'STAGE 2', 'STAGE 1', 'CHAMPIONS', 'MASTERS SHANGHAI', 'MASTERS MADRID']

regions_select = ['NA', 'EMEA', 'PACIFIC', 'CHINA', 'INT']

regions1 = {
    "na": "north-america",
    "eu": "europe",
    "ap": "asia-pacific",
    "la": "latin-america",
    "la-s": "la-s",
    "la-n": "la-n",
    "oce": "oceania",
    "kr": "korea",
    "mn": "mena",
    "gc": "game-changers",
    "br": "Brazil",
    "cn": "china",
}

regions2 = {
    "na": "north-america",
    "eu": "europe",
    "ap": "asia-pacific",
    "sa": "latin-america",
    "jp": "japan",
    "oce": "oceania",
    "mn": "mena",
}

agents_dic = {
    "/img/vlr/game/agents/jett.png": "jett",
    "/img/vlr/game/agents/sova.png" : "sova",
    "/img/vlr/game/agents/fade.png" : "fade",
    "/img/vlr/game/agents/viper.png" : "viper",
    "/img/vlr/game/agents/omen.png" : "omen",
    "/img/vlr/game/agents/killjoy.png" : "killjoy",
    "/img/vlr/game/agents/cypher.png" : "cypher",
    "/img/vlr/game/agents/sage.png" : "sage",
    "/img/vlr/game/agents/phoenix.png" : "phoenix",
    "/img/vlr/game/agents/reyna.png" : "reyna",
    "/img/vlr/game/agents/raze.png" : "raze",
    "/img/vlr/game/agents/breach.png" : "breach",
    "/img/vlr/game/agents/skye.png" : "skye",
    "/img/vlr/game/agents/yoru.png" : "yoru",
    "/img/vlr/game/agents/astra.png" : "astra",
    "/img/vlr/game/agents/kayo.png" : "kayo",
    "/img/vlr/game/agents/chamber.png" : "chamber",
    "/img/vlr/game/agents/neon.png" : "neon",
    "/img/vlr/game/agents/fade.png" : "fade",
    "/img/vlr/game/agents/harbor.png" : "harbor",
    "/img/vlr/game/agents/gekko.png" : "gekko",
    "/img/vlr/game/agents/deadlock.png" : "deadlock",
    "/img/vlr/game/agents/iso.png" : "iso",
    "/img/vlr/game/agents/clove.png" : "clove",
    "/img/vlr/game/agents/brimstone.png" : "brimstone",
}



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