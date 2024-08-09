from audioop import avg
import pandas as pd
import pyspark
from pyspark import sql
import delta
from pyspark.sql.functions import lit 
from pyspark.sql.functions import monotonically_increasing_id
from prev_match_results import *
from utils import *

def _create_delta_spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    builder = SparkSession.builder.appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.jars.packages","io.delta:delta-core_2.12:2.0.0")
    return configure_spark_with_delta_pip(builder).getOrCreate()

spark = _create_delta_spark()

from json_to_df import *

def player_stats_all():
    player_stats = na_kck.unionAll(na_s1).unionAll(na_s2).unionAll(madrid).unionAll(shanghai).unionAll(champs).unionAll(ch_kck).unionAll(ch_s1).unionAll(ch_s2).unionAll(emea_kck).unionAll(emea_s1).unionAll(emea_s2).unionAll(pac_kck).unionAll(pac_s1).unionAll(pac_s2)
    # player_stats.toPandas().to_csv('full_player_stats.csv')
    # new_df = na_player_stats.select('player', 'assists_per_round').where(na_player_stats.player == 'mwzera')
    # print(new_df.show(3))

    player_stats.createOrReplaceTempView("player_stats")
    player_stats = spark.sql("SELECT * from player_stats ")
    player_stats = spark.sql("SELECT player as player, collect_set(org) as org, collect_set(region) as region, SUM(rounds) as rounds, AVG(rating) as rating, AVG(average_combat_score) as ACS, AVG(kill_deaths) as KD, AVG(kill_assists_survived_traded) as KAST, AVG(average_damage_per_round) as ADR, AVG(kills_per_round) as KPR, AVG(assists_per_round) as APR, AVG(first_kills_per_round) as FKPR, AVG(first_deaths_per_round) as FDPR, AVG(headshot_percentage) as HS, AVG(clutch_success_percentage) as CS, MAX(kmax) as KMAX, SUM(kills) as KILLS, SUM(deaths) as DEATHS, SUM(assists) as ASSISTS, SUM(fk) as FK, SUM(fd) as FD, collect_set(event) as event FROM player_stats GROUP by player")
    # print(sqlDF.show(10))
    player_stats.toPandas().to_csv('player_stats.csv')
    # return player_stats



def champion_stats():
    region = "ALL"
    event = "CHAMPIONS"
    # player_stats_all()
    vlr_stats_events('2097/valorant-champions-2024')

    players = spark.read.option("header",True) \
    .option("inferSchema",False) \
    .format("csv") \
    .load("player_stats.csv")

    # player_stats = champs.join(players, champs['player'] == players['player'], 'left').select(players['player'], players['org'], players['region'], champs['rounds'], champs['rating'], champs['kill_deaths'], champs['average_combat_score'], champs['kill_assists_survived_traded'], champs['average_damage_per_round'], champs['kills_per_round'], champs['assists_per_round'], champs['first_kills_per_round'], champs['first_deaths_per_round'], champs['headshot_percentage'], champs['clutch_success_percentage'], champs['kmax'], champs['kills'], champs['deaths'], champs['assists'], champs['fk'], champs['fd'])


    player_stats = champs.join(players, champs['player'] == players['player'], 'left').select(players['player'], players['org'], players['region'], champs['rounds'], champs['rating'], champs['kill_deaths'].alias('KD'), champs['average_combat_score'].alias('ACS'), champs['kill_assists_survived_traded'].alias('KAST'), champs['average_damage_per_round'].alias('ADR'), champs['kills_per_round'].alias('KPR'), champs['assists_per_round'].alias('APR'), champs['first_kills_per_round'].alias('FKPR'), champs['first_deaths_per_round'].alias('FDPR'), champs['headshot_percentage'].alias('HS'), champs['clutch_success_percentage'].alias('CS'), champs['kills'].alias('KILLS'), champs['deaths'].alias('DEATHS'), champs['assists'].alias('ASSISTS'), champs['fk'].alias('FK'), champs['fd'].alias('FD'), champs['kmax'].alias('KMAX'))
    
    if(region != "ALL"):
        player_stats.createTempView("player_stats")
        player_stats = spark.sql(f"SELECT * FROM player_stats WHERE region LIKE '%{region}%'")

    print(player_stats.show(10))
    
   
    player_stats.toPandas().to_csv('player_stats_champions.csv')


def specific_param_stats(event, region):
    # region = "all"
    # event = "CHAMPIONS"
    # player_stats_all()
    players = spark.read.option("header",True) \
    .option("inferSchema",False) \
    .format("csv") \
    .load("player_stats.csv")

    # player_stats = champs.join(players, champs['player'] == players['player'], 'left').select(players['player'], players['org'], players['region'], champs['event'], champs['rounds'], champs['rating'], champs['kill_deaths'].alias('KD'), champs['average_combat_score'].alias('ACS'), champs['kill_assists_survived_traded'].alias('KAST'), champs['average_damage_per_round'].alias('ADR'), champs['kills_per_round'].alias('KPR'), champs['assists_per_round'].alias('APR'), champs['first_kills_per_round'].alias('FKPR'), champs['first_deaths_per_round'].alias('FDPR'), champs['headshot_percentage'].alias('HS'), champs['clutch_success_percentage'].alias('CS'), champs['KMAX'], champs['kills'].alias("KILLS"), champs['deaths'].alias("DEATHS"), champs['assists'].alias("ASSISTS"), champs['fk'].alias('FK'), champs['fd'].alias('FD'), champs['agents_played'].alias('agents_played'))
    player_stats = champs.join(players, champs['player'] == players['player'], 'left').select(players['player'], players['org'], players['region'], champs['event'], champs['rounds'], champs['rating'], champs['kill_deaths'], champs['average_combat_score'], champs['kill_assists_survived_traded'], champs['average_damage_per_round'], champs['kills_per_round'], champs['assists_per_round'], champs['first_kills_per_round'], champs['first_deaths_per_round'], champs['headshot_percentage'], champs['clutch_success_percentage'], champs['KMAX'], champs['kills'], champs['deaths'], champs['assists'], champs['fk'], champs['fd'], champs['agents_played'])

    schema2 = na_s1.schema
    schema1 = player_stats.schema
    # print(na_s1.schema)
    new_stats1 = spark.createDataFrame([], schema1)
    new_stats2 = spark.createDataFrame([], schema2)
    ne = len(event)
    nr = len(region)

    print(event, region)

    for e in event:
        if(e == "ALL"):
            player_stats.createOrReplaceTempView("player_stats")
            new_stats = spark.sql(f"SELECT * FROM player_stats")
            new_stats1 = new_stats
            break
        if(e == "STAGE 1"):
            if(event[0] == e):
                new_stats = na_s1.unionAll(emea_s1).unionAll(pac_s1).unionAll(ch_s1)
                if(ne == 1):
                    new_stats2 = new_stats
                    break
            else:
                stage1_stats = na_s1.unionAll(emea_s1).unionAll(pac_s1).unionAll(ch_s1)
                new_stats = new_stats.unionAll(stage1_stats)
            # print("new_stats_99", new_stats.show(10))
        if(e == "STAGE 2"):
            if(event[0] == e):
                new_stats = na_s2.unionAll(emea_s2).unionAll(pac_s2).unionAll(ch_s2)
                if(ne == 1):
                    new_stats2 = new_stats
                    break
            else:
                stage2_stats = na_s2.unionAll(emea_s2).unionAll(pac_s2).unionAll(ch_s2)
                new_stats = new_stats.unionAll(stage2_stats)
            # print("new_stats_106", new_stats.show(10))
        if(e == "MASTERS MADRID"):
            if(event[0] == e):
                new_stats = madrid
                if(ne == 1):
                    new_stats2 = new_stats
                    break
            else:
                new_stats = new_stats.unionAll(madrid)
        if(e == "MASTERS SHANGHAI"):
            if(event[0] == e):
                new_stats = shanghai
            else:
                new_stats = new_stats.unionAll(shanghai)
        if(e == "CHAMPIONS"):
            if(event[0] == e):
                new_stats = champs
                if(ne == 1):
                    new_stats2 = new_stats
                    break
            else:
                new_stats = new_stats.unionAll(champs)
        if(e == "KICKOFF"):
            if(event[0] == e):
                new_stats = na_kck.unionAll(emea_kck).unionAll(pac_kck).unionAll(ch_kck)
                if(ne == 1):
                    new_stats2 = new_stats
                    # print(new_stats.show(5))
                    break
            else:
                kickoff_stats = na_kck.unionAll(emea_kck).unionAll(pac_kck).unionAll(ch_kck)
                new_stats = new_stats.unionAll(kickoff_stats)
        if(event[0] != e):
            new_stats2 = new_stats
            # print("new_stats2_131", e, new_stats2.show())
            # print("new_stats_132", e, new_stats.show())
    
    # print(new_stats1.show(5))
    # new_stats1.createOrReplaceTempView("new_stats1")
    if(event[0] == 'ALL'):
        final_stats = spark.createDataFrame([], new_stats1.schema)
        final_stats = new_stats1
        # print(new_stats1.show(5))
    else:
        final_stats = spark.createDataFrame([], new_stats2.schema)
        final_stats = new_stats2
        # print(new_stats2.show(5))

    # print(final_stats)

    new_stats = spark.createDataFrame([], final_stats.schema)
    new_stats.createOrReplaceTempView("new_stats")
    final_stats.createOrReplaceTempView("final_stats")
    final_stats1 = spark.createDataFrame([], final_stats.schema)
    
    for r in region:
        if(r != 'ALL'):
            new_stats = spark.sql(f"SELECT * FROM final_stats WHERE region LIKE '{r}'")
            final_stats1 = final_stats1.unionAll(new_stats)
            # print(final_stats1.show(10))
        else:
            final_stats1 = new_stats
            break
    
    # print(final_stats1.show(10), "final_stats_143")
    
    # if(region != "all"):
    #     try:
    #         player_stats.createTempView("player_stats")
    #     except:
    #         pass
    #     player_stats = spark.sql(f"SELECT * FROM player_stats WHERE region LIKE '%{region}%'")

    final_stats1.toPandas().to_csv('new_stats.csv')

    # new_stats = final_stats.toPandas()

    return final_stats1.toPandas()

# player_stats_all()
# champion_stats()

# df = specific_param_stats(["KICKOFF"], ["EMEA"])
# print(df)


# def sample():

#     players = spark.read.option("header",True) \
#     .option("inferSchema",False) \
#     .format("csv") \
#     .load("player_stats.csv")

#     orgs = players.select(players.org).drop_duplicates()
#     print(orgs.show(100))
#     print(orgs.count())

# sample()

