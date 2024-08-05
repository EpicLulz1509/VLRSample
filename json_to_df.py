from audioop import avg
import pandas as pd
import pyspark
from pyspark import sql
import delta
from pyspark.sql.functions import lit 

def _create_delta_spark():
  from pyspark.sql import SparkSession
  from delta import configure_spark_with_delta_pip
  builder = SparkSession.builder.appName("DeltaLakeApp") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
  .config("spark.jars.packages","io.delta:delta-core_2.12:2.0.0")
  return configure_spark_with_delta_pip(builder).getOrCreate()

spark = _create_delta_spark()


def player_stats():
    df1 = spark.read.json('json_files/champions-tour-2024-americas-kickoff_player_stats.json')
    df2 = spark.read.json('json_files/champions-tour-2024-americas-stage-1_player_stats.json')
    df3 = spark.read.json('json_files/champions-tour-2024-americas-stage-2_player_stats.json')

    df4 = spark.read.json('json_files/champions-tour-2024-masters-madrid_player_stats.json')
    df5 = spark.read.json('json_files/champions-tour-2024-masters-shanghai_player_stats.json')
    df6 = spark.read.json('json_files/valorant-champions-2024_player_stats.json')

    df7 = spark.read.json('json_files/champions-tour-2024-china-kickoff_player_stats.json')
    df8 = spark.read.json('json_files/champions-tour-2024-china-stage-1_player_stats.json')
    df9 = spark.read.json('json_files/champions-tour-2024-china-stage-2_player_stats.json')

    df10 = spark.read.json('json_files/champions-tour-2024-emea-kickoff_player_stats.json')
    df11 = spark.read.json('json_files/champions-tour-2024-emea-stage-1_player_stats.json')
    df12 = spark.read.json('json_files/champions-tour-2024-emea-stage-2_player_stats.json')

    df13 = spark.read.json('json_files/champions-tour-2024-pacific-kickoff_player_stats.json')
    df14 = spark.read.json('json_files/champions-tour-2024-pacific-stage-1_player_stats.json')
    df15 = spark.read.json('json_files/champions-tour-2024-pacific-stage-2_player_stats.json')

    df1 = df1.withColumn("region", lit("NA"))
    df2 = df2.withColumn("region", lit("NA"))
    df3 = df3.withColumn("region", lit("NA"))

    df4 = df4.withColumn("region", lit("INT"))
    df5 = df5.withColumn("region", lit("INT"))
    df6 = df6.withColumn("region", lit("INT"))

    df7 = df7.withColumn("region", lit("CHINA"))
    df8 = df8.withColumn("region", lit("CHINA"))
    df9 = df9.withColumn("region", lit("CHINA"))

    df10 = df10.withColumn("region", lit("EMEA"))
    df11 = df11.withColumn("region", lit("EMEA"))
    df12 = df12.withColumn("region", lit("EMEA"))

    df13 = df13.withColumn("region", lit("PACIFIC"))
    df14 = df14.withColumn("region", lit("PACIFIC"))
    df15 = df15.withColumn("region", lit("PACIFIC"))

    df1 = df1.withColumn("event", lit("KICKOFF"))
    df2 = df2.withColumn("event", lit("STAGE 1"))
    df3 = df3.withColumn("event", lit("STAGE 2"))

    df4 = df4.withColumn("event", lit("MASTERS MADRID"))
    df5 = df5.withColumn("event", lit("MASTERS SHANGHAI"))
    df6 = df6.withColumn("event", lit("CHAMPIONS"))

    df7 = df7.withColumn("event", lit("KICKOFF"))
    df8 = df8.withColumn("event", lit("STAGE 1"))
    df9 = df9.withColumn("event", lit("CHINA"))

    df10 = df10.withColumn("event", lit("KICKOFF"))
    df11 = df11.withColumn("event", lit("STAGE 1"))
    df12 = df12.withColumn("event", lit("STAGE 2"))

    df13 = df13.withColumn("event", lit("KICKOFF"))
    df14 = df14.withColumn("event", lit("STAGE 1"))
    df15 = df15.withColumn("event", lit("STAGE 2"))

    player_stats = df1.unionAll(df2).unionAll(df3).unionAll(df4).unionAll(df5).unionAll(df6).unionAll(df7).unionAll(df8).unionAll(df9).unionAll(df10).unionAll(df11).unionAll(df12).unionAll(df13).unionAll(df14).unionAll(df15)
    player_stats.toPandas().to_csv('full_player_stats.csv')
    # new_df = na_player_stats.select('player', 'assists_per_round').where(na_player_stats.player == 'mwzera')
    # print(new_df.show(3))

    player_stats.createOrReplaceTempView("player_stats")
    player_stats = spark.sql("SELECT * from player_stats ")
    sqlDF = spark.sql("SELECT player, collect_set(org) as org, collect_set(region) as region, collect_set(event) as event, SUM(rounds) as rounds, AVG(rating) as rating, AVG(average_combat_score) as ACS, AVG(kill_deaths) as KD,  AVG(kill_assists_survived_traded) as KAST, AVG(average_damage_per_round) as ADR, AVG(kills_per_round) as KPR, AVG(assists_per_round) as APR, AVG(first_kills_per_round) as FKPR, AVG(first_deaths_per_round) as FDPR, AVG(headshot_percentage) as HS, AVG(clutch_success_percentage) as CS, MAX(kmax) as KMAX, SUM(kills) as KILLS, SUM(deaths) as DEATHS, SUM(assists) as ASSISTS, SUM(fk) as FK, SUM(fd) as FD FROM player_stats GROUP by player")
    # print(sqlDF.show(10))
    sqlDF.toPandas().to_csv('player_stats.csv')



player_stats()
