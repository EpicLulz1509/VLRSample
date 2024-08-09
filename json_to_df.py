from audioop import avg
import pandas as pd
import pyspark
from pyspark import sql
import delta
from pyspark.sql.functions import lit 
from pyspark.sql.functions import monotonically_increasing_id

def _create_delta_spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    builder = SparkSession.builder.appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.jars.packages","io.delta:delta-core_2.12:2.0.0")
    return configure_spark_with_delta_pip(builder).getOrCreate()

spark = _create_delta_spark()

na_kck = spark.read.json('json_files/champions-tour-2024-americas-kickoff_player_stats.json')
na_s1 = spark.read.json('json_files/champions-tour-2024-americas-stage-1_player_stats.json')
na_s2 = spark.read.json('json_files/champions-tour-2024-americas-stage-2_player_stats.json')

madrid = spark.read.json('json_files/champions-tour-2024-masters-madrid_player_stats.json')
shanghai = spark.read.json('json_files/champions-tour-2024-masters-shanghai_player_stats.json')
champs = spark.read.json('json_files/valorant-champions-2024_player_stats.json')

ch_kck = spark.read.json('json_files/champions-tour-2024-china-kickoff_player_stats.json')
ch_s1 = spark.read.json('json_files/champions-tour-2024-china-stage-1_player_stats.json')
ch_s2 = spark.read.json('json_files/champions-tour-2024-china-stage-2_player_stats.json')

emea_kck = spark.read.json('json_files/champions-tour-2024-emea-kickoff_player_stats.json')
emea_s1 = spark.read.json('json_files/champions-tour-2024-emea-stage-1_player_stats.json')
emea_s2 = spark.read.json('json_files/champions-tour-2024-emea-stage-2_player_stats.json')

pac_kck = spark.read.json('json_files/champions-tour-2024-pacific-kickoff_player_stats.json')
pac_s1 = spark.read.json('json_files/champions-tour-2024-pacific-stage-1_player_stats.json')
pac_s2 = spark.read.json('json_files/champions-tour-2024-pacific-stage-2_player_stats.json')

na_kck = na_kck.withColumn("region", lit("NA"))
na_s1 = na_s1.withColumn("region", lit("NA"))
na_s2 = na_s2.withColumn("region", lit("NA"))

madrid = madrid.withColumn("region", lit("INT"))
shanghai = shanghai.withColumn("region", lit("INT"))
champs = champs.withColumn("region", lit("INT"))

ch_kck = ch_kck.withColumn("region", lit("CHINA"))
ch_s1 = ch_s1.withColumn("region", lit("CHINA"))
ch_s2 = ch_s2.withColumn("region", lit("CHINA"))

emea_kck = emea_kck.withColumn("region", lit("EMEA"))
emea_s1 = emea_s1.withColumn("region", lit("EMEA"))
emea_s2 = emea_s2.withColumn("region", lit("EMEA"))

pac_kck = pac_kck.withColumn("region", lit("PACIFIC"))
pac_s1 = pac_s1.withColumn("region", lit("PACIFIC"))
pac_s2 = pac_s2.withColumn("region", lit("PACIFIC"))

na_kck = na_kck.withColumn("event", lit("KICKOFF"))
na_s1 = na_s1.withColumn("event", lit("STAGE 1"))
na_s2 = na_s2.withColumn("event", lit("STAGE 2"))

madrid = madrid.withColumn("event", lit("MASTERS MADRID"))
shanghai = shanghai.withColumn("event", lit("MASTERS SHANGHAI"))
champs = champs.withColumn("event", lit("CHAMPIONS"))

ch_kck = ch_kck.withColumn("event", lit("KICKOFF"))
ch_s1 = ch_s1.withColumn("event", lit("STAGE 1"))
ch_s2 = ch_s2.withColumn("event", lit("STAGE 2"))

emea_kck = emea_kck.withColumn("event", lit("KICKOFF"))
emea_s1 = emea_s1.withColumn("event", lit("STAGE 1"))
emea_s2 = emea_s2.withColumn("event", lit("STAGE 2"))

pac_kck = pac_kck.withColumn("event", lit("KICKOFF"))
pac_s1 = pac_s1.withColumn("event", lit("STAGE 1"))
pac_s2 = pac_s2.withColumn("event", lit("STAGE 2"))








# player_stats()
# champion_stats()