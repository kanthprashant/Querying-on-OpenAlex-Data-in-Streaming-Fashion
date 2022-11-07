from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, 
  FloatType, TimestampType, DateType, ArrayType, BooleanType)
from pyspark.sql.functions import (lit, col, upper, explode, size, array, array_min, concat,
  array_max, array_except, isnan, row_number, max, quarter, year, month)
from pyspark.sql import Window
import os, sys, requests, json, time
import pandas as pd
from utils import *

# schema = StructType([
#     StructField('id', StringType(), True),
#     StructField('display_name', StringType(), True),
#     StructField('publication_year', IntegerType(), True),
#     StructField('publication_date', DateType(), True),
#     StructField('host_venue', StructType([
#         StructField('id', StringType(), True),
#         StructField('issn_l', StringType(), True),
#         StructField('issn', ArrayType(StringType(), True)),
#         StructField('display_name', StringType(), True),
#         StructField('publisher', StringType(), True),
#         StructField('type', StringType(), True),
#         StructField('url', StringType(), True),
#         StructField('is_oa', BooleanType(), True),
#         StructField('version', StringType(), True),
#         StructField('license', StringType(), True)
#     ])),
#     StructField('type', StringType(), True),
#     StructField('authorships', ArrayType(StructType([
#         StructField('author_position', StringType(), True),
#         StructField('author', StructType([
#             StructField('id', StringType(), True),
#             StructField('display_name', StringType(), True),
#             StructField('orcid', StringType(), True)
#         ])),
#         StructField('institutions', ArrayType(StructType([
#             StructField('id', StringType(), True),
#             StructField('display_name', StringType(), True),
#             StructField('ror', StringType(), True),
#             StructField('country_code', StringType(), True),
#             StructField('type', StringType(), True)
#         ]), True)),
#         StructField('raw_affiliation_string', StringType(), True)
#     ]), True)),
#     StructField('cited_by_count', IntegerType(), True),
#     StructField('is_retracted', BooleanType(), True),
#     StructField('concepts', ArrayType(StructType([
#         StructField('id', StringType(), True),
#         StructField('wikidata', StringType(), True),
#         StructField('display_name', StringType(), True),
#         StructField('level', IntegerType(), True),
#         StructField('score', FloatType(), True)
#     ]), True)),
#     StructField('referenced_works', ArrayType(StringType(), True)),
#     StructField('related_works', ArrayType(StringType(), True)),
#     StructField('counts_by_year', ArrayType(StructType([
#         StructField('year', IntegerType(), True),
#         StructField('cited_by_count', IntegerType(), True)
#     ]), True)),
#     StructField('cited_by_api_url', StringType(), True),
#     StructField('updated_date', TimestampType(), True)
# ])

schema = StructType([
    StructField('id', StringType(), True),
    StructField('display_name', StringType(), True),
    StructField('publication_date', DateType(), True),
    StructField('host_venue', StructType([
        StructField('id', StringType(), True),
        StructField('issn_l', StringType(), True),
        StructField('issn', ArrayType(StringType(), True)),
        StructField('display_name', StringType(), True),
        StructField('publisher', StringType(), True),
        StructField('type', StringType(), True),
        StructField('url', StringType(), True),
        StructField('is_oa', BooleanType(), True),
        StructField('version', StringType(), True),
        StructField('license', StringType(), True)
    ])),
    StructField('type', StringType(), True),
    StructField('authorships', ArrayType(StructType([
        StructField('author_position', StringType(), True),
        StructField('author', StructType([
            StructField('id', StringType(), True),
            StructField('display_name', StringType(), True),
            StructField('orcid', StringType(), True)
        ])),
        StructField('institutions', ArrayType(StructType([
            StructField('id', StringType(), True),
            StructField('display_name', StringType(), True),
            StructField('ror', StringType(), True),
            StructField('country_code', StringType(), True),
            StructField('type', StringType(), True)
        ]), True)),
        StructField('raw_affiliation_string', StringType(), True)
    ]), True)),
    StructField('cited_by_count', IntegerType(), True),
    StructField('referenced_works', ArrayType(StringType(), True)),
    StructField('counts_by_year', ArrayType(StructType([
        StructField('year', IntegerType(), True),
        StructField('cited_by_count', IntegerType(), True)
    ]), True)),
    StructField('concept', StringType(), True),
    StructField('publication_year', IntegerType(), True),
    StructField('quarter', IntegerType(), True)
])



def forEachStream_publications(df, epoch_id):

  df = df.withColumn('publication_month', month(df.publication_date))\
    .groupBy('publication_year', 'publication_month')\
    .count()\
    .select('publication_year', 'publication_month', col('count').alias('no_publications'))\
    .withColumn('publication_date', concat(col('publication_year'), lit("-"), col('publication_month'), lit("-01 00:00:01")))\
    .select('publication_date', 'no_publications')

  data = df.toPandas().to_json(orient='values')
  logData(f'stream instance ({epoch_id})', data)

  req = requests.post(f'{server}/publications/data', data=data, headers=headers)
  logRequest(f'{server}/publications/data', 'POST', received=False)

def forEachStream_papers(df, epoch_id):
  
  cncpts = [
    'ARTIFICIAL INTELLIGENCE', 'COMPUTER VISION', 'MACHINE LEARNING', 
    'DISTRIBUTED COMPUTING', 'NATURAL LANGUAGE PROCESSING', 'DATA MINING'
  ]  

  rankSpec  = Window.partitionBy("concept").orderBy(col("cited_by_count").desc())
  df_ranked = df.filter(upper(df.concept).isin(cncpts))\
      .withColumn("row_number",row_number().over(rankSpec))
  df_ranked_filtered = df_ranked.filter(df_ranked.row_number <= 10)\
      .select('id', 'display_name', upper(col('concept')), 'cited_by_count')

  data = df_ranked_filtered.toPandas().to_json(orient='values')
  # logData(f'stream instance ({epoch_id})', data)

  req = requests.post(f'{server}/papers/data', data=data, headers=headers)
  logRequest(f'{server}/papers/data', 'POST', received=False)  

def forEachStream_research(df, epoch_id):

  df = df.groupBy('concept')\
    .count()\
    .select('concept', col('count').alias('no_publications'))

  data = df.toPandas().to_json(orient='values')
  # logData(f'stream instance ({epoch_id})', data)

  req = requests.post(f'{server}/research/data', data=data, headers=headers)
  logRequest(f'{server}/research/data', 'POST', received=False)

def forEachStream_authors(df, epoch_id):

  df = df.select('id', 'cited_by_count', explode('authorships').alias('authorships'))\
    .withColumn('author', col('authorships.author.display_name'))\
    .withColumn('author_position', col('authorships.author_position'))\
    .filter(col('author_position') == 'first')\
    .drop('authorships', 'author_position')

  df_agg = df.groupBy('author').agg({'id':'count', 'cited_by_count':'sum'})\
    .withColumnRenamed("count(id)","no_publications")\
    .withColumnRenamed("sum(cited_by_count)","citations")

  data = df.toPandas().to_json(orient='values')
  # logData(f'stream instance ({epoch_id})', data)

  req = requests.post(f'{server}/authors/data', data=data, headers=headers)
  logRequest(f'{server}/authors/data', 'POST', received=False)


if __name__ == '__main__':

  chart_id = sys.argv[1]

  read_path = '/common/users/shared/ppk31_cs543/datalake/curated_quarterly/'
  server = "http://127.0.0.1:5000"
  headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

  spark = SparkSession.builder.appName(chart_id).getOrCreate()
  spark.sparkContext.setLogLevel('WARN')

  forEachStream_map = {
    'publications': forEachStream_publications,
    'papers': forEachStream_papers,
    'research': forEachStream_research,
    'authors': forEachStream_authors             
  }

  df = spark \
    .readStream \
    .format("parquet") \
    .option("header", True)\
    .schema(schema)\
    .option("maxFilesPerTrigger", 1)\
    .load(read_path)

  query_stream = df.writeStream\
    .outputMode("update")\
    .foreachBatch(forEachStream_map[chart_id])\
    .start()
    
  time.sleep(5)

  while query_stream.isActive:
    if not bool(query_stream.status['isDataAvailable']):
      print('Gracefully exiting the query')
      query_stream.stop()