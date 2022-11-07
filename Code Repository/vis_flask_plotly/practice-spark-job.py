import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, 
                               FloatType, TimestampType, DateType, ArrayType, BooleanType)
from pyspark.sql.functions import year
import os
import time
from collections import defaultdict

schema = StructType([
  StructField('id', StringType(), True),
  StructField('display_name', StringType(), True),
  StructField('publication_year', IntegerType(), True),
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
  StructField('is_retracted', BooleanType(), True),
  StructField('concepts', ArrayType(StructType([
    StructField('id', StringType(), True),
    StructField('wikidata', StringType(), True),
    StructField('display_name', StringType(), True),
    StructField('level', IntegerType(), True),
    StructField('score', FloatType(), True)
  ]), True)),
  StructField('referenced_works', ArrayType(StringType(), True)),
  StructField('related_works', ArrayType(StringType(), True)),
  StructField('counts_by_year', ArrayType(StructType([
    StructField('year', IntegerType(), True),
    StructField('cited_by_count', IntegerType(), True)
  ]), True)),
  StructField('cited_by_api_url', StringType(), True),
  StructField('updated_date', TimestampType(), True)
])

# curr_path = '/common/home/as3590/Desktop/CS543/Project 1/'
# data_w_path = '/common/users/shared/ppk31_cs543/app/data/'
# data_r_path = '/common/users/shared/ppk31_cs543/curated/*/'

read_path = '/common/users/shared/ppk31_cs543/curated/*/'
global data
data = defaultdict(int)

def forEachStream(df, epoch_id):
  
  global data

  df_with_yr = df.withColumn("publication_date_yr", year(df.publication_date))
  df_yr_agg = df_with_yr.groupBy('publication_date_yr').agg({'id':'count'})
  pd_df_yr_agg = df_yr_agg.toPandas()

  for i, row in pd_df_yr_agg.iterrows():
    data[pd_df_yr_agg.iloc[i, 0]] += pd_df_yr_agg.iloc[i, 1]

  print ('############')
  print (data)
  print ('############')  
  # df_pubyr_agg.toPandas().to_csv(os.path.join(data_w_path, f'{epoch}.csv'), encoding='utf-8', index=False)


if __name__ == '__main__':

  spark = SparkSession.builder.appName("Chart1").getOrCreate()
  spark.sparkContext.setLogLevel('WARN')

  df = spark \
    .readStream \
    .format("parquet") \
    .option("header", True)\
    .schema(schema)\
    .option("maxFilesPerTrigger", 1)\
    .load(read_path)

  query_stream = df.writeStream\
    .outputMode("update")\
    .foreachBatch(forEachStream)\
    .start()
    
  # data = data.withColumn('total_delay', F.col('ArrDelay') + F.col('DepDelay'))
  # data = data.withColumn('is_delayed', F.when(F.col('total_delay') > 30, 1).otherwise(0))

  # query = data.writeStream.outputMode("update").foreachBatch(batch_split_and_join).start()
    
  time.sleep(5)
  # check for completion and gracefully shutdown
  while query_stream.isActive:
    if not bool(query_stream.status['isDataAvailable']):
      print('### STOPPING THE QUERY ###')
      query_stream.stop()  



# print (data.take(5))
# print (data.first())
# print (data.printSchema())    
# # data.createOrReplaceTempView("TempTable")
# # spark.sql("SELECT * FROM TempTable LIMIT 1")



