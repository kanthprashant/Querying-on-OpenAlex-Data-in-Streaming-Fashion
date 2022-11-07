from pyspark.sql import SparkSession
import os, requests, time, json
import pandas as pd
import sys 

if __name__ == '__main__':

  spark = SparkSession.builder.appName("practice-spark-job-2").getOrCreate()
  spark.sparkContext.setLogLevel('WARN')

  df_dict = {'Col1': [1,2,3,4,5], 'Col2': [2,4,6,8,10]}
  df_pd = pd.DataFrame(df_dict)
  df_spark = spark.createDataFrame(df_pd) 

  df_spark.printSchema()
  df_spark.show()

  print ('######################')
  print (f'Argument 0: {sys.argv[0]}')
  print (f'Argument 1: {sys.argv[1]}')
  print ('######################')