import argparse
import sys
import subprocess
import signal
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, 
                               FloatType, TimestampType, DateType, ArrayType, BooleanType)
from pyspark.sql.functions import (lit, col, upper, explode, size, array, array_max, 
                                   array_min, array_except, isnan, row_number, quarter, max)
from pyspark.sql import Window

from streaming_constants import constants
import pandas as pd
import os
import shutil
import operator
import time
from glob import glob

def clear(object_name):
    _serving_ = constants.object_constants[object_name]['serving']
    for f in os.listdir(_serving_):
        if os.path.exists(os.path.join(_serving_, f)):
            os.remove(os.path.join(_serving_, f))

def forEachStream_1(df, epoch_id):
    global concept_storage, paper_constants
    for idx, c in enumerate(paper_constants['cncpts']):
        cpt_df = df.filter(upper(col('concept')) == c)\
                .select('id', 'display_name', 'cited_by_count')
        if concept_storage[idx] is None:
            concept_storage[idx] = cpt_df.orderBy(col('cited_by_count').desc()).limit(10)
        else:
            concept_storage[idx] = concept_storage[idx].union(cpt_df).orderBy(col('cited_by_count').desc()).limit(10)
        concept_storage[idx].toPandas()\
                            .to_csv(os.path.join(paper_constants['serving'], paper_constants['cncpts_file'][idx]), encoding='utf-8', index=False)

def forEachStream_2(df, epoch_id):
    global prominent_authors, author_constants

    df = df.select('id', 'cited_by_count', 'concept', explode('authorships').alias('authorships'))\
           .withColumn('author', col('authorships.author.display_name'))\
           .filter((col('author').isNotNull()) & ~(isnan(col('author'))))\
           .withColumn('author_position', col('authorships.author_position'))\
           .filter(col('author_position') == 'first')\
           .drop('authorships', 'author_position')

    if prominent_authors is None:
        prominent_authors = df
    else:
        prominent_authors = prominent_authors.union(df)
    
    authors_concepts = prominent_authors.rdd\
                         .map(lambda row: (row.author, set([row.concept])))\
                         .reduceByKey(lambda a,b: a.union(b))\
                         .map(lambda row: (row[0], list(row[1])))\
                         .toDF(['author', 'concepts'])
    author_publications = prominent_authors.groupBy('author')\
                            .count()\
                            .select('author', col('count').alias('publications'))
    author_citations = prominent_authors.groupBy('author')\
                         .sum('cited_by_count')\
                         .select('author', col('sum(cited_by_count)').alias('citations'))
    #author_citations_publications = author_citations.join(author_publications, ['author'], "inner")\
    #                                            .join(authors_concepts, ['author'], "inner")
    
    author_citations.orderBy(col('citations').desc())\
                                 .limit(10)\
                                 .toPandas()\
                                 .to_csv(os.path.join(author_constants['serving'], author_constants['citations']), encoding='utf-8', index=False)
    author_publications.orderBy(col('publications').desc())\
                                 .limit(10)\
                                 .toPandas()\
                                 .to_csv(os.path.join(author_constants['serving'], author_constants['publications']), encoding='utf-8', index=False)

def forEachStream_3(df, epoch_id):
    global concept_growth, selected_concept_growth, research_constants
    
    growth_count = df.groupBy('publication_year', 'concept')\
                     .count()\
                     .select('publication_year', 'concept', col('count').alias('publications'))
    
    if concept_growth is None and selected_concept_growth is None:
        concept_growth = growth_count.select('concept', 'publications')\
                                     .orderBy(col('publications').desc())
        selected_concept_growth = growth_count.filter(upper(col('concept')).isin(research_constants['cncpts']))
        
    else:
        concept_growth = concept_growth.union(growth_count.select('concept', 'publications'))\
                                       .groupBy('concept')\
                                       .sum('publications')\
                                       .select('concept', col('sum(publications)').alias('publications'))\
                                       .orderBy(col('publications').desc())
        selected_concept_growth = selected_concept_growth.union(growth_count.filter(upper(col('concept')).isin(research_constants['cncpts'])))\
                                       .orderBy('publication_year')
        
    concept_growth.limit(10).toPandas()\
                  .to_csv(os.path.join(research_constants['serving'], research_constants['all_cncpts_file']), encoding='utf-8', index=False)
    selected_concept_growth.toPandas()\
                           .to_csv(os.path.join(research_constants['serving'], research_constants['selected_cpts_file']), encoding='utf-8', index=False)

def forEachStream_4(df, epoch_id):
    global quarterly_dist, pub_dist_constants
    
    dist = df.groupBy('publication_year', 'quarter')\
             .count()\
             .select('publication_year', 'quarter', col('count').alias('publications'))
    if quarterly_dist is None:
        quarterly_dist = dist.orderBy('publication_year', 'quarter')
    else:
        quarterly_dist = quarterly_dist.union(dist)\
                                       .orderBy('publication_year', 'quarter')

    quarterly_dist.toPandas()\
                  .to_csv(os.path.join(pub_dist_constants['serving'], pub_dist_constants['year_q_dist']), encoding='utf-8', index=False)


def streaming_Object(spark, object_name, n_trigger):
    schema = constants.schema
    stream_path = constants.stream_path

    if object_name == 'prominent_papers':
        global concept_storage , paper_constants
        concept_storage = [None, None, None, None, None, None]
        paper_constants = constants.object_constants[object_name]
        os.makedirs(paper_constants['serving'], exist_ok=True)
        return stream(n_trigger, spark, stream_path, schema, forEachStream_1)
    
    if object_name == 'prominent_authors':
        global prominent_authors, author_constants
        prominent_authors = None
        author_constants = constants.object_constants[object_name]
        os.makedirs(author_constants['serving'], exist_ok=True)
        return stream(n_trigger, spark, stream_path, schema, forEachStream_2)
    
    if object_name == 'growing_research':
        global concept_growth, selected_concept_growth, research_constants
        concept_growth = None
        selected_concept_growth = None
        research_constants = constants.object_constants[object_name]
        os.makedirs(research_constants['serving'], exist_ok=True)
        return stream(n_trigger, spark, stream_path, schema, forEachStream_3)
    
    if object_name == 'publication_distribution':
        global quarterly_dist, pub_dist_constants
        quarterly_dist = None
        pub_dist_constants = constants.object_constants[object_name]
        os.makedirs(pub_dist_constants['serving'], exist_ok=True)
        return stream(n_trigger, spark, stream_path, schema, forEachStream_4)

def stream(n_trigger, spark, stream_path, schema, forEachStream):
    data = spark\
    .readStream\
    .format("parquet")\
    .option("header", True)\
    .schema(schema)\
    .option("maxFilesPerTrigger", n_trigger)\
    .load(stream_path)

    q_stream = data.writeStream\
        .outputMode("update")\
        .foreachBatch(forEachStream)\
        .start()

    time.sleep(1)

    while q_stream.isActive:
        if not bool(q_stream.status['isDataAvailable']):
            print('Streaming Completed!')
            q_stream.stop()

            return 'stopped'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Streaming Object arguments')
    parser.add_argument('--obj', 
                        type=str, 
                        default=None, 
                        required=True, 
                        help="""which object to stream? 
                                available options: 1) prominent_papers
                                                   2) prominent_authors
                                                   3) growing_research
                                                   4) publication_distribution
                                usages:
                                python <file> --obj <options> 
                            """
                        )
    parser.add_argument('--n_files', 
                        type=int, 
                        default=4, 
                        help="""No. of files to trigger for streaming
                                integer value supported
                                usages:
                                python <file> --n_files <integer>""")

    args = parser.parse_args()

    object_name = args.obj
    n_trigger = args.n_files
    clear(object_name)
    time.sleep(1)
    plot_file = f'plot_{object_name}.py'
    p = subprocess.Popen(['python', plot_file])

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    sqlContext = SQLContext(sc)

    status = streaming_Object(spark, object_name, n_trigger)
    print(status)
    spark.stop()

    time.sleep(10)
    if p is not None:
        os.kill(p.pid, signal.SIGTERM)

    sys.exit(0)
