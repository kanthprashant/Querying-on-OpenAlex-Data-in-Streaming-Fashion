from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, 
                               FloatType, TimestampType, DateType, ArrayType, BooleanType)

class constants:
    stream_path = "/common/users/shared/ppk31_cs543/datalake/curated_quarterly"
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
    
    object_constants = {
                        'prominent_papers': {
                            'serving': '/common/users/shared/ppk31_cs543/serving/1',
                            'cncpts': ['ARTIFICIAL INTELLIGENCE', 'COMPUTER VISION', 'MACHINE LEARNING', 
                                       'DISTRIBUTED COMPUTING', 'NATURAL LANGUAGE PROCESSING', 'DATA MINING'],
                            'cncpts_file': ['top_papers_AI.csv', 'top_papers_CV.csv', 'top_papers_ML.csv', 
                                            'top_papers_DC.csv', 'top_papers_NLP.csv', 'top_papers_DM.csv']
                        },
                        'prominent_authors': {
                            'serving' : '/common/users/shared/ppk31_cs543/serving/2',
                            'publications': 'author_pub.csv',
                            'citations': 'author_cit.csv'
                        },
                        'growing_research': {
                            'serving': '/common/users/shared/ppk31_cs543/serving/3',
                            'all_cncpts_file': 'all_cpt.csv',
                            'selected_cpts_file': 'selected_cpt.csv',
                            'cncpts': ['ARTIFICIAL INTELLIGENCE', 'COMPUTER VISION', 'MACHINE LEARNING', 
                                       'DISTRIBUTED COMPUTING', 'NATURAL LANGUAGE PROCESSING', 'DATA MINING']
                        },
                        'publication_distribution': {
                            'serving': '/common/users/shared/ppk31_cs543/serving/4',
                            'year_q_dist': 'dist.csv'
                        }
    }
