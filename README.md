# Querying-on-OpenAlex-Data-in-Streaming-Fashion
---
- OpenAlex is an index of hundreds of millions of interconnected entities across the global research system. A major part of any research is exploring and surveying various research papers, publications, and articles. An ability to query and fetch the specific information reliably and explore different research topics in general can be valuable. This project aims at efficiently fetching this required information in the form of database queries in a streaming fashion. We aim to analyze the collection of research works to find most prominent papers and authors ranging across multiple research fields.
## Objective:
Our Project aims to answer the following questions:
- What were the most prominent papers (with respect to citation count) in a particular topic such as Machine Learning, Artificial Intelligence, etc.?
- Which authors were most actively involved in publishing new works and how their work impacted forthcoming publications (based on how many future works referenced an author’s works)?
- Which are some quickly growing research areas in the recent years (quantified by accelerated increase in number of publications in the recent years)?
- Distribution of publications over time. Is there a seasonality to the number of works published throughout the year? (Eg. Are the publications concentrated around a particular time of the year?
## Data Collection
We used AWS SDK boto3 to download all Computer Science related works between years 2010 and 2022 from openalex S3 bucket. The downloaded data is 118 GB (approximately 40M entries).
## Data Cleaning/Preprocessing
There are 6 levels of concept that each work is mapped to, lower-level concepts are more general, and higher-level concepts are more specific. Furthermore, each work can be mapped to multiple concepts at same level or no concept at a particular level. Also, as we go higher the distinct concepts tend to become very large, hence it made sense to map them to lower levels of concepts. For this purpose, we map each work to one concept, the logic we use for this filter is to take lowest level of concept other than 0, (as level 0 concept means Computer Science) that has highest score (strength of the connection between the work and this concept). 

One more known issue with OpenAlex data is questionable dates. There are few works that are mapped to future dates like 2051, 2100 etc. We decided to drop these records that they comprised negligible population of the dataset.

After all these pre-processing, we stored the data in a parquet format, partitioned on year and quarters for streaming.
## Execution
We used Python as a programming language and PySpark as the framework to leverage the Rutgers iLab spark cluster. All the processing and aggregations on the streaming data were written as well formatted functions. The querying operations that we perform are AGGREGATIONS, COUNT and SUM. We make use of matplotlib animation for visualization on the streaming data.
## Usage
`python streaming_object.py --obj <object_name> --n_files <number of files to trigger for streaming at once>`
Object Names: 
1) prominent_papers
2) prominent_authors
3) growing_research
4) publication_distribution
