from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
import os, json
import signal, subprocess
from utils import *
import pandas as pd

global data_publications, data_papers, data_research, data_authors
data_publications, data_papers, data_research, data_authors = {}, {}, {}, {}


global spark_process 
spark_process = None

app = Flask(__name__)
# cors = CORS(app, resources={'/*':{'origins': '*'} })
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


# @app.route("/", methods=['GET'])
# @cross_origin()
# def home():
#   logRequest('/', request.method, True)
#   return jsonify({'message': logRequest('/', request.method)})


@app.route("/<chart_id>/start", methods=['GET'])
@cross_origin()
def startCompute(chart_id):
  logRequest(f'/{chart_id}/start', request.method, True)

  global data_publications, data_papers, data_research, data_authors, data_chart5

  if chart_id == 'publications': data_publications = {}
  if chart_id == 'papers': data_papers = {}
  if chart_id == 'research': data_research = {}
  if chart_id == 'authors': data_authors = {}      

  global spark_process

  if spark_process is not None:
    pid = spark_process.pid
    os.kill(pid, signal.SIGTERM)

  spark_process = subprocess.Popen(
    ['spark-submit', '/common/users/shared/ppk31_cs543/app/compute.py', chart_id],     
    stdout=subprocess.DEVNULL
  )

  return jsonify({'message': logRequest(f'/{chart_id}/start', request.method)})

def processIncomingData_publications(data):
  global data_publications

  for date, v in data:
    if len(data_publications.keys()) == 0:
      data_publications['publication_date'] = [date]
      data_publications['no_publications'] = [v]
    else:
      data_publications['publication_date'].append(date)
      data_publications['no_publications'].append(v)

  # for key in data.keys():
  #   data_publications[key] = data_publications[key] + data[key] if key in data_publications else data[key]

  logData(f'accumulated Publications', data_publications)

def processIncomingData_papers(data):
  global data_papers

  cncpts = [
    'ARTIFICIAL INTELLIGENCE', 'COMPUTER VISION', 'MACHINE LEARNING', 
    'DISTRIBUTED COMPUTING', 'NATURAL LANGUAGE PROCESSING', 'DATA MINING'
  ]  

  if len(data_papers.keys()) == 0:
    for cncpt in cncpts:
      data_papers[cncpt] = {}

  data_df = pd.DataFrame(data, columns = ['id', 'display_name', 'concept', 'cited_by_count'])
  for cncpt in cncpts:
    data_df_cncpt = data_df[data_df.concept == cncpt]
    data_papers_df_cncpt = pd.DataFrame(data_papers[cncpt])
    data_concated = pd.concat([data_df_cncpt, data_papers_df_cncpt])
    data_concated = data_concated.sort_values(by='cited_by_count', ascending=False).iloc[:10]
    data_papers[cncpt]['id'] = list(data_concated['id'])
    data_papers[cncpt]['display_name'] = list(data_concated['display_name'])
    data_papers[cncpt]['concept'] = list(data_concated['concept'])
    data_papers[cncpt]['cited_by_count'] = list(data_concated['cited_by_count'])

  logData(f'accumulated Publications', data_papers)

def processIncomingData_research(data):
  global data_research

  for cncpt, v in data:
    if cncpt in data_research:
      data_research[cncpt] += v
    else:
      data_research[cncpt] = v

  print (data_research)

def processIncomingData_authors(data):
  global data_authors

  for author, cites, pubs in data:
    if author in data_research:
      data_authors[author][0] += pubs
      data_authors[author][1] += cites
    else:
      data_authors[author] = [pubs, cites]

  print (data_research)

processingIncomingData_map = {
  'publications': processIncomingData_publications,
  'papers': processIncomingData_papers,
  'research': processIncomingData_research,
  'authors': processIncomingData_authors,
}



@app.route("/<chart_id>/data", methods=['GET', 'POST'])
@cross_origin()
# @cross_origin(origin='http://localhost:3000/*',headers=['Content-Type','Authorization'])
def getOrChangeChartData(chart_id):

  global data_publications, data_papers, data_research, data_authors, data_chart5

  if request.method == 'GET':
    print ('############ GET REQUEST received #############')
    logRequest(f'/{chart_id}/data', request.method, True)
    if chart_id == 'publications': return jsonify(data_publications)
    if chart_id == 'papers': return jsonify(data_papers)
    if chart_id == 'research': return jsonify(data_research)
    if chart_id == 'authors': return jsonify(data_authors)  

  if request.method == 'POST':
    data = request.get_json()

    # logRequest(f'/{chart_id}/data', request.method, True, data)

    processingIncomingData_map[chart_id](data)

    return jsonify({'message': logRequest(f'/{chart_id}/data', request.method)})

# def _corsify_actual_response(response):
#     response.headers.add("Access-Control-Allow-Origin", "*")
#     return response

@app.route("/test-<param>", methods=["GET"])
def testFunc(param):
  return f'Hey there, {param}'

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)