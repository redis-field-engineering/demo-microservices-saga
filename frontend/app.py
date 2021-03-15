from flask import Flask, render_template, request, redirect
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from redisearch import Client, Query, aggregation, reducers, IndexDefinition, TextField, NumericField, TagField, NumericFilter
from redistimeseries.client import Client as RedisTimeseries


# From our local file
from datasetup import setup_data

from os import environ, getcwd

import redis
import yaml
import re
import datetime

app = Flask(__name__,
            static_url_path='/docs',
            static_folder='docs',
)

bootstrap = Bootstrap()

if environ.get('APP_CONF') is not None:
   app_conf = environ.get('APP_CONF')
else:
   app_conf = getcwd() + "/../conf/config.yml"

with open(app_conf) as file:
   cfg = yaml.load(file, Loader=yaml.FullLoader)

rdb = redis.Redis(
    host=cfg['host'],
    port=cfg['port'],
    )

client = Client(
   'MicroServiceSaga',
    host=cfg['host'],
    port=cfg['port'],
   )

rts = RedisTimeseries(
    host=cfg['host'],
    port=cfg['port'],
    )

nav = Nav()
topbar = Navbar('',
    View('Home', 'index'),
    View('Stats', 'show_stats'),
    View('Start', 'start_form'),
    View('Retries', 'show_retries'),
    View('Errors', 'show_errors'),
)
nav.register_element('top', topbar)

@app.route('/')
def index():
   try:
       client.info()
   except redis.exceptions.ResponseError:
       setup_data(cfg)

   mermaid_data = "sequenceDiagram\nautonumber\nStart->>+%s: write\n" %cfg['microservices'][0]['output']
   for ms in cfg['microservices'][1:-1]:
      mermaid_data += "%s->>+%s: read\n" %(ms['name'], ms['input'])
      mermaid_data += "%s->>%s: Process Message\n"  %(ms['name'], ms['name'])
      mermaid_data += "%s->>+%s: write\n" %(ms['name'], ms['output'])
      mermaid_data += "%s->>+ArbiterHash: write\n" %(ms['name'])

   mermaid_data += "%s->>+%s: read\n" %(cfg['microservices'][-1]['name'], cfg['microservices'][-1]['input'])
   mermaid_data += "%s->>%s: Process Message\n"  %(cfg['microservices'][-1]['name'],cfg['microservices'][-1]['name'])
   mermaid_data += "%s->>+Finish: write\n" %(cfg['microservices'][-1]['name'])
   mermaid_data += "%s->>+ArbiterHash: write\n" %(cfg['microservices'][-1]['name'])
        
   return render_template('top.html', microservices = cfg['microservices'], mermaid_data=mermaid_data)

@app.route('/messages')
def show_message():
   messageid = request.args.get('message')
   j = client.search(Query(messageid).limit_fields('id').verbatim()).docs[0].__dict__
   print(j)
   del j['id']
   del j['payload']
   p = re.compile(r'(\d{13,14})\-\d{1,4}')

   for x in j:
      m = p.search(j[x])
      if m:
         j[x] = datetime.datetime.fromtimestamp(float(m.group(1))/1000).strftime('%Y-%m-%d %H:%M:%S.%f')


   return render_template('showmessage.html', messages = j, messageid = messageid)

@app.route('/retry')
def show_retries():
  res = []
  for ms in cfg['microservices']:
     j = client.search(Query('*').add_filter(NumericFilter('%s_RETRY' % ms['name'], 1, NumericFilter.INF, minExclusive=False))).docs
     for w in list(map(lambda x : x.id, j)):
        res.append(w.replace('STATE:', ''))
  return render_template('showretries.html', messages = res)

@app.route('/firemessage', methods = ['POST'])
def firemessage():
   f = request.form.to_dict()
   rdb.xadd(cfg['microservices'][0]['name'], f)
   return redirect("/stats", code=302)

@app.route('/startform')
def start_form():
  return render_template('startform.html')

@app.route('/stats')
def show_stats():
   labels = []
   good_values = []
   retry_values = []
   for ms in cfg['microservices']:
      labels.append(ms['name'])
      try:
         x = rts.get("TS:%s:Ops" % ms['name'])
         good_values.append(x[1])
      except redis.exceptions.ResponseError:
         good_values.append(0)
      try:
         y = rts.get("TS:%s:RETRY:Ops" % ms['name'])
         retry_values.append(y[1])
      except redis.exceptions.ResponseError:
         retry_values.append(0)
   print(labels)
   print(good_values)
   print(retry_values)

   return render_template('stats.html', labels=labels, good_values=good_values, retry_values=retry_values)


@app.route('/errors')
def show_errors():
   errs = list(map(lambda x : x[1], rdb.xrevrange("Errors",  max='+', min='-')))
   return render_template('showerrors.html', errs=errs)

if __name__ == '__main__':
   bootstrap.init_app(app)
   nav.init_app(app)
   app.debug = True
   app.run(port=5010, host="0.0.0.0")
