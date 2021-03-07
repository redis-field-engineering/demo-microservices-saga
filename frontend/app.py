from flask import Flask, render_template, request, redirect
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from redisearch import Client, Query, aggregation, reducers, IndexDefinition, TextField, NumericField, TagField, NumericFilter


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
   app_conf = getcwd() + "/../backend/config.yml"

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

nav = Nav()
topbar = Navbar('',
    View('Home', 'index'),
    View('Retries', 'show_retries'),
)
nav.register_element('top', topbar)

@app.route('/')
def index():
   try:
       client.info()
   except redis.exceptions.ResponseError:
       setup_data(cfg)
   return render_template('top.html', microservices = cfg['microservices'])

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


if __name__ == '__main__':
   bootstrap.init_app(app)
   nav.init_app(app)
   app.debug = True
   app.run(port=5010, host="0.0.0.0")