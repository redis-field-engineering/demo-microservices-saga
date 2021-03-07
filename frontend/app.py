from flask import Flask, render_template, request, redirect
from flask_bootstrap import Bootstrap
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from redisearch import Client, Query, aggregation, reducers, IndexDefinition, TextField, NumericField, TagField


# From our local file
from datasetup import setup_data

from os import environ, getcwd

import redis
import yaml

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
)
nav.register_element('top', topbar)

@app.route('/')
def index():
   try:
       client.info()
   except redis.exceptions.ResponseError:
       setup_data(cfg)
   return render_template('top.html', microservices = cfg['microservices'])

if __name__ == '__main__':
   bootstrap.init_app(app)
   nav.init_app(app)
   app.debug = True
   app.run(port=5010, host="0.0.0.0")
