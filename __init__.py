import sys
sys.path.append('/')

from flask import Flask
from flask.ext.mongoengine import MongoEngine
from flask.ext.cache import Cache

app = Flask(__name__)
app.config["MONGODB_SETTINGS"] = {'DB': "skiptwo"}
app.config["SECRET_KEY"] = "secret"
app.debug = True
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

db = MongoEngine(app)

def register_blueprints(app):
    # Prevents circular imports
    from skiptwo.views import twitch_vods
    app.register_blueprint(twitch_vods)

register_blueprints(app)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
