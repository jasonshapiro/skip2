import sys
sys.path.append('/')

from skiptwo import app

if __name__ == "__main__":

	app.run(host='0.0.0.0', threaded=True)
