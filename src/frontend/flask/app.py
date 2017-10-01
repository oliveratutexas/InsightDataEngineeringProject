from flask import abort
from flask import Flask,redirect
import redis

#r = redis.StrictRedis(host='localhost', port=6379, db=0)
app = Flask(__name__)

@app.route('/demo')
def demo():
    return redirect("http://www.google.com", code=302)

@app.route('/slides')
def slides():
    return redirect("http://www.example.com", code=302)

@app.route('/tree/<tree_name>', methods=['GET'])
def get_tree(tree_name):
    # Bind to PORT if defined, otherwise default to 5000.
    if len(tree_name) == 0:
        abort(404)
    return 'This is where I would put a response....IF I HAD ONE!!!'
