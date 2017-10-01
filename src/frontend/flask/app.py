from flask import abort
from flask import Flask, redirect
import re
import json
import graphviz as gv

import redis


#r = redis.StrictRedis(host='localhost', port=6379, db=0)
app = Flask(__name__)


@app.route('/demo')
def demo():
    return redirect("http://www.google.com", code=302)


@app.route('/slides')
def slides():
    return redirect("http://www.example.com", code=302)

def make_label(post):
    props = ['author','score','body']
    #TODO - filter body...
    return "\n".join((post[prop] for prop in props))

def add_cluster(clusters, key, val):
    if key in clusters:
        clusters[key].append(val)
    else:
        clusters[key] = [val]


def create_vis(match_name,json_obj=None):
    builder = []
    if not json_obj:
        json_obj = json.reads()
    clusters = {}

    for item in json_obj:
        if (item['match'] == ''):
            add_cluster(clusters, item['subreddit'], item)
        else:
            add_cluster(clusters, '!center!', item)

    output_graph = gv.Digraph(name=match_name,format='svg')

    for subreddit,posts in clusters.items():
        clust_graph = gv.Digraph(name=subreddit)
        for post in posts:
            clust_graph.node(post['id'],label=make_label(post))
            clust_graph.edge(post['id'],post['parent_id'])
        output_graph.subgraph(graph=clust_graph)
    output_graph.save()
    output_graph.render()


@app.route('/tree/<tree_name>', methods=['GET'])
def get_tree(tree_name):
    my_str = ''
    
    match_result = re.match('^[a-zA-Z0-9_]+$',tree_name)
    # Bind to PORT if defined, otherwise default to 5000.
    if len(tree_name) == 0 or not match_result:
        abort(404)
    return 'This is where I would put a response....IF I HAD ONE!!!'
