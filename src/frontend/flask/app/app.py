from flask import abort
from flask import Flask, redirect, render_template, send_file, send_from_directory
import re
import json
import graphviz as gv
import redis
import os

r = redis.StrictRedis(host='localhost', port=6379, db=0)
# bound to :5000 + by default
app = Flask(__name__, static_url_path='/static')
requested = dict()
# site_root = 'http://www.linkjoin.live'
site_root = 'localhost'

@app.route('/demo')
def demo():
    return redirect("/static/viz.html", code=302)


@app.route('/slides')
def slides():
    return redirect(
        "https://docs.google.com/presentation/d/1KdAnZx_1cPQSH-Cb50H46OR3RDVuO6AvGmev0U1t_M0/edit?usp=sharing",
        code=302)


def make_label(post):
    props = ['author', 'score', 'body']
    # TODO - filter body...
    return "\n".join((str(post[prop]) for prop in props))


def add_cluster(clusters, key, val):
    if key in clusters:
        clusters[key].append(val)
    else:
        clusters[key] = [val]


def create_vis(subreddit_name, json_obj=None):
    '''
        Sorts posts into their perspective subreddits and
        into the "center" where they point to their
        reference subreddit.
    '''
    if not json_obj:
        match_list = str(r.get(subreddit_name).decode('ascii')).split("\n")
        json_obj = (json.loads(item) for item in match_list)
    clusters = {}

    for item in json_obj:
        if (item['match'] == ''):
            add_cluster(clusters, item['subreddit'], item)
        else:
            add_cluster(clusters, '!center!', item)

    output_graph = gv.Digraph(name=subreddit_name, format='svg')

    for subreddit, posts in clusters.items():
        clust_graph = gv.Digraph(name=subreddit)
        for post in posts:
            clust_graph.node(post['id'], label=make_label(post))
            clust_graph.edge(post['id'], post['parent_id'])
        output_graph.subgraph(graph=clust_graph)

    # save text based file for representation
    output_graph.save()
    output_graph.render(directory='graphs')


@app.route('/tree/<subreddit_name>', methods=['GET'])
def get_tree(subreddit_name):
    '''
        Lazy loads building of the graph that corresponds to the
        subreddit of interest.
    '''
    global requested
    match_result = re.match('^[a-zA-Z0-9_]+$', subreddit_name)
    if len(subreddit_name) == 0 or not match_result:
        abort(404)
    else:
        try:
            # don't generate the same graph twice.
            if subreddit_name not in requested:
                create_vis(subreddit_name)
                requested[
                    subreddit_name] = 'static/graphs/' + subreddit_name + '.gv.svg'

            return redirect(requested[subreddit_name], code=302)
        except Exception as e:
            raise e
            abort(404)

@app.route('/', defaults={'req_path': ''})
@app.route('/<req_path>')
def dir_listing(req_path):
    print('HELLO WORLD!')
    BASE_DIR = './'

    # Joining the base and the requested path
    abs_path = os.path.join(BASE_DIR, req_path)

    # Return 404 if path doesn't exist
    if not os.path.exists(abs_path):
        return abort(404)

    # Check if path is a file and serve
    print('abs_path',abs_path)
    if os.path.isfile(abs_path):
        return send_from_directory(abs_path)

    # Show directory contents
    files = os.listdir(abs_path)
    return render_template('files.html', files=files)


if __name__ == '__main__':
    app.run(host=site_root, port=1234)
