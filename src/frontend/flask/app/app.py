from flask import abort
from flask import Flask, redirect, render_template, send_file, send_from_directory
import re
import json
import graphviz as gv
import redis
import os
import textwrap
# import networkx
from collections import namedtuple

# r = None
app = Flask(__name__, static_url_path='/static')
requested = dict()
# site_root = 'http://www.linkjoin.live'

def filterGraph(match,edges):
    assert(match)
    assert(edges)

    def gen_edge_frame(ind,comments):
        edge_list = []
        edge_map = { comment['id'] : comment for comment in comments}
        cur_id = edges[ind]['id']

        while cur_id in edge_map:
            #put it in the edgelist
            edge_list.append(edge_map[cur_id])
            #get the next element
            cur_id = edge_map[cur_id]['parent_id']

        # assert(len(edge_list) > 1)
        return edge_list

    ans = []

    for ind,item in enumerate(edges):
        #generate each tree from a particular match
        if item['match'] == match:
            # find the longest length
            new_frames = gen_edge_frame(ind,edges)
            ans.extend(new_frames)


    # assert(len(ans) > 1)
    return ans


@app.route('/demo')
def demo():
    return redirect("/static/viz.html", code=302)


@app.route('/slides')
def slides():
    return redirect(
        "https://docs.google.com/presentation/d/1KdAnZx_1cPQSH-Cb50H46OR3RDVuO6AvGmev0U1t_M0/edit?usp=sharing",
        code=302)


def make_label(post):
    props = ['author', 'score']
    # TODO - filter body...
    dedented_text = textwrap.dedent(post['body']).strip()
    formatted_body = textwrap.fill(dedented_text, width=60)
    return "\n".join([str(post[prop]) for prop in props] + [formatted_body])


def add_to_dict(clusters, key, val):
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
        match_list = str(
            r.get(subreddit_name).decode('ascii', errors='ignore')).split("\n")
        json_obj = [json.loads(item) for item in match_list]
        print(json_obj[0])
        comments = filterGraph(subreddit_name,json_obj)


    clusters = {}

    for comment in comments:
        # if (comment['match'] == subreddit_name):
        #     add_to_dict(clusters, '!center!', comment)
        # else:
        add_to_dict(clusters, comment['subreddit'], comment)

    output_graph = gv.Digraph(name=subreddit_name, format='svg')
    output_graph.attr(label='Comment graph for /r/' + subreddit_name)
    output_graph.attr(fontsize='50')

    # in this dumb file format you have to...
    # add all the edges before the nodes you'd think
    # the python package would take care of that but
    # no...it doesn't :/
    # for comment in json_obj:
    #     if(comment.edge['match'] == subreddit_name):
    #         # output_graph.edge('subreddit_name',comment.edge['id'])
    #         # output_graph.edge(comment.edge['parent_id'],comment.edge['id'])
    #         output_graph.edge(comment.edge['id'],comment.edge['parent_id'])
    #         output_graph.edge('subreddit_name',comment.edge['id'])
    #     else:
    #         output_graph.edge(comment.edge['id'],comment.edge['parent_id'])
    #         # output_graph.edge(comment.edge['parent_id'],comment.edge['id'])
    my_subgraphs = []
    for key,cluster in clusters.items():

        temp_graph = gv.Digraph(name='cluster_' + key)
        temp_graph.attr(style='filled')
        temp_graph.attr(color='lightgrey')
        temp_graph.attr(label='/r/' + key)

        for comment in cluster:
            if(comment['match'] == subreddit_name):
                temp_graph.edge(comment['parent_id'],comment['id'])
                temp_graph.edge(comment['id'],'subreddit_name')
            else:
                temp_graph.edge(comment['parent_id'],comment['id'])


        output_graph.subgraph(temp_graph)
        # my_subgraphs.append(temp_graph)


    #add the name of the subreddit as a node
    output_graph.node('subreddit_name',label=subreddit_name,shape='doublecircle')
    for comment in comments:
        print(comment)
        output_graph.node(comment['id'],label=make_label(comment))

    # for comment in json_obj:
    #     output_graph.node(comment['id'],label=make_label(comment))
    #     if (comment['match'] == ''):
    #         add_to_dict(clusters, comment['subreddit'], comment)
    #     else:
    #         add_to_dict(clusters, comment['subreddit'], comment)
    #         add_to_dict(clusters, '!center!', comment)



    # for subreddit, posts in clusters.items():
    #     clust_graph = gv.Digraph(name=subreddit)
    #     for post in posts:
    #         clust_graph.node(post['id'], label=make_label(post))
    #         clust_graph.edge(post['id'], post['parent_id'])
    #     output_graph.subgraph(graph=clust_graph)

    # save text based file for representation
    output_graph.save()
    output_graph.render(directory='static/graphs')


@app.route('/tree/<subreddit_name>', methods=['GET'])
def get_tree(subreddit_name):
    '''
        Lazy loads building of the graph that corresponds to the
        subreddit of interest.
    '''
    global requested
    match_result = re.match('^[a-zA-Z0-9_]+$', subreddit_name)
    if len(subreddit_name) == 0 or not match_result:
        return 'There are no results for ' + subreddit_name + ' try another subreddit!'
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
    BASE_DIR = './'

    # Joining the base and the requested path
    abs_path = os.path.join(BASE_DIR, req_path)

    # Return 404 if path doesn't exist
    if not os.path.exists(abs_path):
        return abort(404)

    # Check if path is a file and serve
    print('abs_path', abs_path)
    if os.path.isfile(abs_path):
        return send_from_directory(abs_path)

    # Show directory contents
    files = os.listdir(abs_path)
    return render_template('files.html', files=files)


if __name__ == '__main__':
    with open('site.config.json') as config_handle:
        global r
        site_config = json.loads(config_handle.read())
        r = redis.StrictRedis(
            site_config['redis_url'], site_config['redis_port'], db=0)
        get_tree('todayilearned')

        # app.run((host)=site_config['host'], port=site_config['port'])
