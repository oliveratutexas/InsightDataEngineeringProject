from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, BooleanType
from pyspark.sql.types import StructType, LongType, IntegerType
from pyspark.sql.functions import udf
from pyspark import SparkContext
from pyspark.sql.functions import regexp_replace, regexp_extract
from graphframes import GraphFrame
import itertools
import redis
import sys
import datetime

# import datetime
# import json


def comments_to_graph(df, id_col, src_col, dest_col):
    '''
    takes in a table of raw reddit data
    returns a graphframe
    '''
    vertices = df.withColumnRenamed(id_col, 'id')
    edges = vertices.select(src_col, dest_col).withColumnRenamed(
        src_col, 'src').withColumnRenamed(dest_col, 'dst')

    graph = GraphFrame(vertices, edges)
    return graph


def remove_singular(df, col):
    '''
        Returns a dataframe w/ the removal of
        all single occurances specified by grouping
    '''
    _count = df.groupby(col).count()
    plural_frames = df.join(_count, col, 'left').where(_count['count'] > 1)
    return plural_frames


# def get_matches(df):
# def get_match_groups(df):
#     '''
#         Returns every sub connected component containing
#         a match
#     '''
#     # TODO - how can I make this case insensitive?
#     match_components = df.where(
#         df['match'] != '').select('component').distinct()
#     return df.join(match_components, 'component', 'right_outer')


def get_matches(df,
                link_regex='((^)([\/]?r\/([a-zA-Z0-9_]+))($))+',
                group_index=4):

    match_frame = df.withColumn('match',
                                regexp_extract('body', link_regex,
                                               group_index))
    return match_frame


#TODO -test this out
def get_matched_components(df):
    '''
    Strips the components that have no links attached to them.
    Returns every sub connected component containing a match
    '''
    match_components = df.where(
        df['match'] != '').select('component').distinct()

    return df.join(match_components, 'component')


def get_path_nodes(graph, src_id):
    '''
    BFS returns an odd format for searching.
    Need our own format.
    '''
    # t3_ begging indicates a link to a post
    path = graph.bfs('id == "' + src_id + '"', 'parent_id LIKE "t3_%"')

    # edge columns begin with e
    # can return if there are not enough edges.
    filtered_cols = list(filter(lambda x: x[0] == 'e', path.columns))
    if (len(filtered_cols) < 2):
        return []

    # there's only one row guaranteed to return because
    # we're working with a tree
    big_row = path.select(*filtered_cols).collect()
    # return the original source id with the
    # edges we used
    ids = [val[1] for val in big_row[0]] + [src_id]
    return ids


def tree_trim(graph):
    '''
    we want to find every comment in this graph that leads to
    a subreddit link.

    requires a graph that has
    a match for every relevant regex

    ### NOTE
    I do not like this function. If I knew scala well, I'd re-implement
    the bfs algorithm to return edge dfs, union the dfs, and innersect
    the original frame to filter out wayward conversations.
    '''
    # find every item where there are matches
    graph.cache()

    search_rows = graph.vertices.where(
        graph.vertices['match'] != '').select('id').collect()
    search_ids = map(lambda x: x[0], search_rows)
    approve_lists = [get_path_nodes(graph, src_id) for src_id in search_ids]
    flattened_approvals = list(itertools.chain.from_iterable(approve_lists))
    result = graph.vertices.where(graph.vertices['id'].isin(flattened_approvals))
    graph.unpersist()
    return result


def link_join(df):
    '''
    "Joins" on links by creating a match_group for each component
    "Blows up" the table for threads containing multiple matches.
    '''

    # TODO - could this one be optimized?
    match_components = df.where(df['match'] != '').select(
        'match', 'component').withColumnRenamed('match',
                                                'match_group').distinct()
    joined_links = match_components.join(df, ['component'], 'left_outer')

    return joined_links


# def partition_graph_gen(df):
#     '''
#     Runs the connected components algorithm for each unique post_id
#     in the graph.
#     '''
#     # don't want to go to other partitions unnecessarily
#     df = df.repartition('post_id')
#     df = df.orderBy('post_id')
#     post_ids = [item[0] for item in df.select('post_id').distinct().collect()]
#     # TODO - do this properly by constructing and object
#     # with the appropriate schema and then unioning it
#     # with the other objects
#     seed = comments_to_graph(
#         df.where(df['post_id'] == post_ids[0]), 'id', 'id', 'parent_id')
#     ccs_seed = seed.connectedComponents()

#     # don't need a unique column tag for the first element
#     union_graph = get_matched_components(
#         remove_singular(ccs_seed, 'component'))

#     # This is bad code, I'm sorry.
#     # had to name pst_id because post_id collides with the
#     # variable name specified by whatever process
#     # spark uses to translate python to jvm code
#     for pst_id in post_ids[1:]:
#         graph = comments_to_graph(
#             df.where(df['post_id'] == pst_id), 'id', 'id', 'parent_id')
#         gccs = graph.connectedComponents()
#         filtered_graph = get_matched_components(
#             remove_singular(gccs, 'component'))

#         # adds a prefix to the column in case of collisions across different
#         # iterations of running CC.
#         filtered_graph = filtered_graph.withColumn(
#             'component', pst_id + filtered_graph['component'])
#         union_graph.union(filtered_graph)

#     union_graph.show()
#     return union_graph

def make_tree_path(edges,from_cond,to_cond):
    '''
    Takes in the edges from a tree;
    returns an edgelist ordered
    '''
    edges.sqlContext.createDat

def partition_graph_gen(df):
    '''
    Filters the table
    '''
    # don't want to go to other partitions unnecessarily
    df = df.repartition('post_id')
    df = df.orderBy('post_id')
    # creates a match column for each line
    match_table = get_matches(df)

    # trims irrelevant conversations from the graph
    graph = comments_to_graph(match_table, 'id', 'id', 'parent_id')
    trimmed_graph = tree_trim(graph)

    # remove singular items and conversations that have
    # no links to other subreddits
    gccs = trimmed_graph.connectedComponents()
    return get_matched_components(remove_singular(gccs, 'component'))


def write_to_redis(df, host='localhost', port=6379):
    '''
    Writes every group of trees to redis
    '''
    df.cache()
    match_rows = df.select('match_group').distinct().collect()
    matches = map(lambda col: col[0], match_rows)
    redis_handle = redis.StrictRedis(host=host, port=port, db=0)
    for match in matches:
        selection = df.where(df['match_group'] == match)
        json_blob = selection.toJSON().collect()
        redis_handle.set(str(match), "\n".join(json_blob))
    df.unpersist()


def get_clean_data(ss, data_path, reddit_schema):
    '''
    Returns a tabular representation of a graph
    with the appropriate fields and fields cleaned
    '''

    # parquet_path = data_path + '/reddit_comments.parquet'
    # write_handle = ss.read.json(data_path)
    # write_handle.write.parquet(parquet_path)
    # data_handle = ss.read.parquet(parquet_path)
    data_handle = ss.read.json(data_path, schema=reddit_schema)

    columns = [
        'id', 'parent_id', 'author', 'body', 'score', 'subreddit', 'link_id'
    ]
    table = data_handle.select(*columns)
    parent_col = table['parent_id']

    # t1_ comments
    # t3_ links
    # need to strip these away to get actual values
    table = table.where(
        parent_col.startswith('t3_') | parent_col.startswith('t1_'))
    table = table.withColumn('parent_id',
                             regexp_replace('parent_id', '(t1_)',
                                            '')).withColumnRenamed(
                                                'link_id', 'post_id')

    return partition_graph_gen(table)


def run_tree_join(ACCESS_KEY, SECRET_KEY, REDIS_SERVER, REDIS_PORT,
                  CHECKPOINT_REMOTE_DIR):

    sc = SparkContext(appName='TreeJoin')

    sc.setCheckpointDir(CHECKPOINT_REMOTE_DIR)

    ss = SparkSession(sc).builder.getOrCreate()
    # ss.conf.set("spark.sql.shuffle.partitions", 4)

    # file_download_path = 's3a://heyyall/test_folder'
    # file_download_path = 's3a://heyyall/big_test'
    # file_download_path = 'RC_2011-01_my_slice_2'
    file_download_path = 's3a://heyyall/reddit_data/RC_2011-01'

    reddit_schema = StructType([
        StructField('archived', BooleanType()),
        StructField('author', StringType()),
        StructField('author_flair_css_class', StringType()),
        StructField('author_flair_text', StringType()),
        StructField('body', StringType()),
        StructField('controversiality', IntegerType()),
        StructField('created_utc', StringType()),
        StructField('distinguished', StringType()),
        StructField('downs', IntegerType()),
        StructField('edited', StringType()),
        StructField('gilded', IntegerType()),
        StructField('id', StringType()),
        StructField('link_id', StringType()),
        StructField('name', StringType()),
        StructField('parent_id', StringType()),
        StructField('retrieved_on', LongType()),
        StructField('score', IntegerType()),
        StructField('score_hidden', BooleanType()),
        StructField('subreddit', StringType()),
        StructField('subreddit_id', StringType()),
        StructField('ups', IntegerType())
    ])

    clean_data = get_clean_data(ss, file_download_path, reddit_schema)
    joined_links = link_join(clean_data)
    joined_links = joined_links.repartition('match_group')
    write_to_redis(joined_links, REDIS_SERVER, REDIS_PORT)


if __name__ == '__main__':

    ACCESS_KEY = sys.argv[1]
    SECRET_KEY = sys.argv[2]
    REDIS_SERVER = sys.argv[3]
    REDIS_PORT = sys.argv[4]
    CHECKPOINT_DIR = sys.argv[5]

    run_tree_join(ACCESS_KEY, SECRET_KEY, REDIS_SERVER, REDIS_PORT,
                  CHECKPOINT_DIR)
