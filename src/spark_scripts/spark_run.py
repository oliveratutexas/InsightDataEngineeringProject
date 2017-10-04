from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import regexp_replace, regexp_extract
from graphframes import GraphFrame
import redis
import datetime
import json


def comments_to_graph(df, id_col, src_col, dest_col):
    '''
    takes in a table of raw reddit data
    returns a graphframe
    '''
    vertices = df.withColumnRenamed(id_col, 'id')
    vertices.cache()
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


def get_matches(df,
                regex='(^|\s+)([\/]?r\/([a-zA-Z0-9_]+))($|\s+)',
                group_index=3):
    '''
        Returns connectedComponents which have the string of interest
    '''
    # TODO - how can I make this case insensitive?
    match_frame = df.withColumn('match',
                                regexp_extract('body', regex, group_index))
    match_components = match_frame.where(
        match_frame['match'] != '').select('component').distinct()
    return match_frame.join(match_components, 'component', 'right_outer')


def link_join(df):
    '''
     "Joins" on links by creating a label for each component
    '''

    # TODO - could this one be optimized?
    match_components = df.where(df['match'] != '').select(
        'match', 'component').withColumnRenamed('match',
                                                'match_group').distinct()
    joined_links = match_components.join(df, ['component'], 'left_outer')

    return joined_links


def produce_filtered_graph(df, label='no_label'):
    '''
    make the entire dataframe into a graph joined by
    a particular column
    '''
    section_graph = comments_to_graph(df, 'id', 'id', 'parent_id')
    single_filtered = remove_singular(
        section_graph.connectedComponents(checkpointInterval=1), 'component')
    ccs = get_matches(single_filtered)

    # TODO - Do I need to append label to column?

    return ccs


def write_to_redis(df, host='localhost', port=3000):
    df.cache()
    match_rows = df.select('match_group').distinct().collect()
    matches = map(lambda col: col[0], match_rows)
    r = redis.StrictRedis(host=host, port=port, db=0)
    for ind, match in enumerate(matches):
        selection = df.where(df['match_group'] == match)
        json_blob = selection.toJSON().collect()
        print(str(match))
        print(json.dumps(json_blob))
        r.set(str(match), "\n".join(json_blob))


def get_clean_data(ss, datapath):

    data_handle = ss.read.json(datapath)

    columns = [
        'id', 'parent_id', 'author', 'body', 'score', 'subreddit', 'link_id'
    ]
    rows = data_handle.select(*columns)
    parent_col = rows['parent_id']

    # t1 & t3 prefixes indicate submission references and other comments
    rows = rows.where(
        parent_col.startswith('t3_') | parent_col.startswith('t1_'))
    rows = rows.withColumn('parent_id',
                           regexp_replace('parent_id', '(t3_|t1_)',
                                          '')).withColumnRenamed(
                                              'link_id', 'post_id')
    rows = rows.repartition(30, 'post_id')

    return produce_filtered_graph(rows)


def run_tree_join():

    # TODO - what's the proper way to use a provided sc
    sc = SparkContext(appName='TreeJoin')

    # TODO - why is this required? It wasn't listed in the documentation
    sc.setCheckpointDir('checkpoint')

    ss = SparkSession(sc).builder.master("local[*]").getOrCreate()

    file_download_path = 'RC_2011-01_my_slice_2'
    # TODO - repartition
    clean_data = get_clean_data(ss, file_download_path)
    joined_links = link_join(clean_data)
    write_to_redis(joined_links, 'localhost', 6379)

    # TODO - at which points should I repartition on on what key?
    output_path = file_download_path + '_output_' + datetime.datetime.now(
    ).strftime("%Y_%m_%d_%H__%M__%S")
    joined_links.orderBy('match_group').write.json(output_path)
    # joined_links.orderBy('match_group')\
    # .write.json(file_download_path + '_output_')
    joined_links.orderBy('match_group').show()


if __name__ == '__main__':
    run_tree_join()
