from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import pyspark
import json
import sys
from pyspark.sql.functions import regexp_replace, col

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'  

#TODO - check this for redundancy
sc = SparkContext('local','SimpleApp')
sc.setCheckpointDir('checkpoint')

ss = SparkSession(sc).builder.master("local").getOrCreate()

#TODO - enable file configuration for security.
# ACCESS_KEY = sys.argv[1]
# SECRET_KEY = sys.argv[2]
SECRET_KEY = ''
ACCESS_KEY = ''


file_download_path = 's3a://heyyall/RC_2006-01'

#sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",ACCESS_KEY)
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",SECRET_KEY)

text_handle = ss.read.json(file_download_path)
# [('archived', 'boolean'), ('author', 'string'), ('author_flair_css_class', 'string'), ('author_flair_text', 'string'), ('body', 'string'), ('controversiality', 'bigint'), ('created_utc', 'string'), ('distinguished', 'string'), ('downs', 'bigint'), ('edited', 'string'), ('gilded', 'bigint'), ('id', 'string'), ('link_id', 'string'), ('name', 'string'), ('parent_id', 'string'), ('retrieved_on', 'bigint'), ('score', 'bigint'), ('score_hidden', 'boolean'), ('subreddit', 'string'), ('subreddit_id', 'string'), ('ups', 'bigint')]

#parent_id remains property on vertex to reconstruct graph downstream
vertices = text_handle.select('id','parent_id','author','body','created_utc','edited','score','subreddit').withColumn('parent_id',regexp_replace('parent_id','(t3_|t2_|t1_|t4_|t5_)',''))
#TODO - refactor this later.
#TODO - drop unused t# prefixes which refer to irrelevant data on edges from edges
# vertices = vertices.withColumn('parent_id',regexp_replace('parent_id','t1_|t2_|t3_|t4_|t5_',''))
#TODO - check and see if there's a more concise way I can do this with selecting and renaming at the same time
edges = text_handle.select('id','parent_id').withColumnRenamed('id','src').withColumn('parent_id',regexp_replace('parent_id','(t3_|t2_|t1_|t4_|t5_)','')).withColumnRenamed('parent_id','dst')
graph = GraphFrame(vertices,edges)
connComp = graph.connectedComponents()

#TODO - check return type
#TODO - try this again when I have a working regex and a big enough dataset
# comment_matches = wCompCol.filter(wCompCol.body.rlike(subreddit_regex))
comment_matches = connComp
components = comment_matches.select('component').distinct().collect()

trees = dict()

for comp in components:
    comp_number = comp[0]
    tree_frame = connComp.where(col( 'component' ) == comp_number)
    # Trees of size 1 have no useful information
    # if(tree_frame.count() > 1):
    trees[comp_number] = tree_frame

#TODO - delete
# This is just an example to make sure the components are showing up as they should. 
for tree_num,tree_frame in trees.items():
    if(tree_frame.count() > 1):
        print(tree_frame.collect())
        break

