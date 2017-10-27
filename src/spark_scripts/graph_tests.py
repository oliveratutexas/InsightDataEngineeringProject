import unittest
from pyspark import SparkContext
from pyspark.sql import SparkSession
import spark_run
from graphframes import GraphFrame

class GraphTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(GraphTest, cls).setUpClass()

        cls.sc = SparkContext(appName='TreeTest')
        cls.sc.setCheckpointDir('checkpoint')

        cls.ss = SparkSession(cls.sc).builder.master("local[*]").getOrCreate()
        cls.ss.conf.set("spark.sql.shuffle.partitions", 10)
        cls.ss.conf.set("spark.executor.memory", "1g")
        cls.ss.conf.set("spark.default.parallelism", "10")


        _lst = [('1', '2', 'someone1', 'body1', 20, '/r/sub1', 'post1'),
                ('2', '1', 'someone2', 'body2', 20, '/r/sub1', 'post1'),
                ('3', '4', 'someone3', 'body3', 20, '/r/sub2', 'post2')]

        cls.columns = [
            'id', 'parent_id', 'author', 'body', 'score', 'subreddit',
            'post_id'
        ]

        cls.generalFrame = cls.ss.createDataFrame(_lst, cls.columns).repartition(10)
        cls.generalFrame.cache()

    def test_link_join(self):
        '''
        Returns the table that can be blocked into connected trees on the
        match_group column.
        '''
        _lst = [
            ('1', '2', 'someone1', 'body /r/sub1', 20, '/r/sub1', 'post1',1,'sub'),
            ('2', '1', 'someone2', 'in sub1', 20, '/r/sub1', 'post1',1,''),
            ('4', '3', 'someone4', 'body2 /r/sub1', 20, '/r/sub2', 'post2',2,'sub'),
            ('3', '4', 'someone3', 'in sub1 and subOther', 20, '/r/sub2', 'post2',2,'subOther'),
            ('5', '6', 'someone5', 'body /r/subOther', 20, '/r/sub1', 'post1',3,'subOther'),
            ('6', '5', 'someone6', 'in sub2', 20, '/r/sub1', 'post1',3,''),
            ('7', '8', 'someone7', 'body2 /r/subOther', 20, '/r/sub2', 'post2',4,'subOther'),
            ('8', '7', 'someone8', 'in sub2', 20, '/r/sub2', 'post2',4,'')
        ]
        _columns = [
            'id', 'parent_id', 'author', 'body', 'score', 'subreddit',
            'post_id','component', 'match'
        ]


        test_frame = self.ss.createDataFrame(_lst, _columns)
        merged_frame = spark_run.link_join(test_frame)
        merged_frame.cache()
        merged_frame.show()
        merged_list = merged_frame.select('match_group').distinct().collect()
        self.assertEqual(len(merged_list),2)

    def test_get_matched_components(self):
        #Trivial case
        _lst = [
            ('1', ''),
            ('1', ''),
            ('2', ''),
            ('2', 'match_here'),
            ('3', ''),
            ('3', ''),
            ('3', ''),
            ('4', ''),
            ('4', 'match_here'),
            ('5', '')
        ]

        # _columns = self.columns + ['component']
        _columns = [
            'component','match'
        ]


        test_frame_1 = self.ss.createDataFrame(_lst,_columns)

        match_frame = spark_run.get_matched_components(test_frame_1)
        match_frame.show()
        self.assertEqual(match_frame.count(),4)

    def test_remove_singular(self):
        result = spark_run.remove_singular(self.generalFrame, 'post_id')
        self.assertEqual(result.count(), 2)

    def test_tree_trim(self):
        '''
        Removes irrelvant branches from original post.
        Input:
            1           4
           2 3*        5 6
                          7*
        Output:
            1           4
             3*          6
                          7*
        '''
        _lst = [
            ('1', 't3_100', 'som1', 'body', 'post1','1_comp',''),
            ('2', '1', 'som2', 'body', 'post1','1_comp',''),
            ('3', '1', 'som3', '/r/match1', 'post1','1_comp','match1'),
            ('4', 't3_2', 'som1', 'body', 'post1','1_comp',''),
            ('5', '4', 'som2', 'body', 'post1','1_comp',''),
            ('6', '4', 'som3', '', 'post1','1_comp',''),
            ('7', '6', 'som3', '/r/match2', 'post1','1_comp','match2')
        ]
        _columns = [
            'id', 'parent_id', 'author', 'body',
            'post_id','component', 'match'
        ]

        test_frame = self.ss.createDataFrame(_lst, _columns)

        graph_frame = spark_run.comments_to_graph(test_frame,'id','id','parent_id')
        trimmed_tree = spark_run.tree_trim(graph_frame)
        self.assertTrue(
            len(trimmed_tree.collect()) == 5
        )

    def test_same_graph(self):
        '''
        contains multiple of the same reference.
        '''
        pass


if __name__ == '__main__':
    unittest.main()
