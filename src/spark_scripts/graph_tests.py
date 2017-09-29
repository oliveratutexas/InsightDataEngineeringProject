import unittest
from pyspark import SparkContext
from pyspark.sql import SparkSession
import spark_run


class GraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(GraphTest, cls).setUpClass()

        cls.sc = SparkContext('local', 'TestContext')
        cls.sc.setCheckpointDir('test_checkpoint')

        cls.ss = SparkSession(cls.sc).builder.master("local").getOrCreate()
        cls.ss.conf.set("spark.sql.shuffle.partitions", 2)
        cls.ss.conf.set("spark.executor.memory", "10m")

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

    def test_get_matches(self):
        #Trivial case
        _lst = [
        ('1', '2', 'someone1', 'Hi I like /r/sub1', 20, '/r/sub1', 'post1',1),
        ('2', '1', 'someone2', 'null', 20, '/r/sub1', 'post1',1),
        ('6', '7', 'no_match', 'no_match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'no_match', 'no_r/match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', 'r/match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', ' r/match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', 'r/match_here ', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', '/r/match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', ' /r/match_here', 20, '/r/sub1', 'post4',2),
        ('6', '7', 'match', '/r/match_here ', 20, '/r/sub1', 'post4',2),
        ('4', '3', 'someone4', '/u/no_match', 20, '/r/sub1', 'post1',3),
        ('3', '4', 'someone3', 'r/subThree', 20, '/r/sub2', 'post2',3)
        ]

        # _columns = self.columns + ['component']
        _columns = [
            'id', 'parent_id', 'author', 'body', 'score', 'subreddit',
            'post_id','component'
        ]


        test_frame_1 = self.ss.createDataFrame(_lst,_columns)

        match_frame = spark_run.get_matches(test_frame_1)
        match_frame.show()
        self.assertEqual(match_frame.count(),12,msg='Check the regex for fit')

    def test_remove_singular(self):
        result = spark_run.remove_singular(self.generalFrame, 'post_id')
        self.assertEqual(result.count(), 2)

    def test_produce_filtered_graph(self):
        _lst = [('1', '2', 'someone1', 'I really like /r/sub1', 20, '/r/sub1', 'post1'),
                ('2', '1', 'someone2', 'body2', 20, '/r/sub1', 'post1'),
                ('3', '4', 'someone3', 'body3', 20, '/r/sub2', 'post2')]

        _columns = [
            'id', 'parent_id', 'author', 'body', 'score', 'subreddit',
            'post_id'
        ]

        test_frame = self.ss.createDataFrame(_lst,_columns)

        # result = spark_run.produce_filtered_graph(self.ss, self.generalFrame,
        #                                           1, 'post_id')
        result = spark_run.produce_filtered_graph(test_frame,
                                                  'no_label')

        result.cache()
        result.show()
        # should remove post2
        self.assertEqual(len(result.collect()), 2)

        # should produce only one component

    def test_tree_trim(self):
        '''
        Trim nodes not reachable from token match.
        (No need to visualize off topic conversations)
        '''

        #basic tree

        #long side conversations pruning
        pass

    def test_multiple_reference(self):
        '''
        Multiple references should yield results
        '''
        pass

    def test_lone_node(self):
        '''
        There are no other nodes
        '''
        pass

    def test_lone_graph(self):
        '''
        Only comments no other node has replied to.
        '''
        pass

    def test_same_graph(self):
        '''
        contains multiple of the same reference.
        '''
        pass


if __name__ == '__main__':
    unittest.main()
