import unittest
import json
import app
import os

class FlaskTests(unittest.TestCase):
    def test_make_data_string(self):
        data = [
            '{"match":"match1","match_group":"match1","id":"1","parent_id":"2","body":"body","author":"user1","score":"20","subreddit":"test_sub_1"}'
        ]
        data = [json.loads(datum) for datum in data]
        name = 'test_match'
        app.create_vis(name,data)
        self.assertTrue(os.path.isfile('./graphs/' + name + '.gv.svg'))

    def test_make_data_string_big(self):
        data = [
            '{"match":"match1","match_group":"match1","id":"1","parent_id":"2","body":"/r/match1","author":"user1","score":"20","subreddit":"test_sub_1"}',
            '{"match":"","match_group":"match1","id":"2","parent_id":"3","body":"body","author":"user2","score":"20","subreddit":"test_sub_1"}',
            '{"match":"match1","match_group":"match1","id":"4","parent_id":"5","body":"/r/match1","author":"user1","score":"20","subreddit":"test_sub_2"}',
            '{"match":"","match_group":"match1","id":"5","parent_id":"6","body":"body","author":"user1","score":"20","subreddit":"test_sub_2"}'
        ]
        data = [json.loads(datum) for datum in data]
        name = 'test_match_2'
        app.create_vis(name,data)
        self.assertTrue(os.path.isfile('./graphs/' + name + '.gv.svg'))



if __name__=="__main__":
    unittest.main()
