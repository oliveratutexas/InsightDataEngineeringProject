import unittest
import json
import app
import os

class FlaskTests(unittest.TestCase):
    def test_make_data_string(self):
        data = [
            '{"match":"match1","match_group":"match1","id":"1","parent_id":"2","body":"body","author":"user1","score":"20"}'
        ]
        data = [json.loads(datum) for datum in data]
        name = 'test_match'
        app.create_vis(name,data)
        self.assertTrue(os.path.isfile('./' + name + '.gv.svg'))

if __name__=="__main__":
    unittest.main()
