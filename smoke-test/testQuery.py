#!/usr/bin/python
import unittest
import requests
import json
import glob


class testQuery(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBuild(self):
        base_url = "http://sandbox:7070/kylin/api"
        url = base_url + "/query"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache"
        }

        sql_files = glob.glob('sql/*.sql')
        index = 0
        for sql_file in sql_files:
            index += 1
            sql_statement = open(sql_file).read().strip()
            payload = "{\"sql\": \"" + sql_statement + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
            print 'Test Query #' + str(index) + ': \n' + sql_statement
            response = requests.request("POST", url, data=payload, headers=headers)

            self.assertEqual(response.status_code, 200, 'Query failed.')
            actual_result = json.loads(response.text)
            print 'Query duration: ' + str(actual_result['duration']) + 'ms'
            del actual_result['duration']

            expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
            self.assertEqual(actual_result, expect_result, 'Query result does not equal.')


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    unittest.main()
