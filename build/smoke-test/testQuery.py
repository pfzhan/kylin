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

    def testQuery(self):
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
            sql_fp = open(sql_file)
            sql_statement = ''
            sql_statement_lines = open(sql_file).readlines()
            for sql_statement_line in sql_statement_lines:
                if not sql_statement_line.startswith('--'):
                    sql_statement += sql_statement_line.strip() + ' '
            payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
            print 'Test Query #' + str(index) + ': \n' + sql_statement
            response = requests.request("POST", url, data=payload, headers=headers)

            self.assertEqual(response.status_code, 200, 'Query failed.')
            print response.text

            actual_result = json.loads(response.text)
            print 'Query duration: ' + str(actual_result['duration']) + 'ms'
            del actual_result['duration']
            del actual_result['hitExceptionCache']
            del actual_result['storageCacheUsed']
            del actual_result['totalScanCount']

            expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
            self.assertEqual(actual_result, expect_result, 'Query result does not equal.')


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    unittest.main()
