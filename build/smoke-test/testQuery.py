#!/usr/bin/python

import unittest
import requests
import json
import glob
import sys

IS_PLUS = 1

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
            'cache-control': "no-cache",
            'accept' : "application/vnd.apache.kylin-v2+json"
        }

        sql_files = glob.glob('sql/*.sql')
        index = 0
        for sql_file in sql_files:
            index += 1
            if IS_PLUS =='0' and sql_file.endswith('-plus.sql'):
                print 'Skip Plus SQL file: ' + sql_file
                continue

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

            actual_result = json.loads(response.text)
            print actual_result
            print 'Query duration: ' + str(actual_result['data']['duration']) + 'ms'
            del actual_result['data']['duration']
            del actual_result['data']['hitExceptionCache']
            del actual_result['data']['storageCacheUsed']
            del actual_result['data']['totalScanCount']
            del actual_result['data']['totalScanBytes']

            expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
            self.assertEqual(actual_result, expect_result, 'Query result does not equal.')


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    IS_PLUS = sys.argv.pop()
    unittest.main()
