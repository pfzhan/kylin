#!/usr/bin/python

import unittest
import requests

class testDiag(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDiag(self):
        url = "http://sandbox:7070/kylin/api/diag/project/learn_kylin/download"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache"
        }

        response = requests.get(url, headers = headers)

        self.assertEqual(response.status_code, 200, 'Diagnosis failed.')

if __name__ == '__main__':
    print 'Test Diagnogis for Kylin sample.'
    unittest.main()
