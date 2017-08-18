#!/usr/bin/python

import unittest
import requests
import os
import zipfile

class testDiag(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDiag(self):
        url = "http://sandbox:7070/kylin/api/kybot/dump"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache",
            'accept' : "application/vnd.apache.kylin-v2+json"
        }

        response = requests.get(url, headers = headers)
        self.assertEqual(response.status_code, 200, "Diagnosis rest call failed.")

        with open("kybot_pack.zip", "wb") as pack:
            pack.write(response.content)

        self.assertTrue(os.path.exists("kybot_pack.zip"), "Diagnosis pack not found.")

        zf = zipfile.ZipFile("kybot_pack.zip")
        file_list = zf.namelist()
        print "file list:"
        print file_list

        base_dir = file_list[0].split("/")[0]
        print "base dir: " + base_dir

        self.assertTrue(base_dir + "/metadata/table/DEFAULT.KYLIN_SALES.json" in file_list)
        self.assertTrue(base_dir + "/metadata/model_desc/kylin_sales_model.json" in file_list)
        self.assertTrue(base_dir + "/metadata/cube/kylin_sales_cube.json" in file_list)
        self.assertTrue(base_dir + "/metadata/cube_desc/kylin_sales_cube.json" in file_list)
        self.assertTrue(base_dir + "/metadata/project/learn_kylin.json" in file_list)
        self.assertTrue(base_dir + "/logs/kylin.log" in file_list)
        self.assertTrue(base_dir + "/logs/kybot.log" in file_list)
        self.assertTrue(base_dir + "/logs/kylin.out" in file_list)
        self.assertTrue(base_dir + "/conf/kylin.properties" in file_list)
        self.assertTrue(base_dir + "/info" in file_list)
        self.assertTrue(base_dir + "/kylin_env" in file_list)
        self.assertTrue(base_dir + "/commit_SHA1" in file_list)
        self.assertNotEqual(len([x for x in file_list if x.startswith(base_dir + "/jobs/")]), 0, 'jobs not found')

if __name__ == '__main__':
    print 'Test Diagnosis for Kylin sample.'
    unittest.main()
