import json
import os

import requests
from config import common_config


class FavoriteQuery:

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def import_sql_files(self, project_name, file_path):
        headers_n = self.headers.copy()
        headers_n.pop('Content-Type')
        req_url = self.base_url + '/query/favorite_queries/sql_files?project=' + project_name
        with open(file_path, 'rb') as sql_fd:
            files = {'files': (os.path.basename(file_path), sql_fd)}
            response = requests.post(url=req_url, headers=headers_n, files=files)
            return response

    @staticmethod
    def capable_sql_num(resp):
        return json.loads(resp.text)['data']['capable_sql_num']

    def add_to_favorite_queries(self, project_name, sql_imported):
        req_url = self.base_url + '/query/favorite_queries'
        payload = {'project': project_name, 'sqls': sql_imported}
        response = requests.post(url=req_url, headers=self.headers, json=payload)
        return response

    def list_favorite_queries(self, project_name):
        url = self.base_url + '/query/favorite_queries?project=' + project_name
        response = requests.request('GET', url, headers=self.headers)
        return response

    def accelerate_now(self, project_name, accelerate_size):
        req_url = self.base_url + '/query/favorite_queries/accept?project={}' \
                                  '&accelerate_size={}'.format(project_name, accelerate_size)
        response = requests.request('PUT', url=req_url, headers=self.headers)
        return response
