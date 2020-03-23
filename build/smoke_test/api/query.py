import json

import requests
from config import common_config


class Query:

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def execute_query(self, project_name, sql):
        url = self.base_url + '/query'
        payload = {'acceptPartial': True, 'limit': 500, 'offset': 0, 'project': project_name,
                   'sql': sql, 'backdoorToggles ': {'DEBUG_TOGGLE_HTRACE_ENABLED': False}}
        response = requests.request('POST', url, json=payload, headers=self.headers)
        return response

    @staticmethod
    def is_pushdown_query(response):
        return json.loads(response.text)['data']['pushDown'] is True

    @staticmethod
    def get_engine_type(resp):
        return json.loads(resp.text)['data']['engineType']

    @staticmethod
    def get_model_alias(resp):
        if not json.loads(resp.text)['data']['realizations']:
            return 'No realization found'
        return json.loads(resp.text)['data']['realizations'][0]['modelAlias']

    @staticmethod
    def hit_table_index(resp):
        for r in json.loads(resp.text)['data']['realizations']:
            if r.get('indexType', None) == 'Table Index':
                return True
        return False

    @staticmethod
    def result_row_count(resp):
        return json.loads(resp.text)['data']['resultRowCount']
