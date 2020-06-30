#
# Copyright (C) 2020 Kyligence Inc. All rights reserved.
#
# http://kyligence.io
#
# This software is the confidential and proprietary information of
# Kyligence Inc. ("Confidential Information"). You shall not disclose
# such Confidential Information and shall use it only in accordance
# with the terms of the license agreement you entered into with
# Kyligence Inc.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
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
