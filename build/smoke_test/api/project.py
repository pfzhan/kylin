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


class Project:
    # All project APIs

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def create_project(self, project_desc_data):
        url = self.base_url + '/projects'

        payload = project_desc_data
        response = requests.request('POST', url, json=payload, headers=self.headers, verify=False)
        return response

    def set_source_type(self, project_name):
        url = self.base_url + '/projects/{}/source_type'.format(project_name)
        payload = {
            'source_type': 9
        }
        response = requests.request('PUT', url, json=payload, headers=self.headers, verify=False)
        return response

    def get_projects(self):
        req_url = self.base_url + '/projects?page_offset=0&page_size=10000'
        return requests.get(url=req_url, headers=self.headers)

    @staticmethod
    def get_project_desc(project_name, resp):
        for p in json.loads(resp.text)['data']['value']:
            if p.get('name', None) == project_name:
                return p
        return {}

    @staticmethod
    def projects_size(resp):
        if 'total_size' in json.loads(resp.text)['data']:
            return json.loads(resp.text)['data']['total_size']

        return json.loads(resp.text)['data']['size']

    def delete_project(self, project_name):
        req_url = self.base_url + '/projects/' + project_name
        return requests.delete(url=req_url, headers=self.headers)

    def set_acceleration_rule(self, payload):
        req_url = self.base_url + '/query/favorite_queries/rules'
        return requests.put(url=req_url, json=payload, headers=self.headers)

    def session_get_projects(self, session):
        req_url = self.base_url + '/projects?page_offset=0&page_size=5'
        return session.get(url=req_url)

    def grant_project_access(self, project_uuid, username):
        req_url = self.base_url + '/access/ProjectInstance/' + project_uuid
        payload = {'permission': 'ADMINISTRATION', 'principal': True, 'sid': username}
        return requests.post(url=req_url, json=payload, headers=self.headers)
