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


class Model:
    """All model APIs"""

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def create_model(self, model_desc_data):
        url = self.base_url + '/models'
        # json description of sample_model
        payload = model_desc_data
        response = requests.request('POST', url, json=payload, headers=self.headers)
        return response

    def get_model_id(self, project_name):
        url = self.base_url + '/models?page_offset=0&page_size=10&exact=false&' \
                              'model_name=&sort_by=&reverse=true&project=' + project_name
        response = requests.request('GET', url, headers=self.headers)
        model_id = json.loads(response.text)['data']['value'][0]['uuid']
        return model_id

    def clone_model(self, project_name, model_id):
        url = self.base_url + '/models/' + model_id + '/clone'
        payload = {'project': project_name, 'new_model_name': model_id + '_clone'}
        response = requests.request('POST', url, json=payload, headers=self.headers)
        return response

    def delete_model(self, project_name, model_name):
        url = self.base_url + '/models/' + model_name + '?project=' + project_name
        response = requests.request('DELETE', url, headers=self.headers)
        return response

    def edit_model(self, edited_model_desc):
        req_url = self.base_url + '/models/semantic'
        return requests.put(url=req_url, json=edited_model_desc, headers=self.headers)

    def add_aggregate_indices(self, payload):
        req_url = self.base_url + '/index_plans/rule'
        return requests.put(url=req_url, json=payload, headers=self.headers)

    def add_table_index(self, payload):
        req_url = self.base_url + '/index_plans/table_index'
        return requests.post(url=req_url, json=payload, headers=self.headers)

    def load_data(self, project_name, model_id):
        """
        Automatically trigger model build action
        :param project_name:
        :param model_id:
        :return:
        """
        req_url = self.base_url + '/models/{}/segments'.format(model_id)
        payload = {'project': project_name}
        return requests.post(url=req_url, json=payload, headers=self.headers)

    def list_models(self, project_name):
        url = self.base_url + '/models?exact=false&model_name=&sort_by=&reverse=true&project=' + project_name
        response = requests.request('GET', url, headers=self.headers)
        return response.json()

    def get_rule(self, project_name, model_id):
        req_url = self.base_url + '/index_plans/rule?project=' + project_name + '&model=' + model_id
        return requests.get(url=req_url, headers=self.headers).json()
