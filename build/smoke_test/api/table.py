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
import requests
from config import common_config


class Table:
    """All table APIs"""

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def load_table(self, project_name, load_table_list, need_sampling=False):
        #  this is for loading table and sampling table
        url = self.base_url + '/tables'
        payload = {'project': project_name,
                   'data_source_type': 9,
                   'tables': load_table_list,
                   'databases': [],
                   'sampling_rows': 100000,
                   'need_sampling': need_sampling
                   }
        response = requests.request('POST', url, json=payload, headers=self.headers)

        # tmp solution for new project's default database is DEFAULT
        update_db = requests.request("PUT", self.base_url + "/projects/{}/default_database".format(project_name),
                                     json={'default_database': 'SSB'}, headers=self.headers)

        assert 200 == update_db.status_code
        return response

    def get_sampling_rows(self, project_name, table, database='SSB'):
        url = self.base_url + \
              '/tables?project={}&database={}&table={}&is_fuzzy=false&ext=true'.format(project_name, database, table)
        response = requests.request('GET', url, headers=self.headers)
        return response
