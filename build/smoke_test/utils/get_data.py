# coding:utf-8
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

import os
import json


class ProjectConfig():
    """load config file xxxx.json and translate to parameters"""

    def __init__(self, config_file):
        root_dir = os.environ.get('root_dir')
        self.config_file = os.path.join(root_dir, config_file)

    def retrieve_project_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        # project & model name
        self.project_name = self.config.get('project_desc_data').get('name')
        self.model_name = self.config.get('model_desc_data').get('name')

        # project_desc
        self.project_desc_data = self.config.get('project_desc_data')

        self.project_desc_data_resp = self.config.get('project_desc_data_resp')

        self.project_desc_data_resp_ignore = self.config.get('project_desc_data_resp_ignore')

        # resource_data
        self.resource_data = self.config.get('resource_data')

        # load table list
        self.load_table_list = self.config.get('load_table_list')

        # model description
        self.model_desc_data = self.config.get('model_desc_data')

        # index info
        self.aggregation_groups = self.config.get('aggregation_groups')

    def retrieve_smart_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        # project name
        self.project_name = self.config.get('project').get('project_name')

        # project_desc
        self.project_desc_data = self.config.get('project_desc_data')

        # load table list
        self.load_table_list = self.config.get('load_table_list')

        # rule-based sql
        self.sql_rule_based = self.config.get('sql_rule_based')

        # imported sql
        self.sql_imported = self.config.get('sql_imported')

    def retrieve_expert_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        # authorization of admin
        self.auth_of_admin = self.config.get('access_info').get('auth_of_admin')

        # authorization of test
        self.auth_of_test = self.config.get('access_info').get('auth_of_test')

        # user test's attributes
        self.user_test_attributes = self.config.get('user_test_attributes')

        # project info
        self.project_name = self.config.get('project_info').get('project_name')
        self.project_desc = self.config.get('project_info').get('project_desc')
        self.acceleration_rule_desc = self.config.get('project_info').get('acceleration_rule_desc')

        # model info
        self.model_name = self.config.get('model_info').get('model_name')
        self.model_desc = self.config.get('model_info').get('model_desc')
        self.edited_model_desc = self.config.get('model_info').get('edited_model_desc')
        self.table_index_desc = self.config.get('model_info').get('table_index_desc')
        self.aggregate_index_desc = self.config.get('model_info').get('aggregate_index_desc')

        # load table list
        self.tables_should_load = self.config.get('tables_should_load')

        # specified queries
        self.specified_queries = self.config.get('specified_queries')

        self.revision_specified_queries = self.config.get('revision_specified_queries')

    def retrieve_view_config(self):
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        # authorization of admin
        self.auth_of_admin = self.config.get("access_info").get("auth_of_admin")

        # authorization of test
        self.auth_of_test = self.config.get("access_info").get("auth_of_test")

        # user test's attributes
        self.user_test_attributes = self.config.get("user_test_attributes")

        # project info
        self.project_name_one = self.config.get("project_infos").get("project_name_one")
        self.project_desc_one = self.config.get("project_infos").get("project_desc_one")
        self.project_name_two = self.config.get("project_infos").get("project_name_two")
        self.project_desc_two = self.config.get("project_infos").get("project_desc_two")

        # sample model info
        self.sample_model_name = self.config.get('sample_model_info').get('model_name')
        self.sample_model_desc = self.config.get("sample_model_info").get("sample_model_desc")
        self.sample_table_index_desc = self.config.get("sample_model_info").get("sample_table_index")
        self.sample_aggregate_index_desc = self.config.get("sample_model_info").get("sample_aggregate_index_desc")

        # load table list
        self.tables_should_load = self.config.get("tables_should_load")
        self.sample_table = self.config.get('sample_table')
        self.sample_list = self.config.get('sample_list')

        # agg_sql
        self.sql_agg = self.config.get('sql_agg')

        # sql_snapshot
        self.sql_snapshot = self.config.get('sql_snapshot')

        # sql_table_index
        self.sql_table_index = self.config.get('sql_table_index')

    def get_sql(self):
        sql_dict = self.config.get('sql')
        return sql_dict


def id_for_test(fixture_value):
    """Generate ids for test cases"""
    return '-'.join(fixture_value.split('.')[1].split('_')[-2:])
