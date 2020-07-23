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
import shutil
import time

import pytest
from utils import ProjectConfig, id_for_test
from utils.diagnosis_utils import verify_diagnosis_content
from api.project import Project
from api.query import Query
from api.job import Job
from api.model import Model
from api.table import Table
from api.favorite_query import FavoriteQuery
from data.ki.model_hierarchy import model_hierarchy
from data.ki.model_list_sample_response import model_list_data

from utils import json_utils

# get jsons list
config_dir = ['data/sample_data/expert_smoke_conf.json']

TEST_FULL_DIAGNOSIS = 'test_full_diagnosis'


@pytest.fixture(params=config_dir, ids=id_for_test, name='config')
def load_config(request):
    config = ProjectConfig(request.param)
    config.retrieve_project_config()
    return config


class TestSmoke:
    """
    Metastore: PostgreSQL
    Datasource: Hive
    """

    @pytest.mark.smoketest
    def test_create_project(self, config):
        # create project
        project = Project()
        response = project.create_project(config.project_desc_data)
        assert response.status_code == 200

        response = project.get_projects()
        assert response.status_code == 200

        # enable semi automatic mode
        time.sleep(5)
        resp = project.enable_semi_automatic_mode(config.project_name)
        assert resp.status_code == 200

    @pytest.mark.smoketest
    def test_set_source_type(self, config):
        # set source type
        project = Project()
        response = project.set_source_type(config.project_name)
        assert response.status_code == 200

    @pytest.mark.smoketest
    def test_load_table(self, config):
        # load table
        table = Table()
        response = table.load_table(config.project_name, config.load_table_list)
        assert response.status_code == 200

    @pytest.mark.smoketest
    def test_create_model(self, config):
        # create model
        model = Model()
        response = model.create_model(config.model_desc_data)
        assert response.status_code == 200

    @pytest.mark.smoketest
    def test_pushdown_query(self, config):
        query = Query()

        sql1 = config.get_sql().get('sql_measure')
        response1 = query.execute_query(config.project_name, sql1)
        assert response1.status_code == 200
        assert query.is_pushdown_query(response1) is True

        sql_list = config.get_sql().get('sql_push_down_list')
        for sql in sql_list:
            response = query.execute_query(config.project_name, sql)
            assert response.status_code == 200
            assert query.is_pushdown_query(response) is True

    @pytest.mark.smoketest
    def test_add_agg_group(self, config):
        model = Model()
        # add agg group
        project_name = config.project_name
        model_id = model.get_model_id(project_name)
        agg_group_desc = {'project': project_name,
                          'model_id': model_id,
                          'dimensions': [6, 7],
                          'aggregation_groups': config.aggregation_groups,
                          'load_data': True}
        response = model.add_aggregate_indices(agg_group_desc)
        assert response.status_code == 200
        # sleep 65 seconds to ensure the job has been submitted
        time.sleep(65)
        job = Job()
        job_status = job.get_first_job_status(project_name)
        assert job_status == 'FINISHED'
        # kylin.job.event.poll-interval-second=60, to ensure the postAddCuboidEvent has been successfully finished
        time.sleep(65)

    @pytest.mark.smoketest
    def test_execute_query(self, config):
        query = Query()
        model_name = config.model_name
        sql1 = config.get_sql().get('sql_measure')
        response1 = query.execute_query(config.project_name, sql1)
        assert response1.status_code == 200
        assert query.get_engine_type(response1) == 'NATIVE' and query.get_model_alias(response1) == model_name

        sql2 = config.get_sql().get('sql_count')
        response2 = query.execute_query(config.project_name, sql2)
        assert response2.status_code == 200
        assert query.get_engine_type(response2) == 'NATIVE' and query.get_model_alias(response2) == model_name

        sql3 = config.get_sql().get('sql_dimension')
        response3 = query.execute_query(config.project_name, sql3)
        assert response3.status_code == 200
        assert query.get_engine_type(response3) == 'NATIVE' and query.get_model_alias(response3) == model_name
        time.sleep(10)

    @pytest.mark.smoketest
    def test_fq(self, config):
        favorite_query = FavoriteQuery()
        # test trigger fq detection
        response = favorite_query.list_favorite_queries(config.project_name)
        assert response.status_code == 200
        time.sleep(60)

    @pytest.mark.smoketest
    def test_diagnostic_package(self, config):
        project = Project()
        project.garbage_clean(config.project_name)
        time.sleep(10)

        kylin_home = os.environ.get('KYLIN_HOME')
        full_diagnosis_dir = os.path.join(kylin_home, TEST_FULL_DIAGNOSIS)
        if os.path.exists(full_diagnosis_dir):
            shutil.rmtree(full_diagnosis_dir)
        os.mkdir(full_diagnosis_dir)
        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/diag.sh') + 
                  ' -destDir ' + full_diagnosis_dir)
        diagnostic_pkg_dir = os.path.join(full_diagnosis_dir, os.listdir(full_diagnosis_dir)[0])
        diagnostic_pkg = os.path.join(diagnostic_pkg_dir, os.listdir(diagnostic_pkg_dir)[0])
        os.system('unzip ' + diagnostic_pkg + ' -d ' + full_diagnosis_dir)
        diagnosis_path = diagnostic_pkg_dir
        verify_diagnosis_content(diagnosis_path, config.project_name)

    @pytest.mark.kitest
    def test_model_list(self, config):
        model = Model()
        ignored_data = [
            'uuid',
            'last_modified',
            'version',
            'create_time',
            'storage',
            'usage',
            'total_size',
            'description',
            'filter_condition',
            'partition_time_column',
            'msg',
            'last_build_end'
        ]
        response = model.list_models(config.project_name)
        json_utils.compare_keys(response, model_list_data, ignored_data)

    @pytest.mark.kitest
    def test_model_hierarchy(self, config):
        model = Model()
        ignored_data = ['last_modify_time', 'hierarchy_dims', 'joint_dims', 'index_black_list']
        model_id = model.get_model_id(config.project_name)
        if len(model_id) != 0:
            response = model.get_rule(config.project_name, model_id)
            json_utils.compare_keys(response, model_hierarchy, ignored_data)
