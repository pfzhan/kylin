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
from random import randint

import requests
import pytest
from utils import ProjectConfig, id_for_test
from utils.diagnosis_utils import verify_diagnosis_content
from api.project import Project
from api.query import Query
from api.job import Job
from api.model import Model
from api.table import Table
from api.favorite_query import FavoriteQuery
from api.user import User
from config import common_config

TEST_JOB_DIAGNOSIS = 'test_job_diagnosis'
TEST_METADATA_BACKUP = 'test_metadata_backup'

# the wait interval is a subtle variable, be careful
_WAIT_INTERVAL = 65

config_dir = ['data/sample_data/expert_sample_conf.json']


@pytest.fixture(params=config_dir, ids=id_for_test, name='config')
def load_config(request):
    config = ProjectConfig(request.param)
    config.retrieve_expert_config()
    return config


class TestExpertMode:

    @staticmethod
    def start_instance(bin_path, port):
        if os.popen('lsof -t -i:' + str(port)).read():
            return
        # exit venv for starting newten in python2.6 instead of python3.6
        os.system('deactivate')
        try_time = 60
        while try_time:
            pid_line = os.popen('lsof -t -i:' + str(port)).read()
            if pid_line:
                break
            # kylin start
            os.system('bash -vx ' + bin_path + ' start')
            time.sleep(10)
            try_time -= 1
        time.sleep(120)
        activate_cmd = 'source ' + os.path.join(os.environ.get('PYTHON_VENV_HOME'), 'bin/activate')
        os.system(activate_cmd)

    @staticmethod
    def _headers_authorized(headers, auth_role):
        headers_n = headers.copy()
        headers_n.pop('Authorization', None)
        headers_n['Authorization'] = auth_role
        return headers_n

    @pytest.mark.p1
    def test_instance_started(self):
        kylin_home = os.environ.get('KYLIN_HOME')
        assert kylin_home
        # kylin start
        self.start_instance(os.path.join(kylin_home, 'bin/kylin.sh'), common_config.port)

    @pytest.mark.p1
    def test_user_signup(self, config):
        headers_admin = self._headers_authorized(common_config.base_headers, config.auth_of_admin)
        user_admin = User(headers_admin)
        resp = user_admin.add_user(config.user_test_attributes)
        assert resp.status_code == 200

    @pytest.mark.p1
    def test_table_index_build(self, config):
        headers_admin = self._headers_authorized(common_config.base_headers, config.auth_of_admin)
        project = Project(headers_admin)
        # create project
        resp = project.create_project(config.project_desc)
        assert resp.status_code == 200

        # enable semi automatic mode
        # io.kyligence.kap.rest.controller.NProjectController#updateProjectGeneralInfo
        time.sleep(5)
        resp = project.enable_semi_automatic_mode(config.project_name)
        assert resp.status_code == 200

        # set source type
        resp = project.set_source_type(config.project_name)
        assert resp.status_code == 200

        # sleep 10 seconds for starting auto favorite scheduler
        time.sleep(10)
        # set acceleration rule
        acceleration_rule_desc = config.acceleration_rule_desc
        acceleration_rule_desc['project'] = config.project_name
        resp = project.set_acceleration_rule(acceleration_rule_desc)
        assert resp.status_code == 200

        # load table source
        table = Table(headers_admin)
        resp = table.load_table(config.project_name, config.tables_should_load)
        assert resp.status_code == 200

        model = Model(headers_admin)
        # create model
        resp = model.create_model(config.model_desc)
        assert resp.status_code == 200

        # execute some queries before index built
        # simulate user actions
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        query = Query(headers_test)

        for sql in config.specified_queries:
            resp = query.execute_query(config.project_name, sql)
            assert resp.status_code == 200
            assert query.is_pushdown_query(resp) is True

        # add aggregate index
        aggregate_index_desc = config.aggregate_index_desc
        aggregate_index_desc['project'] = config.project_name
        aggregate_index_desc['model_id'] = config.model_desc.get('uuid')
        resp = model.add_aggregate_indices(aggregate_index_desc)
        assert resp.status_code == 200

        # wait jobs to be scheduled
        time.sleep(_WAIT_INTERVAL)
        # monitor job status
        job = Job(headers_test)
        assert job.await_all_jobs(config.project_name), 'all jobs should have finished.'

        # add table index
        table_index_desc = config.table_index_desc
        table_index_desc['project'] = config.project_name
        table_index_desc['model_id'] = config.model_desc.get('uuid')
        resp = model.add_table_index(table_index_desc)
        assert resp.status_code == 200

        # wait jobs to be scheduled
        time.sleep(_WAIT_INTERVAL)
        # monitor job status
        job = Job(headers_test)
        assert job.await_all_jobs(config.project_name), 'all jobs should have finished.'


    @pytest.mark.p1
    def test_user_sign_inout(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        user_test = User(headers_test)
        project = Project(headers_test)

        session = requests.Session()
        resp = user_test.session_sign_in(session)
        assert resp.status_code == 200

        resp = project.session_get_projects(session)
        assert resp.status_code == 200
        assert project.projects_size(resp)

        resp = user_test.session_sign_out(session)
        assert resp.status_code == 200

        resp = project.session_get_projects(session)
        assert resp.status_code == 200
        assert project.projects_size(resp) == 0

    def fq_self_detection(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        # trigger fq self-detection
        time.sleep(randint(5, 8))
        favorite_query = FavoriteQuery(headers_test)
        resp = favorite_query.list_favorite_queries(config.project_name)
        return resp.status_code == 200

    @pytest.mark.p1
    def test_table_index_query(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        query = Query(headers_test)
        retry_times = 3
        while retry_times > 0:
            for sql in config.specified_queries:
                resp = query.execute_query(config.project_name, sql)
                assert resp.status_code == 200
                assert query.get_engine_type(resp) == 'NATIVE'
                assert query.hit_table_index(resp), 'should hit table index.'
                assert query.result_row_count(resp), 'result should not be empty.'
            time.sleep(randint(3, 6))
            retry_times -= 1

    @pytest.mark.p1
    def test_user_permission(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        user_test = User(headers_test)
        project = Project(headers_test)

        resp = user_test.sign_in()
        assert resp.status_code == 200
        user_test_desc = user_test.get_user_desc(resp)

        resp = project.get_projects()
        assert resp.status_code == 200
        assert project.projects_size(resp) > 0

        resp = user_test.set_roles(uuid=user_test_desc['uuid'], username=user_test_desc['username'],
                                   roles=['ALL_USERS'])
        assert resp.status_code == 200
        resp = project.get_projects()
        assert resp.status_code == 200
        assert project.projects_size(resp) == 0

    @pytest.mark.p1
    def test_project_permission(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        user_test = User(headers_test)
        resp = user_test.sign_in()
        assert resp.status_code == 200
        user_test_desc = user_test.get_user_desc(resp)

        headers_admin = self._headers_authorized(common_config.base_headers, config.auth_of_admin)
        project_admin = Project(headers_admin)
        resp = project_admin.get_projects()
        assert resp.status_code == 200
        project_desc = project_admin.get_project_desc(config.project_name, resp)
        resp = project_admin.grant_project_access(project_desc.get('uuid'), user_test_desc.get('username'))
        assert resp.status_code == 200

        resp = project_admin.get_projects()
        assert resp.status_code == 200
        assert project_admin.projects_size(resp) > 0

    @pytest.mark.p1
    def test_model_revision(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        query = Query(headers_test)
        for sql in config.revision_specified_queries:
            resp = query.execute_query(config.project_name, sql)
            assert resp.status_code == 200
            assert query.get_engine_type(resp) != 'NATIVE'
            assert query.result_row_count(resp), 'result should not be empty.'
        model = Model(headers_test)
        resp = model.edit_model(config.edited_model_desc)
        assert resp.status_code == 200
        time.sleep(5)

        # change caused by https://github.com/Kyligence/KAP/issues/3615
        # measure list
        measures = []

        for model_object in model.list_models(config.project_name)['data']['value']:
            if model_object['uuid'] == config.model_desc.get('uuid'):
                for measure in model_object['all_measures']:
                    measures.append(measure['id'])

        # add aggregate index
        aggregate_index_desc = config.aggregate_index_desc
        for agg_group in aggregate_index_desc['aggregation_groups']:
            agg_group['measures'] = measures
        model.add_aggregate_indices(aggregate_index_desc)

        time.sleep(5)

        # add some validations after revision
        # load data, known bug: https://github.com/Kyligence/KAP/issues/11773
        resp = model.load_data(config.project_name, config.model_desc.get('uuid'))
        assert resp.status_code == 200
        # wait job to be scheduled
        time.sleep(_WAIT_INTERVAL)
        # monitor job status
        job = Job(headers_test)
        assert job.await_all_jobs(config.project_name), 'all jobs should have finished.'
        # wait index to be ready
        time.sleep(_WAIT_INTERVAL)
        # validate queries
        retry_times = 3
        while retry_times > 0:
            for sql in config.revision_specified_queries:
                resp = query.execute_query(config.project_name, sql)
                assert resp.status_code == 200
                assert query.get_engine_type(resp) == 'NATIVE'
                assert query.result_row_count(resp), 'result should not be empty.'
            time.sleep(randint(3, 6))
            retry_times -= 1

    @pytest.mark.p1
    def test_metadata_cleanup(self, config):
        # TODO
        pass

    @pytest.mark.p1
    def test_favorites_cleanup(self, config):
        # TODO
        pass

    def get_log_files(self, current):
        result = []
        if os.path.isdir(current):
            for p in os.listdir(current):
                result.extend(self.get_log_files(os.path.join(current, p)))
        else:
            result.append(current)
        return result

    def get_log_lines(self, log_dir):
        if not os.path.isdir(log_dir):
            return 0
        if not os.path.exists(log_dir):
            return 0
        log_lines = 0
        for p in self.get_log_files(log_dir):
            with open(p) as f:
                log_lines += sum(1 for _ in f)
        return log_lines

    @pytest.mark.p1
    def test_job_diagnosis(self, config):
        headers_test = self._headers_authorized(common_config.base_headers, config.auth_of_test)
        project = Project()
        project.garbage_clean(config.project_name)
        time.sleep(10)

        job = Job(headers_test)

        resp = job.get_jobs(config.project_name)
        assert resp.status_code == 200
        job_desc = job.extract_1st_job_desc(resp)
        assert job_desc is not None
        job_diagnosis_dir = os.path.join(TEST_JOB_DIAGNOSIS, '')
        if os.path.exists(job_diagnosis_dir):
            shutil.rmtree(job_diagnosis_dir)
        os.mkdir(job_diagnosis_dir)
        kylin_home = os.environ.get('KYLIN_HOME')
        assert kylin_home

        # trigger fq self-detection
        assert self.fq_self_detection(config), 'fq_self_detection should be successful'

        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/diag.sh') + ' -job ' + job_desc.get(
            'id') + ' -destDir ' + job_diagnosis_dir)
        diagnostic_pkg_dir = os.path.join(job_diagnosis_dir, os.listdir(job_diagnosis_dir)[0])
        diagnostic_pkg = os.path.join(diagnostic_pkg_dir, os.listdir(diagnostic_pkg_dir)[0])
        os.system('unzip ' + diagnostic_pkg + ' -d ' + job_diagnosis_dir)
        diagnosis_path = diagnostic_pkg_dir

        verify_diagnosis_content(diagnosis_path, config.project_name)

    @pytest.mark.p1
    def test_metadata_backup_restore(self, config):
        metadata_backup_dir = os.path.join(TEST_METADATA_BACKUP, '')
        if os.path.exists(metadata_backup_dir):
            shutil.rmtree(metadata_backup_dir)
        os.mkdir(metadata_backup_dir)
        kylin_home = os.environ.get('KYLIN_HOME')
        assert kylin_home
        # kylin stop
        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/kylin.sh') + ' stop')
        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/metastore.sh') + ' backup ' + metadata_backup_dir)
        metadata_backup_path = os.path.join(metadata_backup_dir, os.listdir(metadata_backup_dir)[0])
        # check backup
        assert os.path.exists(os.path.join(metadata_backup_path, config.project_name))
        assert os.path.exists(os.path.join(metadata_backup_path, '_global'))
        assert os.path.exists(os.path.join(metadata_backup_path, 'UUID'))

        # destroy metadata
        metadata_erase_cmd = os.environ.get('METADATA_ERASE_CMD')
        assert metadata_erase_cmd
        print('Metadata erase command is ' + metadata_erase_cmd)
        os.system(metadata_erase_cmd)
        # kylin start
        self.start_instance(os.path.join(kylin_home, 'bin/kylin.sh'), common_config.port)
        # check after metadata were destroyed
        headers_n = self._headers_authorized(common_config.base_headers, config.auth_of_admin)
        project_admin = Project(headers_n)
        resp = project_admin.get_projects()
        assert resp.status_code == 200
        assert project_admin.projects_size(resp) == 0

        # kylin stop
        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/kylin.sh') + ' stop')
        # metadata restore
        os.system('bash -vx ' + os.path.join(kylin_home, 'bin/metastore.sh') + ' overwrite ' + metadata_backup_path)
        # kylin start
        self.start_instance(os.path.join(kylin_home, 'bin/kylin.sh'), common_config.port)
        # execute some queries after metadata were restored
        self.test_table_index_query(config)
