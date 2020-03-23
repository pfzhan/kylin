# coding:utf-8

import time
import json

import pytest
from utils import ProjectConfig, id_for_test
from api.project import Project
from api.query import Query
from api.job import Job
from api.model import Model
from api.table import Table

# the wait interval is a subtle variable, be careful
_WAIT_INTERVAL = 65

config_dir = ['data/sample_data/expert_view_sampling_conf.json']


@pytest.fixture(params=config_dir, ids=id_for_test, name='config')
def load_config(request):
    config = ProjectConfig(request.param)
    config.retrieve_view_config()
    return config


class TestView:

    @staticmethod
    def stream_line(config, project_desc, project_name, need_sampling=False):
        project = Project()
        model = Model()
        # create project
        resp = project.create_project(project_desc)
        assert resp.status_code == 200

        # set source type
        resp = project.set_source_type(project_name)
        assert resp.status_code == 200

        # load table
        table = Table()
        response = table.load_table(project_name, config.sample_table, need_sampling)
        assert response.status_code == 200

        # create model
        model_desc = config.sample_model_desc
        model_desc['project'] = project_name
        response = model.create_model(config.sample_model_desc)
        assert response.status_code == 200

    @staticmethod
    def wait_job_done(project_name):
        # sleep 65 seconds to ensure the job has been submitted
        time.sleep(_WAIT_INTERVAL)
        job = Job()
        job_status = job.await_all_jobs(project_name)
        assert job_status is True
        # kylin.job.event.poll-interval-second=60, to ensure the postAddCuboidEvent has been successfully finished
        time.sleep(_WAIT_INTERVAL)

    @pytest.mark.view_sampling
    def test_sampling(self, config):
        project = Project()
        table = Table()

        project_name = config.project_name_one
        project_desc = config.project_desc_one

        # stream line
        self.stream_line(config, project_desc, project_name, True)

        # wait jobs done
        self.wait_job_done(project_name)

        sample_list = config.sample_list
        for tl in sample_list:
            response2 = table.get_sampling_rows(project_name, tl)
            if response2.status_code == 200:
                res_json = json.loads(response2.text)['data']['tables'][0]['sampling_rows']
                result_samplings_row = len(res_json)
                assert result_samplings_row == 10
        # task is successful, delete it
        resp = project.delete_project(project_name)
        assert resp.status_code == 200

    @pytest.mark.view_sampling
    def test_add_index_and_query(self, config):
        project = Project()
        model = Model()
        query = Query()
        project_name = config.project_name_two
        project_desc = config.project_desc_two

        # stream line
        self.stream_line(config, project_desc, project_name)

        # add table index
        sample_table_index_desc = config.sample_table_index_desc
        sample_table_index_desc['project'] = project_name
        sample_table_index_desc['model_id'] = config.sample_model_desc.get('uuid')
        response = model.add_table_index(sample_table_index_desc)
        assert response.status_code == 200

        # add agg group
        sample_agg_index_desc = config.sample_aggregate_index_desc
        sample_agg_index_desc['project'] = project_name
        response = model.add_aggregate_indices(sample_agg_index_desc)
        assert response.status_code == 200

        # wait jobs done
        self.wait_job_done(project_name)

        model_name = config.sample_model_name
        # hit agg_index
        sql1 = config.get_sql().get("sql_agg")
        for s in sql1:
            response1 = query.execute_query(project_name, s)
            assert response1.status_code == 200
            assert query.get_engine_type(response1) == 'NATIVE' and query.get_model_alias(response1) == model_name

        # hit table_index
        sql2 = config.get_sql().get("sql_table_index")
        response2 = query.execute_query(project_name, sql2)

        assert response2.status_code == 200
        assert query.get_engine_type(response2) == 'NATIVE' and query.get_model_alias(response2) == model_name

        sql3 = config.get_sql().get("sql_snapshot")
        response3 = query.execute_query(project_name, sql3)
        assert response3.status_code == 200
        assert query.get_engine_type(response3) == 'NATIVE' and query.get_model_alias(response3) == model_name

        # task is successful, delete it
        resp = project.delete_project(project_name)
        assert resp.status_code == 200
