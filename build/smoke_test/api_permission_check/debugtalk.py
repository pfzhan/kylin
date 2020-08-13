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

import requests
import time
import uuid
import base64

from requests_toolbelt import MultipartEncoder

base_url = 'http://localhost:17071/kylin/api'
project = "project_api_check"

roles = {
    'ADMIN': 'QURNSU46S1lMSU4=',
    'PROJECT_ADMIN': 'cHJvamVjdF9hZG1pbjpLeWxpbkAxMjM=',
    'MANAGEMENT': 'bWFuYWdlbWVudDpLeWxpbkAxMjM=',
    'OPERATION': 'b3BlcmF0aW9uOkt5bGluQDEyMw==',
    'QUERY': 'cXVlcnk6S3lsaW5AMTIz',
    'ALL': 'YWxsOkt5bGluQDEyMw=='
}


headers = {
    'accept': "application/vnd.apache.kylin-v4+json",
    'accept-language': "cn",
    'Content-Type': 'application/json;charset=utf-8',
    'authorization': 'Basic QURNSU46S1lMSU4='
}


def get_roles():
    return roles


def get_role(role):
    return [roles[role]]


def get_base_url():
    return base_url


def get_project_name():
    return project


def get_user_name_by_role(role):
    return str(base64.b64decode(roles[role]), 'UTF-8').split(":")[0]

def get_user_id_by_role(role):
    return get_user_id(get_user_name_by_role(role))


def get_password_by_role(role):
    return str(base64.b64decode(roles[role]), 'UTF-8').split(":")[1]


def get_random_uuid():
    return str(uuid.uuid4())


def get_save_query_name():
    tmp_str = int(round(time.time() * 1000))
    return str(tmp_str)


def get_project_uuid(project_name):
    url = base_url + "/projects" + "?page_size=1000"

    projects = requests.request("GET", url, headers=headers).json()['data']['value']

    for project in projects:
        if project['name'] == project_name:
            return project['uuid']


def get_data_range(tmp_str, model_name, project_name):
    date_range = get_model_latest_date(model_name, project_name)
    if tmp_str == 'start':
        ts = int(date_range['start_time'])
    else:
        ts = int(date_range['start_time']) + 86400000

    return ts


def get_model_uuid(model_name, project_name):
    headers = {
        'Accept': 'application/vnd.apache.kylin-v4-public+json',
        'Accept-Language': 'en',
        'Authorization': 'Basic QURNSU46S1lMSU4=',
        'Content-Type': 'application/json;charset=utf-8',
    }
    url = base_url + "/models/%s/%s/model_desc" % (project_name, model_name)
    res = requests.request("GET", url, headers=headers).json()
    if res['data'] is not None:
        return res['data']['uuid']

    return get_random_uuid()


def delete_model_by_model_name(model_name, project_name):
    url = base_url + "/models/" + get_model_uuid(model_name, project_name)

    querystring = {"project": project_name}

    payload = ""

    requests.request("DELETE", url, data=payload, headers=headers, params=querystring).json()


def build_indices_manually(model_name, project_name):
    url = "{base_url}/index_plans/table_index".format(base_url=base_url)

    col_order = ['P_LINEORDER.LO_LINENUMBER', 'P_LINEORDER.LO_PARTKEY']
    payload = {
        'project': project_name,
        'model_id': get_model_uuid(model_name, project_name),
        'col_order': col_order,
        'id': '',
        'load_data': False,
        'shard_by_columns': [],
        'sort_by_columns': []
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()
    print(response)


def load_data(model_name, project_name):
    url = base_url + "/models/" + get_model_uuid(model_name, project_name) + "/model_segments"

    payload = {
        'start': 312825600000,
        'end': 315417600000,
        'partition_desc': {
            'partition_date_column': 'SSB.P_LINEORDER.LO_ORDERDATE',
            'partition_date_format': get_partition_column_format(project_name),
        },
        'segment_holes': [],
        'project': project_name
    }

    response = requests.request("PUT", url, json=payload, headers=headers).json()


def get_segment_id(model_name, project_name):
    url = "{base_url}/models/{model_uuid}/segments".format(base_url=base_url, model_uuid=get_model_uuid(model_name, project_name))
    params = {"project": project_name}

    response = requests.request("GET", url, headers=headers, params=params).json()
    return response['data']['value'][0]['id']


def prepare_segment(model_name, project_name):
    build_indices_manually(model_name, project_name)
    load_data(model_name, project_name)


def create_user(user_name, password):
    url = "{base_url}/user".format(base_url=base_url)

    payload = {
        'authorities': ['ALL_USERS'],
        'disabled': False,
        'username': user_name,
        'password': password
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()

def get_user_id(user_name):
    url = "{base_url}/user?name={user_name}".format(base_url=base_url, user_name=user_name)
    response = requests.request("GET", url, headers=headers).json()

    user = next(filter(lambda user: user['username'] == user_name, response['data']['value']), None)
    if user:
        return user['uuid']


def delete_user(user_name):
    user_id = get_user_id(user_name)
    if user_id:
        url = "{base_url}/user/{uuid}".format(base_url=base_url, uuid=user_id)
        response = requests.request("DELETE", url, headers=headers).json()


def prepare_table_index(model_name, project_name):
    url = "{base_url}/index_plans/table_index".format(base_url=base_url, model_uuid=get_model_uuid(model_name, project_name))

    payload = {
        'col_order': [
            "P_LINEORDER.LO_LINENUMBER",
            "P_LINEORDER.LO_PARTKEY"
        ],
        'id': '',
        'load_data': 'false',
        'model_id': get_model_uuid(model_name, project_name),
        'project': project_name,
        'shard_by_columns': [],
        'sort_by_columns': []
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()


def get_table_index_id(model_name, project_name):
    url = "{base_url}/index_plans/table_index".format(base_url=base_url)

    params = {
                'project': project_name,
                'model': get_model_uuid(model_name, project_name)
            }

    response = requests.request("GET", url, headers=headers, params=params).json()
    return response['data']['value'][0]['id']


def get_partition_column_format(project_name):
    url = "{base_url}/tables/partition_column_format".format(base_url=base_url)

    params = {
                'project': project_name,
                'table': 'SSB.P_LINEORDER',
                'partition_column': 'LO_ORDERDATE'
            }

    response = requests.request("GET", url, headers=headers, params=params).json()
    return response['data']


def prepare_authorized_user(user_name, password):
    create_user(user_name, password)
    grant_project_permission_to_single_user(user_name)


def grant_project_permission_to_single_user(user_name):
    url = "{base_url}/access/ProjectInstance/{project_uuid}".format(base_url=base_url, project_uuid=get_project_uuid(project))

    payload = {
        'permission': 'READ',
        'principal': 'true',
        'sid': user_name,
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()


def create_agg(model_id, project_name):
    url = "{base_url}/index_plans/rule".format(base_url=base_url)

    payload = {
        "project": project_name,
        "model_id": model_id,
        "aggregation_groups": [
            {
                "includes": [
                    7
                ],
                "measures": [
                    100000
                ],
                "select_rule": {
                    "mandatory_dims": [
                        7
                    ],
                    "hierarchy_dims": [],
                    "joint_dims": [],
                    "dim_cap": None
                }
            }
        ],
        "load_data": False,
        "global_dim_cap": None
    }

    response = requests.request("PUT", url, json=payload, headers=headers).json()


def get_model_latest_date(model_name, project_name):
    url = "{base_url}/models/{model_id}/data_range/latest_data".format(base_url=base_url, model_id=get_model_uuid(model_name, project_name))

    params = {
                'project': project_name,
            }

    response = requests.request("GET", url, headers=headers, params=params).json()
    return response['data']


def export_models_metadata(project_name, model_name):
    url = "{base_url}/metastore/backup/models?project={project_name}".format(base_url=base_url, project_name=project_name)

    headers = {
        'accept': "application/vnd.apache.kylin-v4-public+json",
        'accept-language': "en",
        'Content-Type': 'application/json;charset=utf-8',
        'authorization': 'Basic QURNSU46S1lMSU4='
    }

    payload = {
        'names': [model_name]
    }

    export_model_content = requests.request("POST", url, json=payload, headers=headers).content
    model_zip = os.path.join("/tmp", "temp_model.zip")
    with open(model_zip, 'wb') as f:
        f.write(export_model_content)


def multipart_encoder(filename, model_name):
    fields = {
        'file': (filename, open(filename, mode='rb+'), 'text/plain'),
        'names': model_name
    }
    return MultipartEncoder(fields)


def approve_recommendation(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name)

    payload = {
        "project": project_name,
        "ids": [1]
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()


def prepare_approve_recommendation(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name) + "/validation"

    payload = {
        "project": project_name,
        "ids": [1]
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()


def delete_recommendation(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name)

    querystring = {"project": project_name, "ids": [1]}

    payload = ""

    requests.request("DELETE", url, data=payload, headers=headers, params=querystring).json()


def delete_all_recommendation(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name) + "/all"

    querystring = {"project": project_name}

    payload = ""

    requests.request("DELETE", url, data=payload, headers=headers, params=querystring).json()


def get_recommendation(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name)

    querystring = {"project": project_name}

    payload = ""

    requests.request("GET", url, data=payload, headers=headers, params=querystring).json()


def get_recommendation_detail(model_name, project_name):
    url = base_url + "/recommendations/" + get_model_uuid(model_name, project_name) + "/1"

    querystring = {"project": project_name}

    payload = ""

    requests.request("GET", url, data=payload, headers=headers, params=querystring).json()


def await_all_table_job_finished(project_name, table_name):
    url = base_url + "/jobs?project={project}&time_filter=4".format(project=project_name)

    finished_status = ['SUCCEED', 'DISCARDED', 'SUICIDAL', 'FINISHED']
    from datetime import datetime
    start = datetime.now()
    # 1 minutes
    while (datetime.now() - start).total_seconds() < 60 * 1:
        jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')

        if not jobs or not any(filter(lambda job: job.get('target_model') == table_name
                                      and job.get('job_status') not in finished_status, jobs)):
            return

        time.sleep(10)

    # discard all jobs
    jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')

    unfinished_jobs = list(filter(lambda job: job.get('job_status') not in finished_status, jobs))
    discard_job(project_name, list(map(lambda job: job.get('id'), unfinished_jobs)))

    time.sleep(10)


def get_running_job_id_by_status(project_name):
    statuses = ['PENDING', 'RUNNING', 'ERROR', 'STOPPED']
    url = base_url + "/jobs?project={project}&time_filter=4".format(project=project_name)
    jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')
    ret = next(filter(lambda job: job.get('job_status') in statuses, jobs), None)

    if ret:
        return ret.get('id')

    load_table(project_name, ['SSB.P_LINEORDER'])

    time.sleep(10)
    jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')
    ret = next(filter(lambda job: job.get('job_status') in statuses, jobs))

    return ret.get('id')


def get_finished_job_id_by_status(project_name, statuses=None):
    statuses = ['SUCCEED', 'DISCARDED', 'SUICIDAL', 'FINISHED']
    url = base_url + "/jobs?project={project}&time_filter=4".format(project=project_name)
    jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')
    ret = next(filter(lambda job: job.get('job_status') in statuses, jobs), None)

    if ret:
        return ret.get('id')

    load_table(project_name, ['SSB.P_LINEORDER'])

    await_all_table_job_finished(project_name, table_name='SSB.P_LINEORDER')

    time.sleep(10)
    jobs = requests.request("GET", url, headers=headers).json().get('data').get('value')
    ret = next(filter(lambda job: job.get('job_status') in statuses, jobs))

    return ret.get('id')


def load_table(project_name, tables=None):
    if not tables:
        tables = ['SSB.DATES']
    url = base_url + "/tables"
    payload = {
        "data_source_type": 9,
        "databases": [],
        "need_sampling": True,
        "project": project_name,
        "sampling_rows": 20000000,
        "tables": tables
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()


def discard_job(project_name, job_ids):
    url = base_url + "/jobs/status"
    payload = {
        "action": 'DISCARD',
        "job_ids": job_ids,
        "project": project_name
    }

    response = requests.request("PUT", url, json=payload, headers=headers).json()
