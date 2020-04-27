import requests
import time
import uuid
import base64

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


def get_password_by_role(role):
    return str(base64.b64decode(roles[role]), 'UTF-8').split(":")[1]


def get_random_uuid():
    return str(uuid.uuid4())


def get_save_query_name():
    tmp_str = int(round(time.time() * 1000))
    return str(tmp_str)


def get_start_time(table_name, flag_time):
    url = base_url + "/tables?project=%s&database=SSB&table=%s&is_fuzzy=false&ext=true" % (project, table_name)

    response = requests.request("GET", url, headers=headers).json()['data']['tables'][0]
    if response['segment_range'] is not None and response['segment_range']['date_range_end'] is not None:
        start_time = response['segment_range']['date_range_end']
        int(round((time.time() + 1 * 1 * 60) * 1000))
        end_time = start_time + 60 * 1000
        # return start_time, end_time
    else:
        # return int(round(time.time() * 1000)), int(round(time.time() * 1000)) + 60 * 1000
        start_time = int(round(time.time() * 1000))
        end_time = int(round(time.time() * 1000)) + + 60 * 1000
    if flag_time == 'start':
        return start_time
    else:
        return end_time


def get_project_uuid(project_name):
    url = base_url + "/projects" + "?page_size=1000"

    projects = requests.request("GET", url, headers=headers).json()['data']['value']

    for project in projects:
        if project['name'] == project_name:
            return project['uuid']


def get_data_range(tmp_str):
    if tmp_str == 'start':
        ts = int(round(time.time() * 1000))
    else:
        ts = int(round((time.time() + 1 * 1 * 60) * 1000))

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
            'partition_date_format': 'yyyy-MM-dd',
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
    print(response)


if __name__ == '__main__':
    create_user("a_user", "Kylin@123")

