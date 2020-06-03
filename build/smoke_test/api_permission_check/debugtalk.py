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


def delete_user(user_name):
    url = "{base_url}/user/{user_name}".format(base_url=base_url, user_name=user_name)

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