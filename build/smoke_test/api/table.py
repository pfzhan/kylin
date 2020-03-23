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
