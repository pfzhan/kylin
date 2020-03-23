import time
import json

import requests
from config import common_config


class Job:
    """All Job APIs"""

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def get_first_job_status(self, project_name):

        url = self.base_url + '/jobs?page_size=10&time_filter=4&project=' + project_name
        payload = ''
        try_time = 0
        finish_status_list = ['ERROR', 'FINISHED', 'DISCARDED']
        job_status = 'Failed'
        while try_time <= 50:
            response = requests.request('GET', url, json=payload, headers=self.headers)
            if response.status_code != 200:
                print('Failed to submit this job.')
                return job_status
            try:
                job_status = self.extract_1st_job_desc(response)['job_status']
            except Exception:
                print('No job found.')
                return job_status
            if job_status in finish_status_list:
                return job_status
            time.sleep(30)
            try_time += 1
        return job_status

    def get_jobs(self, project_name):
        req_url = self.base_url + '/jobs'
        parameters = {'job_names': None,
                      'page_offset': 0,
                      'page_size': 10,
                      'time_filter': 4,
                      'sort_by': 'create_time',
                      'reverse': True, 'status': None,
                      'subject_alias': None,
                      'project': project_name}
        return requests.get(url=req_url, params=parameters, headers=self.headers)

    @staticmethod
    def extract_1st_job_desc(resp):
        all_jobs = json.loads(resp.text)['data']['value']
        if len(all_jobs) > 0:
            print("first job>>>>>>>>>>>>>>>")
            print(all_jobs[0])
            print(">>>>>>>>>>>>>>>>>>>>>>>>")
        for j in all_jobs:
            if j.get('job_status') == 'FINISHED':
                return j
        return all_jobs[0]

    def waiting_job_size(self, project_name):
        req_url = self.base_url + '/jobs/waiting_jobs/models?project=' + project_name
        resp = requests.get(url=req_url, headers=self.headers)
        waiting_job_size = -1
        if resp.status_code == 200:
            waiting_job_size = json.loads(resp.text)['data']['size']
        return waiting_job_size

    def await_all_jobs(self, project_name):
        run_flag = ['READY', 'RUNNING']
        # done_flag = ['ERROR', 'PAUSED', 'DISCARDED', 'SUCCEED', 'SUICIDAL']
        req_url = self.base_url + '/jobs?page_size=100&time_filter=4&project=' + project_name
        try_times = 120
        wait_interval = 10
        while try_times > 0:
            if self.waiting_job_size(project_name) > 0:
                try_times -= 1
                time.sleep(wait_interval)
                continue
            resp = requests.get(url=req_url, headers=self.headers)
            if resp.status_code != 200:
                try_times -= 6
                time.sleep(wait_interval)
                continue

            all_finished = True
            for j in json.loads(resp.text)['data']['value']:
                if j.get('job_status') in run_flag:
                    all_finished = False
                    break
            if all_finished:
                break
            try_times -= 1
            time.sleep(wait_interval)
        return try_times > 0
