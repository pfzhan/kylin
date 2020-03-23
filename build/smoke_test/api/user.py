import json

import requests
from config import common_config


class User:

    def __init__(self, headers=common_config.json_headers):
        self.base_url = common_config.base_url
        self.headers = headers

    def add_user(self, payload):
        # attributes like:
        # {'username':'xxx','password':'xxx','disabled':false,'authorities':['ALL_USERS', 'ROLE_ADMIN']}
        req_url = self.base_url + '/user'
        return requests.post(url=req_url, json=payload, headers=self.headers)

    def drop_user(self, username):
        req_url = self.base_url + '/user/' + username
        return requests.delete(url=req_url, headers=self.headers)

    def sign_in(self):
        req_url = self.base_url + '/user/authentication'
        return requests.post(url=req_url, headers=self.headers)

    def session_sign_in(self, session):
        req_url = self.base_url + '/user/authentication'
        return session.post(url=req_url, headers=self.headers)

    def session_sign_out(self, session):
        req_url = self.base_url + '/j_spring_security_logout'
        return session.get(url=req_url)

    @staticmethod
    def get_user_desc(resp):
        return json.loads(resp.text)['data']

    def set_roles(self, uuid, username, roles: []):
        req_url = self.base_url + '/user'
        payload = {'uuid': uuid, 'username': username, 'authorities': roles}
        return requests.put(url=req_url, json=payload, headers=self.headers)
