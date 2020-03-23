#!/usr/bin/env python
# encoding: utf-8
host = 'localhost'
port = '17071'
base_url = 'http://{host}:{port}/kylin/api'.format(host=host, port=port)

base_headers = {
    'Accept': 'application/vnd.apache.kylin-v4+json',
    'Content-Type': 'application/json;charset=utf-8',
    'Accept-Language': 'en',
}

file_headers = {
    'accept': 'application/vnd.apache.kylin-v4+json',
    'accept-encoding': 'gzip, deflate',
    'authorization': 'Basic QURNSU46S1lMSU4=',
}

json_headers = {
    'accept': 'application/vnd.apache.kylin-v4+json',
    'Content-Type': 'application/json;charset=utf-8',
    'authorization': 'Basic QURNSU46S1lMSU4=',
}

import_headers = {
    'accept': 'application/vnd.apache.kylin-v4+json',
    'enctype': 'application/x-www-form-urlencoded',
    'Authorization': 'Basic QURNSU46S1lMSU4=',
}
