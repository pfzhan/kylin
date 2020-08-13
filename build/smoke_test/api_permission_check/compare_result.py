#!/usr/bin/env python
# encoding: utf-8
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

import csv
import os

from httprunner.api import HttpRunner
from debugtalk import get_roles


def get_role_by_header(header):
    source_roles = get_roles()
    role = dict(zip(source_roles.values(), source_roles.keys()))
    return role[header]


def parse_res(res_details):
    base_record = []
    for detail in res_details:
        base_record = base_record + detail['records']
    apis_dict = {}
    for record in base_record:
        try:
            role = get_role_by_header(
                eval(record['meta_datas']['data'][0]['request']['headers'])['authorization'].split(" ")[1])
        except Exception:
            pass
        name = record['name']
        status = record['status']
        record.clear()
        record['status'] = status
        record['role'] = role
        if name not in apis_dict.keys():
            apis_dict[name] = [record]
        else:
            apis_dict[name].append(record)
    return apis_dict


# 权限包含关系如下： ADMIN > Management > Operation > Query，
# 即 ADMIN 包含了其他三种权限，
# Management 包含了 Operation 和 Query 权限，
# Operation 包含了 Query 权限。

def get_api_mini(apis_dict):
    for i in apis_dict:
        for api in apis_dict[i]:
            status = api['status']
            role = api['role']
            if status == 'success' and role == 'ALL':
                apis_dict[i] = 'All'
                break
            elif status == 'success' and role == 'QUERY':
                apis_dict[i] = 'Query'
                break
            elif status == 'success' and role == 'OPERATION':
                apis_dict[i] = 'Operation'
                break
            elif status == 'success' and role == 'MANAGEMENT':
                apis_dict[i] = 'Management'
                break
            elif status == 'success' and role == 'PROJECT_ADMIN':
                apis_dict[i] = 'Project ADMIN'
                break
            elif status == 'success' and role == 'ADMIN':
                apis_dict[i] = 'Global ADMIN'
                break
    return apis_dict


def get_expect_result(file_path):
    result = {}
    with open(file_path, 'r', encoding="UTF-8-sig") as f:
        red = csv.DictReader(f)
        for d in red:
            result.setdefault(d['功能'], d['权限'])
        return result


def compare_two_dict(expect_result, actually_result):
    tmp_keys = list(actually_result.keys())
    for i in tmp_keys:
        try:
            if expect_result[i] != actually_result[i]:
                print('%s, expect role: %s, actual role: %s' % (i, expect_result[i], actually_result[i]))
        except KeyError as e:
            pass
        except IndexError as n:
            pass


def process(root_dir):
    runner = HttpRunner()
    runner.run(os.path.join(root_dir, 'api_permission_check/testsuites/demo_testsuite.yml'))
    res_details = runner._summary['details']
    print("res_details: \n" + str(res_details) + "\n")
    apis_dict = parse_res(res_details)
    print("apis_dict: \n" + str(apis_dict) + "\n")

    actually_result = get_api_mini(apis_dict)
    print("actually_result: \n" + str(actually_result) + "\n")

    csv_expect_result = os.path.join(root_dir, 'api_permission_check/test_expect_result.csv')
    expect_result = get_expect_result(csv_expect_result)
    print("expect_result: \n" + str(expect_result) + "\n")

    print("Does not meet the expected api permission items: \n")
    compare_two_dict(expect_result, actually_result)


if __name__ == '__main__':
    root_dir = os.environ.get('root_dir')
    process(root_dir)
