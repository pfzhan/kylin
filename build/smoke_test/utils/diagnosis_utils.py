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
import os

METADATA_LIST = ['accelerate_ratio', 'dataflow', 'dataflow_details', 'execute', 'favorite', 'index_plan',
                 'job_stats', 'model_desc', 'query_history_time_offset', 'table']


def verify_metadata(diagnosis_path, project_name):
    for item in METADATA_LIST:
        assert len(os.listdir(os.path.join(diagnosis_path, '{}/{}'.format(project_name, item))))
    assert os.path.exists(
        os.path.join(diagnosis_path, '_global/project/{}.json'.format(project_name)))

def verify_diagnosis_content(diagnosis_path, project_name):
    assert os.path.isdir(os.path.join(diagnosis_path, 'hadoop_conf'))
    assert os.path.isdir(os.path.join(diagnosis_path, 'conf'))
    assert os.path.isdir(os.path.join(diagnosis_path, 'system_metrics'))
    assert os.path.isdir(os.path.join(diagnosis_path, 'audit_log'))
    assert os.path.exists(os.path.join(diagnosis_path, 'logs/diag.log'))
    assert os.path.exists(os.path.join(diagnosis_path, 'conf/kylin.properties'))

    assert get_log_lines(os.path.join(diagnosis_path, 'logs')), 'kylin log lines illegal.'
    assert get_log_lines(os.path.join(diagnosis_path, 'spark_logs')), 'spark log lines illegal.'
 
    diagnosis_meta_dir = os.path.join(diagnosis_path, 'metadata')
    diagnosis_meta_path = os.path.join(diagnosis_meta_dir, os.listdir(diagnosis_meta_dir)[0])
    verify_metadata(diagnosis_meta_path, project_name)


def get_log_files(current):
    result = []
    if os.path.isdir(current):
        for p in os.listdir(current):
            result.extend(get_log_files(os.path.join(current, p)))
    else:
        result.append(current)
    return result


def get_log_lines(log_dir):
    if not os.path.isdir(log_dir):
        return 0
    if not os.path.exists(log_dir):
        return 0
    log_lines = 0
    for p in get_log_files(log_dir):
        with open(p) as f:
            log_lines += sum(1 for _ in f)
    return log_lines
