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

import argparse
import functools
import json
import logging
import os
import re
import sys
import uuid

logging.basicConfig(level='DEBUG', format='%(asctime)-15s %(levelname)s: %(message)s', stream=sys.stdout)

INPUT_ROOT = './metadata'
OUTPUT_ROOT = './newten_meta'
TABLE_BLACK_LIST = []
DEFAULT_AUTHORIZED = 1
BYPASS_PROJECT_LIST = []
BYPASS_MODEL_LIST = []
OFFLINE_MODEL_LIST = []

table_content_dict = {}
model_content_dict = {}
cube_model_dict = {}
model_raw_table_dict = {}

release_notes = [{
    "version": "1.0",
    "release_notes": [
        "1. kafka stream project(kylin.source.default=1) will append project to BYPASS_PROJECT_LIST",
        "2. cc in lookup table will append model to BYPASS_MODEL_LIST",
        "3. cc in join condition will append model to BYPASS_MODEL_LIST",
        "4. cc in partition date column will append model to BYPASS_MODEL_LIST",
        "5. partition_date_format should not empty "
        "while partition_date_column exists will append model to BYPASS_MODEL_LIST",
        "6. table is fact table in partition model and is also dimension table in other models "
        "will append other models to OFFLINE_MODEL_LIST",
        "7. cc has same name with different expression or cc has different name with same expression "
        "in same fact table will append model to BYPASS_MODEL_LIST (actually it is also impossible in ke3)",
        "8. not supported measures corr, semi additive will remove from cube",
        "9. update lookup table's kind to FACT"
    ]
}]

PROJECT_TEMPALTE = '''
{
    "uuid" : "",
    "version" : "4.0.0.0",
    "name" : "",
    "status" : "ENABLED",
    "description" : "",
    "maintain_model_type" : "MANUAL_MAINTAIN",
    "override_kylin_properties" : {
      "kylin.source.default" : "9"
    },
    "push_down_range_limited" : true,
    "segment_config" : {
      "auto_merge_enabled" : false,
      "auto_merge_time_ranges" : [ "WEEK", "MONTH", "YEAR" ],
      "volatile_range" : {
        "volatile_range_number" : 0,
        "volatile_range_enabled" : false,
        "volatile_range_type" : "DAY"
      },
      "retention_range" : {
        "retention_range_number" : 1,
        "retention_range_enabled" : false,
        "retention_range_type" : "MONTH"
      }
    }
  }
'''

DATAFLOW_TEMPLATE = '''
{
  "uuid" : "",
  "last_modified" : 1554807337998,
  "create_time" : 1554807337664,
  "version" : "4.0.0.0",
  "description" : null,
  "owner" : null,
  "create_time_utc" : 1554807337664,
  "status" : "OFFLINE",
  "query_hit_count" : 0,
  "segments" : []
}
'''

INDEX_PLAN_TEMPLATE = '''
{
  "uuid" : "",
  "last_modified" : 1554879670503,
  "create_time" : 1554879670483,
  "version" : "4.0.0.0",
  "description" : null,
  "index_plan_override_encodings" : { },
  "rule_based_index" : {
    "dimensions" : [],
    "measures" : [],
    "aggregation_groups" : [],
    "index_black_list" : [ ],
    "layout_id_mapping" : [],
    "index_start_id" : 0
  },
  "indexes" : [ ],
  "override_properties" : { },
  "engine_type" : 80,
  "next_aggregation_index_id" : 0,
  "next_table_index_id" : 20000000000
}
'''

TABLE_COLUMN_ROW_ACL_TEMPLATE = '''
{
  "table" : null,
  "uuid" : "",
  "last_modified" : 1572605399208,
  "create_time" : 1568701283526,
  "version" : "4.0.0.0"
}
'''


def migrate_user():
    if not os.path.exists(INPUT_ROOT + '/user'):
        return
    if not os.path.exists(OUTPUT_ROOT + '/_global/user'):
        os.makedirs(OUTPUT_ROOT + '/_global/user')
    for user_name in os.listdir(INPUT_ROOT + '/user'):
        with open(INPUT_ROOT + '/user/' + user_name, 'r') as user_file:
            user_json = json.load(user_file)
            user_json['create_time'] = user_json.get('create_time', user_json['last_modified'])
            user_json['version'] = '4.0.0.0'
            with open(OUTPUT_ROOT + '/_global/user/' + user_name, 'w') as new_user_file:
                json.dump(user_json, new_user_file, indent=2)


def migrate_user_group():
    if not os.path.exists(INPUT_ROOT + '/user_group'):
        return
    if not os.path.exists(OUTPUT_ROOT + '/_global'):
        os.makedirs(OUTPUT_ROOT + '/_global')
    with open(INPUT_ROOT + '/user_group', 'r') as user_group_file:
        user_group_json = json.load(user_group_file)
        user_group_json['create_time'] = user_group_json.get('create_time', user_group_json['last_modified'])
        user_group_json['version'] = '4.0.0.0'
        if not user_group_json['uuid']:
            user_group_json['uuid'] = str(uuid.uuid4())
        with open(OUTPUT_ROOT + '/_global/user_group', 'w') as new_user_group_file:
            json.dump(user_group_json, new_user_group_file, indent=2)


def migrate_role_permission():
    if not os.path.exists(INPUT_ROOT + '/acl'):
        return
    if not os.path.exists(OUTPUT_ROOT + '/_global/acl'):
        os.makedirs(OUTPUT_ROOT + '/_global/acl')
    for project_uuid in os.listdir(INPUT_ROOT + '/acl'):
        with open(INPUT_ROOT + '/acl/' + project_uuid, 'r') as project_role_file:
            project_role_json = json.load(project_role_file)
            if not project_role_json['uuid']:
                project_role_json['uuid'] = str(uuid.uuid4())
            project_role_json['create_time'] = project_role_json.get('create_time', project_role_json['last_modified'])
            project_role_json['version'] = '4.0.0.0'
            with open(OUTPUT_ROOT + '/_global/acl/' + project_uuid, 'w') as new_project_role_file:
                json.dump(project_role_json, new_project_role_file, indent=2)


def prepare_project():
    ret = []
    if not os.path.exists(INPUT_ROOT + '/table'):
        return ret
    project_table_dict = {}
    for table_name in os.listdir(INPUT_ROOT + '/table'):
        plain_table_name, table_project = table_name.split(
            '--') if '--' in table_name else (table_name[0:-5], 'default.json')
        table_project = table_project.split('.')[0]
        if table_project not in project_table_dict:
            project_table_dict[table_project] = []
        project_table_dict[table_project].append(plain_table_name)
        if plain_table_name.split('.')[-1] in TABLE_BLACK_LIST:
            logging.info("skip table %s, blacklist", plain_table_name)
            continue
        with open(INPUT_ROOT + '/table/' + table_name) as table_file:
            table_json = json.load(table_file)
            table_json['columns'] = [col for col in table_json['columns'] if 'cc_expr' not in col]
            table_content_dict[(plain_table_name, table_project)] = table_json
    if not os.path.exists(INPUT_ROOT + '/project'):
        return ret
    for project_name in os.listdir(INPUT_ROOT + '/project'):
        if not os.path.exists(OUTPUT_ROOT + '/_global/project'):
            os.makedirs(OUTPUT_ROOT + '/_global/project')
        project_json = json.loads(PROJECT_TEMPALTE)
        plain_project_name = project_name.split('.')[0]
        if plain_project_name in BYPASS_PROJECT_LIST:
            continue
        with open(INPUT_ROOT + '/project/' + project_name) as project_file:
            project_origin_json = json.load(project_file)
            project_json['uuid'] = project_origin_json['uuid']
            project_json['name'] = project_origin_json['name']
            project_json['last_modified'] = project_origin_json.get('last_modified', 0)
            project_json['create_time'] = project_origin_json.get('create_time_utc', 0)
            project_json['create_time_utc'] = project_origin_json.get('create_time_utc', 0)
            database_count = {}
            for table_name in project_table_dict.get(project_json['name'], []):
                db_name = table_name.split('.')[0]
                if db_name not in database_count:
                    database_count[db_name] = 0
                database_count[db_name] += 1
            if len(database_count) > 0:
                project_json['default_database'] = max(database_count.items(), key=lambda x: x[1])[0]

            user_whole = []
            group_whole = []
            if os.path.exists(INPUT_ROOT + '/acl/' + project_origin_json['uuid']):
                with open(INPUT_ROOT + '/acl/' + project_origin_json['uuid'], 'r') as project_role_file:
                    project_role_json = json.load(project_role_file)
                    for role_entry in project_role_json['entries']:
                        if 'a' in role_entry:
                            group_whole.append(role_entry['a'])
                        elif 'p' in role_entry:
                            user_whole.append(role_entry['p'])
            project_table = project_table_dict.get(plain_project_name, [])
            project_table_column = {}
            for table_name in project_table:
                project_table_column[table_name] = [col_info['name'] for col_info in
                                                    table_content_dict[(table_name, plain_project_name)]['columns']]

            pm = ProjectMigrator(plain_project_name, user_whole, group_whole, project_table, project_table_column,
                                 list(set([x['realization'] for x in project_origin_json['realizations']])))
            ret.append(pm)
        with open(OUTPUT_ROOT + '/_global/project/' + project_name, 'w') as new_project_file:
            json.dump(project_json, new_project_file, indent=2)
        for sub in ['rule', 'dataflow', 'index_plan', 'model_desc', 'table']:
            if not os.path.exists(OUTPUT_ROOT + '/' + plain_project_name + '/' + sub):
                os.makedirs(OUTPUT_ROOT + '/' + plain_project_name + '/' + sub)
    return ret


def prepare_cube_model():
    if not os.path.exists(INPUT_ROOT + '/cube_desc'):
        return
    for cube_name in os.listdir(INPUT_ROOT + '/cube_desc'):
        with open(INPUT_ROOT + '/cube_desc/' + cube_name) as cube_file:
            cube_json = json.load(cube_file)
            cube_model_dict[cube_json['name']] = cube_json['model_name']
    if not os.path.exists(INPUT_ROOT + '/raw_table_desc'):
        return
    for raw_table_name in os.listdir(INPUT_ROOT + '/raw_table_desc'):
        with open(INPUT_ROOT + '/raw_table_desc/' + raw_table_name) as raw_table_file:
            raw_table_json = json.load(raw_table_file)
            model_name = raw_table_json['model_name']
            model_raw_table_dict[model_name] = [raw_table_json] + model_raw_table_dict[
                model_name] if model_name in model_raw_table_dict else [raw_table_json]


def migrate_table_index(index_plan_json, model_json, raw_table_list):
    col_id_dict = {}
    for col in model_json['all_named_columns']:
        col_id_dict[col['column']] = col['id']
    indexes = []
    start_id = index_plan_json['next_table_index_id']
    for raw_table in raw_table_list:
        new_index = {
            'id': start_id,
            'dimensions': [],
            'layouts': [{
                'id': start_id + 1,
                "col_order": [],
                "shard_by_columns": [],
                "sort_by_columns": [],
                "storage_type": 20,
                "update_time": raw_table['last_modified'],
                "manual": True,
                "auto": False,
            }]
        }
        start_id += 10000
        all_ids = []
        shard_by_ids = []
        sort_by_ids = []
        for col in raw_table['columns']:
            col_id = col_id_dict[col['table'] + '.' + col['column']]
            if col.get('is_sortby'):
                sort_by_ids.append(col_id)
            if col.get('is_shardby'):
                shard_by_ids.append(col_id)
            all_ids.append(col_id)
        dim_ids = list(all_ids)
        dim_ids.sort()
        new_index['dimensions'] = dim_ids
        new_index['layouts'][0]['col_order'] = all_ids
        new_index['layouts'][0]['shard_by_columns'] = shard_by_ids
        new_index['layouts'][0]['sort_by_columns'] = sort_by_ids
        indexes.append(new_index)
    index_plan_json['indexes'] = indexes
    index_plan_json['next_table_index_id'] = start_id


def migrate_project_table_column_row_acl(project_name, user_whole, group_whole, table_whole, table_column_whole):
    user_table_blacklist = {}
    user_column_blacklist = {}
    user_row_whitelist = {}
    group_table_blacklist = {}
    group_column_blacklist = {}
    group_row_whitelist = {}
    # table acl
    if os.path.exists(INPUT_ROOT + '/table_acl/' + project_name):
        with open(INPUT_ROOT + '/table_acl/' + project_name, 'r') as table_acl_file:
            table_acl_json = json.load(table_acl_file)
            table_blacklist_dict = table_acl_json['userTableBlackList']
            for user_name in table_blacklist_dict.keys():
                table_blacklist = table_blacklist_dict[user_name]['tables']
                if len(table_blacklist) < 1:
                    continue
                user_table_blacklist[user_name] = table_blacklist

            table_blacklist_dict = table_acl_json['groupTableBlackList']
            for group_name in table_blacklist_dict.keys():
                table_blacklist = table_blacklist_dict[group_name]['tables']
                if len(table_blacklist) < 1:
                    continue
                group_table_blacklist[group_name] = table_blacklist

    # column acl
    if os.path.exists(INPUT_ROOT + '/column_acl/' + project_name):
        with open(INPUT_ROOT + '/column_acl/' + project_name, 'r') as column_acl_file:
            column_acl_json = json.load(column_acl_file)
            column_blacklist_dict = column_acl_json['userColumnBlackList']
            for user_name in column_blacklist_dict.keys():
                table_column_dict = column_blacklist_dict[user_name]['columnsWithTable']
                for table_name in table_column_dict.keys():
                    column_blacklist = table_column_dict[table_name]
                    if len(column_blacklist) < 1:
                        continue
                    if user_name not in user_column_blacklist:
                        user_column_blacklist[user_name] = {}
                    user_column_blacklist[user_name][table_name] = column_blacklist

            column_blacklist_dict = column_acl_json['groupColumnBlackList']
            for group_name in column_blacklist_dict.keys():
                table_column_dict = column_blacklist_dict[group_name]['columnsWithTable']
                for table_name in table_column_dict.keys():
                    column_blacklist = table_column_dict[table_name]
                    if len(column_blacklist) < 1:
                        continue
                    if group_name not in group_column_blacklist:
                        group_column_blacklist[group_name] = {}
                    group_column_blacklist[group_name][table_name] = column_blacklist

    # row acl
    if os.path.exists(INPUT_ROOT + '/row_acl/' + project_name + '/user'):
        for user_name in os.listdir(INPUT_ROOT + '/row_acl/' + project_name + '/user'):
            for table_name_suffix in os.listdir(INPUT_ROOT + '/row_acl/' + project_name + '/user/' + user_name):
                table_name = os.path.splitext(table_name_suffix)[0]
                with open(INPUT_ROOT + '/row_acl/' + project_name + '/user/' + user_name + '/' + table_name_suffix) \
                        as row_acl_file:
                    row_acl_json = json.load(row_acl_file)
                    for column_name, row_value_list in row_acl_json['rowACL'].items():
                        row_whitelist = [e[1] for e in row_value_list]
                        if len(row_whitelist) < 1:
                            continue
                        if user_name not in user_row_whitelist:
                            user_row_whitelist[user_name] = {}
                        if table_name not in user_row_whitelist[user_name]:
                            user_row_whitelist[user_name][table_name] = {}
                        user_row_whitelist[user_name][table_name][column_name] = row_whitelist

    if os.path.exists(INPUT_ROOT + '/row_acl/' + project_name + '/group'):
        for group_name in os.listdir(INPUT_ROOT + '/row_acl/' + project_name + '/group'):
            for table_name_suffix in os.listdir(INPUT_ROOT + '/row_acl/' + project_name + '/group/' + group_name):
                table_name = os.path.splitext(table_name_suffix)[0]
                with open(INPUT_ROOT + '/row_acl/' + project_name + '/group/' + group_name + '/' + table_name_suffix) \
                        as row_acl_file:
                    row_acl_json = json.load(row_acl_file)
                    for column_name, row_value_list in row_acl_json['rowACL'].items():
                        row_whitelist = [e[1] for e in row_value_list]
                        if len(row_whitelist) < 1:
                            continue
                        if group_name not in group_row_whitelist:
                            group_row_whitelist[group_name] = {}
                        if table_name not in group_row_whitelist[group_name]:
                            group_row_whitelist[group_name][table_name] = {}
                        group_row_whitelist[group_name][table_name][column_name] = row_whitelist

    # merge user table, column and row acl
    for user_name in user_whole:
        acl_json = json.loads(TABLE_COLUMN_ROW_ACL_TEMPLATE)
        acl_json['uuid'] = str(uuid.uuid4())
        should_fill = 0
        if user_name in user_row_whitelist.keys():
            for table_name in user_row_whitelist[user_name].keys():
                should_fill = 1
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = {}
                if 'column' not in acl_json['table'][table_name]:
                    acl_json['table'][table_name]['column'] = None
                acl_json['table'][table_name]['row'] = user_row_whitelist[user_name][table_name]
        if user_name in user_column_blacklist.keys():
            for table_name in user_column_blacklist[user_name].keys():
                should_fill = 1
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = {}
                if 'row' not in acl_json['table'][table_name]:
                    acl_json['table'][table_name]['row'] = None
                acl_json['table'][table_name]['column'] = [col for col in table_column_whole[table_name] if
                                                           col not in user_column_blacklist[user_name][table_name]]

        new_table_whole = table_whole
        if user_name in user_table_blacklist.keys():
            should_fill = 1
            new_table_whole = [tbl for tbl in table_whole if tbl not in user_table_blacklist[user_name]]

        if should_fill:
            for table_name in new_table_whole:
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = None
        else:
            if not DEFAULT_AUTHORIZED:
                acl_json['table'] = {}

        # persist user acl info
        if not os.path.exists(OUTPUT_ROOT + '/' + project_name + '/acl/user'):
            os.makedirs(OUTPUT_ROOT + '/' + project_name + '/acl/user')
        with open(OUTPUT_ROOT + '/' + project_name + '/acl/user/' + user_name + '.json', 'w') as new_acl_file:
            json.dump(acl_json, new_acl_file, indent=2)

    # merge group table, column and row acl
    for group_name in group_whole:
        acl_json = json.loads(TABLE_COLUMN_ROW_ACL_TEMPLATE)
        acl_json['uuid'] = str(uuid.uuid4())
        should_fill = 0
        if group_name in group_row_whitelist.keys():
            for table_name in group_row_whitelist[group_name].keys():
                should_fill = 1
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = {}
                if 'column' not in acl_json['table'][table_name]:
                    acl_json['table'][table_name]['column'] = None
                acl_json['table'][table_name]['row'] = group_row_whitelist[group_name][table_name]
        if group_name in group_column_blacklist.keys():
            for table_name in group_column_blacklist[group_name].keys():
                should_fill = 1
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = {}
                if 'row' not in acl_json['table'][table_name]:
                    acl_json['table'][table_name]['row'] = None
                acl_json['table'][table_name]['column'] = [col for col in table_column_whole[table_name] if
                                                           col not in group_column_blacklist[group_name][table_name]]

        new_table_whole = table_whole
        if group_name in group_table_blacklist.keys():
            should_fill = 1
            new_table_whole = [tbl for tbl in table_whole if tbl not in group_table_blacklist[group_name]]

        if should_fill:
            for table_name in new_table_whole:
                if not acl_json['table']:
                    acl_json['table'] = {}
                if table_name not in acl_json['table']:
                    acl_json['table'][table_name] = None
        else:
            if not DEFAULT_AUTHORIZED:
                acl_json['table'] = {}

        # persist group acl info
        if not os.path.exists(OUTPUT_ROOT + '/' + project_name + '/acl/group'):
            os.makedirs(OUTPUT_ROOT + '/' + project_name + '/acl/group')
        with open(OUTPUT_ROOT + '/' + project_name + '/acl/group/' + group_name + '.json', 'w') as new_acl_file:
            json.dump(acl_json, new_acl_file, indent=2)


class ProjectMigrator:
    def __init__(self, project, user_whole, group_whole, tables, table_columns, cubes):
        self.project = project
        self.user_whole = user_whole
        self.group_whole = group_whole
        self.tables = tables
        self.table_columns = table_columns
        self.cubes = cubes

    def migrate(self):
        logging.info('start migrate project %s', self.project)
        logging.info('migrating table, column and row acl')
        migrate_project_table_column_row_acl(self.project, self.user_whole, self.group_whole, self.tables,
                                             self.table_columns)
        logging.info('migrate table, column and row acl succeed')

        for table_name in self.tables:
            logging.info('migrating table %s', table_name)
            self.migrate_table(table_name)

        for cube_name in self.cubes:
            try:
                model_name = cube_model_dict[cube_name]
                if model_name in BYPASS_MODEL_LIST:
                    logging.warning("skip cube %s because model %s in bypass model list", cube_name, model_name)
                    continue

                with open(INPUT_ROOT + '/cube/' + cube_name + '.json') as cube_instance_file:
                    cube_instance_json = json.load(cube_instance_file)
                with open(INPUT_ROOT + '/cube_desc/' + cube_name + '.json') as cube_file:
                    cube_json = json.load(cube_file)
                with open(INPUT_ROOT + '/model_desc/' + model_name + '.json') as model_file:
                    model_json = json.load(model_file)

                tables = [model_json['fact_table'].split('.')[-1]] + [x['table'].split('.')[-1] for x in
                                                                      model_json['lookups']]
                if len(set(tables) & set(TABLE_BLACK_LIST)) > 0:
                    logging.info("cube %s has tables %s in blacklist", cube_name,
                                 str(set(tables) & set(TABLE_BLACK_LIST)))
                    continue
                uuid = cube_json['uuid']
                logging.info('migrating cube %s -- %s, uuid %s', self.project, cube_name, uuid)
                self.migrate_model_and_cube(model_name, cube_instance_json, cube_json, model_json)
            except Exception as e:
                logging.exception(
                    'cube %s -- %s migrate failed', self.project, cube_name)
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project + '/model_desc/' + uuid + '.json')
                except:
                    pass
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project + '/index_plan/' + uuid + '.json')
                except:
                    pass
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project + '/dataflow/' + uuid + '.json')
                except:
                    pass
        logging.info('migrate project %s finished', self.project)

    def migrate_table(self, plain_table_name):
        table_json = table_content_dict[(plain_table_name, self.project)]
        with open(OUTPUT_ROOT + '/' + self.project + '/table/' + plain_table_name + '.json', 'w') as new_table_file:
            table_json['source_type'] = 9
            json.dump(table_json, new_table_file, indent=2)

    def migrate_model_and_cube(self, model_name, cube_instance_json, cube_json, model_json):
        uuid = cube_json['uuid']
        with open(OUTPUT_ROOT + '/' + self.project + '/model_desc/' + uuid + '.json', 'w') as new_model_file:
            model_json['uuid'] = uuid
            model_json['alias'] = cube_json['name']
            model_json['join_tables'] = model_json['lookups']
            for table in model_json['join_tables']:
                table['kind'] = 'LOOKUP'

            self.model_all_named_columns(model_json, cube_json, self.project)

            # skip corr and semi additive measure
            measures = cube_json['measures']
            illegal_measures = [measure for measure in measures if measure['function']['expression'] == 'CORR'
                                or measure['function'].get('semi_additive_info')]

            if illegal_measures:
                logging.warning("model %s's measures %s is not support will be remove", model_name,
                                ", ".join([m['name'] for m in illegal_measures]))

            model_json['all_measures'] = [
                self.migrate_measure(x, 100000 + i) for i, x in enumerate(cube_json['measures'])
                if x not in illegal_measures]
            model_json['management_type'] = 'MODEL_BASED'
            if 'partition_desc' in model_json:
                model_json['partition_desc']['partition_type'] = 'APPEND'
            del model_json['dimensions']
            del model_json['metrics']
            del model_json['name']
            del model_json['lookups']
            json.dump(model_json, new_model_file, indent=2)
        model_content_dict[model_name] = model_json
        with open(OUTPUT_ROOT + '/' + self.project + '/index_plan/' + uuid + '.json', 'w') as new_cube_file:
            index_plan_json = json.loads(INDEX_PLAN_TEMPLATE)
            index_plan_json['uuid'] = uuid
            index_plan_json['rule_based_index']['measures'] = [x['id'] for x in model_json['all_measures']]
            self.migrate_aggregation_groups(index_plan_json, model_json, cube_json)
            index_plan_json['rule_based_index']['dimensions'] = list(functools.reduce(lambda x, y: x | y, [set(
                x['includes']) for x in index_plan_json['rule_based_index']['aggregation_groups']], set([])))
            if 'global_dim_cap' in cube_json:
                index_plan_json['rule_based_index']['global_dim_cap'] = cube_json['global_dim_cap']
            if model_name in model_raw_table_dict:
                raw_table_list = model_raw_table_dict[model_name]
                migrate_table_index(index_plan_json, model_json, raw_table_list)
            json.dump(index_plan_json, new_cube_file, indent=2)
        with open(OUTPUT_ROOT + '/' + self.project + '/dataflow/' + uuid + '.json', 'w') as new_cube_file:
            dataflow_json = json.loads(DATAFLOW_TEMPLATE)
            dataflow_json['uuid'] = uuid
            cube_status = cube_instance_json['status']
            if cube_status == 'DESCBROKEN':
                dataflow_json['status'] = 'BROKEN'
            if model_name in OFFLINE_MODEL_LIST or cube_status == 'DISABLED':
                dataflow_json['status'] = 'OFFLINE'
            json.dump(dataflow_json, new_cube_file, indent=2)

    def migrate_aggregation_groups(self, index_plan_json, model_json, aggregation_groups_json):
        col_id_dict = {}
        for col in model_json['all_named_columns']:
            col_id_dict[col['column']] = col['id']

        def _implicit_base_cube(cube_json):
            implicit_dims = [dim['column'] for dim in cube_json['rowkey']['rowkey_columns']]
            ret = {'select_rule': {}, 'includes': [col_id_dict[dim] for dim in implicit_dims]}
            ret['select_rule']['hierarchy_dims'] = []
            ret['select_rule']['mandatory_dims'] = [col_id_dict[dim] for dim in implicit_dims]
            ret['select_rule']['joint_dims'] = []
            return ret

        def _replace_id_list(olds):
            return [col_id_dict[x] for x in olds]

        def _replace_group(group):
            ret = {'select_rule': {}, 'includes': [col_id_dict[x] for x in group['includes']]}
            ret['select_rule']['hierarchy_dims'] = [_replace_id_list(x)
                                                    for x in group['select_rule']['hierarchy_dims']]
            ret['select_rule']['mandatory_dims'] = [col_id_dict[x]
                                                    for x in group['select_rule']['mandatory_dims']]
            ret['select_rule']['joint_dims'] = [_replace_id_list(x)
                                                for x in group['select_rule']['joint_dims']]
            if 'dim_cap' in group['select_rule']:
                ret['select_rule']['dim_cap'] = group['select_rule']['dim_cap']
            return ret

        index_plan_json['rule_based_index']['aggregation_groups'] = [
            _replace_group(x) for x in aggregation_groups_json['aggregation_groups']]
        index_plan_json['rule_based_index']['aggregation_groups'].append(_implicit_base_cube(aggregation_groups_json))

    def migrate_measure(self, measure, measure_id):
        origin_param = measure['function']['parameter']

        def recursive(p):
            if 'next_parameter' in p and p['next_parameter'] is not None:
                next_p = recursive(p['next_parameter'])
                del p['next_parameter']
                return [p] + next_p
            return [p]

        measure['function']['parameters'] = recursive(origin_param)
        measure['id'] = measure_id
        measure['name'] = re.sub(r'[^A-Za-z0-9_]', '_', measure['name'])
        return measure

    def model_all_named_columns(self, model_json, cube_json, project_name):
        tables = [{'table': model_json['fact_table']}] + [x for x in model_json['lookups']]
        columns = []
        for table_ref in tables:
            x = table_ref['table']
            t = table_content_dict[(x, project_name)]
            cols = json.loads(json.dumps(t['columns']))
            for col in cols:
                if 'alias' in table_ref:
                    col['column'] = table_ref['alias'] + '.' + col['name']
                else:
                    col['column'] = t['name'] + '.' + col['name']
                if 'cc_expr' in col:
                    continue
                col['name'] = col['column'].replace('.', '_0_DOT_0_')
                columns.append(col)

        dimensions = {}
        name_count = {}
        includes = []

        def get_include_full_name(col_name):
            if len(col_name.split('.')) == 1:
                dimension_list = model_json.get('dimensions') or []
                for dimension in dimension_list:
                    if any(filter(lambda c: c == col_name, dimension.get('columns') or [])):
                        return dimension['table'] + "." + col_name
            return col_name

        for agg_group in cube_json['aggregation_groups']:
            for i, include in enumerate(agg_group['includes']):
                include = get_include_full_name(include)
                agg_group['includes'][i] = include

            for i, hierarchy_dims in enumerate(agg_group['select_rule']['hierarchy_dims']):
                for j, hierarchy in enumerate(hierarchy_dims):
                    hierarchy = get_include_full_name(hierarchy)
                    agg_group['select_rule']['hierarchy_dims'][i][j] = hierarchy

            for i, mandatory in enumerate(agg_group['select_rule']['mandatory_dims']):
                mandatory = get_include_full_name(mandatory)
                agg_group['select_rule']['mandatory_dims'][i] = mandatory

            for i, joint_dims in enumerate(agg_group['select_rule']['joint_dims']):
                for j, joint in enumerate(joint_dims):
                    joint = get_include_full_name(joint)
                    agg_group['select_rule']['joint_dims'][i][j] = joint

        for i, rowkey in enumerate(cube_json['rowkey']['rowkey_columns']):
            column = get_include_full_name(rowkey['column'])
            cube_json['rowkey']['rowkey_columns'][i]['column'] = column

        for x in cube_json['aggregation_groups']:
            includes.extend(x['includes'])

        includes = list(set(includes))
        for x in includes:
            dimensions[x] = {'name': x.split('.')[1]}
        for x in cube_json['dimensions']:
            dim_key = x['table'] + '.' + \
                      x['derived'][0] if x['column'] is None else x['table'] + '.' + x['column']
            dimensions[dim_key] = {'name': x['name'] if x['name'] else x['column'] if x['column'] else x['derived'][0]}
        for x in dimensions.keys():
            name = dimensions[x]['name']
            count = name_count.get(name, 0)
            dimensions[x]['name'] += '' if count == 0 else '_' + x.split('.')[0]
            name_count[name] = count + 1

        id_index = 0
        for column in columns:
            column['id'] = id_index
            if column['column'] in dimensions:
                column['status'] = 'DIMENSION'
                column['name'] = dimensions[column['column']]['name']
            del column['datatype']
            id_index += 1

        if 'computed_columns' in model_json:
            for cc in model_json['computed_columns']:
                col = {'id': id_index, 'status': 'DIMENSION',
                       'name': cc['tableAlias'] + '_0_DOT_0_' + cc['columnName'],
                       'column': cc['tableAlias'] + '.' + cc['columnName']}
                columns.append(col)
                id_index += 1
        model_json['all_named_columns'] = columns


def check_project():
    for project_file in os.listdir(os.path.join(INPUT_ROOT, "project")):
        project_name = project_file.split(".")[0]
        project_checker = ProjectChecker(project_name)
        project_checker.check()


class ProjectChecker:
    """
    1. kafka stream project(kylin.source.default=1) will append project to BYPASS_PROJECT_LIST
    2. cc in lookup table will append model to BYPASS_MODEL_LIST
    3. cc in join condition will append model to BYPASS_MODEL_LIST
    4. cc in partition date column will append model to BYPASS_MODEL_LIST
    5. partition_date_format should not empty while partition_date_column exists will append model to BYPASS_MODEL_LIST
    6. table is fact table in partition model and is also dimension table in other models
    will append other models to OFFLINE_MODEL_LIST
    7. cc has same name with different expression or cc has different name with same expression in same fact table
    will append model to BYPASS_MODEL_LIST (actually it is also impossible in ke3)
    """
    project_name = None
    models = []
    tables = []
    bypass = False

    def __init__(self, project_name):
        self.project_name = project_name
        with open(os.path.join(INPUT_ROOT, "project", project_name + ".json")) as project_file:
            project_json = json.load(project_file)
            if project_json.get('override_kylin_properties').get('kylin.source.default') == '1':
                BYPASS_PROJECT_LIST.append(project_name)
                logging.warning("project %s is streaming project will bypass", self.project_name)

            self.models = project_json.get('models') or []
            self.tables = project_json.get('tables') or []

    def check(self):
        computed_columns_list = []
        from collections import defaultdict
        model_table_dict = defaultdict(dict)

        for model in self.models:
            with open(os.path.join(INPUT_ROOT, "model_desc", model + ".json")) as model_file:
                model_json = json.load(model_file)
                computed_columns = model_json.get('computed_columns') or []
                # check cc in lookup table
                cc_not_in_fact_table_list = [cc for cc in computed_columns if
                                             cc['tableIdentity'] != model_json['fact_table']]

                if cc_not_in_fact_table_list:
                    logging.warning("project %s model %s has computed column %s in lookup table",
                                    self.project_name, model, ", ".join([cc['columnName'] for cc in
                                                                         cc_not_in_fact_table_list]))
                    BYPASS_MODEL_LIST.append(model)
                else:
                    computed_columns_list.append({
                        "model": model,
                        "computed_columns": computed_columns
                    })

                # check cc in join condition
                lookups = model_json.get('lookups') or []

                import itertools
                join_columns = list(itertools.chain(*[list(itertools.chain(
                    lookup['join']['primary_key'], lookup['join']['foreign_key'])) for lookup in lookups]))

                cc_in_join_condition = [cc for cc in computed_columns if "{tableAlias}.{columnName}".format(
                    tableAlias=cc['tableAlias'], columnName=cc['columnName']) in join_columns]

                if cc_in_join_condition:
                    BYPASS_MODEL_LIST.append(model)
                    logging.warning(
                        "project %s find computed column %s in model %s lookups's join column", self.project_name,
                        ", ".join([cc['columnName'] for cc in cc_in_join_condition]), model)

                # check cc in partition date column && partition_date_format exists
                if model_json.get('partition_desc') and model_json.get('partition_desc').get('partition_date_column'):
                    partition_date_column = model_json.get('partition_desc').get('partition_date_column')

                    if not model_json.get('partition_desc').get('partition_date_format'):
                        BYPASS_MODEL_LIST.append(model)
                        logging.warning(
                            "project %s find partition_date_format is empty in model %s", self.project_name,  model)

                    cc_in_partition_date_column = [cc for cc in computed_columns if "{tableAlias}.{columnName}".format(
                        tableAlias=cc['tableAlias'], columnName=cc['columnName']) == partition_date_column]
                    if cc_in_partition_date_column:
                        BYPASS_MODEL_LIST.append(model)
                        logging.warning(
                            "project %s find computed column %s in model %s partition date column", self.project_name,
                            ", ".join([cc['columnName'] for cc in cc_in_partition_date_column]), model)

                model_table_dict[model] = {
                    "fact_table": model_json['fact_table'],
                    "lookup_tables": [lookup['table'] for lookup in model_json['lookups']],
                    "is_partition": model_json.get('partition_desc') and
                                    model_json['partition_desc'].get('partition_type') == 'TIME'
                }

        # check table is fact table and dimension table (in partition model)
        for model, table in model_table_dict.items():
            if table['is_partition']:
                dim_table_models = [k for k, v in model_table_dict.items() if table['fact_table'] in v['lookup_tables']]
                if dim_table_models:
                    OFFLINE_MODEL_LIST.extend(dim_table_models)
                    logging.warning(
                        "project %s find model %s's fact table %s is lookup table in "
                        "model %s", self.project_name, model, table['fact_table'], ", ".join(dim_table_models))

        from collections import defaultdict
        cc_model_name_dict = defaultdict(list)
        expression_model_name_dict = defaultdict(list)
        model_cc_expression = defaultdict(list)
        for computed_columns_item in computed_columns_list:
            model = computed_columns_item['model']
            computed_columns = computed_columns_item['computed_columns']
            for cc in computed_columns:
                cc_name = "{table_name}.{columnName}".format(table_name=cc['tableAlias'], columnName=cc['columnName'])
                cc_model_name_dict[cc_name].append(model)
                expression = re.sub(r'\n', '', cc['expression'])
                expression_model_name_dict[expression].append(model)
                model_cc_expression[model].append({
                    'columnName': cc_name,
                    'expression': expression,
                })

        # check same name cc with different expression in same fact table
        for cc_name, model_name_list in cc_model_name_dict.items():
            expressions = set()
            for model_name in model_name_list:
                expressions.update((item['expression'] for item
                                    in model_cc_expression[model_name] if item['columnName'] == cc_name))
            if len(expressions) > 1:
                BYPASS_MODEL_LIST.extend(model_name_list)
                logging.warning(
                    "project %s find same name computed column %s with different expression "
                    "in different fact table in modes %s",
                    self.project_name, cc_name.split(".")[1], ", ".join(model_name_list))

        # check different name with same expression in same fact table
        for expression, model_name_list in expression_model_name_dict.items():
            cc_names = set()
            for model_name in model_name_list:
                cc_names.update((item['columnName'] for item
                                 in model_cc_expression[model_name] if item['expression'] == expression))

            from itertools import groupby
            for fact_table, cc_name_iterator in groupby(cc_names, lambda cc: cc.split('.')[0]):
                cc_name_list = list(cc_name_iterator)
                if len(cc_name_list) > 1:
                    BYPASS_MODEL_LIST.extend(model_name_list)
                    logging.warning(
                        "project %s find different name computed column with same expression %s "
                        "in different fact table in modes %s",
                        self.project_name, expression, ", ".join(model_name_list))


if __name__ == "__main__":

    help_info = """please keep with 'kylin.acl.project-internal-default-permission-granted' in kylin.properties"""
    parser = argparse.ArgumentParser(description='migration from 2.x/3.x metadata')
    parser.add_argument('-i', default='./metadata', help='the folder of input metadata')
    parser.add_argument('-o', default='./newten_meta', help='the folder of output metadata')
    parser.add_argument('-g', default=True, help=help_info)
    args = parser.parse_args()

    print('''This tool is not responsible for the migration of user and user group information 
    during user system integration when LDAP, AD and third-party permissions.''')

    INPUT_ROOT = args.i
    OUTPUT_ROOT = args.o
    if not args.g:
        DEFAULT_AUTHORIZED = 0

    migrate_user()
    migrate_user_group()
    migrate_role_permission()
    check_project()
    pms = prepare_project()
    prepare_cube_model()
    for pm in pms:
        pm.migrate()
