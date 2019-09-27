import json
import os
import logging
import functools
import traceback
import argparse
import sys

logging.basicConfig(
    level='DEBUG', format='%(asctime)-15s %(levelname)s: %(message)s', stream=sys.stdout)

INPUT_ROOT = './metadata'
OUTPUT_ROOT = './newten_meta'
TABLE_BLACK_LIST = []

table_content_dict = {}
model_content_dict = {}
cube_model_dict = {}
model_raw_table_dict = {}

PROJECT_TEMPALTE = '''
{
    "uuid" : "",
    "version" : "3.0.0.0",
    "mvcc" : 2,
    "name" : "",
    "owner" : "ADMIN",
    "status" : "ENABLED",
    "description" : "",
    "ext_filters" : [ ],
    "maintain_model_type" : "MANUAL_MAINTAIN",
    "override_kylin_properties" : {
      "kylin.source.default" : "9"
    },
    "push_down_range_limited" : true,
    "segment_config" : {
      "auto_merge_enabled" : true,
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
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "description" : null,
  "owner" : null,
  "create_time_utc" : 1554807337664,
  "status" : "ONLINE",
  "cost" : 50,
  "query_hit_count" : 0,
  "event_error" : false,
  "segments" : [],
  "storage_location_identifier" : null
}
'''

INDEX_PLAN_TEMPLATE = '''
{
  "uuid" : "",
  "last_modified" : 1554879670503,
  "create_time" : 1554879670483,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "description" : null,
  "index_plan_override_encodings" : { },
  "rule_based_index" : {
    "dimensions" : [],
    "measures" : [],
    "aggregation_groups" : [],
    "index_black_list" : [ ],
    "parent_forward" : 3,
    "layout_id_mapping" : [],
    "index_start_id" : 0
  },
  "indexes" : [ ],
  "override_properties" : { },
  "segment_range_start" : 0,
  "segment_range_end" : 9223372036854775807,
  "auto_merge_time_ranges" : null,
  "retention_range" : 0,
  "notify_list" : [ ],
  "status_need_notify" : [ ],
  "engine_type" : 80,
  "next_aggregation_index_id" : 0,
  "next_table_index_id" : 20000000000
}
'''


def prepare_project():
    ret = []
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
    for project_name in os.listdir(INPUT_ROOT + '/project'):
        if not os.path.exists(OUTPUT_ROOT + '/_global/project'):
            os.makedirs(OUTPUT_ROOT + '/_global/project')
        project_json = json.loads(PROJECT_TEMPALTE)
        plain_project_name = project_name.split('.')[0]
        with open(INPUT_ROOT + '/project/' + project_name) as project_file:
            project_origin_json = json.load(project_file)
            project_json['uuid'] = project_origin_json['uuid']
            project_json['name'] = project_origin_json['name']
            project_json['last_modified'] = project_origin_json.get('last_modified', 0)
            project_json['create_time'] = project_origin_json.get('create_time_utc', 0)
            project_json['create_time_utc'] = project_origin_json.get('create_time_utc', 0)

            pm = ProjectMigrator(plain_project_name, list(
                set([x['realization'] for x in project_origin_json['realizations']])),
                project_table_dict.get(plain_project_name, []))
            ret.append(pm)
        with open(OUTPUT_ROOT + '/_global/project/' + project_name, 'w') as new_project_file:
            json.dump(project_json, new_project_file)
        for sub in ['rule', 'dataflow', 'index_plan', 'model_desc', 'table']:
            if not os.path.exists(OUTPUT_ROOT + '/' + plain_project_name + '/' + sub):
                os.makedirs(OUTPUT_ROOT + '/' + plain_project_name + '/' + sub)
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/3fcc884d-a4da-4afa-bba5-f22241b127d4.json', 'w') as f:
            f.write('''{
  "uuid" : "3fcc884d-a4da-4afa-bba5-f22241b127d4",
  "last_modified" : 0,
  "create_time" : 1554287746679,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "conds" : [ {
    "@class" : "io.kyligence.kap.metadata.favorite.FavoriteRule$Condition",
    "leftThreshold" : null,
    "rightThreshold" : "ADMIN"
  } ],
  "name" : "submitter",
  "enabled" : true
}''')
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/744e60ef-4e14-4489-b87e-7ff2479ff813.json', 'w') as f:
            f.write('''{
  "uuid" : "744e60ef-4e14-4489-b87e-7ff2479ff813",
  "last_modified" : 0,
  "create_time" : 1554287746679,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "conds" : [ {
    "@class" : "io.kyligence.kap.metadata.favorite.FavoriteRule$Condition",
    "leftThreshold" : "0",
    "rightThreshold" : "180"
  } ],
  "name" : "duration",
  "enabled" : false
}''')
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/71283af5-3b60-43e6-8539-ea46de9c707f.json', 'w') as f:
            f.write('''{
  "uuid" : "71283af5-3b60-43e6-8539-ea46de9c707f",
  "last_modified" : 0,
  "create_time" : 1554287746679,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "conds" : [ {
    "@class" : "io.kyligence.kap.metadata.favorite.FavoriteRule$Condition",
    "leftThreshold" : null,
    "rightThreshold" : "ROLE_ADMIN"
  } ],
  "name" : "submitter_group",
  "enabled" : true
}''')
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/a0f6d6fb-f98f-4461-a107-69503c4a653d.json', 'w') as f:
            f.write('''{
  "uuid" : "a0f6d6fb-f98f-4461-a107-69503c4a653d",
  "last_modified" : 0,
  "create_time" : 1554287746678,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "conds" : [ {
    "@class" : "io.kyligence.kap.metadata.favorite.FavoriteRule$Condition",
    "leftThreshold" : null,
    "rightThreshold" : "0.1"
  } ],
  "name" : "frequency",
  "enabled" : true
}''')
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/a88950b9-6f9e-4d83-9982-01e18b8ef365.json', 'w') as f:
            f.write('''{
  "uuid" : "a88950b9-6f9e-4d83-9982-01e18b8ef365",
  "last_modified" : 0,
  "create_time" : 1554287746679,
  "version" : "3.0.0.0",
  "mvcc" : 0,
  "conds" : [ ],
  "name" : "blacklist",
  "enabled" : false
}''')
    return ret


def prepare_cube_model():
    for cube_name in os.listdir(INPUT_ROOT + '/cube_desc'):
        with open(INPUT_ROOT + '/cube_desc/' + cube_name) as cube_file:
            cube_json = json.load(cube_file)
            cube_model_dict[cube_json['name']] = cube_json['model_name']
    for raw_table_name in os.listdir(INPUT_ROOT + '/raw_table_desc'):
        with open(INPUT_ROOT + '/raw_table_desc/' + raw_table_name) as raw_table_file:
            raw_table_json = json.load(raw_table_file)
            model_name = raw_table_json['model_name']
            model_raw_table_dict[model_name] = [
                raw_table_json] + model_raw_table_dict[model_name] if model_name in model_raw_table_dict else [raw_table_json]


def model_all_named_columns(model_json, project_name):
    tables = [{'table': model_json['fact_table']}] + \
        [x for x in model_json['lookups']]
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
            col['name'] = col['column'].replace('.', '_')
            columns.append(col)

    dimensions = [x['table'].split('.')[-1] + '.' + y for x in model_json['dimensions']
                  for y in x['columns']]
    id_index = 0
    for column in columns:
        column['id'] = id_index
        if column['column'] in dimensions:
            column['status'] = 'DIMENSION'
        id_index += 1

    if 'computed_columns' in model_json:
        for cc in model_json['computed_columns']:
            col = {'id': id_index, 'status': 'DIMENSION',
                   'name': cc['tableAlias'] + '_' + cc['columnName'], 'column': cc['tableAlias'] + '.' + cc['columnName']}
            columns.append(col)
            id_index += 1
    model_json['all_named_columns'] = columns


def migrate_measure_parameter(measure, measure_id):
    origin_param = measure['function']['parameter']

    def recursive(p):
        if 'next_parameter' in p and p['next_parameter'] is not None:
            next_p = recursive(p['next_parameter'])
            del p['next_parameter']
            return [p] + next_p
        return [p]
    measure['function']['parameters'] = recursive(origin_param)
    measure['id'] = measure_id
    measure['name'] = measure['name'].replace('.', '_')
    return measure


def migrate_aggregation_groups(index_plan_json, model_json, aggregation_groups_json):
    col_id_dict = {}
    for col in model_json['all_named_columns']:
        col_id_dict[col['column']] = col['id']

    def _replace_id_list(olds):
        return [col_id_dict[x] for x in olds]

    def _replace_group(group):
        ret = {'select_rule': {}}
        ret['includes'] = [col_id_dict[x] for x in group['includes']]
        ret['select_rule']['hierarchy_dims'] = [_replace_id_list(x)
                                                for x in group['select_rule']['hierarchy_dims']]
        ret['select_rule']['mandatory_dims'] = [col_id_dict[x]
                                                for x in group['select_rule']['mandatory_dims']]
        ret['select_rule']['joint_dims'] = [_replace_id_list(x)
                                            for x in group['select_rule']['joint_dims']]
        return ret
    index_plan_json['rule_based_index']['aggregation_groups'] = [
        _replace_group(x) for x in aggregation_groups_json['aggregation_groups']]


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
        for col in raw_table['columns']:
            col_id = col_id_dict[col['table'] + '.' + col['column']]
            all_ids.append(col_id)
        dim_ids = list(all_ids)
        dim_ids.sort()
        new_index['dimensions'] = dim_ids
        new_index['layouts'][0]['col_order'] = all_ids
        indexes.append(new_index)
    index_plan_json['indexes'] = indexes
    index_plan_json['next_table_index_id'] = start_id


class ProjectMigrator:
    def __init__(self, project, cubes, tables):
        self.project = project
        self.cubes = cubes
        self.tables = tables

    def migrate(self):
        logging.info('start migrate project %s', self.project)
        for table_name in self.tables:
            logging.info('migrating table %s', table_name)
            self.migrate_table(table_name)

        for cube_name in self.cubes:
            try:
                model_name = cube_model_dict[cube_name]
                with open(INPUT_ROOT + '/cube_desc/' + cube_name + '.json') as cube_file:
                    cube_json = json.load(cube_file)
                with open(INPUT_ROOT + '/model_desc/' + model_name + '.json') as model_file:
                    model_json = json.load(model_file)

                tables = [model_json['fact_table'].split('.')[-1]] + [x['table'].split('.')[-1]
                                                                      for x in model_json['lookups']]
                if len(set(tables) & set(TABLE_BLACK_LIST)) > 0:
                    logging.info("cube %s has tables %s in blacklist",
                                 cube_name, str(set(tables) & set(TABLE_BLACK_LIST)))
                    continue
                uuid = cube_json['uuid']
                logging.info('migrating cube %s -- %s, uuid %s',
                             self.project, cube_name, uuid)
                self.migrate_model_and_cube(
                    cube_name, model_name, cube_json, model_json)
            except Exception as e:
                logging.exception(
                    'cube %s -- %s migrate failed', self.project, cube_name)
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project +
                              '/model_desc/' + uuid + '.json')
                except:
                    pass
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project +
                              '/index_plan/' + uuid + '.json')
                except:
                    pass
                try:
                    os.remove(OUTPUT_ROOT + '/' + self.project +
                              '/dataflow/' + uuid + '.json')
                except:
                    pass
        logging.info('migrate project %s finished', self.project)

    def migrate_table(self, plain_table_name):
        table_json = table_content_dict[(plain_table_name, self.project)]
        with open(OUTPUT_ROOT + '/' + self.project + '/table/' + plain_table_name + '.json', 'w') as new_table_file:
            table_json['source_type'] = 9
            json.dump(table_json, new_table_file, indent=2)

    def migrate_model_and_cube(self, cube_name, model_name, cube_json, model_json):
        uuid = cube_json['uuid']
        with open(OUTPUT_ROOT + '/' + self.project + '/model_desc/' + uuid + '.json', 'w') as new_model_file:
            model_json['uuid'] = uuid
            model_json['alias'] = cube_json['name']
            model_json['join_tables'] = model_json['lookups']
            model_all_named_columns(model_json, self.project)
            model_json['all_measures'] = [
                migrate_measure_parameter(x, 100000+i) for i, x in enumerate(cube_json['measures'])]
            model_json['management_type'] = 'MODEL_BASED'
            if 'partition_desc' in model_json:
                model_json['partition_desc']['partition_type'] = 'APPEND'
            del model_json['dimensions']
            del model_json['metrics']
            del model_json['name']
            json.dump(model_json, new_model_file, indent=2)
        model_content_dict[model_name] = model_json
        with open(OUTPUT_ROOT + '/' + self.project + '/index_plan/' + uuid + '.json', 'w') as new_cube_file:
            index_plan_json = json.loads(INDEX_PLAN_TEMPLATE)
            index_plan_json['uuid'] = uuid
            index_plan_json['rule_based_index']['measures'] = [
                x['id'] for x in model_json['all_measures']]
            migrate_aggregation_groups(
                index_plan_json, model_json, cube_json)
            index_plan_json['rule_based_index']['dimensions'] = list(functools.reduce(lambda x, y: x | y, [set(
                x['includes']) for x in index_plan_json['rule_based_index']['aggregation_groups']], set([])))
            if model_name in model_raw_table_dict:
                raw_table_list = model_raw_table_dict[model_name]
                migrate_table_index(
                    index_plan_json, model_json, raw_table_list)
            json.dump(index_plan_json, new_cube_file, indent=2)
        with open(OUTPUT_ROOT + '/' + self.project + '/dataflow/' + uuid + '.json', 'w') as new_cube_file:
            dataflow_json = json.loads(DATAFLOW_TEMPLATE)
            dataflow_json['uuid'] = uuid
            json.dump(dataflow_json, new_cube_file, indent=2)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='migration from 2.x/3.x metadata')
    parser.add_argument('-i', help='the folder of input metadata')
    parser.add_argument('-o', help='the folder of output metadata')
    args = parser.parse_args()

    INPUT_ROOT = args.i
    OUTPUT_ROOT = args.o

    pms = prepare_project()
    prepare_cube_model()
    for pm in pms:
        pm.migrate()
