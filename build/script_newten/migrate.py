import json
import os
import logging
import functools
import traceback

logging.basicConfig(level='DEBUG', format='%(asctime)-15s %(message)s')
INPUT_ROOT = './metadata'
OUTPUT_ROOT = './newten_meta'
OUTPUT_PROJECT_ROOT = OUTPUT_ROOT + '/CPIC_FRP'
TABLE_BLACK_LIST = [
    'DIM_NEW_CLASSIFY_LABLE_D',
    'DIM_PERIOD_MONTH_VIEW',
    'DIM_REPORT_PERIOD_D',
    'DM_CUSTOM_FLEXIBLE_F',
    'DM_CUSTOM_NONAUTO_KH',
    'DM_DEFINED_FLEXIBLE_F',
    'DM_FLEXIBLE_NONAUTO_VIEW',
    'DM_FLEXIBLE_PZ_ALL_VIEW',
    'DM_SELL_CHANNEL_VIEW',
    'D_NONAUTO_PRODUCT_T',
    'IX_ACC_DQXB_MONTHLY_QD_VIEW',
    'IX_ACC_DQXB_MONTHLY_VIEW',
    'IX_QLC_PREMIUM_CLAIM_DAILY_VIEW',
    'IX_SUB_INSURED_MONTHLY_VIEW',
    'JK_NONAUTO_DAILY_T_VIEW'
]

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
    for project_name in os.listdir(INPUT_ROOT + '/project'):
        os.makedirs(OUTPUT_ROOT + '/_global/project', exist_ok=True)
        project_json = json.loads(PROJECT_TEMPALTE)
        plain_project_name = project_name.split('.')[0]
        with open(INPUT_ROOT + '/project/' + project_name) as project_file:
            project_origin_json = json.load(project_file)
            project_json['uuid'] = project_origin_json['uuid']
            project_json['name'] = project_origin_json['name']
            project_json['last_modified'] = project_origin_json['last_modified']
            project_json['create_time'] = project_origin_json['create_time_utc']
            project_json['create_time_utc'] = project_origin_json['create_time_utc']
        with open(OUTPUT_ROOT + '/_global/project/' + project_name, 'w') as new_project_file:
            json.dump(project_json, new_project_file)
        for sub in ['rule', 'dataflow', 'index_plan', 'model_desc', 'table']:
            os.makedirs(OUTPUT_ROOT + '/' + plain_project_name +
                        '/' + sub, exist_ok=True)
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/3fcc884d-a4da-4afa-bba5-f22241b127d4', 'w') as f:
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
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/744e60ef-4e14-4489-b87e-7ff2479ff813', 'w') as f:
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
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/71283af5-3b60-43e6-8539-ea46de9c707f', 'w') as f:
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
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/a0f6d6fb-f98f-4461-a107-69503c4a653d', 'w') as f:
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
        with open(OUTPUT_ROOT + '/' + plain_project_name + '/rule/a88950b9-6f9e-4d83-9982-01e18b8ef365', 'w') as f:
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


def migrate_table(table_name):
    with open(INPUT_ROOT + '/table/' + table_name) as table_file:
        table_json = json.load(table_file)
        plain_table_name = table_name.split(
            '--')[0] if '--' in table_name else table_name[0:-5]
        if plain_table_name.split('.')[-1] in TABLE_BLACK_LIST:
            logging.info("skip table %s", plain_table_name)
            return
        with open(OUTPUT_PROJECT_ROOT + '/table/' + plain_table_name + '.json', 'w') as new_table_file:
            table_json['source_type'] = 9
            json.dump(table_json, new_table_file, indent=2)
        table_content_dict[plain_table_name] = table_json


def __model_all_named_columns(model_json):
    tables = [model_json['fact_table']] + [x['table']
                                           for x in model_json['lookups']]
    columns = []
    for x in tables:
        t = table_content_dict[x]
        cols = t['columns']
        for col in cols:
            col['column'] = t['name'] + '.' + col['name']
            columns.append(col)

    dimensions = [x['table'].split('.')[-1] + '.' + y for x in model_json['dimensions']
                  for y in x['columns']]
    id_index = 0
    for column in columns:
        column['id'] = id_index
        if column['column'] in dimensions:
            column['status'] = 'DIMENSION'
        id_index += 1
    model_json['all_named_columns'] = columns


def __migrate_measure_parameter(measure, measure_id):
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


def __migrate_aggregation_groups(index_plan_json, model_json, aggregation_groups_json):
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


def __migrate_table_index(index_plan_json, model_json, raw_table_list):
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


def migrate_model_and_cube(cube_name):
    model_name = cube_model_dict[cube_name.split('.')[0]]
    with open(INPUT_ROOT + '/cube_desc/' + cube_name) as cube_file:
        cube_json = json.load(cube_file)
    with open(INPUT_ROOT + '/model_desc/' + model_name + '.json') as model_file:
        model_json = json.load(model_file)

    tables = [model_json['fact_table'].split('.')[-1]] + [x['table'].split('.')[-1]
                                                          for x in model_json['lookups']]
    if len(set(tables) & set(TABLE_BLACK_LIST)) > 0:
        logging.info("cube %s has tables %s in blacklist", cube_name, str(set(tables) & set(TABLE_BLACK_LIST)))
        return
    uuid = cube_json['uuid']
    with open(OUTPUT_PROJECT_ROOT + '/model_desc/' + uuid + '.json', 'w') as new_model_file:
        model_json['uuid'] = uuid
        model_json['alias'] = cube_json['name']
        model_json['join_tables'] = model_json['lookups']
        __model_all_named_columns(model_json)
        model_json['all_measures'] = [
            __migrate_measure_parameter(x, 100000+i) for i, x in enumerate(cube_json['measures'])]
        model_json['management_type'] = 'MODEL_BASED'
        del model_json['dimensions']
        del model_json['metrics']
        del model_json['name']
        json.dump(model_json, new_model_file, indent=2)
    model_content_dict[model_name] = model_json
    with open(OUTPUT_PROJECT_ROOT + '/index_plan/' + uuid + '.json', 'w') as new_cube_file:
        index_plan_json = json.loads(INDEX_PLAN_TEMPLATE)
        index_plan_json['uuid'] = uuid
        index_plan_json['rule_based_index']['measures'] = [
            x['id'] for x in model_json['all_measures']]
        __migrate_aggregation_groups(index_plan_json, model_json, cube_json)
        index_plan_json['rule_based_index']['dimensions'] = list(functools.reduce(lambda x, y: x | y, [set(
            x['includes']) for x in index_plan_json['rule_based_index']['aggregation_groups']], set([])))
        if model_name in model_raw_table_dict:
            raw_table_list = model_raw_table_dict[model_name]
            __migrate_table_index(index_plan_json, model_json, raw_table_list)
        json.dump(index_plan_json, new_cube_file, indent=2)
    with open(OUTPUT_PROJECT_ROOT + '/dataflow/' + uuid + '.json', 'w') as new_cube_file:
        dataflow_json = json.loads(DATAFLOW_TEMPLATE)
        dataflow_json['uuid'] = uuid
        json.dump(dataflow_json, new_cube_file, indent=2)


if __name__ == "__main__":
    prepare_project()
    prepare_cube_model()
    for table_name in os.listdir(INPUT_ROOT + '/table'):
        logging.info('migrating table %s', table_name)
        migrate_table(table_name)

    for cube_name in os.listdir(INPUT_ROOT + "/cube_desc"):
        logging.info('migrating cube %s', cube_name)
        try:
            migrate_model_and_cube(cube_name)
        except Exception as e:
            logging.exception('model %s migrate failed', cube_name)
