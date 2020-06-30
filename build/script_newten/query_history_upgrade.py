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
import re
import subprocess
import time
from datetime import datetime, timedelta
from sys import argv

import mysql.connector
import psycopg2
import pytz
from influxdb import InfluxDBClient
from tzlocal import get_localzone

QUERY_HISTORY_SURFIX = 'query_history'
QUERY_HISTORY_REALIZATION_SURFIX = 'query_history_realization'

QUERY_ID = "query_id"
PROJECT_NAME = "project_name"
SQL_TEXT = "sql_text"
SQL_PATTERN = "sql_pattern"
QUERY_DURATION = "duration"
TOTAL_SCAN_BYTES = "total_scan_bytes"
TOTAL_SCAN_COUNT = "total_scan_count"
RESULT_ROW_COUNT = "result_row_count"
SUBMITTER = "submitter"
REALIZATIONS = "realizations"
QUERY_SERVER = "server"
ERROR_TYPE = "error_type"
ENGINE_TYPE = "engine_type"
IS_CACHE_HIT = "cache_hit"
QUERY_STATUS = "query_status"
IS_INDEX_HIT = "index_hit"
QUERY_TIME = "query_time"
MONTH = "month"
QUERY_FIRST_DAY_OF_MONTH = "query_first_day_of_month"
QUERY_FIRST_DAY_OF_WEEK = "query_first_day_of_week"
QUERY_DAY = "query_day"
IS_TABLE_INDEX_USED = "is_table_index_used"
IS_AGG_INDEX_USED = "is_agg_index_used"
IS_TABLE_SNAPSHOT_USED = "is_table_snapshot_used"

MODEL = "model"
LAYOUT_ID = "layout_id"
INDEX_TYPE = "index_type"

INSERT_HISTORY_SQL = 'INSERT INTO {table_name} (' + ','.join((QUERY_ID, SQL_TEXT, SQL_PATTERN, QUERY_DURATION, TOTAL_SCAN_BYTES,
                                                   TOTAL_SCAN_COUNT,
                                                   RESULT_ROW_COUNT, SUBMITTER, REALIZATIONS, QUERY_SERVER, ERROR_TYPE,
                                                   ENGINE_TYPE, IS_CACHE_HIT,
                                                   QUERY_STATUS, IS_INDEX_HIT, QUERY_TIME, MONTH,
                                                   QUERY_FIRST_DAY_OF_MONTH, QUERY_FIRST_DAY_OF_WEEK,
                                                   QUERY_DAY, IS_TABLE_INDEX_USED, IS_AGG_INDEX_USED,
                                                   IS_TABLE_SNAPSHOT_USED, PROJECT_NAME)) + ')  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

INSERT_HISTORY_REALIZATION_SQL = 'INSERT INTO {table_name} (' + ','.join((MODEL, LAYOUT_ID, INDEX_TYPE, QUERY_ID, QUERY_DURATION,
                                                               QUERY_TIME, PROJECT_NAME)) + ') VALUES (%s,%s,%s,%s,%s,%s,%s)'

MYSQL_INSERT_QUERY_HISTORY_SQL = 'CREATE TABLE IF NOT EXISTS `%s` ( \
    id bigint not null auto_increment,  \
    `query_id` VARCHAR(50),  \
    `sql_text` TEXT,  \
    `sql_pattern` TEXT,  \
    `duration` BIGINT,  \
    `total_scan_bytes` BIGINT,  \
    `total_scan_count` BIGINT,  \
    `result_row_count` BIGINT,  \
    `submitter` VARCHAR(255),  \
    `realizations` TEXT,  \
    `server` VARCHAR(50),  \
    `error_type` VARCHAR(50),  \
    `engine_type` VARCHAR(30),  \
    `cache_hit` BOOLEAN,  \
    `query_status` VARCHAR(20),  \
    `index_hit` BOOLEAN,  \
    `query_time` BIGINT,  \
    `month` VARCHAR(10),  \
    `query_first_day_of_month` BIGINT,  \
    `query_first_day_of_week` BIGINT,  \
    `query_day` BIGINT,  \
    `is_table_index_used` BOOLEAN,  \
    `is_agg_index_used` BOOLEAN,  \
    `is_table_snapshot_used` BOOLEAN,  \
    `project_name` VARCHAR(100),  \
    `reserved_field_1` VARCHAR(50), \
    `reserved_field_2` VARCHAR(50), \
    `reserved_field_3` longblob, \
    `reserved_field_4` longblob, \
    primary key(`id`,`project_name`) \
) DEFAULT CHARSET=utf8'

MYSQL_CREATE_QUERY_HISTORY_INDEX1 = 'ALTER table %s ADD INDEX %s_ix1(`query_time`)'
MYSQL_CREATE_QUERY_HISTORY_INDEX2 = 'ALTER table %s ADD INDEX %s_ix2(`query_first_day_of_month`)'
MYSQL_CREATE_QUERY_HISTORY_INDEX3 = 'ALTER table %s ADD INDEX %s_ix3(`query_first_day_of_week`)'
MYSQL_CREATE_QUERY_HISTORY_INDEX4 = 'ALTER table %s ADD INDEX %s_ix4(`query_day`)'
MYSQL_CREATE_QUERY_HISTORY_INDEX5 = 'ALTER table %s ADD INDEX %s_ix5(`duration`)'

MYSQL_INSERT_QUERY_HISTORY_REALIZATION_SQL = 'CREATE TABLE IF NOT EXISTS `%s` ( \
  id bigint not null auto_increment,  \
  `query_id` VARCHAR(255) , \
  `model` VARCHAR(255),  \
  `layout_id` VARCHAR(255), \
  `index_type` VARCHAR(255),  \
  `duration` BIGINT,  \
  `query_time` BIGINT,  \
  `project_name` VARCHAR(255), \
  primary key (`id`,`project_name`) \
) DEFAULT CHARSET=utf8'

MYSQL_CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = 'ALTER table %s ADD INDEX %s_ix1(`query_time`)'
MYSQL_CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = 'ALTER table %s ADD INDEX %s_ix2(`model`)'

POSTGRE_CREATE_QUERY_HISTORY_SQL = 'CREATE TABLE IF NOT EXISTS %s ( \
    id  serial, \
    query_id  VARCHAR(50),  \
    sql_text  TEXT,  \
    sql_pattern  TEXT,  \
    duration  BIGINT,  \
    total_scan_bytes  BIGINT,  \
    total_scan_count  BIGINT,  \
    result_row_count  BIGINT,  \
    submitter  VARCHAR(255),  \
    realizations  TEXT,  \
    server  VARCHAR(50),  \
    error_type  VARCHAR(50),  \
    engine_type  VARCHAR(30),  \
    cache_hit  BOOLEAN,  \
    query_status  VARCHAR(20),  \
    index_hit  BOOLEAN,  \
    query_time  BIGINT,  \
    month  VARCHAR(10),  \
    query_first_day_of_month  BIGINT,  \
    query_first_day_of_week  BIGINT,  \
    query_day  BIGINT,  \
    is_table_index_used  BOOLEAN,  \
    is_agg_index_used  BOOLEAN,  \
    is_table_snapshot_used  BOOLEAN,  \
    project_name  VARCHAR(100),  \
    reserved_field_1 VARCHAR(50), \
    reserved_field_2 VARCHAR(50), \
    reserved_field_3 bytea, \
    reserved_field_4 bytea, \
    primary key ( id , project_name ) \
)'

POSTGRE_CREATE_QUERY_HISTORY_INDEX1 = 'CREATE INDEX %s_ix1 ON %s USING btree ( query_time )'
POSTGRE_CREATE_QUERY_HISTORY_INDEX2 = 'CREATE INDEX %s_ix2 ON %s USING btree ( query_first_day_of_month )'
POSTGRE_CREATE_QUERY_HISTORY_INDEX3 = 'CREATE INDEX %s_ix3 ON %s USING btree ( query_first_day_of_week )'
POSTGRE_CREATE_QUERY_HISTORY_INDEX4 = 'CREATE INDEX %s_ix4 ON %s USING btree ( query_day )'
POSTGRE_CREATE_QUERY_HISTORY_INDEX5 = 'CREATE INDEX %s_ix5 ON %s USING btree ( duration )'

POSTGRE_INSERT_QUERY_HISTORY_REALIZATION_SQL = 'CREATE TABLE IF NOT EXISTS %s ( \
    id  serial, \
    query_id  VARCHAR(255) , \
    model  VARCHAR(255),  \
    layout_id  VARCHAR(255), \
    index_type  VARCHAR(255),  \
    duration  BIGINT,  \
    query_time  BIGINT,  \
    project_name  VARCHAR(255), \
    primary key(id , project_name) \
)'

POSTGRE_CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = 'CREATE INDEX %s_ix1 ON %s USING btree ( query_time )'
POSTGRE_CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = 'CREATE INDEX %s_ix2 ON %s USING btree ( model )'

def list_all_tables(client):
    return client.query("show measurements")

def get_query_history(client, table_name, limit, offset):
    sql = "select * from " + table_name + " limit " + str(limit) + " offset " + str(offset)
    result_set = client.query(sql)
    if len(result_set.raw['series']) < 1:
        return None
    return result_set.raw['series'][0]

def get_rdbms_params(metadata_url, metadata_password):
    first = True
    params = dict()
    for split in re.split(",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)", metadata_url):
        split_str = str(split)
        if first:
            cut = split_str.rindex('@')
            if cut > 0:
                schema = split_str[cut + 1:].strip()
                if not schema == 'jdbc':
                    print("only support RDBMS storage for query history!")
                    exit(1)
            first = False
        else:
            cut = split_str.index('=')
            if (cut < 0):
                k = split_str.strip()
                v = ""
            else:
                k = split_str[0:cut].strip()
                v = split_str[cut + 1:].strip()
            params[k] = v
    if metadata_password is not None:
        params['password'] = metadata_password
    return params


def get_rdbms_connection(params):
    if 'driverClassName' not in params:
        print('invalid metadata url, please set driverClassName')
        exit(1)

    driver_class = params['driverClassName']
    url = params['url']
    if len(url.split('//')[1].split(':')) > 1:
        host = url.split('//')[1].split(':')[0]
        port = url.split('//')[1].split(':')[1].split('/')[0]
    else:
        host = url.split('//')[1].split('/')[0]
        port = None
    database = url.split('//')[1].split('/')[1].split('?')[0]

    if driver_class == 'org.postgresql.Driver':
        port = 5432 if port is None else port
        connection = psycopg2.connect(host=host, port=port, user=params['username'], password=params['password'], database=database)
    elif driver_class == 'com.mysql.jdbc.Driver':
        port = 3306 if port is None else port
        connection = mysql.connector.connect(host=host, port=port, user=params['username'], passwd=params['password'], database=database, use_pure=True, charset='utf8')
    else:
        print(driver_class + ' is not supported yet!')
        exit(1)

    return connection

def is_table_exist(connection, tablename):
    cursor = connection.cursor()
    cursor.execute('select * from information_schema.tables where table_name=%s', (tablename,))
    cursor.fetchall()
    return bool(cursor.rowcount)

def create_query_history_table_if_not_exist(connection, driverClassName, tablename):
    if is_table_exist(connection, tablename):
        return

    cursor = connection.cursor()
    if driverClassName == 'com.mysql.jdbc.Driver':
        cursor.execute(MYSQL_INSERT_QUERY_HISTORY_SQL % tablename)
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_INDEX1 % (tablename, tablename))
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_INDEX2 % (tablename, tablename))
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_INDEX3 % (tablename, tablename))
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_INDEX4 % (tablename, tablename))
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_INDEX5 % (tablename, tablename))
    elif driverClassName == 'org.postgresql.Driver':
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_SQL % tablename)
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_INDEX1 % (tablename, tablename))
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_INDEX2 % (tablename, tablename))
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_INDEX3 % (tablename, tablename))
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_INDEX4 % (tablename, tablename))
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_INDEX5 % (tablename, tablename))
    connection.commit()

def create_query_history_realization_table_if_not_exist(connection, driverClassName, tablename):
    cursor = connection.cursor()
    if is_table_exist(connection, tablename):
        return

    if driverClassName == 'com.mysql.jdbc.Driver':
        cursor.execute(MYSQL_INSERT_QUERY_HISTORY_REALIZATION_SQL % tablename)
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_REALIZATION_INDEX1 % (tablename, tablename))
        cursor.execute(MYSQL_CREATE_QUERY_HISTORY_REALIZATION_INDEX2 % (tablename, tablename))
    elif driverClassName == 'org.postgresql.Driver':
        cursor.execute(POSTGRE_INSERT_QUERY_HISTORY_REALIZATION_SQL % tablename)
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_REALIZATION_INDEX1 % (tablename, tablename))
        cursor.execute(POSTGRE_CREATE_QUERY_HISTORY_REALIZATION_INDEX2 % (tablename, tablename))
    connection.commit()

def get_time_zone():
    time_zone = getProperties('kylin.web.timezone')
    if time_zone is None or time_zone == "":
        time_zone = get_localzone().zone
    return time_zone

def get_query_first_day_of_month(time_zone, query_time):
    dt = datetime.fromtimestamp(query_time / 1000.0, pytz.timezone(time_zone))
    first_day_of_month = dt.replace(year=dt.year, month=dt.month, day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(time.mktime(first_day_of_month.timetuple()) * 1000.0)

def get_query_first_day_of_week(time_zone, query_time):
    dt = datetime.fromtimestamp(query_time / 1000.0, pytz.timezone(time_zone))
    first_day_of_week = dt - timedelta(days=dt.weekday())
    first_day_of_week = first_day_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(time.mktime(first_day_of_week.timetuple()) * 1000.0)

def get_query_start_day(time_zone, query_time):
    dt = datetime.fromtimestamp(query_time / 1000.0, pytz.timezone(time_zone))
    start_day = dt.replace(year=dt.year, month=dt.month, day=dt.day, hour=0, minute=0, second=0, microsecond=0)
    return int(time.mktime(start_day.timetuple()) * 1000.0)

def get_row_value(row, column_name_map, column_name):
    if column_name in column_name_map:
        return row[column_name_map[column_name]]
    else:
        return None

def insert_to_rdbms_query_history(connection, result_set, table_name, project_name):
    cursor = connection.cursor()
    column_name_map = dict()
    rows = result_set['values']
    columns = result_set['columns']
    for index in range(len(columns)):
        column_name = columns[index]
        column_name_map[column_name] = index
    data = list()
    for row in rows:
        converted_row = list()
        converted_row.append(row[column_name_map[QUERY_ID]])
        converted_row.append(row[column_name_map[SQL_TEXT]])
        converted_row.append(row[column_name_map[SQL_PATTERN]])
        converted_row.append(row[column_name_map[QUERY_DURATION]])
        converted_row.append(row[column_name_map[TOTAL_SCAN_BYTES]])
        converted_row.append(row[column_name_map[TOTAL_SCAN_COUNT]])
        converted_row.append(row[column_name_map[RESULT_ROW_COUNT]])
        converted_row.append(row[column_name_map[SUBMITTER]])
        converted_row.append(get_row_value(row, column_name_map, REALIZATIONS))
        converted_row.append(row[column_name_map[QUERY_SERVER]])
        converted_row.append(get_row_value(row, column_name_map, ERROR_TYPE))
        converted_row.append(row[column_name_map[ENGINE_TYPE]])
        converted_row.append(row[column_name_map[IS_CACHE_HIT]])
        converted_row.append(row[column_name_map[QUERY_STATUS]])
        converted_row.append(True if row[column_name_map[IS_INDEX_HIT]] == 'true' else False)
        converted_row.append(row[column_name_map[QUERY_TIME]])
        converted_row.append(row[column_name_map[MONTH]])
        # compute first day of month, first day of week, start day
        time_zone = get_time_zone()
        query_time = row[column_name_map[QUERY_TIME]]
        converted_row.append(get_query_first_day_of_month(time_zone, query_time))
        converted_row.append(get_query_first_day_of_week(time_zone, query_time))
        converted_row.append(get_query_start_day(time_zone, query_time))
        converted_row.append(True if row[column_name_map[IS_TABLE_INDEX_USED]] == 'true' else False)
        converted_row.append(True if row[column_name_map[IS_AGG_INDEX_USED]] == 'true' else False)
        converted_row.append(True if row[column_name_map[IS_TABLE_SNAPSHOT_USED]] == 'true' else False)
        converted_row.append(project_name)
        data.append(converted_row)
    cursor.executemany(INSERT_HISTORY_SQL.format(table_name=table_name), data)
    connection.commit()

def insert_to_rdbms_query_realization(connection, result_set, table_name, project_name):
    cursor = connection.cursor()
    column_name_map = dict()
    rows = result_set['values']
    columns = result_set['columns']
    for index in range(len(columns)):
        column_name = columns[index]
        column_name_map[column_name] = index
    data = list()
    for row in rows:
        converted_row = list()
        converted_row.append(row[column_name_map[MODEL]])
        converted_row.append(row[column_name_map[LAYOUT_ID]])
        converted_row.append(row[column_name_map[INDEX_TYPE]])
        converted_row.append(row[column_name_map[QUERY_ID]])
        converted_row.append(row[column_name_map[QUERY_DURATION]])
        converted_row.append(convert_to_timestamp(row[column_name_map['time']]))
        converted_row.append(project_name)
        data.append(converted_row)
    cursor.executemany(INSERT_HISTORY_REALIZATION_SQL.format(table_name=table_name), data)
    connection.commit()

def convert_to_timestamp(time_in_str):
    time_dt = datetime.strptime(time_in_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    time_dt = time_dt.replace(tzinfo=pytz.timezone(get_time_zone()))
    return int(time.mktime(time_dt.timetuple()) * 1000.0)

def getProperties(propertyKey):
    kylin_home = os.environ.get('KYLIN_HOME')
    out = subprocess.Popen(['bash', kylin_home + '/bin/get-properties.sh', propertyKey, 'DEC'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    if stdout is None:
        return None
    return str(stdout).rstrip()

if __name__ == "__main__":
    parameter_dict = {}
    for user_input in argv[1:]:
        if "=" not in user_input:
            continue
        varname = user_input.split("=")[0]
        varvalue = user_input.split("=")[1]
        parameter_dict[varname] = varvalue

    kylin_home = os.environ.get('KYLIN_HOME')
    if kylin_home is None:
        print("please set KYLIN_HOME!")
        exit(1)

    address = getProperties('kap.influxdb.address')
    host = address.split(':')[0]
    port = address.split(':')[1]
    username = getProperties('kap.influxdb.username')
    password = getProperties('kap.influxdb.password')
    metadata_url = getProperties('kylin.metadata.url')
    metadata_identifier = metadata_url.split('@')[0]

    influx_client = InfluxDBClient(host=host,
                                   port=port,
                                   database="KE_HISTORY",
                                   username=username,
                                   password=password)

    if 'metadata_password' in parameter_dict:
        metadata_password = parameter_dict['metadata_password']
    else:
        metadata_password = None
    data_source_properties = get_rdbms_params(metadata_url, metadata_password)
    rdbms_connection = get_rdbms_connection(data_source_properties)
    query_history_table_name = metadata_identifier + "_" + QUERY_HISTORY_SURFIX
    query_history_realization_table_name = metadata_identifier + "_" + QUERY_HISTORY_REALIZATION_SURFIX
    create_query_history_table_if_not_exist(rdbms_connection, data_source_properties['driverClassName'],
                                            query_history_table_name)
    create_query_history_realization_table_if_not_exist(rdbms_connection, data_source_properties['driverClassName'],
                                                        query_history_realization_table_name)
    result = list_all_tables(influx_client)
    for measure in result.raw['series'][0]['values']:
        measure_name = measure[0]
        match = re.search(metadata_identifier + '_' + '(.*)' + '_' + QUERY_HISTORY_SURFIX + '(.*)', measure_name)
        if match is None:
            continue
        project_name = match.group(1)
        realization = False
        if match.group(2) is not None and 'realization' in match.group(2):
            realization = True
        limit = 10000
        offset = 0
        while True:
            query_histories = get_query_history(influx_client, measure_name, limit, offset)
            if query_histories is None:
                break
            if realization:
                insert_to_rdbms_query_realization(rdbms_connection, query_histories, query_history_realization_table_name, project_name)
            else:
                insert_to_rdbms_query_history(rdbms_connection, query_histories, query_history_table_name, project_name)
            if len(query_histories['values']) < limit:
                break
            offset += limit
    rdbms_connection.close()
