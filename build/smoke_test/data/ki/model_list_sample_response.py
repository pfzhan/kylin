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
model_list_data = {
    'code': '000',
    'data': {
        'value': [{
            'uuid': 'b80d3c05-2817-45f0-b4fc-56909daa8fcd',
            'last_modified': 1561636509510,
            'create_time': 1561638398827,
            'version': '3.0.0.0',
            'mvcc': 0,
            'alias': 'sample_model_hive',
            'owner': 'ADMIN',
            'config_last_modifier': None,
            'config_last_modified': 0,
            'description': '',
            'fact_table': 'SSB.P_LINEORDER',
            'fact_table_alias': None,
            'management_type': 'MODEL_BASED',
            'join_tables': [],
            'filter_condition': '',
            'partition_desc': {
                'partition_date_column': 'P_LINEORDER.LO_ORDERDATE',
                'partition_date_start': 0,
                'partition_date_format': 'yyyy-MM-dd',
                'partition_type': 'APPEND',
                'partition_condition_builder': 'org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder'
            },
            'capacity': 'MEDIUM',
            'segment_config': {
                'auto_merge_enabled': None,
                'auto_merge_time_ranges': None,
                'volatile_range': None,
                'retention_range': None
            },
            'data_check_desc': None,
            'semantic_version': 0,
            'all_named_columns': [{
                'id': 0,
                'name': 'LO_SHIPMODE',
                'column': 'P_LINEORDER.LO_SHIPMODE'
            }, {
                'id': 1,
                'name': 'LO_LINENUMBER',
                'column': 'P_LINEORDER.LO_LINENUMBER'
            }, {
                'id': 2,
                'name': 'LO_ORDTOTALPRICE',
                'column': 'P_LINEORDER.LO_ORDTOTALPRICE'
            }, {
                'id': 3,
                'name': 'LO_SUPPLYCOST',
                'column': 'P_LINEORDER.LO_SUPPLYCOST'
            }, {
                'id': 4,
                'name': 'LO_SUPPKEY',
                'column': 'P_LINEORDER.LO_SUPPKEY'
            }, {
                'id': 5,
                'name': 'LO_QUANTITY',
                'column': 'P_LINEORDER.LO_QUANTITY'
            }, {
                'id': 6,
                'name': 'P_LINEORDER_LO_PARTKEY',
                'column': 'P_LINEORDER.LO_PARTKEY',
                'status': 'DIMENSION'
            }, {
                'id': 7,
                'name': 'P_LINEORDER_LO_ORDERKEY',
                'column': 'P_LINEORDER.LO_ORDERKEY',
                'status': 'DIMENSION'
            }, {
                'id': 8,
                'name': 'LO_CUSTKEY',
                'column': 'P_LINEORDER.LO_CUSTKEY'
            }, {
                'id': 9,
                'name': 'LO_SHIPPRIOTITY',
                'column': 'P_LINEORDER.LO_SHIPPRIOTITY'
            }, {
                'id': 10,
                'name': 'LO_DISCOUNT',
                'column': 'P_LINEORDER.LO_DISCOUNT'
            }, {
                'id': 11,
                'name': 'LO_ORDERPRIOTITY',
                'column': 'P_LINEORDER.LO_ORDERPRIOTITY'
            }, {
                'id': 12,
                'name': 'LO_ORDERDATE',
                'column': 'P_LINEORDER.LO_ORDERDATE'
            }, {
                'id': 13,
                'name': 'LO_REVENUE',
                'column': 'P_LINEORDER.LO_REVENUE'
            }, {
                'id': 14,
                'name': 'V_REVENUE',
                'column': 'P_LINEORDER.V_REVENUE'
            }, {
                'id': 15,
                'name': 'LO_COMMITDATE',
                'column': 'P_LINEORDER.LO_COMMITDATE'
            }, {
                'id': 16,
                'name': 'LO_EXTENDEDPRICE',
                'column': 'P_LINEORDER.LO_EXTENDEDPRICE'
            }, {
                'id': 17,
                'name': 'LO_TAX',
                'column': 'P_LINEORDER.LO_TAX'
            }],
            'all_measures': [{
                'name': 'SUM_REVENUE',
                'function': {
                    'expression': 'SUM',
                    'parameters': [{
                        'type': 'column',
                        'value': 'P_LINEORDER.V_REVENUE'
                    }],
                    'returntype': 'bigint'
                },
                'id': 100000
            }, {
                'name': 'COUNT_ALL',
                'function': {
                    'expression': 'COUNT',
                    'parameters': [{
                        'type': 'constant',
                        'value': '1'
                    }],
                    'returntype': 'bigint'
                },
                'id': 100001
            }],
            'column_correlations': [],
            'multilevel_partition_cols': [],
            'computed_columns': [],
            'canvas': {
                'coordinate': {
                    'P_LINEORDER': {
                        'x': 821.1222222222221,
                        'y': 84.54444444444447,
                        'width': 220.0,
                        'height': 195.0
                    }
                },
                'zoom': 9.0
            },
            'status': 'ONLINE',
            'last_build_end': '728611200000',
            'storage': 2759,
            'usage': 3,
            'model_broken': False,
            'simplified_tables': [{
                'table': 'SSB.P_LINEORDER',
                'columns': [{
                    'name': 'LO_ORDERKEY',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_LINENUMBER',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_CUSTKEY',
                    'datatype': 'integer',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_PARTKEY',
                    'datatype': 'integer',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_SUPPKEY',
                    'datatype': 'integer',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_ORDERDATE',
                    'datatype': 'date',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_ORDERPRIOTITY',
                    'datatype': 'varchar(4096)',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_SHIPPRIOTITY',
                    'datatype': 'integer',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_QUANTITY',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_EXTENDEDPRICE',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_ORDTOTALPRICE',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_DISCOUNT',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_REVENUE',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_SUPPLYCOST',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_TAX',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_COMMITDATE',
                    'datatype': 'date',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'LO_SHIPMODE',
                    'datatype': 'varchar(4096)',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }, {
                    'name': 'V_REVENUE',
                    'datatype': 'bigint',
                    'cardinality': 0,
                    'comment': None,
                    'is_computed_column': False
                }]
            }],
            'simplified_dimensions': [{
                'id': 6,
                'name': 'P_LINEORDER_LO_PARTKEY',
                'column': 'P_LINEORDER.LO_PARTKEY',
                'status': 'DIMENSION'
            }, {
                'id': 7,
                'name': 'P_LINEORDER_LO_ORDERKEY',
                'column': 'P_LINEORDER.LO_ORDERKEY',
                'status': 'DIMENSION'
            }],
            'simplified_measures': [{
                'id': 100000,
                'expression': 'SUM',
                'name': 'SUM_REVENUE',
                'return_type': 'bigint',
                'parameter_value': [{
                    'type': 'column',
                    'value': 'P_LINEORDER.V_REVENUE'
                }],
                'converted_columns': []
            }, {
                'id': 100001,
                'expression': 'COUNT',
                'name': 'COUNT_ALL',
                'return_type': 'bigint',
                'parameter_value': [{
                    'type': 'constant',
                    'value': '1'
                }],
                'converted_columns': []
            }]
        }],
        'total_size': 1
    },
    'msg': ''
}