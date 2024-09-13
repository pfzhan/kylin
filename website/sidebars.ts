/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

const sidebars = {
    // By default, Docusaurus generates a sidebar from the docs folder structure
    DocumentSideBar: [
        {
            type: 'doc',
            id: 'overview'
        },
        {
            type: 'category',
            label: 'Quick Start',
            link: {
                type: 'doc',
                id: 'quickstart/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'quickstart/tutorial',
                }
            ],
        },
        {
            type: 'category',
            label: 'Deployment',
            link: {
                type: 'doc',
                id: 'deployment/intro',
            },
            items: [
                {
                    type: 'category',
                    label: 'On Premises',
                    link: {
                        type: 'doc',
                        id: 'deployment/on-premises/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'deployment/on-premises/prerequisite',
                        },
                        {
                            type: 'category',
                            label: 'Metastore',
                            link: {
                                type: 'doc',
                                id: 'deployment/on-premises/rdbms_metastore/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/rdbms_metastore/use_mysql_as_metadb'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/rdbms_metastore/usepg_as_metadb'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/rdbms_metastore/query_history_fields'
                                },
                            ]
                        },
                        {
                            type: 'category',
                            label: 'Deploy Mode',
                            link: {
                                type: 'doc',
                                id: 'deployment/on-premises/deploy_mode/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/cluster_deployment'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/service_discovery'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/rw_separation'
                                },
                            ]
                        },
                        {
                            type: 'category',
                            label: 'Install and Uninstall',
                            link: {
                                type: 'doc',
                                id: 'deployment/on-premises/installation/intro',
                            },
                            items: [
                                {
                                    type: 'category',
                                    label: 'Install On Platforms',
                                    link: {
                                        type: 'doc',
                                        id: 'deployment/on-premises/installation/platform/intro',
                                    },
                                    items: [
                                        {
                                            type: 'doc',
                                            id: 'deployment/on-premises/installation/platform/install_on_apache_hadoop',
                                        },
                                    ],
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/installation/uninstallation',
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/installation/install_validation',
                                },
                            ],
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Configuration',
                    link: {
                        type: 'doc',
                        id: 'configuration/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'configuration/config'
                        },

                        {
                            type: 'doc',
                            id: 'configuration/hadoop_queue_config'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/https'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/log_rotate'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/query_cache'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/spark_dynamic_allocation'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/spark_rpc_encryption'
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Datasource',
            link: {
                type: 'doc',
                id: 'datasource/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'datasource/import_hive'
                },
                {
                    type: 'doc',
                    id: 'datasource/data_sampling'
                },
                {
                    type: 'doc',
                    id: 'datasource/logical_view'
                },
            ],
        },
        {
            type: 'category',
            label: 'Internal Table',
            link: {
                type: 'doc',
                id: 'internaltable/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'internaltable/internal_table_management'
                },
                {
                    type: 'doc',
                    id: 'internaltable/load_data_into_internal_table'
                },
                {
                    type: 'doc',
                    id: 'internaltable/query_internal_table'
                }
            ],
        },
        {
            type: 'category',
            label: 'Model',
            link: {
                type: 'doc',
                id: 'modeling/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'modeling/manual_modeling'
                },
                {
                    type: 'category',
                    label: 'Auto Modeling',
                    link: {
                        type: 'doc',
                        id: 'modeling/auto_modeling/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/data_modeling_by_SQL'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/basic_concept_actions'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/rule_setting'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/scalar_subquery'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Auto Index Optimization',
                    link: {
                        type: 'doc',
                        id: 'modeling/auto_modeling/optimize_index/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/optimize_index/index_optimization'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/auto_modeling/optimize_index/review_index'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'modeling/model_management'
                },
                {
                    type: 'doc',
                    id: 'modeling/snapshot_management'
                },
                {
                    type: 'category',
                    label: 'Advanced Mode Design',
                    link: {
                        type: 'doc',
                        id: 'modeling/model_design/intro'
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'Measures',
                            link: {
                                type: 'doc',
                                id: 'modeling/model_design/measure_design/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/topn'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/count_distinct_bitmap'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/count_distinct_hllc'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/percentile_approx'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/corr'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/collect_set'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/computed_column'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/slowly_changing_dimension'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/aggregation_group'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/table_index'
                        },
                        {
                            type: 'category',
                            label: 'Model Advanced Settings',
                            link: {
                                type: 'doc',
                                id: 'modeling/model_design/advance_guide/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/model_metadata_managment'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/multilevel_partitioning'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/fast_bitmap'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/integer_encoding'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/precompute_join_relations'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Data Loading',
                    link: {
                        type: 'doc',
                        id: 'modeling/load_data/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'modeling/load_data/full_build'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/load_data/by_date'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/load_data/build_index'
                        },
                        {
                            type: 'category',
                            label: 'Segment Operation and Settings',
                            link: {
                                type: 'doc',
                                id: 'modeling/load_data/segment_operation_settings/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/load_data/segment_operation_settings/segment_merge'
                                },
                            ],
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Streaming',
                    link: {
                        type: 'doc',
                        id: 'modeling/real-time/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'modeling/real-time/prerequisite'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/real-time/real-time'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/real-time/custom-parser'
                        },
                    ],
                },
            ],
        },

        {
            type: 'category',
            label: 'Query',
            link: {
                type: 'doc',
                id: 'query/intro'
            },
            items: [
                {
                    type: 'category',
                    label: 'Specification',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/specification/sql_spec'
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/data_type'
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/operators'
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/functions'

                        },
                        {
                            type: 'category',
                            label: 'Advanced Functions',
                            items: [
                                {
                                    type: 'doc',
                                    id: 'query/specification/window_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/grouping_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/bitmap_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/intersect_function',
                                },
                            ]
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/model_priority'
                        },
                    ]
                },
                {
                    type: 'category',
                    label: 'Web UI',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/web_ui/insight'
                        },
                        {
                            type: 'doc',
                            id: 'query/web_ui/query_history'
                        },
                    ]
                },
                {
                    type: 'category',
                    label: 'Execution Principles',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/principles/precalculation'
                        },
                        {
                            type: 'doc',
                            id: 'query/principles/transformations'
                        },
                        {
                            type: 'doc',
                            id: 'query/principles/by_model'
                        },
                        {
                            type: 'doc',
                            id: 'query/principles/by_internal_table'
                        },
                        {
                            type: 'doc',
                            id: 'query/principles/push_down'
                        },
                    ]
                },
                {
                    type: 'category',
                    label: 'Query Optimization',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/optimization/data_skipping'
                        },
                        {
                            type: 'doc',
                            id: 'query/optimization/partial_match_join'
                        },
                        {
                            type: 'doc',
                            id: 'query/optimization/sum_expression'
                        },
                        {
                            type: 'doc',
                            id: 'query/optimization/count_distinct_expr'
                        },
                        {
                            type: 'doc',
                            id: 'query/optimization/query_enhanced'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'query/async_query'
                },
            ],
        },
        {
            type: 'category',
            label: 'Maintenance Guide',
            link: {
                type: 'doc',
                id: 'operations/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'operations/overview'
                },
                {
                    type: 'category',
                    label: 'Project Managing',
                    link: {
                        type: 'doc',
                        id: 'operations/project-managing/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/project-managing/project_management'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/project_settings'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/toolbar'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/alerting'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Access Control',
                    link: {
                        type: 'doc',
                        id: 'operations/access-control/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/access-control/user_management'
                        },
                        {
                            type: 'doc',
                            id: 'operations/access-control/group_management'
                        },
                        {
                            type: 'category',
                            label: 'Data Access Control',
                            link: {
                                type: 'doc',
                                id: 'operations/access-control/data-access-control/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/access-control/data-access-control/project_acl'
                                },
                            ],
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'System Operation',
                    link: {
                        type: 'doc',
                        id: 'operations/system-operation/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'Diagnosis',
                            link: {
                                type: 'doc',
                                id: 'operations/system-operation/diagnosis/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/diagnosis',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/query_flame_graph',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/build_flame_graph',
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/update-session-table',
                        },
                        {
                            type: 'category',
                            label: 'CLI Operation Tool',
                            link: {
                                type: 'doc',
                                id: 'operations/system-operation/cli_tool/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/environment_dependency_check',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/diagnosis'
                                },
                                {
                                    type: 'category',
                                    label: 'Metadata Tool',
                                    link: {
                                        type: 'doc',
                                        id: 'operations/system-operation/cli_tool/metadata_tool/intro',
                                    },
                                    items: [
                                        {
                                            type: 'doc',
                                            id: 'operations/system-operation/cli_tool/metadata_tool/metadata_backup_restore'
                                        },
                                    ],
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/rollback'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/maintenance_mode'
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/guardian',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/junk_file_clean',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/limit_query',
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Job Monitoring',
                    link: {
                        type: 'doc',
                        id: 'operations/job-monitoring/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_concept_settings'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_operations'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_diagnosis'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_exception_resolve'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'System Monitoring',
                    link: {
                        type: 'doc',
                        id: 'operations/system-monitoring/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'InfluxDB',
                            link: {
                                type: 'doc',
                                id: 'operations/system-monitoring/influxdb/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-monitoring/influxdb/influxdb'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-monitoring/influxdb/influxdb_maintenance'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-monitoring/metrics_intro',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-monitoring/service'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Logs',
                    link: {
                        type: 'doc',
                        id: 'operations/logs/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/logs/system_log'
                        },
                        {
                            type: 'doc',
                            id: 'operations/logs/audit_log'
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Integration',
            link: {
                type: 'doc',
                id: 'integration/intro',
            },
            items: [
                {
                    type: 'category',
                    label: 'Drivers',
                    link: {
                        type: 'doc',
                        id: 'integration/driver/intro'
                    },
                    items: [
                        {
                          type: 'doc',
                          id: 'integration/driver/jdbc',
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Rest API',
            link: {
                type: 'doc',
                id: 'restapi/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'restapi/authentication'
                },
                {
                    type: 'doc',
                    id: 'restapi/project_api'
                },
                {
                    type: 'category',
                    label: 'Model Management',
                    link: {
                        type: 'doc',
                        id: 'restapi/model_api/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_build_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_import_and_export_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_multilevel_partitioning_api'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'restapi/segment_management_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/snapshot_management_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/query_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/data_source_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/custom_jar_manager_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/async_query_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/job_api'
                },
                {
                    type: 'category',
                    label: 'ACL Management API',
                    link: {
                        type: 'doc',
                        id: 'restapi/acl_api/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/user_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/user_group_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/project_acl_api'
                        },
                        // {
                        //     type: 'doc',
                        //     id: 'restapi/acl_api/acl_api'
                        // },
                    ],
                },
                {
                    type: 'doc',
                    id: 'restapi/callback_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/error_code'
                },
            ],
        },
    ],
    DevelopmentSideBar: [
        {
            type: 'category',
            label: 'Development Guide',
            link: {
                type: 'doc',
                id: 'development/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'development/coding_convention'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_contribute'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_write_doc'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_debug_kylin_in_local'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_debug_kylin_in_ide'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_test'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_package'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_subscribe_mailing_list'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_understand_kylin_design'
                }

            ],
        },
    ],
    CommunitySideBar: [
        {
            type: 'doc',
            id: 'community',
            label: 'Community',
        },
    ],
    DownloadSideBar: [
        {
            type: 'doc',
            id: 'download',
            label: 'Download',
        },
    ],
};

export default sidebars;
