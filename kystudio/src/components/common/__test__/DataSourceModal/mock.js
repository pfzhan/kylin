export const states = {
  callback: () => {},
  databaseSizeObj: {DB1: 3, SSB: 5, DB3: 1},
  datasource: {
    name: 'hive',
    type: 9,
    port: '',
    host: ''
  },
  editType: 9,
  firstEditType: 9,
  form: {
    csvSettings: {},
    databaseSizeObj: null,
    needSampling: true,
    project: {
      create_time: 1583551693588,
      create_time_utc: 1583551693588,
      default_database: 'SSB',
      description: '',
      keytab: null,
      last_modified: 1598658034308,
      mvcc: 85438,
      name: 'xm_test',
      override_kylin_properties: {
        'kap.metadata.semi-automatic-mode': 'true',
        'kap.query.metadata.expose-computed-column': 'true',
        'kylin.cube.frequency-time-window': '1',
        'kylin.cube.low-frequency-threshold': '5',
        'kylin.job.notification-admin-emails': '32412434@qq.com',
        'kylin.job.notification-on-empty-data-load': 'true',
        'kylin.job.notification-on-job-error': 'false',
        'kylin.metadata.semi-automatic-mode': 'true',
        'kylin.query.metadata.expose-computed-column': 'true',
        'kylin.source.default': 9
      },
      owner: 'ADMIN',
      principal: null,
      segment_config: {
        auto_merge_enabled: true,
        auto_merge_time_ranges: ['WEEK', 'MONTH', 'QUARTER', 'YEAR'],
        create_empty_segment_enabled: false,
        retention_range: {
          retention_range_enabled: false,
          retention_range_number: 1,
          retention_range_type: 'MONTH'
        },
        volatile_range: {
          volatile_range_enabled: false,
          volatile_range_number: 0,
          volatile_range_type: 'DAY'
        }
      },
      status: 'ENABLED',
      uuid: '1738a6f9-e836-4b43-bd9d-faebc0f4b465',
      version: '4.0.0.0'
    },
    samplingRows: 20000000,
    selectedDatabases: [],
    selectedTables: [],
    settings: {
      creator: '',
      description: '',
      host: '',
      isAuthentication: false,
      name: '',
      password: '',
      port: '',
      type: '',
      username: ''
    }
  },
  isShow: true,
  project: {
    create_time: 1583551693588,
    create_time_utc: 1583551693588,
    default_database: 'SSB',
    description: '',
    keytab: null,
    last_modified: 1598658034308,
    mvcc: 85438,
    name: 'xm_test',
    override_kylin_properties: {
      'kap.metadata.semi-automatic-mode': 'true',
      'kap.query.metadata.expose-computed-column': 'true',
      'kylin.cube.frequency-time-window': '1',
      'kylin.cube.low-frequency-threshold': '5',
      'kylin.job.notification-admin-emails': '32412434@qq.com',
      'kylin.job.notification-on-empty-data-load': 'true',
      'kylin.job.notification-on-job-error': 'false',
      'kylin.metadata.semi-automatic-mode': 'true',
      'kylin.query.metadata.expose-computed-column': 'true',
      'kylin.source.default': 8,
      'kylin.source.jdbc.dialect': 'gbase8a',
      'kylin.source.jdbc.connection-url': 'url',
      'kylin.source.jdbc.pass': 'admin',
      'kylin.source.jdbc.user': 'admin'
    },
    owner: 'ADMIN',
    principal: null,
    segment_config: {
      auto_merge_enabled: true,
      auto_merge_time_ranges: ['WEEK', 'MONTH', 'QUARTER', 'YEAR'],
      create_empty_segment_enabled: false,
      retention_range: {
        retention_range_enabled: false,
        retention_range_number: 1,
        retention_range_type: 'MONTH'
      },
      volatile_range: {
        volatile_range_enabled: false,
        volatile_range_number: 0,
        volatile_range_type: 'DAY'
      }
    },
    status: 'ENABLED',
    uuid: '1738a6f9-e836-4b43-bd9d-faebc0f4b465',
    version: '4.0.0.0'
  }
}

export const kafkaMeta = {
  batchTableColumns: [],
  isShowHiveTree: false,
  kafka_config: {
    batch_table_identity: '',
    database: 'ABC',
    has_shadow_table: false,
    kafka_bootstrap_servers: '10.1.2.212:19092',
    name: 'AAA',
    parser_name: 'io.kyligence.kap.parser.TimedJsonStreamParser',
    project: 'project',
    source_type: 1,
    starting_offsets: 'latest',
    subscribe: 'ssb_topic'
  },
  project: 'project',
  table_desc: {
    columns: [],
    database: 'ABC',
    name: 'AAA',
    source_type: 1
  }
}
