export const realizations = [{
  indexType: "Table Index",
  layoutId: 20000010001,
  modelAlias: "model_text",
  modelId: "adf65a2d-0b10-48bd-9e9b-9792b2c55eef",
  partialMatchModel: false,
  unauthorized_columns: [],
  unauthorized_tables: [],
  valid: true,
  visible: true,
  snapshots: ['snapshot1', 'snapshot2']
}]

export const currentRow = {
  row: {
    cache_hit: false,
    count: 0,
    duration: 1074,
    editorH: 250,
    engine_type: "HIVE",
    error_type: "No realization found",
    exception: false,
    flexHeight: 252,
    hightlight_realizations: false,
    id: 230,
    index_hit: false,
    insertTime: 0,
    project_name: "xm_test",
    queryHistoryInfo: {
      error_msg: "There is no compatible model to accelerate this sql.",
      exactly_match: false,
      execution_error: false,
      scan_segment_num: 0,
      state: "SUCCESS"
    },
    queryRealizations: null,
    query_id: "b3b89153-141c-4ae8-8b9a-924fb889e1e2",
    query_status: "SUCCEEDED",
    query_time: 1600410798490,
    realizations: realizations,
    result_row_count: 500,
    server: ["sandbox.hortonworks.com:7072"],
    sql_limit: "select * from SSB.DATES_VIEW↵LIMIT 500",
    sql_pattern: "SELECT *↵FROM SSB.DATES_VIEW↵LIMIT 1",
    sql_text: "select * from SSB.DATES_VIEW↵LIMIT 500",
    submitter: "ADMIN",
    total_scan_bytes: 0,
    total_scan_count: 500
  }
}

export const extraoptions = {
  affectedRowCount: 0,
  appMasterURL: "/kylin/sparder/SQL/execution/?id=9317",
  columnMetas: [{
    autoIncrement: false,
    caseSensitive: false,
    catelogName: null,
    columnType: 91,
    columnTypeName: "DATE",
    currency: false,
    definitelyWritable: false,
    displaySize: 2147483647,
    isNullable: 1,
    label: "d_datekey",
    name: "d_datekey",
    precision: 0,
    readOnly: false,
    scale: 0,
    schemaName: null,
    searchable: false,
    signed: true,
    tableName: null,
    writable: false
  }, {
    autoIncrement: false,
    caseSensitive: false,
    catelogName: null,
    columnType: 5,
    columnTypeName: "number",
    currency: false,
    definitelyWritable: false,
    displaySize: 2147483647,
    isNullable: 1,
    label: "d_cuskey",
    name: "d_cuskey",
    precision: 0,
    readOnly: false,
    scale: 0,
    schemaName: null,
    searchable: false,
    signed: true,
    tableName: null,
    writable: false
  }],
  duration: 662,
  engineType: "HIVE",
  exception: false,
  exceptionMessage: null,
  hitExceptionCache: false,
  isException: false,
  is_prepare: false,
  is_stop_by_user: false,
  is_timeout: false,
  partial: false,
  prepare: false,
  pushDown: true,
  queryId: "92ad159f-caa1-4483-a573-03c206cd5917",
  queryStatistics: null,
  realizations: [],
  resultRowCount: 500,
  results: [["1992-01-01"]],
  scanBytes: [0],
  scanRows: [1000],
  server: "sandbox.hortonworks.com:7072",
  shufflePartitions: 1,
  signature: null,
  stopByUser: false,
  storageCacheUsed: false,
  suite: null,
  timeout: false,
  totalScanBytes: 0,
  totalScanRows: 1000,
  traceUrl: null,
  traces: [
    {
      "name": "GET_ACL_INFO",
      "group": "PREPARATION",
      "duration": 18
    },
    {
      "name": "SQL_TRANSFORMATION",
      "group": "PREPARATION",
      "duration": 24
    },
    {
      "name": "SQL_PARSE_AND_OPTIMIZE",
      "group": "PREPARATION",
      "duration": 154
    },
    {
      "name": "MODEL_MATCHING",
      "group": "PREPARATION",
      "duration": 12
    },
    {
      "name": "SQL_PUSHDOWN_TRANSFORMATION",
      "group": null,
      "duration": 41
    },
    {
      "name": "PREPARE_AND_SUBMIT_JOB",
      "group": null,
      "duration": 25
    }
  ]
}

export const queryExportData = {
  acceptPartial: true,
  backdoorToggles: {
    DEBUG_TOGGLE_HTRACE_ENABLED: false
  },
  limit: 500,
  offset: 0,
  project: "xm_test",
  sql: "select * from SSB.DATES",
  stopId: "query_1ej9rss4q"
}

