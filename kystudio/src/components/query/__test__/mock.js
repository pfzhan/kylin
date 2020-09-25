export const realizations = [{
  indexType: "Table Index",
  layoutId: 20000010001,
  modelAlias: "model_text",
  modelId: "adf65a2d-0b10-48bd-9e9b-9792b2c55eef",
  partialMatchModel: false,
  unauthorized_columns: [],
  unauthorized_tables: [],
  valid: true,
  visible: true
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

