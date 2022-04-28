export const project = {
  auto_merge_enabled: true,
  auto_merge_time_ranges: ['WEEK', 'MONTH'],
  converter_class_names: 'org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter,org.apache.kylin.query.util.PowerBIConverter,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.security.RowFilter,io.kyligence.kap.query.security.HackSelectStarWithColumnACL,io.kyligence.kap.query.util.SparkSQLFunctionConverter',
  data_load_empty_notification_enabled: false,
  default_database: 'SSB',
  expose_computed_column: true,
  favorite_rules: {
    count_enable: true,
    count_value: 10,
    duration_enable: false,
    freq_enable: false,
    freq_value: 0.1,
    max_duration: 180,
    min_duration: 0,
    recommendation_enable: true,
    recommendations_value: 20,
    submitter_enable: true,
    user_groups: ['ROLE_ADMIN'],
    users: ['ADMIN']
  },
  frequency_time_window: 'MONTH',
  job_error_notification_enabled: false,
  job_notification_emails: [],
  kerberos_project_level_enabled: false,
  low_frequency_threshold: 0,
  principal: null,
  project: 'xm_test_1',
  push_down_enabled: true,
  retention_range: {
    retention_range_enabled: false,
    retention_range_number: 1,
    retention_range_type: 'MONTH'
  },
  runner_class_name: 'io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl',
  scd2_enabled: false,
  semi_automatic_mode: true,
  storage_quota_size: 14293651161088,
  storage_quota_tb_size: '13.00',
  threshold: 20,
  tips_enabled: true,
  volatile_range: {
    volatile_range_enabled: false,
    volatile_range_number: 0,
    volatile_range_type: 'DAY'
  },
  yarn_queue: 'default'
}

export const favoriteRules = {
  count_enable: true,
  count_value: 0,
  duration_enable: false,
  effective_days: 2,
  excluded_tables: [],
  excluded_tables_enable: false,
  max_duration: 0,
  min_duration: 0,
  min_hit_count: 30,
  recommendation_enable: true,
  recommendations_value: 20,
  submitter_enable: true,
  update_frequency: 2,
  user_groups: [],
  users: []
}

export const groupAndUser = {
  group: ['test', 'groupA'],
  user: ['ADMIN', 'ANALYST', 'fanfan', 'fengys', 'gaoyuan']
}
