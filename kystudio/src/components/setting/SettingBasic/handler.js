export const projectTypeIcons = {
  MANUAL_MAINTAIN: 'el-icon-ksd-model_designer',
  AUTO_MAINTAIN: 'el-icon-ksd-sql_acceleration'
}
export const autoMergeTypes = [
  'DAY',
  'WEEK',
  'MONTH',
  'YEAR'
]
export const volatileTypes = [
  'DAY',
  'WEEK',
  'MONTH',
  'YEAR'
]
export const retentionTypes = [
  'DAY',
  'WEEK',
  'MONTH',
  'YEAR'
]
export const initialFormValue = {
  push_down_enabled: true,
  push_down_range_limited: true,
  auto_merge_enabled: true,
  auto_merge_time_ranges: [ 'WEEK', 'MONTH' ],
  storage_garbage: true,
  storage_quota_size: 0,
  volatile_range: {
    volatile_range_number: 0,
    volatile_range_enabled: false,
    volatile_range_type: 'DAY'
  },
  retention_range: {
    retention_range_number: 0,
    retention_range_enabled: false,
    retention_range_type: 'DAY'
  },
  alias: '',
  project: '',
  description: '',
  maintain_model_type: ''
}
export function _getProjectGeneralInfo (data) {
  return {
    project: data.project,
    alias: data.alias || data.project,
    description: data.description,
    maintain_model_type: data.maintain_model_type
  }
}
export function _getSegmentSettings (data, project) {
  return {
    project: data.project,
    auto_merge_time_ranges: data.auto_merge_time_ranges,
    auto_merge_enabled: project ? project.auto_merge_enabled : data.auto_merge_enabled,
    volatile_range: {
      ...data.volatile_range,
      volatile_range_enabled: project ? project.auto_merge_enabled : data.auto_merge_enabled
    },
    retention_range: {
      ...data.retention_range,
      retention_range_enabled: project ? project.retention_range.retention_range_enabled : data.retention_range.retention_range_enabled
    }
  }
}
export function _getPushdownConfig (data) {
  return {
    project: data.project,
    push_down_enabled: data.push_down_enabled,
    push_down_range_limited: data.push_down_range_limited
  }
}
export function _getStorageQuota (data) {
  return {
    project: data.project,
    storage_garbage: data.storage_garbage,
    storage_quota_size: data.storage_quota_size
  }
}
