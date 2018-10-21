export const editTypes = {
  INCREMENTAL_SETTING: 'changeTableType',
  INCREMENTAL_LOADING: 'changeDataRange',
  REFRESH_RANGE: 'refreshRange',
  DATA_MERGE: 'dataMerge'
}

export const fieldVisiableMaps = {
  [editTypes.INCREMENTAL_SETTING]: ['partitionColumn', 'newDataRange'],
  [editTypes.INCREMENTAL_LOADING]: ['newDataRange'],
  [editTypes.REFRESH_RANGE]: ['freshDataRange'],
  [editTypes.DATA_MERGE]: ['isMergeable', 'autoMergeConfigs', 'volatileConfig']
}

export const titleMaps = {
  [editTypes.INCREMENTAL_SETTING]: 'incrementalLoading',
  [editTypes.INCREMENTAL_LOADING]: 'incrementalLoading',
  [editTypes.REFRESH_RANGE]: 'refreshRange',
  [editTypes.DATA_MERGE]: 'dataMerge'
}

export const autoMergeTypes = [
  'HALF_MINUTE',
  'FIVE_MINUTE',
  'TEN_MINUTE',
  'HALF_HOUR',
  'HOUR',
  'SIX_HOUR',
  'HALF_DAY',
  'DAY',
  'WEEK',
  'MONTH',
  'QUARTER',
  'YEAR'
]

export const volatileTypes = [
  'minute', 'hour', 'day', 'month', 'year'
]
