export const editTypes = {
  INCREMENTAL_SETTING: 'changeTableType',
  INCREMENTAL_LOADING: 'changeDataRange',
  REFRESH_RANGE: 'refreshRange',
  DATA_MERGE: 'dataMerge',
  PUSHDOWN_CONFIG: 'pushdownConfig'
}

export const fieldVisiableMaps = {
  [editTypes.INCREMENTAL_SETTING]: ['partitionColumn', 'newDataRange'],
  [editTypes.INCREMENTAL_LOADING]: ['newDataRange'],
  [editTypes.REFRESH_RANGE]: ['freshDataRange'],
  [editTypes.DATA_MERGE]: ['isMergeable', 'autoMergeConfigs', 'volatileConfig'],
  [editTypes.PUSHDOWN_CONFIG]: ['isAsyncPushDown']
}

export const titleMaps = {
  [editTypes.INCREMENTAL_SETTING]: 'incrementalLoading',
  [editTypes.INCREMENTAL_LOADING]: 'incrementalLoading',
  [editTypes.REFRESH_RANGE]: 'refreshRange',
  [editTypes.DATA_MERGE]: 'dataMerge',
  [editTypes.PUSHDOWN_CONFIG]: 'pushdownRange'
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
  'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'
]
