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
  'HOUR', 'DAY', 'WEEK'
]

export const validateTypes = {
  NEW_DATA_RANGE: 'newDataRange',
  PARTITION_COLUMN: 'partitionColumn',
  VOLATILE_VALUE: 'volatileConfig.value'
}

export const validate = {
  [validateTypes.NEW_DATA_RANGE] (rule, value, callback) {
    const { isMinRangeDisabled, isMaxRangeDisabled, userRange } = this.table
    const [ minUserRange, maxUserRange ] = userRange
    const [ minValue, maxValue ] = value || []

    if (!minValue && !maxValue) {
      callback(new Error(this.$t('emptyInput')))
    } else if (isMinRangeDisabled && minValue > minUserRange) {
      callback(new Error(this.$t('minValueInvaild')))
    } else if (isMaxRangeDisabled && maxValue < maxUserRange) {
      callback(new Error(this.$t('maxValueInvaild')))
    } else {
      callback()
    }
  },
  [validateTypes.PARTITION_COLUMN] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseSelect')))
    } else {
      callback()
    }
  },
  [validateTypes.VOLATILE_VALUE] (rule, value, callback) {
    if (~[undefined, null, ''].indexOf(value)) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else {
      callback()
    }
  }
}
