export const viewTypes = {
  DATA_LOAD: 'data-load',
  COLUMNS: 'columns',
  SAMPLE: 'sample',
  STATISTICS: 'statistics',
  EXTEND_INFORMATION: 'extend-information',
  KAFKA: 'kafka',
  ACCESS: 'access'
}

export function getSelectedTableDetail (tableDetail) {
  const columns = tableDetail.columns.map(column => {
    // for newten
    // const columnDetail = tableDetail.columns_stats.find(columnStats => columnStats.column_name === column.name)

    // return {
    //   ...column,
    //   ...columnDetail
    // }
    return column
  })

  return {
    ...tableDetail,
    mapper_rows: tableDetail.mapper_rows || [],
    sample_rows: tableDetail.sample_rows || [],
    total_rows: tableDetail.total_rows || [],
    userRange: getUserRange(tableDetail),
    allRange: getAllSegmentsRange(tableDetail),
    columns
  }
}

/**
 * 获取用户DataRange
 * @desc table有两个range，一个是query range，一个是ready range
 *       query range是用户可查询数据范围
 *       ready range是segment数据ready范围
 *       query range小于或等于ready range
 * @param {*} table
 */
function getUserRange (table) {
  const [ minQueryableRange, maxQueryableRange ] = getQueryableRange(table)
  const [ minReadyableRange, maxReadyableRange ] = getReadySegmentsRange(table)

  let minUserRange = 0
  let maxUserRange = 0

  minQueryableRange && minQueryableRange > minReadyableRange
    ? (minUserRange = minQueryableRange)
    : (minUserRange = minReadyableRange)

  maxQueryableRange && maxQueryableRange < maxReadyableRange
    ? (maxUserRange = maxQueryableRange)
    : (maxUserRange = maxReadyableRange)

  return [ minUserRange, maxUserRange ]
}

function getQueryableRange (table) {
  return ~table.start && ~table.end ? [table.start, table.end] : []
}

function getReadySegmentsRange (table) {
  return ~table.ready_start && ~table.ready_end ? [ table.ready_start, table.ready_end ] : []
}

/**
 * 获取所有Segment数据范围
 * @param {*} table
 */
function getAllSegmentsRange (table) {
  const segmentKeyValuePairs = Object.entries(table.segment_ranges)
  let maxTime = -Infinity
  let minTime = Infinity
  for (const [key] of segmentKeyValuePairs) {
    const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
    startTime < minTime ? (minTime = +startTime) : null
    endTime > minTime ? (maxTime = +endTime) : null
  }
  return [ minTime, maxTime ]
}
