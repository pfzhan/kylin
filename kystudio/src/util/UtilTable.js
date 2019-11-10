import { isDatePartitionType } from './'

export function getFormattedTable (originData = {}) {
  const partitionColumn = originData.partitioned_column && originData.partitioned_column.split('.')[1] || ''
  const dateTypeColumns = originData.columns.filter(column => isDatePartitionType(column.datatype))
  const storageType = getStorageType(originData)
  const [ startTime, endTime ] = _getSegmentRange(originData)
  return {
    uuid: originData.uuid,
    name: originData.name,
    database: originData.database,
    fullName: `${originData.database}.${originData.name}`,
    updateAt: originData.last_modified,
    datasource: originData.source_type,
    cardinality: originData.cardinality,
    partitionColumn,
    format: originData.partitioned_column_format || 'yyyy-MM-dd',
    storageType,
    storageSize: ~originData.storage_size ? originData.storage_size : null,
    totalRecords: ~originData.total_records ? originData.total_records : null,
    columns: originData.columns,
    dateTypeColumns,
    startTime,
    endTime,
    create_time: originData.create_time,
    sampling_rows: originData.sampling_rows,
    __data: originData
  }
}

function getStorageType (originData = {}) {
  const isLookup = originData.lookup
  const isFact = originData.root_fact
  const hasPartition = originData.partitioned_column

  if (hasPartition) {
    return 'incremental'
  } else if (isFact) {
    return 'full'
  } else if (isLookup) {
    return 'snapshot'
  } else {
    return null
  }
}

function _getSegmentRange (originData) {
  const segmentRange = originData.segment_range
  if (segmentRange) {
    const startTime = segmentRange.date_range_start
    const endTime = segmentRange.date_range_end
    return [ startTime, endTime ]
  } else {
    return []
  }
}

/**
 * 获取所有Segment数据范围
 * @param {*} table
 */
export function getAllSegmentsRange (table) {
  // const segmentKeyValuePairs = Object.entries(table.segment_ranges)
  // let maxTime = -Infinity
  // let minTime = Infinity
  // for (const [key] of segmentKeyValuePairs) {
  //   const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
  //   startTime < minTime ? (minTime = +startTime) : null
  //   endTime > maxTime ? (maxTime = +endTime) : null
  // }
  // return segmentKeyValuePairs.length ? [ minTime, maxTime ] : []
  return []
}

export function getAllSegments (segmentData) {
  // return Object.entries(segmentData).map(([key, value]) => {
  //   const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
  //   return { startTime, endTime, status: value }
  // }).sort((segmentA, segmentB) => {
  //   return segmentA.startTime < segmentB.startTime ? -1 : 1
  // })
  return []
}

/**
 * 获取用户DataRange
 * @desc table有两个range，一个是query range，一个是ready range
 *       query range是用户可查询数据范围
 *       ready range是segment数据ready范围
 *       query range小于或等于ready range
 * @param {*} table
 */
export function getUserRange (table) {
  const [ minQueryableRange, maxQueryableRange ] = getQueryableRange(table)
  const [ minReadyableRange, maxReadyableRange ] = getReadySegmentsRange(table)

  let minUserRange = 0
  let maxUserRange = 0

  const isUsingMinQueryRange = (minQueryableRange && minQueryableRange > minReadyableRange) || !minReadyableRange
  isUsingMinQueryRange
    ? (minUserRange = minQueryableRange)
    : (minUserRange = minReadyableRange)

  const isUsingMaxQueryRange = (maxQueryableRange && maxQueryableRange < maxReadyableRange) || !maxReadyableRange
  isUsingMaxQueryRange
    ? (maxUserRange = maxQueryableRange)
    : (maxUserRange = maxReadyableRange)

  return minUserRange && maxUserRange ? [ minUserRange, maxUserRange ] : []
}

function getQueryableRange (table) {
  return ~table.actual_query_start && ~table.actual_query_end ? [table.actual_query_start, table.actual_query_end] : []
}

function getReadySegmentsRange (table) {
  return ~table.ready_start && ~table.ready_end ? [ table.ready_start, table.ready_end ] : []
}
