/**
 * 获取所有Segment数据范围
 * @param {*} table
 */
export function getAllSegmentsRange (table) {
  const segmentKeyValuePairs = Object.entries(table.segment_ranges)
  let maxTime = -Infinity
  let minTime = Infinity
  for (const [key] of segmentKeyValuePairs) {
    const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
    startTime < minTime ? (minTime = +startTime) : null
    endTime > maxTime ? (maxTime = +endTime) : null
  }
  return [ minTime, maxTime ]
}

export function getAllSegments (segmentData) {
  return Object.entries(segmentData).map(([key, value]) => {
    const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
    return { startTime, endTime, status: value }
  }).sort((segmentA, segmentB) => {
    return segmentA.startTime < segmentB.startTime ? -1 : 1
  })
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

  return [ minUserRange, maxUserRange ]
}

function getQueryableRange (table) {
  return ~table.actual_query_start && ~table.actual_query_end ? [table.actual_query_start, table.actual_query_end] : []
}

function getReadySegmentsRange (table) {
  return ~table.ready_start && ~table.ready_end ? [ table.ready_start, table.ready_end ] : []
}
