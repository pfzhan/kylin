import { getAllSegments, getAllSegmentsRange, getUserRange } from '../../../util/UtilTable'

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
  const segments = getAllSegments(tableDetail.segment_ranges)
  const userRange = getUserRange(tableDetail)
  const allRange = getAllSegmentsRange(tableDetail)

  return {
    ...tableDetail,
    mapper_rows: tableDetail.mapper_rows || [],
    sample_rows: tableDetail.sample_rows || [],
    total_rows: tableDetail.total_rows || [],
    userRange: userRange.length ? userRange : allRange,
    allRange,
    ...checkRangeDisabled(segments),
    columns
  }
}

function checkRangeDisabled (segments) {
  return {
    isMinRangeDisabled: segments[0] && segments[0].status === 'NEW',
    isMaxRangeDisabled: segments[segments.length - 1] && segments[segments.length - 1].status === 'NEW'
  }
}
