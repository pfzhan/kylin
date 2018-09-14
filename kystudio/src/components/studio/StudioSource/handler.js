import bgImage from './bg.png'

export const viewTypes = {
  DATA_LOAD: 'data-load',
  COLUMNS: 'columns',
  SAMPLE: 'sample',
  STATISTICS: 'statistics',
  EXTEND_INFORMATION: 'extend-information',
  KAFKA: 'kafka',
  ACCESS: 'access'
}

export function getSelectedTableDetail (tableInfo, tableDetail) {
  const columns = tableInfo.columns.map(column => {
    // for newten
    // const columnDetail = tableDetail.columns_stats.find(columnStats => columnStats.column_name === column.name)

    // return {
    //   ...column,
    //   ...columnDetail
    // }
    return column
  })

  return {
    ...tableInfo,
    ...tableDetail,
    mapper_rows: tableDetail.mapper_rows || [],
    sample_rows: tableDetail.sample_rows || [],
    total_rows: tableDetail.total_rows || [],
    dataRanges: getDateRangeSegments(tableDetail),
    columns
  }
}

function getDateRangeSegments (tableDetail) {
  return Object.entries(tableDetail.segment_ranges).map(([dateRangeKey, status]) => {
    const [startTime, endTime] = dateRangeKey.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
    return {
      startTime: +startTime,
      endTime: +endTime,
      status: 'NEW',
      color: '#CFD8DC',
      pointColor: '#8E9FA8',
      backgroundImage: bgImage
    }
  }).filter((segment) => {
    return !(segment.startTime === segment.endTime || segment.startTime === 0 || segment.startTime === 1309891513770)
  })
}
