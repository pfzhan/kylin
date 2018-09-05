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
  const { data_source_properties, frequency, last_modified } = tableDetail

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
    data_source_properties,
    frequency,
    last_modified,
    mapper_rows: tableDetail.mapper_rows || [],
    sample_rows: tableDetail.sample_rows || [],
    total_rows: tableDetail.total_rows || [],
    columns
  }
}
