export function getModelDataRanges (model) {
  return Object.entries(model.segment_ranges).map(([dateRangeKey, status]) => {
    const [startTime, endTime] = dateRangeKey.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
    return {
      startTime: +startTime,
      endTime: +endTime,
      status,
      ...getStatusColor(status)
    }
  }).filter((segment) => {
    return !(segment.startTime === segment.endTime || segment.startTime === 0 || segment.startTime === 1309891513770)
  })
}

export function getTableDataRanges (table, models) {
  const tableRanges = table.dataRanges

  for (const model of models) {
    model.dataRanges.forEach(modelRange => {
      if (modelRange.status === 'NEW') {
        const currentTableRange = tableRanges.find(tableRange => modelRange.startTime === tableRange.startTime)
        currentTableRange.status = 'NEW'
        currentTableRange.color = 'transparent'
        currentTableRange.pointColor = '#8E9FA8'
      }
    })
  }
  return tableRanges
}

function getStatusColor (status) {
  switch (status) {
    case 'READY':
      return {
        color: '#4cb050',
        pointColor: '#1A731E'
      }
    case 'NEW':
      return {
        color: 'transparent',
        pointColor: '#8E9FA8'
      }
    default:
      return {}
  }
}
