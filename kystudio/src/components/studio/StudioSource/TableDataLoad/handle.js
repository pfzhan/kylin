import bgImage from './bg.png'

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
  let minRangeStartTime = Infinity
  let minRangeEndTime = -Infinity

  for (const model of models) {
    model.dataRanges.forEach(modelRange => {
      if (modelRange.status === 'READY') {
        if (modelRange.startTime < minRangeStartTime) {
          minRangeStartTime = modelRange.startTime
        }
        if (modelRange.endTime > minRangeEndTime) {
          minRangeEndTime = modelRange.endTime
        }
      }
    })
  }
  tableRanges.forEach(tableRange => {
    if (tableRange.startTime >= minRangeStartTime && tableRange.endTime <= minRangeEndTime) {
      tableRange.status = 'READY'
      tableRange.color = '#4CB050'
      tableRange.pointColor = '#1A731E'
      tableRange.backgroundImage = null
    }
  })
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
        color: '#CFD8DC',
        pointColor: '#8E9FA8',
        backgroundImage: bgImage
      }
    default:
      return {}
  }
}
