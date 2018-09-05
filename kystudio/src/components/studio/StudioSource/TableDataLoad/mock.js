export function getModelTableData () {
  const tableData = []
  const count = Math.random() * 10

  tableData.push({
    name: `Model_name_${0}`,
    progress: 100
  })

  for (let i = 1; i < count; i++) {
    tableData.push({
      name: `Model_name_${i}`,
      progress: Math.random() * 100
    })
  }

  return tableData
}

const dateRanges = []

for (let i = 1; i < 4; i++) {
  dateRanges.push({
    startDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i),
    endDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i + 1),
    color: 'transparent',
    pointColor: '#8E9FA8'
  })
}

for (let i = 4; i < 49; i++) {
  dateRanges.push({
    startDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i),
    endDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i + 1),
    color: '#4cb050',
    pointColor: '#1A731E'
  })
}

for (let i = 49; i < 62; i++) {
  dateRanges.push({
    startDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i),
    endDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i + 1),
    color: 'transparent',
    pointColor: '#8E9FA8'
  })
}

// for (let i = 1; i < 9; i++) {
//   dateRanges.push({
//     startDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i),
//     endDate: new Date('Mon Jan 1 2017 00:00:00 GMT+0800 (中国标准时间)').setDate(i + 1),
//     isDisabled: false
//   })
// }

export { dateRanges }
