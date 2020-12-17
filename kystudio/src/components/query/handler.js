export function getOptions (that) {
  if (that.charts.type === 'lineChart') {
    return initLineChart(that)
  } else if (that.charts.type === 'barChart') {
    return initBarChart(that)
  } else if (that.charts.type === 'pieChart') {
    return initPieChart(that)
  }
}

// 折线图
function initLineChart (that) {
  const dataMap = combinationData(that)
  const xData = Object.keys(dataMap)
  const yData = Object.values(dataMap)
  return {
    color: ['#3398DB'],
    xAxis: {
      type: 'category',
      data: xData,
      nameLocation: 'start'
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      data: yData,
      type: 'line'
    }],
    tooltip: {},
    dataZoom: [
      {
        show: true,
        realtime: true,
        start: 30,
        end: 70
      }
    ]
  }
}

// 柱状图
function initBarChart (that) {
  const dataMap = combinationData(that)
  const xData = Object.keys(dataMap)
  const yData = Object.values(dataMap)
  return {
    color: ['#3398DB'],
    xAxis: [{
      type: 'category',
      data: xData,
      nameLocation: 'start'
    }],
    yAxis: [{
      type: 'value'
    }],
    series: [{
      type: 'bar',
      barWidth: '60%',
      data: yData
    }],
    tooltip: {},
    dataZoom: [
      {
        show: true,
        realtime: true,
        start: 30,
        end: 70
      }
    ]
  }
}

// 饼图
function initPieChart (that) {
  const dataMap = combinationData(that)
  const xData = Object.keys(dataMap).slice(0, 1000)
  const yData = Object.values(dataMap).slice(0, 1000)
  const pieList = []
  xData.forEach((item, index) => {
    pieList.push({name: item, value: yData[index]})
  })
  return {
    series: [{
      type: 'pie',
      data: pieList,
      emphasis: {
        labelLine: {
          show: xData.length <= 50
        }
      },
      label: {
        show: xData.length <= 50
      }
    }],
    tooltip: {
      trigger: 'item'
    }
  }
}

function combinationData (that) {
  const { dimension, measure } = that.charts
  const dataMap = {}
  const dimensionIndex = that.tableMeta.findIndex(item => item.name === dimension)
  const measureIndex = that.tableMeta.findIndex(item => item.name === measure)
  const xSourceData = that.filterTableData().map(it => it[dimensionIndex])
  const ySourceData = that.filterTableData().map(it => it[measureIndex])
  xSourceData.forEach((item, index) => {
    if (item in dataMap) {
      dataMap[item] = +dataMap[item] + (+ySourceData[index])
    } else {
      dataMap[item] = +ySourceData[index]
    }
  })
  return dataMap
}

export function compareDataSize (that) {
  const data = combinationData(that)
  return { xData: Object.keys(data), yData: Object.values(data) }
}
