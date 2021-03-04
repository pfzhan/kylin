import * as handlerJs from '../handler'

const _data = {
  charts: {
    type: 'pieChart',
    dimension: 'LO_ORDERKEY',
    measure: 'LO_COMMITDATE'
  },
  tableMetaBackup: [
    {
      autoIncrement: false,
      caseSensitive: true,
      catelogName: null,
      columnType: -5,
      columnTypeName: 'BIGINT',
      currency: false,
      definitelyWritable: false,
      displaySize: 20,
      isNullable: 1,
      label: 'LO_ORDERKEY',
      name: 'LO_ORDERKEY',
      precision: 19,
      readOnly: false,
      scale: 0,
      schemaName: null,
      searchable: false,
      signed: false,
      tableName: null,
      writable: false
    },
    {
      autoIncrement: false,
      caseSensitive: true,
      catelogName: null,
      columnType: -5,
      columnTypeName: 'BIGINT',
      currency: false,
      definitelyWritable: false,
      displaySize: 20,
      isNullable: 1,
      label: 'LO_LINENUMBER',
      name: 'LO_LINENUMBER',
      precision: 19,
      readOnly: false,
      scale: 0,
      schemaName: null,
      searchable: false,
      signed: false,
      tableName: null,
      writable: false
    }
  ],
  extraoption: {
    results: [
      ['1', '1', '8', '156', '1', '19960102', '5-LOW', '0', '17', '1795455', '13125181', '4', '1723636', '63369', '2', '19960212', 'TRUCK'],
      ['1', '2', '8', '68', '1', '19960102', '5-LOW', '0', '36', '3485016', '13125181', '9', '3171364', '58083', '6', '19960228', 'MAIL'],
    ]
  }
}

describe('handler func', () => {
  it('init', () => {
    expect(handlerJs.getOptions(_data)).toEqual({'series': [{'data': [{'name': '1', 'value': NaN}], 'emphasis': {'labelLine': {'show': true}}, 'label': {'show': true}, 'type': 'pie'}], 'tooltip': {'trigger': 'item'}})
    _data.charts.type = 'barChart'
    expect(handlerJs.getOptions(_data)).toEqual({'color': ['#3398DB'], 'dataZoom': [{'end': 70, 'realtime': true, 'show': true, 'start': 30}], 'series': [{'barWidth': '60%', 'data': [NaN], 'type': 'bar'}], 'tooltip': {}, 'xAxis': [{'data': ['1'], 'nameLocation': 'start', 'type': 'category'}], 'yAxis': [{'type': 'value'}]})
    _data.charts.type = 'lineChart'
    expect(handlerJs.getOptions(_data)).toEqual({'color': ['#3398DB'], 'dataZoom': [{'end': 70, 'realtime': true, 'show': true, 'start': 30}], 'series': [{'data': [NaN], 'type': 'line'}], 'tooltip': {}, 'xAxis': {'data': ['1'], 'nameLocation': 'start', 'type': 'category'}, 'yAxis': {'type': 'value'}})
  })
})
