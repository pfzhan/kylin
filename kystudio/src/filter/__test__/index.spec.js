import Vue from 'vue'
import Vuex from 'vuex'
import filterElements from '../index'

describe('vue-filter', () => {
  Vue.use(Vuex)
  const store = new Vuex.Store({
    state: {
      system: {
        timeZone: 'gmt'
      }
    }
  })

  window.kapVm = new Vue({
    store
  })
  it('nullValFilter', () => {
    expect(filterElements.nullValFilter(null)).toEqual('null')
    expect(filterElements.nullValFilter(3)).toBe(3)
  })
  it('utcDate', () => {
    expect(filterElements.utcDate(1580988555393)).toBeInstanceOf(Date)
    expect(filterElements.utcDate([1580988555393, 1580988555393])).toBeInstanceOf(Array)
  })
  it('utcTime', () => {
    expect(filterElements.utcTime('')).toBe('')
    expect(filterElements.utcTime('time')).toEqual('time')
    expect(filterElements.utcTime(1580988555393)).toEqual('2020-02-06 11:29:15')
    expect(filterElements.utcTime(1571529965000)).toEqual('2019-10-20 00:06:05')
  })
  it('utcTimeOrInt', () => {
    expect(filterElements.utcTimeOrInt('')).toBe('')
    expect(filterElements.utcTimeOrInt('time')).toBe('')
    expect(filterElements.utcTimeOrInt(1580988555393)).toEqual('2020-02-06 11:29:15')
    expect(filterElements.utcTimeOrInt(1571529965000)).toEqual('2019-10-20 00:06:05')
    expect(filterElements.utcTimeOrInt(1580988555393, true)).toEqual(1580988555393)
  })
  it('gmtTime', () => {
    expect(filterElements.gmtTime('')).toBe('')
    expect(filterElements.gmtTime('time')).toBe('')
    expect(filterElements.gmtTime(1571529965000)).toEqual('2019-10-20 00:06:05')
    expect(filterElements.gmtTime(1580988555393)).toEqual('2020-02-06 11:29:15')
    expect(filterElements.gmtTime(1580947565000)).toEqual('2020-02-06 00:06:05')
  })
  it('timeFormatHasTimeZone', () => {
    expect(filterElements.timeFormatHasTimeZone('')).toBe('')
    expect(filterElements.timeFormatHasTimeZone('time')).toBe('')
    expect(filterElements.timeFormatHasTimeZone(1571529965000)).toEqual('2019-10-20 08:06:05 GMT+8')
    expect(filterElements.timeFormatHasTimeZone(1580988555393)).toEqual('2020-02-06 19:29:15 GMT+8')
    expect(filterElements.timeFormatHasTimeZone(1580947565000)).toEqual('2020-02-06 08:06:05 GMT+8')
  })
  it('fixed', () => {
    expect(filterElements.fixed('string', 2)).toBe('0')
    expect(filterElements.fixed(1000, 2)).toBe('1000')
    expect(filterElements.fixed(1000.000, 2)).toBe('1000')
  })
  it('omit', () => {
    expect(filterElements.omit('kyligence', 5, '...')).toBe('kylig...')
    expect(filterElements.omit('kyligence')).toBe('kyligence')
    expect(filterElements.omit('kyligence', 5)).toBe('kylig')
    expect(filterElements.omit('麒麟-kyligence', 5)).toBe('麒麟-')
    expect(filterElements.omit('kylin', 5)).toBe('kylin')
    expect(filterElements.omit(null)).toBeNull()
  })
  it('number', () => {
    expect(filterElements.number(3.15926, 2)).toEqual(3.16)
    expect(filterElements.number(3.15926)).toBe(3)
    expect(filterElements.number('string', 2)).toBe(0)
  })
  it('readableNumber', () => {
    expect(filterElements.readableNumber(123456789)).toBe('123,456,789')
    expect(filterElements.readableNumber(1234567890)).toBe('1,234,567,890')
    expect(filterElements.readableNumber(123)).toBe('123')
    expect(() => {
      filterElements.readableNumber('kyligence')
    }).toThrow('')
  })
  it('tofixedTimer', () => {
    expect(filterElements.tofixedTimer(100000000, 2)).toBe('1666.67 mins')
    expect(filterElements.tofixedTimer(100000000)).toBe('1667 mins')
    expect(filterElements.tofixedTimer('kyligence')).toBe('NaN seconds')
  })
  it('dataSize', () => {
    expect(filterElements.dataSize(102400000000000)).toBe('93.13 TB')
    expect(filterElements.dataSize(102400000000)).toBe('95.37 GB')
    expect(filterElements.dataSize(102400000)).toBe('97.66 MB')
    expect(filterElements.dataSize(1024)).toBe('1.00 KB')
    expect(filterElements.dataSize(1023)).toBe('1023 B')
    expect(filterElements.dataSize(0)).toBe('0 B')
    expect(filterElements.dataSize(-1023)).toBe(-1023)
    expect(filterElements.dataSize('1024')).toBe('1.00 KB')
    expect(filterElements.dataSize('kyligence')).toBe('kyligence')
    expect(filterElements.dataSize('')).toBe('')
    expect(filterElements.dataSize(null)).toBe(null)
    expect(filterElements.dataSize(undefined)).toBe(undefined)
  })
  it('timeSize', () => {
    expect(filterElements.timeSize(1000)).toBe('1 seconds')
    expect(filterElements.timeSize(1000000)).toBe('16.67 minutes')
    expect(filterElements.timeSize(10000000)).toBe('2.78 hours')
    expect(filterElements.timeSize(1000000000)).toBe('11.57 days')
    expect(filterElements.timeSize('1000000000')).toBe('11.57 days')
    expect(filterElements.timeSize('kyligence')).toBe('NaN seconds')
  })
  it('filterArr', () => {
    expect(filterElements.filterArr(['a', 'b', 'c'], 'a', true)).toEqual(['b', 'c'])
    expect(filterElements.filterArr(['a', 'ab', 'c'], 'a', true)).toEqual(['c'])
    expect(filterElements.filterArr(['a', 'A', 'c'], 'a', false)).toEqual(['A', 'c'])
    expect(filterElements.filterArr(null)).toEqual([])
    expect(() => {
      filterElements.filterArr('null')
    }).toThrow()
  })
  it('filterObjArr', () => {
    expect(filterElements.filterObjArr([{name: 'kyligence', id: 1}, {name: 'admin', id: 2}, {name: 'query', id: 3}], 'name', 'admin', true)).toEqual([{name: 'kyligence', id: 1}, {name: 'query', id: 3}])
    expect(filterElements.filterObjArr([{name: 'kyligenceAdmin', id: 1}, {name: 'admin', id: 2}, {name: 'query', id: 3}], 'name', 'admin', true)).toEqual([{name: 'query', id: 3}])
    expect(filterElements.filterObjArr([{name: 'Admin', id: 1}, {name: 'admin', id: 2}, {name: 'query', id: 3}], 'name', 'admin', false)).toEqual([{name: 'Admin', id: 1}, {name: 'query', id: 3}])
    expect(filterElements.filterObjArr(null)).toEqual([])
    expect(() => {
      filterElements.filterObjArr('null')
    }).toThrow()
  })
  it('arrayToStr', () => {
    expect(filterElements.arrayToStr([1, 2, 3])).toBe('1,2,3')
    expect(filterElements.arrayToStr(null)).toBeUndefined()
  })
  it('toGMTDate', () => {
    expect(filterElements.toGMTDate(1580988555393)).toBe('2020-02-06 19:29:15 GMT+8')
  })
  it('toServerGMTDate', () => {
    expect(filterElements.toServerGMTDate(1580988555393)).toBe('2020-02-06 11:29:15 GMT+0')
  })
})
