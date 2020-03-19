import * as util from '../index'

describe('util-index', () => {
  it('isEmptyObjectFun', () => {
    const obj1 = {}
    const obj2 = {a: 1}
    expect(util.isEmptyObject(obj1)).toBeTruthy()
    expect(util.isEmptyObject(obj2)).not.toBeTruthy()
  })

  it('collectObject', () => {
    const obj = {
      a: null,
      b: 1
    }
    expect(util.collectObject(obj, ['a', 'b'], true, true)).toBe(JSON.stringify({b: 1}))
    expect(util.collectObject(obj, ['a', 'b'], true, false)).toBe(JSON.stringify({a: null, b: 1}))
    expect(util.collectObject(obj, ['a', 'b'], false, true)).toEqual({b: 1})
    expect(util.collectObject(obj, ['a', 'b'], false, false)).toEqual({a: null, b: 1})
  })

  it('fromObjToArr', () => {
    const obj = {
      a: 1
    }
    expect(util.fromObjToArr(obj)).toEqual([{key: 'a', value: 1}])
  })

  it('fromArrToObj', () => {
    const arr = [{key: 'a', value: 1}]
    expect(util.fromArrToObj(arr)).toEqual({a: 1})
  })

  it('fromArrToObjArr', () => {
    const arr = [0, 1, 2]
    expect(util.fromArrToObjArr(arr)).toEqual([{value: 0}, {value: 1}, {value: 2}])
  })

  it('sampleGuid', () => {
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
    expect(util.sampleGuid()).not.toBe(util.sampleGuid())
  })

  it('parsePath', () => {
    const path = 'a.b'
    const obj = {a: {b: {c: 1}}}
    const pathObj = util.parsePath(path)
    expect(pathObj(obj)).toEqual({c: 1})
  })

  it('changeDataAxis', () => {
    const arr = [[0, 0, 0], [1, 1, 1], [2, 2, 2]]
    expect(util.changeDataAxis(arr)).toEqual([[0, 1, 2], [0, 1, 2], [0, 1, 2]])
  })

  it('scToFloat', () => {
    const num = '3.4556645445E7'
    expect(util.scToFloat(num)).toBe('34556645.45')
    expect(util.scToFloat('3.4556645445')).toBe('3.4556645445')
  })

  it('showNull', () => {
    expect(util.showNull(null)).toBe('null')
    expect(util.showNull(1)).toBe(1)
  })

  it('groupData', () => {
    const arr = [
      {a: 1, key: 'key1'},
      {a: 2, key: 'key2'}
    ]
    expect(util.groupData(arr, 'key')).toEqual({key1: [{a: 1, key: 'key1'}], key2: [{a: 2, key: 'key2'}]})
  })

  it('objArrKeyToArr', () => {
    const arr = [
      {a: 1, key: 'key1'},
      {a: 2, key: 'key2'}
    ]
    expect(util.groupData(arr, 'key')).toEqual({key1: [{a: 1, key: 'key1'}], key2: [{a: 2, key: 'key2'}]})
  })

  it('countObjWithSomeKey', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.countObjWithSomeKey(arr, 'a', 2)).toBe(0)
    expect(util.countObjWithSomeKey(arr, 'a', 1)).toBe(2)
    expect(util.countObjWithSomeKey(arr, 'a', 0)).toBe(1)
  })

  it('indexOfObjWithSomeKey', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.indexOfObjWithSomeKey(arr, 'a', 0)).toBe(2)
    expect(util.indexOfObjWithSomeKey(arr, 'b', 2)).toBe(0)
    expect(util.indexOfObjWithSomeKey(arr, '1', 2)).toBe(-1)
  })

  it('indexOfObjWithSomeKeys', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.indexOfObjWithSomeKeys(arr, 'a', 1, 'b', 2)).toBe(0)
    expect(util.indexOfObjWithSomeKeys(arr, 'a', 1, 'b', 3)).toBe(1)
    expect(util.indexOfObjWithSomeKeys(arr, 'a', 0, 'b', 4)).toBe(2)
    expect(util.indexOfObjWithSomeKeys(arr, 'a', 1, 'b', 4)).toBe(-1)
  })

  it('getObjectBySomeKeys', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.getObjectBySomeKeys(arr, 'a', 0)).toEqual({a: 0, b: 4})
    expect(util.getObjectBySomeKeys(arr, 'b', 2)).toEqual({a: 1, b: 2})
  })

  it('getObjectByFilterKey', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.getObjectBySomeKeys(arr, 'a', 0)).toEqual({a: 0, b: 4})
    expect(util.getObjectBySomeKeys(arr, 'b', 2)).toEqual({a: 1, b: 2})
  })

  it('getDiffObjInArrays', () => {
    const arr1 = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    const arr2 = [
      {a: 1, b: 2},
      {a: 2, b: 3},
      {a: 0, b: 4}
    ]
    const arr3 = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.getDiffObjInArrays(arr1, arr2, ['a', 'b'])).toEqual([{a: 1, b: 3}])
    expect(util.getDiffObjInArrays(arr2, arr3, ['a', 'b'])).toEqual([{a: 2, b: 3}])
    expect(util.getDiffObjInArrays(arr1, arr3, ['a', 'b'])).toEqual([])
  })

  it('objectArraySort', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.objectArraySort(arr, true, 'a')).toEqual([{a: 0, b: 4}, {a: 1, b: 3}, {a: 1, b: 2}])
    expect(util.objectArraySort(arr, false, 'b')).toEqual([{a: 0, b: 4}, {a: 1, b: 3}, {a: 1, b: 2}])
  })

  it('arrSortByArr', () => {
    const arr1 = ['a', 'b', 'c', 'd']
    const arr2 = ['d', 'a']
    expect(util.arrSortByArr(arr1, arr2)).toEqual(['d', 'b', 'c', 'a'])
  })

  it('topArrByArr', () => {
    const arr1 = [1, 2, 3]
    const arr2 = [3, 2]
    expect(util.topArrByArr(arr1, arr2)).toEqual([3, 2, 1])
  })

  it('objectClone', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    const obj = {a: 1, b: 2}
    expect(util.objectClone(arr)).toEqual([{a: 1, b: 2}, {a: 1, b: 3}, {a: 0, b: 4}])
    expect(util.objectClone(obj)).toEqual({a: 1, b: 2})
    expect(util.objectClone(1)).toBe(1)
  })

  it('changeObjectArrProperty', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    util.changeObjectArrProperty(arr, '*', 'b', 10)
    expect(arr).toEqual([{a: 1, b: 10}, {a: 1, b: 10}, {a: 0, b: 10}])
  })

  it('filterObjectArray', () => {
    const arr = [
      {a: 1, b: 2},
      {a: 1, b: 3},
      {a: 0, b: 4}
    ]
    expect(util.filterObjectArray(arr, 'b', 3)).toEqual([{a: 1, b: 3}])
    expect(util.filterObjectArray(arr, 'a', 1)).toEqual([{a: 1, b: 2}, {a: 1, b: 3}])
  })

  it('objectToStr', () => {
    const obj = {a: 1, b: 2}
    expect(util.objectToStr(obj)).toBe(JSON.stringify({a: 1, b: 2}))
    expect(util.objectToStr(1)).toBe('1')
  })
})