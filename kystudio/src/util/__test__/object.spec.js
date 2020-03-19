import { get, set, push } from '../object'

test('copy new object', () => {
  const obj = {a: 1, b: 2}
  const str = 'a.b'
  const nll = null

  expect(get(obj, str)).toBeUndefined()
  expect(get(nll, str)).toBeNull()
})

test('object set new value', () => {
  const oldObj = {a: 1, b: 2}
  const str = 'kap.c'
  const value = 'new item'

  expect(set(oldObj, str, value)).toEqual({ a: 1, b: 2, kap: { c: 'new item' } })
  expect(set(oldObj, 'c', value)).toEqual({ a: 1, b: 2, c: 'new item' })
  expect(set(null, 'c', value)).toEqual({ c: 'new item' })
  expect(set(null, true, value)).toBeNull()
})

test('push new item', () => {
  const oldObj = {a: 1, b: 2}
  const str = 'kap.'
  const value = 'new item'

  expect(push(oldObj, str, value)).toBeNull()
  expect(push({a: {b: []}}, 'a.b', 'c')).toEqual({ a: { b: ['c']}})
})