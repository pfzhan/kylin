import { createLocalVue, mount } from 'vue-test-utils'
import ElementUI from 'kyligence-ui'
import { handleSuccess } from '../business'
// import layoutFull from 'components/layout/layout_full'

describe('api report', () => {
  const localVue = createLocalVue()
  localVue.use(ElementUI)
  // const wrapper = mount(layoutFull, {
  //   localVue
  // })
  it('api success', () => {
    const fn = jest.fn()
    const result = {
      data: {
        code: '000',
        data: {
          content: 'test',
        },
        msg: ''
      }
    }
    let resultData = null
    let callback = (res, code, msg) => {
      resultData = {res, code, msg}
    }
    handleSuccess(result, callback)
    expect(resultData).toEqual({"code": "000", "msg": "", "res": {"content": "test"}})
    handleSuccess(null, callback)
    expect(resultData).toEqual({"res": null, "msg": "", "code": "000"})
    expect(() => handleSuccess(null, '')).toThrow()
  })
})

