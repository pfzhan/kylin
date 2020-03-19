import Vue from 'vue'
import sinon from 'sinon'
import { validate, validateTypes } from '../index'
import VueI18n from 'vue-i18n'
import enKylinLocale from '../../locale/en'
import zhKylinLocale from '../../locale/zh-CN'
Vue.use(VueI18n)
enKylinLocale.kylinLang = enKylinLocale.default
zhKylinLocale.kylinLang = zhKylinLocale.default
Vue.locale('en', enKylinLocale)
Vue.locale('zh-cn', zhKylinLocale)

describe('validate', () => {
  const vm = new Vue({})
  const callback = sinon.spy()
  const { GROUP_NAME, USERNAME, PASSWORD, CONFIRM_PASSWORD, PROJECT_NAME } = validateTypes
  it('validate-groupname', () => {
    validate[GROUP_NAME].bind(vm)(null, '', callback)
    expect(callback.args[0][0].message).toBe(vm.$t('kylinLang.common.userGroupNameEmpty'))
    validate[GROUP_NAME].bind(vm)(null, '_abc123', callback)
    expect(callback.args[1][0].message).toBe(vm.$t('kylinLang.common.userGroupNameFormatValidTip'))
    validate[GROUP_NAME].bind(vm)(null, 'abc123', callback)
    expect(callback.args[2][0]).toBeUndefined()
  })

  it('validate-username', () => {
    validate[USERNAME].bind(vm)(null, '', callback)
    expect(callback.args[3][0].message).toBe(vm.$t('kylinLang.common.usernameEmpty'))
    validate[USERNAME].bind(vm)(null, '_abc123', callback)
    expect(callback.args[4][0].message).toBe(vm.$t('kylinLang.common.userNameFormatValidTip'))
    validate[USERNAME].bind(vm)(null, 'abc123', callback)
    expect(callback.args[5][0]).toBeUndefined()
  })

  it('validate-password', () => {
    validate[PASSWORD].bind(vm)(null, '', callback)
    expect(callback.args[6][0].message).toBe(vm.$t('kylinLang.common.passwordEmpty'))
    validate[PASSWORD].bind(vm)(null, 'abc123', callback)
    expect(callback.args[7][0].message).toBe(vm.$t('kylinLang.common.passwordLength'))
    validate[PASSWORD].bind(vm)(null, 'abcd1234', callback)
    expect(callback.args[8][0].message).toBe(vm.$t('kylinLang.user.tip_password_unsafe'))
    validate[PASSWORD].bind(vm)(null, 'kyligence@1', callback)
    expect(callback.args[9][0]).toBeUndefined()
  })

  it('validate-confirm-password', () => {
    vm.form = {
      password: 'kyligence@1',
      newPassword: 'kyligence@2'
    }
    validate[CONFIRM_PASSWORD].bind(vm)(null, '', callback)
    expect(callback.args[10][0].message).toBe(vm.$t('kylinLang.common.passwordEmpty'))
    validate[CONFIRM_PASSWORD].bind(vm)(null, 'kyligence@2', callback)
    expect(callback.args[11][0].message).toBe(vm.$t('kylinLang.common.passwordConfirm'))
    vm.form = {
      password: 'kyligence@1',
      newPassword: 'kyligence@1'
    }
    validate[CONFIRM_PASSWORD].bind(vm)(null, 'kyligence@1', callback)
    expect(callback.args[12][0]).toBeUndefined()
  })

  it('validate-project-name', () => {
    validate[PROJECT_NAME].bind(vm)(null, '', callback)
    expect(callback.args[13][0].message).toBe(vm.$t('kylinLang.common.noProject'))
    validate[PROJECT_NAME].bind(vm)(null, 'abc123@', callback)
    expect(callback.args[14][0].message).toBe(vm.$t('kylinLang.common.nameFormatValidTip'))
    validate[PROJECT_NAME].bind(vm)(null, '_abcd1234', callback)
    expect(callback.args[15][0].message).toBe(vm.$t('kylinLang.common.nameFormatValidTip1'))
    validate[PROJECT_NAME].bind(vm)(null, 'abcd1234', callback)
    expect(callback.args[16][0]).toBeUndefined()
  })
})