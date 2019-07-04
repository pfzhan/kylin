import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  // 获取KyAccount账号信息
  // 如果返回的data是空，表示未登陆过；否则显示已登录用户的token。
  getKyAccount () {
    return Vue.resource(apiUrl + 'kyaccount').get()
  },
  getCurrentAccountName () {
    return Vue.resource(apiUrl + 'kyaccount/current').get()
  },
  // KyAccount 登陆
  loginKybot (param) {
    // return Vue.resource(apiUrl + 'kyaccount/auth?username=' + param.username + '&password=' + param.password).save()
    return Vue.resource(apiUrl + 'kyaccount/auth').save(param)
  },
  // 获取KyBot自动上传是否开启
  getkybotStatus () {
    return Vue.resource(apiUrl + 'kybot/daemon/status').get()
  },
  kybotLogOut () {
    return Vue.resource(apiUrl + 'kyaccount/logout').save()
  }
}
