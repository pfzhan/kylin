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
  // KyAccount 登陆
  loginKybot (param) {
    // return Vue.resource(apiUrl + 'kyaccount/auth').save(param)
    // let apiUrl1 = apiUrl.substring(0, apiUrl.length - 1)
    return Vue.resource(apiUrl + 'kyaccount/auth?username=' + param.username + '&password=' + param.password).save()
  },
  // 获取KyBot自动上传是否开启
  getkybotStatus () {
    return Vue.resource(apiUrl + 'kybot/daemon/status').get()
  },
  // 开启KyBot自动上传
  startKybot () {
    return Vue.resource(apiUrl + 'kybot/daemon/start').save()
  },
  // 关闭KyBot自动上传
  stopKtbot () {
    return Vue.resource(apiUrl + 'kybot/daemon/stop').save()
  }
}
