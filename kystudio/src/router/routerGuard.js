import * as types from '../store/types'
import { menusData } from '../config'
import store from '../store'
import ElementUI from 'kyligence-ui'
let MessageBox = ElementUI.MessageBox
var selectedProject = store.state.project.selected_project
export function bindRouterGuard (router) {
  router.beforeEach((to, from, next) => {
    // 处理在模型添加的业务窗口刷新浏览器
    if (to.name === 'ModelEdit' && to.params.action === 'add' && from.name === null) {
      router.push({name: 'ModelList'})
      return
    }
    if (from.name === 'ModelEdit' && to.name !== '' && !to.params.ignoreIntercept) {
      MessageBox.confirm(window.kapVm.$t('kylinLang.common.willGo'), window.kapVm.$t('kylinLang.common.tip'), {
        confirmButtonText: window.kapVm.$t('kylinLang.common.go'),
        cancelButtonText: window.kapVm.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        if (to.name === 'refresh') {
          next()
          this.$nextTick(() => {
            this.$router.replace('studio/model')
          })
          return
        }
        next()
      }).catch(() => {
        next(false)
      })
      return
    }
    ElementUI.Message.closeAll() // 切换路由的时候关闭message
    store.state.config.showLoadingBox = false // 切换路由的时候关闭全局loading
    if (to.matched && to.matched.length) {
      store.state.config.layoutConfig.gloalProjectSelectShow = to.name !== 'Overview'
      // 确保在非点击菜单的路由跳转下还能够正确定位到指定的active name
      menusData.forEach((menu) => {
        if (menu.name.toLowerCase() === to.name.toLowerCase()) {
          store.state.config.routerConfig.currentPathName = menu.path
        }
      })
      let prepositionRequest = () => {
        let authenticationPromise = store.dispatch(types.LOAD_AUTHENTICATION)
        let projectPromise = store.dispatch(types.LOAD_ALL_PROJECT)
        let rootPromise = Promise.all([
          authenticationPromise,
          projectPromise
        ])
        rootPromise.then(() => {
          store.commit(types.SAVE_CURRENT_LOGIN_USER, { user: store.state.system.authentication.data })
          let configPromise = store.dispatch(types.GET_CONF, {
            projectName: selectedProject
          })
          configPromise.then(() => {
            next()
          })
        }, (res) => {
          next()
        })
      }
      // 如果是从登陆过来的，所有信息都要重新获取
      if (from.name === 'Login' && (to.name !== 'access' && to.name !== 'Login')) {
        prepositionRequest()
      } else if (from.name !== 'access' && from.name !== 'Login' && to.name !== 'access' && to.name !== 'Login') {
        // 如果是非登录页过来的，内页之间的路由跳转的话，就需要判断是否已经拿过权限
        if (store.state.system.authentication === null && store.state.system.serverConfig === null) {
          prepositionRequest()
        } else {
          next()
        }
      } else {
        next()
      }
    } else {
      router.replace('/access/login')
    }
  })
  router.afterEach(route => {
    var scrollBoxDom = document.getElementById('scrollBox')
    if (scrollBoxDom) {
      scrollBoxDom.scrollTop = 0
    }
  })
  return router
}
