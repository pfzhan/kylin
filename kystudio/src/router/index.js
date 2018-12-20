import Vue from 'vue'
import Router from 'vue-router'
import topLeftRightView from 'components/layout/layout_left_right_top'
import layoutFull from 'components/layout/layout_full'
import projectList from 'components/project/project_list'
import login from 'components/user/login'
import newQuery from 'components/query/new_query'
import queryHistory from 'components/query/query_history'
import favoriteQuery from 'components/query/favorite_query'
import dashboard from 'components/dashboard'
import messages from 'components/messages'
import jobs from 'components/monitor/jobs'
import { bindRouterGuard } from './routerGuard.js'
Vue.use(Router)
let routerOptions = {
  routes: [
    {
      path: '/access',
      name: 'access',
      component: layoutFull,
      children: [{
        name: 'Login',
        path: 'login',
        component: login
      }]
    },
    // 刷新使用中转路由
    {
      path: '/refresh',
      name: 'refresh',
      component: topLeftRightView
    },
    {
      path: '/',
      redirect: '/dashboard',
      name: 'default',
      component: topLeftRightView,
      children: [{
        name: 'Dashboard',
        path: 'dashboard',
        component: dashboard
      }, {
        name: 'Setting',
        path: '/setting',
        component: () => import('../components/setting/setting.vue')
      }, {
        name: 'Source',
        path: 'studio/source',
        component: () => import('../components/studio/StudioSource/index.vue')
      }, {
        name: 'FavoriteQuery',
        path: 'studio/favorite_query',
        component: favoriteQuery
      }, {
        name: 'ModelList',
        path: 'studio/model',
        // path: 'studio/:subaction',
        // component: modelTab
        component: () => import('../components/studio/StudioModel/ModelList/index.vue')
      }, {
        name: 'ModelEdit',
        path: 'studio/model/:modelName/:action',
        // path: 'studio/:subaction',
        // component: modelTab
        meta: {},
        component: () => import('../components/studio/StudioModel/ModelTabs/index.vue')
      }, {
        name: 'Project',
        path: 'admin/project',
        component: projectList
      },
      {
        name: 'User',
        path: 'admin/user',
        component: () => import('../components/admin/User/index.vue')
      },
      {
        name: 'Group',
        path: 'admin/group',
        component: () => import('../components/admin/Group/index.vue')
      },
      {
        name: 'GroupDetail',
        path: 'admin/group/:groupName',
        component: () => import('../components/admin/Group/index.vue')
      },
      {
        name: 'Job',
        path: 'monitor/job',
        component: jobs
      },
      {
        name: 'NewQuery',
        path: 'query/new_query',
        component: newQuery
      },
      {
        name: 'QueryHistory',
        path: 'query/query_history',
        component: queryHistory
      },
      // {
      //   name: 'Dashboard',
      //   path: 'dashboard',
      //   component: dashboard
      // },
      {
        name: 'Messages',
        path: 'messages',
        component: messages
      }]
    }
  ]
}
let router = new Router(routerOptions)
router = bindRouterGuard(router)
export default router
