import Vue from 'vue'
import Router from 'vue-router'
import Component from 'vue-class-component'
import topLeftRightView from 'components/layout/layout_left_right_top'
import layoutFull from 'components/layout/layout_full'
import projectList from 'components/project/project_list'
import projectAuthority from 'components/project/project_authority'
import login from 'components/user/login'
import Insight from 'components/query/insight'
import queryHistory from 'components/query/query_history'
// import Acceleration from 'components/studio/Acceleration/acceleration'  // 加速引擎界面整体隐藏
import dashboard from 'components/dashboard/homepage'
import jobs from 'components/monitor/jobs'
import { bindRouterGuard } from './routerGuard.js'

Vue.use(Router)
Component.registerHooks([
  'beforeRouteEnter',
  'beforeRouteLeave',
  'beforeRouteUpdate'
])
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
        // 无权限路由
        name: 'noAuthority',
        path: 'noauthority',
        component: () => import('../components/noAuthority/index.vue')
      }, {
        path: '/404',
        name: '404',
        component: () => import('../components/noAuthority/index.vue')
      }, {
        name: 'Setting',
        path: '/setting',
        component: () => import('../components/setting/setting.vue')
      }, {
        name: 'Source',
        path: 'studio/source',
        component: () => import('../components/studio/StudioSource/index.vue')
      },
      // {
      //   name: 'Acceleration',
      //   path: 'studio/acceleration',
      //   component: Acceleration
      // }
      {
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
        name: 'ModelSubPartitionValues',
        path: 'studio/model_sub_partition_values/:modelName/:modelId',
        component: () => import('../components/studio/StudioModel/SubPartitionValues/SubPartitionValues.vue')
      }, {
        name: 'Snapshot',
        path: 'studio/snapshot',
        component: () => import('../components/studio/snapshot/snapshot.vue')
      }, {
        name: 'Project',
        path: 'admin/project',
        component: projectList
      }, {
        name: 'ProjectAuthority',
        path: 'admin/project/:projectName',
        component: projectAuthority
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
        component: () => import('../components/admin/User/index.vue')
      },
      {
        name: 'SystemCapacity',
        path: 'admin/systemcapacity',
        component: () => import('../components/admin/SystemCapacity/index.vue'),
        pathToRegexpOptions: {  // 路由正则匹配规则参数 options
          sensitive: true  // 是否大小写敏感
        }
      },
      {
        name: 'Job',
        path: 'monitor/job',
        component: jobs
      },
      {
        name: 'Insight',
        path: 'query/insight',
        component: Insight
      },
      {
        name: 'QueryHistory',
        path: 'query/queryhistory',
        component: queryHistory
      },
      {
        path: '*',
        // redirect: '/404',
        beforeEnter: (to, from, next) => {
          if (to.path === '/') next('/dashboard')
          else next('/404')
        }
      }]
    }
  ]
}
let router = new Router(routerOptions)
router = bindRouterGuard(router)
export default router