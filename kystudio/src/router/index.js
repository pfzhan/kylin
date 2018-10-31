import Vue from 'vue'
import Router from 'vue-router'
import topLeftRightView from 'components/layout/layout_left_right_top'
import layoutFull from 'components/layout/layout_full'
import projectList from 'components/project/project_list'
// import modelTab from 'components/model/model_tab'
import cubeList from 'components/cube/cube_list'
import cubeEdit from 'components/cube/edit/cube_desc_edit'
import login from 'components/user/login'
// import insight from 'components/insight/insight'
import setting from 'components/security/reset_password'
import newQuery from 'components/query/new_query'
import queryHistory from 'components/query/query_history'
import favoriteQuery from 'components/query/favorite_query'
import overview from 'components/overview'
import messages from 'components/messages'
// import groupDetail from 'components/security/groupDetail'
import cluster from 'components/monitor/cluster'
import admin from 'components/monitor/admin'
import jobs from 'components/monitor/jobs'
Vue.use(Router)

export default new Router({
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
      component: layoutFull
    },
    {
      path: '/',
      redirect: '/dashboard',
      name: 'default',
      component: topLeftRightView,
      children: [{
        name: 'Dashboard',
        path: 'dashboard',
        component: overview
      }, {
        name: 'Studio',
        path: 'studio/source',
        component: () => import('../components/studio/StudioSource/index.vue')
      }, {
        name: 'ModelList',
        path: 'studio/model',
        // path: 'studio/:subaction',
        // component: modelTab
        component: () => import('../components/studio/StudioModel/ModelList/index.vue')
      }, {
        name: 'ModelEdit',
        path: 'studio/modeledit/:modelName/:action',
        // path: 'studio/:subaction',
        // component: modelTab
        meta: {},
        component: () => import('../components/studio/StudioModel/ModelTabs/index.vue')
      }, {
        name: 'Cubes',
        path: 'cubes',
        component: cubeList
      }, {
        name: 'CubeEdit',
        path: 'cubeEdit',
        component: cubeEdit
      },
      {
        name: 'Project',
        path: 'security/project',
        component: projectList
      },
      {
        name: 'User',
        path: 'security/user',
        component: () => import('../components/security/SecurityUser/index.vue')
      },
      {
        name: 'Group',
        path: 'security/group',
        component: () => import('../components/security/SecurityGroup/index.vue')
      },
      {
        name: 'GroupDetail',
        path: 'security/group/:groupName',
        component: () => import('../components/security/SecurityUser/index.vue')
      },
      // {
      //   name: 'Insight',
      //   path: 'insight',
      //   component: insight
      // },
      {
        name: 'Job',
        path: 'monitor/job',
        component: jobs
      },
      {
        name: 'Cluster',
        path: 'monitor/cluster',
        component: cluster
      },
      {
        name: 'Admin',
        path: 'monitor/admin',
        component: admin
      },
      {
        name: 'Setting',
        path: 'setting',
        component: setting
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
      {
        name: 'FavoriteQuery',
        path: 'query/favorite_query',
        component: favoriteQuery
      },
      {
        name: 'Overview',
        path: 'overview',
        component: overview
      },
      {
        name: 'Messages',
        path: 'messages',
        component: messages
      }]
    }
  ]
})
