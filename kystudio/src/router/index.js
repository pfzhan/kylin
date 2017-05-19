import Vue from 'vue'
import Router from 'vue-router'
import hello from 'components/hello'
import demo from 'components/demo'
import button from 'components/demo/button'
import naming from 'components/demo/naming'
import topLeftRightView from 'components/layout/layout_left_right_top'
import layoutFull from 'components/layout/layout_full'
import dashbord from 'components/dashbord'
import projectList from 'components/project/project_list'
import modelTab from 'components/model/model_tab'
import cubeList from 'components/cube/cube_list'
import cubeEdit from 'components/cube/edit/cube_desc_edit'
import system from 'components/system/system_overview'
import login from 'components/user/login'
import insight from 'components/insight/insight'
import monitor from 'components/monitor/monitor'
import setting from 'components/system/reset_password'
Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/access',
      name: 'access',
      component: layoutFull,
      children: [{
        name: 'login',
        path: 'login',
        component: login
      }]
    },
    {
      path: '/',
      redirect: '/dashbord',
      name: 'default',
      component: topLeftRightView,
      children: [{
        name: 'Dashbord',
        path: 'dashbord',
        component: dashbord
      }, {
        name: 'Project',
        path: 'project',
        component: projectList
      }, {
        name: 'Studio',
        path: 'studio/:subaction',
        component: modelTab
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
        name: 'system',
        path: 'system',
        component: system
      },
      {
        name: 'Insight',
        path: 'insight',
        component: insight
      },
      {
        name: 'Monitor',
        path: 'monitor',
        component: monitor
      },
      {
        name: 'Setting',
        path: 'setting',
        component: setting
      }]
    }, {
      path: '/demo',
      name: 'demo',
      redirect: { name: 'button' },
      component: demo,
      children: [{
        name: 'button',
        path: 'basic',
        component: button
      },
      {
        name: 'naming',
        path: 'naming',
        component: naming
      }
      ]
    }, {
      path: '/hello',
      name: 'welcome',
      component: hello
    }
  ]
})
