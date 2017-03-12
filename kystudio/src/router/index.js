import Vue from 'vue'
import Router from 'vue-router'
import hello from '../components/hello'
import demo from '../components/demo'
import button from '../components/demo/button'
import naming from '../components/demo/naming'
import topLeftRightView from '../components/layout/layout_left_right_top'

import dashbord from '../components/dashbord'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'default',
      component: topLeftRightView,
      children: [{
        name: 'Dashbord',
        path: 'dashbord',
        component: dashbord
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
    }, {
      path: '/model',
      name: 'model',
      component: topLeftRightView,
      children: [{
        path: 'list'
      }]
    }
  ]
})
