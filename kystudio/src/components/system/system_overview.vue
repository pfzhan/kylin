<template>
  <div class="system_box paddingbox ksd-common-tab">
     <tab  class="modeltab ksd-common-tab"   :tabslist="editableTabs" v-on:clicktab="checkTab" :active="rootActiveName" v-on:removetab="removeTab" >
        <el-tab-pane :label="$t('kylinLang.common.overview')" name="overview" slot="defaultPane">
          <el-tabs v-model="activeName" class="el-tabs--default"  @tab-click="handleClick">
            <el-tab-pane :label="$t('system')" name="config" >
              <system  v-if="isAdmin && activeName === 'config'"></system>
            </el-tab-pane>
            <el-tab-pane :label="$t('user')" name="user">
             <users :fromLogin="fromLogin" v-if="activeName === 'user'"></users>
            </el-tab-pane>
            <el-tab-pane :label="$t('kylinLang.common.group')" name="group">
             <groups :fromLogin="fromLogin" v-if="activeName === 'group'" v-on:addtab="addTab"></groups>
            </el-tab-pane>
          </el-tabs>
        </el-tab-pane>
       <template scope="props">
        <component :is="props.item.content" :extraoption="props.item.extraoption" :ref="props.item.content"></component>
       </template>
    </tab>
    
  </div>
</template>
<script>
import tab from '../common/tab'
import users from './users_list'
import system from './system'
import groups from './groups'
import { hasRole } from '../../util/business'
export default {
  data () {
    return {
      activeName: 'system',
      fromLogin: {needReset: false},
      rootActiveName: 'overview',
      editableTabs: []
    }
  },
  components: {
    'system': system,
    'users': users,
    'groups': groups,
    'tab': tab
  },
  locales: {
    'en': {user: 'User', system: 'System'},
    'zh-cn': {user: '用户', system: '系统'}
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  methods: {
    handleClick (a) {
      this.$router.push(this.activeName)
    },
    checkTab (name) {
      this.rootActiveName = name
    },
    addTab (groupName, data) {
      let tabs = this.editableTabs
      let hasTab = false
      tabs.forEach((tab, index) => {
        if (tab.name === groupName) {
          hasTab = true
        }
      })
      if (!hasTab) {
        this.editableTabs.push({
          name: groupName,
          title: groupName,
          content: users,
          closable: true,
          extraoption: {
            groupName: groupName,
            userList: data
          }
        })
      }
      this.rootActiveName = groupName
    },
    removeTab (targetName, stayTabName) {
      if (targetName === 'OverView') {
        return
      }
      let tabs = this.editableTabs
      let activeName = this.rootActiveName
      let activeView = this.currentView
      if (activeName === targetName) {
        tabs.forEach((tab, index) => {
          if (stayTabName) {
            if (tab.name === stayTabName) {
              activeName = tabs[index].name
              activeView = tabs[index].content
            }
          } else if (tab.name === targetName) {
            let nextTab = tabs[index + 1] || tabs[index - 1]
            if (nextTab) {
              activeName = nextTab.name
              activeView = nextTab.content
            } else {
              activeName = 'overview'
              activeView = 'system'
            }
          }
        })
      }
      this.rootActiveName = activeName
      this.currentView = activeView
      this.editableTabs = tabs.filter(tab => tab.name !== targetName)
    }
  },
  mounted () {
    var hash = location.hash
    var subRouter = hash.replace(/.*\/(.*)$/, '$1')
    // if (subRouter === 'user' || subRouter === 'config') {
    //   this.activeName = subRouter
    // }
    this.activeName = subRouter
  },
  beforeRouteEnter (to, from, next) {
    if (from.path === '/access/login') {
      next(vm => {
        vm.$set(vm.$store.state.system, 'needReset', true)
      })
    } else {
      next()
    }
  }
}
</script>
<style lang="less">
.system_box{
  .el-tabs__header{
    border-bottom: 1px solid #393e53
  }
  .el-tabs__header{
    margin-left: 0px!important;
    padding-left: 30px;
  }
}
</style>
