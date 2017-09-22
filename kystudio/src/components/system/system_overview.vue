<template>
  <div class="system_box paddingbox ksd-common-tab">
    <el-tabs v-model="activeName" type="card"  @tab-click="handleClick">
      <el-tab-pane :label="$t('system')" name="config" >
        <system v-if="isAdmin"></system>
      </el-tab-pane>
      <el-tab-pane :label="$t('user')" name="user">
       <users :fromLogin="fromLogin"></users>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import users from './users_list'
import system from './system'
import { hasRole } from '../../util/business'
export default {
  data () {
    return {
      activeName: 'system',
      fromLogin: {needReset: false}
    }
  },
  components: {
    'system': system,
    'users': users
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
    }
  },
  mounted () {
    var hash = location.hash
    var subRouter = hash.replace(/.*\/(.*)$/, '$1')
    if (subRouter === 'user' || subRouter === 'config') {
      this.activeName = subRouter
    }
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
