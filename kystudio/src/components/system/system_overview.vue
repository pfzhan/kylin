<template>
  <div class="paddingbox ksd-common-tab">
    <el-tabs v-model="activeName" type="card"  @tab-click="handleClick">
      <el-tab-pane :label="$t('system')" name="config">
        <system></system>
      </el-tab-pane>
      <el-tab-pane :label="$t('user')" name="user">
       <users></users>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import users from './users_list'
import system from './system'
export default {
  data () {
    return {
      activeName: 'system'
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
  }
}
</script>
<style scoped="">

</style>
