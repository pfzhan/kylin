<template>
  <div class="paddingbox ksd-common-tab">
    <el-tabs v-model="activeName" type="card" >
      <el-tab-pane :label="$t('system')" name="system">
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
  }
}
</script>
<style scoped="">

</style>
