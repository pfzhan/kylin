<template>
  <el-form :model="userDetail" label-position="top"  ref="editRoleForm" class="edit-role">
    <el-form-item :label="$t('username')">
      <el-input size="medium" v-model="userDetail.username" :disabled="true"></el-input>
    </el-form-item>
    <el-form-item :label="$t('role')">
      <el-radio-group v-model="userDetail.admin">
        <el-radio :label="true">{{$t('admin')}}</el-radio>
        <el-radio :label="false">{{$t('user')}}</el-radio>
      </el-radio-group>
    </el-form-item>
  </el-form>
</template>
<script>
export default {
  name: 'edit_role',
  props: ['userDetail'],
  created () {
    let _this = this
    this.$on('editRoleFormValid', (t) => {
      _this.$emit('validSuccess', this.userDetail)
    })
  },
  locales: {
    'en': {username: 'User Name', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Administrator', user: 'User'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '系统管理员', user: '普通用户'}
  }
}
</script>
<style lang="less">
.edit-role {
}
</style>

