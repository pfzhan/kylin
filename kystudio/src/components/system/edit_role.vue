<template>
  <el-form :model="userDetail" label-position="top"  ref="editRoleForm">
    <el-form-item :label="$t('username')">
      <el-tag
       type="primary"
        :closable="false">{{userDetail.username}}
    </el-tag>
    </el-form-item>
    <el-form-item :label="$t('role')">
      <!-- <el-checkbox v-model="userDetail.analyst">{{$t('analyst')}}</el-checkbox>
      <el-checkbox v-model="userDetail.modeler">{{$t('modeler')}}</el-checkbox> -->
      <el-checkbox v-model="userDetail.admin">{{$t('admin')}}</el-checkbox>
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
    'en': {username: 'User Name', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员'}
  }
}
</script>
<style scoped="">
 .input_width{
  width: 36%
 }
</style>
