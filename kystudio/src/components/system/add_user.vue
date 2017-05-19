<template>
  <el-form :model="newUser" label-position="top" :rules="rules"  ref="addUserForm">
    <el-form-item :label="$t('username')" prop="username">
      <el-input  v-model="newUser.username"></el-input>
    </el-form-item>
    <el-form-item :label="$t('password')" prop="password">
      <el-input type="password" v-model="newUser.password"></el-input>
    </el-form-item>
    <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword">
      <el-input type="password" v-model="newUser.confirmPassword"></el-input>
    </el-form-item>
    <el-form-item :label="$t('role')">
      <el-checkbox v-model="newUser.analyst">{{$t('analyst')}}</el-checkbox>
      <el-checkbox v-model="newUser.modeler">{{$t('modeler')}}</el-checkbox>
      <el-checkbox v-model="newUser.admin">{{$t('admin')}}</el-checkbox>
    </el-form-item>
  </el-form>
</template>
<script>
export default {
  name: 'add_user',
  props: ['newUser'],
  data () {
    return {
      rules: {
        username: [
        { required: true, message: '', trigger: 'change' }
        ],
        password: [
        { required: true, message: '', trigger: 'change' },
        {validator: this.validate, trigger: 'blur'}
        ],
        confirmPassword: [
        { required: true, message: '', trigger: 'change' },
        {validator: this.validatePass, trigger: 'blur'}
        ]
      }
    }
  },
  methods: {
    validate: function (rule, value, callback) {
      if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(this.newUser.password)) {
        callback(new Error(this.$t('tip_password_unsafe')))
      } else {
        callback()
      }
    },
    validatePass: function (rule, value, callback) {
      if (value !== this.newUser.password) {
        callback(new Error(this.$t('passwordConfirm')))
      } else {
        callback()
      }
    }
  },
  created () {
    let _this = this
    this.$on('addUserFormValid', (t) => {
      _this.$refs['addUserForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', this.newUser)
        }
      })
    })
  },
  locales: {
    'en': {username: 'Username', password: 'Password', confirmNewPassword: 'Confirm new password', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', tip_password_unsafe: 'The password should contain at least one numbers, letters and special characters.', passwordConfirm: 'Password and confirm password are not the same.'},
    'zh-cn': {username: '用户名', password: '密码', confirmNewPassword: '确认密码', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', tip_password_unsafe: '密码包含至少一个数字、字母及特殊字符.', passwordConfirm: '两次密码不一致, 请检查'}
  }
}
</script>
<style scoped="">
</style>
