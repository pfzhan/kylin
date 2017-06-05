<template>
  <div class="reset_password">
    <el-form :model="userDetail" label-position="top" :rules="rules"  ref="resetPasswordForm">
      <el-form-item :label="$t('username')">
        <el-alert
         type="info"
          :title="userDetail.username"
          :closable="false">
      </el-alert>
      </el-form-item>
      <el-form-item :label="$t('role')" v-if="!isAdmin">
        <el-tag type="success" v-if="userDetail.admin === true">{{$t('admin')}}</el-tag>
        <el-tag type="primary" v-if="userDetail.modeler === true">{{$t('modeler')}}</el-tag>
        <el-tag v-if="userDetail.analyst === true">{{$t('analyst')}}</el-tag>
      </el-form-item>
      <el-form-item :label="$t('oldPassword')" prop="oldPassword" v-if="">
        <el-input type="password" v-model="userDetail.oldPassword"></el-input>
      </el-form-item>
      <el-form-item :label="$t('password')" prop="password">
        <el-input type="password" v-model="userDetail.password"></el-input>
      </el-form-item>
      <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword">
        <el-input type="password" v-model="userDetail.confirmPassword"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { hasRole } from '../../util/business'
export default {
  name: 'reset_password',
  props: ['userDetail'],
  data () {
    return {
      rules: {
        oldPassword: [
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
      if (this.userDetail.password.length < 8) {
        callback(new Error(this.$t('passwordLength')))
      } else if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(this.userDetail.password)) {
        callback(new Error(this.$t('tip_password_unsafe')))
      } else {
        callback()
      }
    },
    validatePass: function (rule, value, callback) {
      if (value !== this.userDetail.password) {
        callback(new Error(this.$t('passwordConfirm')))
      } else {
        callback()
      }
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    let _this = this
    this.$on('resetPasswordFormValid', (t) => {
      _this.$refs['resetPasswordForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', this.userDetail)
        }
      })
    })
  },
  locales: {
    'en': {username: 'User Name', role: 'Role', password: 'Password', oldPassword: 'Old Password', confirmNewPassword: 'Confirm new password', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', tip_password_unsafe: 'The password should contain at least one numbers, letters and special characters.', passwordConfirm: 'Password and confirm password are not the same.', passwordLength: 'the length of password is at least 8'},
    'zh-cn': {username: '用户名', role: '角色', password: '新密码', oldPassword: '旧密码', confirmNewPassword: '确认密码', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', tip_password_unsafe: '密码包含至少一个数字、字母及特殊字符.', passwordConfirm: '两次密码不一致, 请检查', passwordLength: '密码长度至少8位'}
  }
}
</script>
<style lang="less">
.reset_password {

}
</style>
