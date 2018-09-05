<template>
  <div class="reset-password">
    <el-form :model="userDetail" label-position="top" :rules="rules"  ref="resetPasswordForm">
      <el-form-item :label="$t('username')">
        <el-input size="medium" v-model="curUser.username" :disabled="true"></el-input>
      </el-form-item>
      <el-form-item :label="$t('oldPassword')" prop="oldPassword" v-if="!isAdmin">
        <el-input size="medium" type="password" v-model="userDetail.oldPassword"></el-input>
      </el-form-item>
      <el-form-item :label="$t('password')" prop="password">
        <el-input size="medium" type="password" v-model="userDetail.password"></el-input>
      </el-form-item>
      <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword">
        <el-input size="medium" type="password" v-model="userDetail.confirmPassword"  name="confirmPassword"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import { hasRole } from '../../util/business'
export default {
  name: 'reset_password',
  props: ['curUser', 'show'],
  data () {
    return {
      userDetail: {
        password: '',
        confirmPassword: '',
        oldPassword: ''
      },
      rules: {
        oldPassword: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ],
        password: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' },
        {validator: this.validate, trigger: 'blur'}
        ],
        confirmPassword: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' },
        {validator: this.validatePass, trigger: 'blur'}
        ]
      }
    }
  },
  methods: {
    resetData: function () {
      this.$set(this.userDetail, 'confirmPassword', '')
      this.$set(this.userDetail, 'password', '')
      this.$set(this.userDetail, 'oldPassword', '')
    },
    validate: function (rule, value, callback) {
      if (this.userDetail.password.length < 8) {
        callback(new Error(this.$t('passwordLength')))
      } else if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(this.userDetail.password)) {
        callback(new Error(this.$t('kylinLang.user.tip_password_unsafe')))
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
      console.log(hasRole(this, 'ROLE_ADMIN'))
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  // watch: {
  //   'show' (v) {
  //     if (!v) {
  //       console.log(v)
  //       this.resetData()
  //     }
  //   }
  // },
  created () {
    let _this = this
    this.$on('resetPasswordFormValid', (t) => {
      _this.$refs['resetPasswordForm'].validate((valid) => {
        if (valid) {
          this.userDetail.username = this.curUser.username
          _this.$emit('validSuccess', this.userDetail)
        }
      })
    })
  },
  locales: {
    'en': {username: 'User Name', role: 'Role', password: 'Password', oldPassword: 'Old Password', confirmNewPassword: 'Confirm new password', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', passwordConfirm: 'Password and confirm password are not the same.', passwordLength: 'The minimal length of password is 8'},
    'zh-cn': {username: '用户名', role: '角色', password: '新密码', oldPassword: '旧密码', confirmNewPassword: '确认密码', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', passwordConfirm: '两次密码不一致, 请检查', passwordLength: '密码长度至少8位'}
  }
}
</script>
<style lang="less">
.reset-password {
  .el-input__inner {
    width: 390px;
  }
}
</style>
