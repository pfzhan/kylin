<template>
  <div class="add_user">
    <el-form :model="newUser" label-position="top" :rules="rules"  ref="addUserForm">
      <el-form-item :label="$t('username')" prop="username">
        <el-input size="medium" v-model="newUser.username"></el-input>
      </el-form-item>
      <el-form-item :label="$t('password')" prop="password">
        <el-input size="medium" type="password" v-model="newUser.password"></el-input>
      </el-form-item>
      <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword">
        <el-input size="medium" type="password" v-model="newUser.confirmPassword"></el-input>
      </el-form-item>
      <el-form-item :label="$t('role')">
        <el-radio-group v-model="newUser.admin">
          <el-radio :label="true">{{$t('admin')}}</el-radio>
          <el-radio :label="false">{{$t('user')}}</el-radio>
        </el-radio-group>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
export default {
  name: 'add_user',
  props: ['newUser'],
  data () {
    return {
      rules: {
        username: [
        { required: true, message: this.$t('usernameEmpty'), trigger: 'change' },
        { validator: this.validateUser, trigger: 'blur' }
        ],
        password: [
        { required: true, message: this.$t('passwordEmpty'), trigger: 'change' },
        {validator: this.validate, trigger: 'blur'}
        ],
        confirmPassword: [
        { required: true, message: this.$t('passwordEmpty'), trigger: 'change' },
        {validator: this.validatePass, trigger: 'blur'}
        ]
      }
    }
  },
  methods: {
    validateUser (rule, value, callback) {
      if (!value) {
        callback(new Error(this.$t('usernameEmpty')))
      } else {
        callback()
      }
    },
    validate: function (rule, value, callback) {
      if (!value) {
        callback(new Error(this.$t('passwordEmpty')))
      } else if (value.length < 8) {
        callback(new Error(this.$t('passwordLength')))
      } else if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(this.newUser.password)) {
        callback(new Error(this.$t('kylinLang.user.tip_password_unsafe')))
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
    'en': {username: 'User Name', password: 'Password', confirmNewPassword: 'Confirm new password', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Administrator', user: 'User', passwordConfirm: 'Password and confirm password are not the same.', usernameEmpty: 'user name required', passwordEmpty: 'password required', passwordLength: 'the length of password is at least 8'},
    'zh-cn': {username: '用户名', password: '密码', confirmNewPassword: '确认密码', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '系统管理员', user: '普通用户', passwordConfirm: '两次密码不一致, 请检查', usernameEmpty: '用户名不能为空', passwordEmpty: '密码不能为空', passwordLength: '密码长度至少8位'}
  }
}
</script>
<style lang="less">
.add_user {
}
</style>
