<template>
  <div class="login_kybot">
    <el-form :model="kyBotAccount" :rules="rules" ref="loginKybotForm" >
      <el-form-item prop="username">
        <el-input v-model="kyBotAccount.username" :placeholder="$t('kylinLang.common.username')"></el-input>
      </el-form-item>
      <el-form-item prop="password">
        <el-input v-model="kyBotAccount.password" type="password" :placeholder="$t('kylinLang.common.password')"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button @click="loginKyBot" :loading="loginLoading" class="btn-loginKybot">{{$t('login')}}</el-button>
      </el-form-item>
    </el-form>
    <p class="no-account"><span style="color: rgba(255,255,255,0.6);">{{$t('noAccount')}}?</span> <a @click="signUp" target="_blank">{{$t('singUp')}}</a></p>
  </div>
</template>
<script>
  import { mapActions } from 'vuex'
  import { handleSuccess, handleError } from '../../util/business'
  export default {
    name: 'help',
    data () {
      return {
        rules: {
          username: [
            { trigger: 'blur', validator: this.validateUserName }
          ],
          password: [
            { trigger: 'blur', required: true, message: this.$t('noUserPwd') }
          ]
        },
        kyBotAccount: {
          username: '',
          password: ''
        },
        loginLoading: false
      }
    },
    methods: {
      ...mapActions({
        getKybotAccount: 'GET_KYBOT_ACCOUNT',
        loginKybot: 'LOGIN_KYBOT',
        getKyStatus: 'GET_KYBOT_STATUS',
        startKybot: 'START_KYBOT',
        stopKybot: 'STOP_KYBOT',
        getAgreement: 'GET_AGREEMENT'
      }),
      validateUserName (rule, value, callback) {
        if (value === '') {
          callback(new Error(this.$t('usernameEmpty')))
        } else {
          callback()
        }
      },
      // 开启自动上传服务
      startService () {
        this.loginLoading = true
        this.startKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            this.loginLoading = false
            if (data) {
              this.$message({
                type: 'success',
                message: this.$t('openSuccess')
              })
              // this.$emit('openSwitch')
              this.$emit('closeLoginForm')
            }
          })
        })
      },
      // 登录kybot
      loginKyBot () {
        this.$refs['loginKybotForm'].validate((valid) => { // 表单验证通过之后
          if (valid) {
            this.loginLoading = true
            let param = {
              username: this.kyBotAccount.username,
              password: this.kyBotAccount.password
            }
            this.loginKybot(param).then((result) => { // 登录
              handleSuccess(result, (data, code, msg) => {
                this.loginLoading = false
                // A首先获取有没有开启过自动上传的服务，开启了则更新switch的按钮状态其他什么都不做
                // B否则
                //  a)检测有没有同意过协议 ：如果没有同意弹出同意并开启自动上传的层
                //  b)如果已经同意过协议则直接发送开启自动服务
                // A
                if (data) {
                  this.$emit('closeLoginOpenKybot')
                  this.$message({
                    type: 'success',
                    message: this.$t('openSuccess')
                  })
                  this.getKyStatus().then((res) => {
                    handleSuccess(res, (data, code, status, msg) => {
                      if (data) { // 开启了 则开启
                        // this.$emit('openSwitch')
                        this.$emit('closeLoginForm')
                      } else {
                        // a
                        this.$emit('closeLoginForm')
                        this.getAgreement().then((res) => {
                          handleSuccess(res, (data, code, status, msg) => {
                            if (!data) { // 没有同意过协议 开协议层
                              this.$emit('closeLoginOpenKybot')
                            }
                          })
                        })
                      }
                    }, (errResp) => {
                      handleError(errResp)
                    })
                  })
                } else {
                  // this.$message({
                  //   type: 'error',
                  //   duration: 0,  // 不自动关掉提示
                  //   showClose: true,    // 给提示框增加一个关闭按钮
                  //   message: msg
                  // })
                  handleError(result)
                }
              }, (res) => {
                handleError(res)
                this.loginLoading = false
              })
            })
          }
        })
      },
      signUp () {
        let lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'zh-cn'
        var windowA = window.open()
        windowA.location.href = 'https://account.kyligence.io/#/extra-signup?lang=' + lang
      }
    },
    locales: {
      'en': {singUp: 'Sign Up Now', noAccount: "Don't have an account", usernameEmpty: 'Please enter username', noUserPwd: 'Password required', login: 'Login', openSuccess: 'Login Success'},
      'zh-cn': {singUp: '立即注册', noAccount: '还没有账号', usernameEmpty: '请输入用户名', noUserPwd: '密码不能为空', login: '登录', openSuccess: '登录成功'}
    }
  }
</script>
<style lang="less">
.login_kybot{
  .el-input{
    margin-right: 0;
  }
  .btn-loginKybot {
    width: 100%;
    margin: 0;
    background: #35a8fe;
    color: #fff;
  }
  .no-account {
    height: 20px;
    line-height: 20px;
    margin:-10px 0 30px 0;
    text-align: left;
    color: #1c71d8;
  }
}
</style>
