<template>
  <div class="login_kybot">
    <el-form :model="kyBotAccount" :rules="rules" ref="loginKybotForm" >
      <el-form-item prop="username">
        <el-input v-model="kyBotAccount.username" placeholder="username"></el-input>
      </el-form-item>
      <el-form-item prop="password">
        <el-input v-model="kyBotAccount.password" placeholder="password"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button @click="loginKyBot" :loading="loginLoading" class="btn-loginKybot">Login</el-button>  
      </el-form-item>
    </el-form>
    <p class="no-account">{{$t('noAccount')}}? <a @click="signUp" target="_blank">{{$t('singUp')}}</a></p>
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
        stopKybot: 'STOP_KYBOT'
      }),
      validateUserName (rule, value, callback) {
        console.log('vallue', value)
        if (value === '') {
          callback(new Error(this.$t('usernameEmpty')))
        } else {
          callback()
        }
      },
      // 登录kybot
      loginKyBot () {
        this.$refs['loginKybotForm'].validate((valid) => {
          if (valid) {
            this.loginLoading = true
            let param = {
              username: this.kyBotAccount.username,
              password: this.kyBotAccount.password
            }
            this.loginKybot(param).then((result) => {
              handleSuccess(result, (data, code, status, msg) => {
                console.log('登录成功', result)
                // _this.kyBotUploadVisible = false
                // _this.infoKybotVisible = true
                this.loginLoading = false
                this.$emit('onLogin')
                // 检测有没有开启
                this.getKyStatus().then((resp) => {
                  // if (!resp.data) {// 未开启
                  // }
                })
              }, (res) => {
                handleError(res, (data, code, status, msg) => {
                  this.$message({
                    type: 'error',
                    message: msg
                  })
                })
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
      'en': {singUp: 'Sign Up Now', noAccount: "Don't have an account"},
      'ch-zh': {singUp: '立即注册', noAccount: '还没有账号'}
    }
  }
</script>
<style lang="less">
.login_kybot{
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
  }
}
</style>
