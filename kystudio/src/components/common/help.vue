<template>
<div class="help_box">
<el-dropdown @command="handleCommand">
  <span class="el-dropdown-link">
    {{$t('kylinLang.common.help')}} <icon name="angle-down"></icon>
  </span>
  <el-dropdown-menu slot="dropdown">
    <el-dropdown-item command="kapmanual">{{$t('Manual')}}</el-dropdown-item>
    <el-dropdown-item command="kybot">
      {{$t('kybotAuto')}}
      <el-switch
        v-model="isopend"
        on-color="#13ce66"
        off-color="#ff4949"
        @change="changeKystaus"
        @click.native.stop
        @openSwitch="openSwitch"
        @closeSwitch="closeSwitch">
      </el-switch>

    </el-dropdown-item>
    <el-dropdown-item command="kybotservice">{{$t('kybotService')}}</el-dropdown-item>
    <el-dropdown-item command="aboutkap">{{$t('aboutKap')}}</el-dropdown-item>
  </el-dropdown-menu>
</el-dropdown>


<a :href="url" target="_blank"></a>
<el-dialog v-model="aboutKapVisible" title="关于KAP">
  <about_kap :about="serverAbout">
  </about_kap>
</el-dialog>
<el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" size="tiny" @close="resetLoginKybotForm">
  <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginOpenKybot"></login_kybot>
  <!-- <el-form :model="kyBotAccount" :rules="rules" ref="loginKybotForm" >
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
  <p class="no-account">No account? <a href="javascript:;">Sign up</a> now</p> -->
</el-dialog>
<el-dialog v-model="infoKybotVisible" :title="$t('kybotAuto')" size="tiny">
  <start_kybot @onStart="closeStartLayer"></start_kybot>
  <!-- <p>KyBot通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。</p>
  <p>
    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">我已阅读并同意遵守《KyBot用户协议》</el-checkbox>
  </p>
  <el-button @click="startService" :loading="startLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeAndOpen')}}</el-button> -->
</el-dialog>
</div>
</template>
<script>
  import { mapActions } from 'vuex'

  import aboutKap from '../common/about_kap.vue'
  import loginKybot from '../common/login_kybot.vue'
  import startKybot from '../common/start_kybot.vue'
  import { handleSuccess, handleError } from '../../util/business'

  export default {
    name: 'help',
    data () {
      return {
        aboutKapVisible: false,
        url: '',
        kyBotUploadVisible: false,
        kyBotAccount: {
          username: '',
          password: ''
        },
        infoKybotVisible: false,
        // agreeKyBot: false,
        isopend: false, // 是否已开启
        // rules: {
        //   username: [
        //     { trigger: 'blur', validator: this.validateUserName }
        //   ],
        //   password: [
        //     { trigger: 'blur', required: true, message: this.$t('noUserPwd') }
        //   ]
        // },
        // loginLoading: false,
        startLoading: false
      }
    },
    methods: {
      ...mapActions({
        getAboutKap: 'GET_ABOUTKAP',
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
      handleCommand (val) {
        var _this = this
        if (val === 'kapmanual') {
          this.url = 'http://manual.kyligence.io/'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'kybotservice') {
          this.url = 'https://kybot.io/#/home'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'aboutkap') {
          // 发请求 kap/system/license
          this.getAboutKap().then((result) => {
          }, (resp) => {
            console.log(resp)
          })
          this.aboutKapVisible = true
        } else if (val === 'kybot') {
          // 需要先检测有没有登录 待修改
          this.kyBotUploadVisible = true
        }
      },
      closeLoginForm () {
        this.kyBotUploadVisible = false
        // this.infoKybotVisible = true
      },
      closeLoginOpenKybot () {
        this.kyBotUploadVisible = false
        this.infoKybotVisible = true
      },
      // // 登录kybot
      // loginKyBot () {
      //   let _this = this
      //   this.loginLoading = true
      //   let param = {
      //     username: this.kyBotAccount.username,
      //     password: this.kyBotAccount.password
      //   }
      //   this.loginKybot(param).then((result) => {
      //     handleSuccess(result, (data, code, status, msg) => {
      //       console.log('登录成功', result)
      //       _this.kyBotUploadVisible = false
      //       _this.infoKybotVisible = true
      //       _this.loginLoading = false
      //     }, (res) => {
      //       handleError(res, (data, code, status, msg) => {
      //         this.$message({
      //           type: 'error',
      //           message: msg
      //         })
      //       })
      //       _this.loginLoading = false
      //     })
      //   })
      // },
      // 同意协议并开启自动服务
      startService () {
        // 同意协议并开启自动服务
        // this.startLoading = true
        this.startKybot().then((resp) => {
          console.log('开启 1：', resp)
          handleSuccess(resp, (data, code, status, msg) => {
            console.log('开启2 ：', data)
            if (data) {
              this.$message({
                type: 'success',
                message: this.$t('openSuccess')
              })
            }
          })
        })
      },
      stopService () {
        // this.startLoading = true
        console.log('stop stop ', this.stopKybot)
        this.stopKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            console.log('关闭2 ：', data)
            if (data) {
              this.$message({
                type: 'success',
                message: this.$t('closeSuccess')
              })
            }
          })
        })
      },
      resetLoginKybotForm () {
        this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
      },
      // 改变kybot自动上传状态
      changeKystaus (status) {
        console.log('new status :', status)
        if (status) { // 开启
          this.startService()
        } else { // 关闭
          // 需要先检测有没有登录  待修改
          this.kyBotUploadVisible = true
          this.stopService()
        }
      },
      closeStartLayer () {
        this.infoKybotVisible = false
      },
      // 开启switch事件
      openSwitch () {
        this.isopend = true
      },
      // 关闭switch事件
      closeSwitch () {
        this.isopend = false
      }
    },
    computed: {
      serverAbout () {
        return this.$store.state.system.serverAboutKap
      },
      kyStatus () {
        return this.$store.state.kybot.kyStatus
      }
    },
    created () {
      // 获取是否已开启自动上传kybot switch 按钮切换状态
      this.getKyStatus().then((res) => {
        console.log('res', res)
        handleSuccess(res, (data, code, status, msg) => {
          console.log('data 状态;', data)
          if (!data) {
            console.log('1:', data)
            this.isopend = false
          } else {
            this.isopend = true
            console.log('2:', data)
          }
        }, (errResp) => {
          handleError(errResp, (data, code, status, msg) => {
            console.log(data, code, status, msg)
            if (status === 400) {
              this.$message({
                type: 'success',
                message: msg
              })
            }
          })
        })
      })
      // this.getKybotAccount().then((res) => {
      //   console.log('switch status res ==', res)
      //   handleSuccess(res, (data, code, status, msg) => {
      //     console.log('data 状态;', data)
      //     if (!data) {
      //       console.log('1:', data)
      //       this.isopend = false
      //     } else {
      //       this.isopend = true
      //       console.log('2:', data)
      //     }
      //   }, (res) => {
      //     handleError(res, (data, code, status, msg) => {
      //       console.log(data, code, status, msg)
      //       if (status === 400) {
      //         this.$message({
      //           type: 'success',
      //           message: msg
      //         })
      //       }
      //     })
      //   })
      // }).then(() => { // 检测是否已经开启自动上传
      //   this.getKyStatus().then((res) => {
      //     console.log('')
      //   })
      // })
    },
    components: {
      'about_kap': aboutKap,
      'login_kybot': loginKybot,
      'start_kybot': startKybot
    },
    locales: {
      'en': {usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required', agreeAndOpen: 'agree the protocol and open the automatic service', kybotAuto: 'KyBot Auto Upload', openSuccess: 'open successfully', closeSuccess: 'close successfully'},
      'zh-cn': {usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空', agreeAndOpen: '同意协议并开启自动服务', kybotAuto: 'KyBot 自动上传', openSuccess: '成功开启', closeSuccess: '成功关闭'}
    }
  }
</script>
<style lang="less">
.help_box{
  line-height: 30px;
  text-align: left;
  .el-dropdown{
	cursor: pointer;
	svg{
	  vertical-align: middle;
	}
  }	
  .el-dialog__header {height:70px;line-height:70px;padding:0 20px;text-align:left;}
  .el-dialog__title {color:#red;font-size:14px;}
  .el-dialog__body {padding:0 50px;}
  
}

</style>
