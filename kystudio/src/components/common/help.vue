<template>
<div class="help_box">
<el-dropdown @command="handleCommand">
  <span class="el-dropdown-link">
    Help <icon name="angle-down"></icon>
  </span>
  <el-dropdown-menu slot="dropdown">
    <el-dropdown-item command="kapmanual">KAP Manual</el-dropdown-item>
    <el-dropdown-item command="kybot">
      KyBot自动上传
      <el-switch
        v-if="switchVisible"
        v-model="isopend"
        on-color="#13ce66"
        off-color="#ff4949"
        @change="changeKystaus"
        @click.native.stop>
      </el-switch>

    </el-dropdown-item>
    <el-dropdown-item command="kybotservice">KyBot Service</el-dropdown-item>
    <el-dropdown-item command="aboutkap">About KAP</el-dropdown-item>
  </el-dropdown-menu>
</el-dropdown>


<a :href="url" target="_blank"></a>
<el-dialog v-model="aboutKapVisible" title="关于KAP">
  <about_kap :about="serverAbout">
  </about_kap>
</el-dialog>
<el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" size="tiny" @close="resetLoginKybotForm">
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
  <p class="no-account">No account? <a href="javascript:;">Sign up</a> now</p>
</el-dialog>
<el-dialog v-model="infoKybotVisible" title="KyBot自动上传" size="tiny">
  <p>KyBot通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。</p>
  <p>
    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">我已阅读并同意遵守《KyBot用户协议》</el-checkbox>
  </p>
  <el-button @click="afterAgree" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeAndOpen')}}</el-button>
</el-dialog>
</div>
</template>
<script>
  import { mapActions } from 'vuex'

  import aboutKap from '../common/about_kap.vue'
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
        agreeKyBot: false,
        isopend: false, // 是否已开启
        switchVisible: false, // 是否显示switch 按钮
        rules: {
          username: [
            { trigger: 'blur', validator: this.validateUserName }
          ],
          password: [
            { trigger: 'blur', required: true, message: this.$t('noUserPwd') }
          ]
        },
        loginLoading: false
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
          this.kyBotUploadVisible = true
        }
      },
      // 登录kybot
      loginKyBot () {
        let _this = this
        this.loginLoading = true
        let param = {
          username: this.kyBotAccount.username,
          password: this.kyBotAccount.password
        }
        this.loginKybot(param).then((result) => {
          handleSuccess(result, (data, code, status, msg) => {
            console.log('登录成功', result)
            _this.kyBotUploadVisible = false
            _this.infoKybotVisible = true
            _this.loginLoading = false
          }, (res) => {
            handleError(res, (data, code, status, msg) => {
              this.$message({
                type: 'error',
                message: msg
              })
            })
            _this.loginLoading = false
          })
        })
      },
      // 同意协议并开启自动服务
      afterAgree () {
      },
      clickOpen (e) {
        console.log(e)
        e.stopPropagation()
      },
      swicthOpen (e) {
        console.log(e)
        // e.stopPropagation()
      },
      resetLoginKybotForm () {
        this.$refs['loginKybotForm'].resetFields()
      },
      // 改变kybot自动上传状态
      changeKystaus (status) {
        console.log('new status :', status)
        // if (status) { // 开启

        // } else { // 关闭

        // }
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
      let _this = this
      this.getKybotAccount().then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          if (!data) {
            _this.switchVisible = false
          } else {
            _this.switchVisible = true
          }
        }, (res) => {
          handleError(res, (data, code, status, msg) => {
            console.log(data, code, status, msg)
            if (status === 400) {
              this.$message({
                type: 'success',
                message: msg
              })
            }
          })
        })
      }).then(() => { // 检测是否已经开启自动上传
        _this.getKyStatus()
      })
    },
    components: {
      'about_kap': aboutKap
    },
    locales: {
      'en': {usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required'},
      'zh-cn': {usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空'}
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
  .btn-agree {
    display: block;
    margin: 20px auto;
  }
}

</style>
