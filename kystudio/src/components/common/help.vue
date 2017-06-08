<template>
<div class="help_box">
<el-dropdown @command="handleCommand" @click.native="dropHelp">
  <span class="el-dropdown-link">
    {{$t('kylinLang.common.help')}} <i class="el-icon-caret-bottom"></i>
  </span>
  <el-dropdown-menu slot="dropdown" >
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
<el-dialog v-model="aboutKapVisible" :title="$t('aboutKap')">
  <about_kap :about="serverAbout">
  </about_kap>
</el-dialog>
<el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" size="tiny" @close="resetLoginKybotForm">
  <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginOpenKybot"></login_kybot>
</el-dialog>
<el-dialog v-model="infoKybotVisible" :title="$t('kybotAuto')" size="tiny">
  <start_kybot @closeStartLayer="closeStartLayer" @openSwitch="openSwitch" :propAgreement="infoKybotVisible"></start_kybot>
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
        isopend: false, // 是否已开启
        startLoading: false,
        flag: true,
        switchTimer: 0
      }
    },
    methods: {
      ...mapActions({
        getAboutKap: 'GET_ABOUTKAP',
        getKybotAccount: 'GET_KYBOT_ACCOUNT',
        loginKybot: 'LOGIN_KYBOT',
        getKyStatus: 'GET_KYBOT_STATUS',
        startKybot: 'START_KYBOT',
        stopKybot: 'STOP_KYBOT',
        getAgreement: 'GET_AGREEMENT'
      }),
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
            // console.log(resp)
          })
          this.aboutKapVisible = true
        } else if (val === 'kybot') {
          // 需要先检测有没有登录 待修改
          // this.kyBotUploadVisible = true
          if (_this.isopend) {
            return
          }
          this.checkLogin(() => {
            this.getStatus(true)
          })
        }
      },
      dropHelp () {
        this.getStatus()
      },
      resetLoginKybotForm () {
        this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
      },
      closeLoginForm () {
        this.kyBotUploadVisible = false
      },
      closeLoginOpenKybot () {
        this.kyBotUploadVisible = false
        this.infoKybotVisible = true
      },
      // 同意协议并开启自动服务
      startService () {
        this.startKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            if (data) {
              this.isopend = true
              this.$message({
                type: 'success',
                message: this.$t('openSuccess')
              })
              this.infoKybotVisible = false
            }
          })
        })
      },
      // 关闭服务
      stopService () {
        this.stopKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            if (data) {
              this.isopend = false
              this.$message({
                type: 'success',
                message: this.$t('closeSuccess')
              })
            }
          })
        })
      },
      // 检测登录
      checkLogin (callback) {
        this.getKybotAccount().then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            console.log(data)
            if (!data) {
              this.kyBotUploadVisible = true
              this.isopend = false
            } else {
              callback() // off -> on 先检测登录状态 没有登录则弹登录 ； 否则直接开启
            }
          }, (errResp) => {
            handleError(errResp, (data, code, status, msg) => {
              if (status === 400) {
                this.$message({
                  type: 'success',
                  message: msg
                })
              }
            })
          })
        })
      },
      // 获取同意协议
      getAgreementInfo () {
        this.getAgreement().then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (!data) { // 没有同意过协议 开协议层
              this.infoKybotVisible = true
            } else {
              this.startService()
            }
          })
        })
      },
      // 获取是否开启
      getStatus (showAgreement, callback) {
        if (!this.flag) {
          return
        }
        this.flag = false
        this.getKyStatus().then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (!data) {
              // this.isopend = false
              showAgreement && this.getAgreementInfo()
            } else {
              this.isopend = true
            }
            this.flag = true
          }, (errResp) => {
            handleError(errResp, (data, code, status, msg) => {
              if (status === 400) {
                this.$message({
                  type: 'success',
                  message: msg
                })
              }
            })
            this.flag = true
          })
        })
      },
      // 改变kybot自动上传状态
      changeKystaus (status) {
        if (this.switchTimer) {
          clearTimeout(this.switchTimer)
        }
        this.switchTimer = setTimeout(() => {
          if (status) { // 开启
            // 需要先检测有没有登录
            this.checkLogin(() => {
              this.getStatus(true)
            })
          } else { // 关闭
            this.stopService()
          }
        }, 200)
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
    // watch: ('isopend', () => {
    //   this.isopend = false
    //   alert(3)
    // }),
    // watch: {
    //   isopend (o, n) {
    //     this.checkLogin(() => {
    //       this.getStatus(true)
    //     })
    //   }
    // },
    components: {
      'about_kap': aboutKap,
      'login_kybot': loginKybot,
      'start_kybot': startKybot
    },
    locales: {
      'en': {usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required', agreeAndOpen: 'agree the protocol and open the automatic service', kybotAuto: 'KyBot Auto Upload', openSuccess: 'open successfully', closeSuccess: 'close successfully', Manual: 'KAP Manual', kybotService: 'KyBot Service', aboutKap: 'About KAP'},
      'zh-cn': {usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空', agreeAndOpen: '同意协议并开启自动服务', kybotAuto: 'KyBot自动上传', openSuccess: '成功开启', closeSuccess: '成功关闭', Manual: 'KAP手册', kybotService: 'KyBot服务', aboutKap: '关于KAP'}
    }
  }
</script>
<style lang="less">
  @import '../../less/config.less';
  .help_box {
    line-height: 30px;
    text-align: left;
    .el-dropdown {
  	cursor: pointer;
  	svg{
  	  vertical-align: middle;
  	}
    }
    .el-dialog__header {height:70px;line-height:70px;padding:0 20px;text-align:left;}
    .el-dialog__title {color:#red;font-size:14px;}
    .el-dialog__body {padding:0 50px;}
    .el-dropdown-link{
      color: @fff;
    }
    .el-dropdown-link:hover{
      color: @fff;
    }
    .el-icon-caret-bottom{
      font-size: 12px;
    }
  }
</style>
