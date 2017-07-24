<template>
<div class="help_box" id="help">
<el-dropdown trigger="click" @visible-change="dropHelp" @command="handleCommand">
  <span class="el-dropdown-link">
    {{$t('kylinLang.common.help')}} <i class="el-icon-caret-bottom"></i>
  </span>
  <el-dropdown-menu slot="dropdown" >
    <el-dropdown-item command="kapmanual">{{$t('Manual')}}</el-dropdown-item>
    <el-dropdown-item command="kybot">
      <div v-if='!isLogin'>
        <el-button style="color: #fff;font-size: 12px;" type="text" @click="alertkybot=true">{{$t('kybotAuto')}}</el-button>
        <el-switch
          id="header-switch"
          v-model="isopend"
          on-color="#13ce66"
          off-color="#ff4949"
          @change="changeKystaus"
          @click.native.stop
          @openSwitch="openSwitch"
          @closeSwitch="closeSwitch">
        </el-switch>
      </div>
    </el-dropdown-item>
    <el-dropdown-item command="kybotservice">{{$t('kybotService')}}</el-dropdown-item>
    <el-dropdown-item command="updatelicense" style="border-top:solid 1px #444b67">{{$t('updateLicense')}}</el-dropdown-item>
    <el-dropdown-item command="aboutkap" >{{$t('aboutKap')}}</el-dropdown-item>
  </el-dropdown-menu>
</el-dropdown>


<a :href="url" target="_blank"></a>
<el-dialog v-model="aboutKapVisible" :title="$t('aboutKap')" id="about-kap">
  <about_kap :about="serverAbout" :aboutKapVisible="aboutKapVisible">
  </about_kap>
</el-dialog>
<el-dialog id="login-kybotAccount" v-model="kyBotUploadVisible" :title="$t('signIn')" size="tiny" @close="resetLoginKybotForm">
  <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginOpenKybot"></login_kybot>
</el-dialog>
<el-dialog v-model="infoKybotVisible" :title="$t('kybotAuto')" size="tiny">
  <start_kybot @closeStartLayer="closeStartLayer" @openSwitch="openSwitch" :propAgreement="infoKybotVisible"></start_kybot>
</el-dialog>
<el-dialog v-model="alertkybot" :title="$t('autoUpload')" size="tiny">
  <div v-if="$lang=='en'"  >
    <div class="ksd-left">By analyzing diagnostic package, <a href='https://kybot.io/'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly</div>
    <el-button type="primary" @click="alertkybot = false">{{$t('ok')}}</el-button>
  </span>
  </div>
  <div v-if="$lang=='zh-cn'" >
    <div class="ksd-left"><a href="https://kybot.io/#/home?src=kap240">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传</div>
    <el-button type="primary" @click="alertkybot = false">{{$t('ok')}}</el-button>
  </span>
  </div>
</el-dialog>

  <el-dialog :title="$t('license')" v-model="updateLicenseVisible" size="small">
    <update_license ref="licenseEnter" :updateLicenseVisible="updateLicenseVisible" v-on:validSuccess="licenseValidSuccess"></update_license>
    <div slot="footer" class="dialog-footer">
      <el-button @click="updateLicenseVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" :loading="loadCheck" @click="licenseForm">{{$t('save')}}</el-button>
    </div>
  </el-dialog>
</div>
</template>
<script>
  import { mapActions } from 'vuex'

  import aboutKap from '../common/about_kap.vue'
  import loginKybot from '../common/login_kybot.vue'
  import startKybot from '../common/start_kybot.vue'
  import updateLicense from '../user/license'
  import { handleSuccess, handleError } from '../../util/business'

  export default {
    name: 'help',
    props: ['isLogin'],
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
        switchTimer: 0,
        alertkybot: false,
        updateLicenseVisible: false,
        loadCheck: false
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
        getAgreement: 'GET_AGREEMENT',
        saveLicenseContent: 'SAVE_LICENSE_CONTENT',
        saveLicenseFile: 'SAVE_LICENSE_FILE'
      }),
      handleCommand (val) {
        var _this = this
        if (val === 'kapmanual') {
          this.url = 'http://manual.kyligence.io/'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'kybotservice') {
          this.url = 'https://kybot.io/#/home?src=kap240'
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
        } else if (val === 'updatelicense') {
          this.updateLicenseVisible = true
        } else if (val === 'kybot') {
          // 需要先检测有没有登录 待修改
          // this.kyBotUploadVisible = true
          // if (_this.isopend) {
          //   return
          // }
          // this.checkLogin(() => {
          //   this.getStatus(true)
          // })
        }
      },
      dropHelp (s) {
        if (s) {
          this.getStatus()
        }
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
            if (!data) {
              this.kyBotUploadVisible = true
              this.isopend = false
            } else {
              callback() // off -> on 先检测登录状态 没有登录则弹登录 ； 否则直接开启
            }
          }, (errResp) => {
            handleError(errResp)
          })
        })
      },
      // 获取同意协议
      getAgreementInfo () {
        this.getAgreement().then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (!data) { // 没有同意过协议 开协议层
              this.infoKybotVisible = true
              this.isopend = false
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
            handleError(errResp)
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
      },
      licenseForm: function () {
        this.$refs['licenseEnter'].$emit('licenseFormValid')
      },
      licenseValidSuccess: function (license) {
        this.loadCheck = true
        if (license.useFile) {
          let formData = new FormData()
          formData.append('file', license.file[0].raw)
          this.saveLicenseFile(formData).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              this.updateLicenseVisible = false
              this.loadCheck = false
            })
          }, (res) => {
            handleError({data: res})
          })
        } else {
          this.saveLicenseContent(license.content).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              this.updateLicenseVisible = false
              this.loadCheck = false
            })
          }, (res) => {
            handleError(res)
          })
        }
        this.loadCheck = false
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
    components: {
      'about_kap': aboutKap,
      'login_kybot': loginKybot,
      'start_kybot': startKybot,
      'update_license': updateLicense
    },
    locales: {
      'en': {autoUpload: 'Auto Upload', usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required', agreeAndOpen: 'agree the protocol and open the automatic service', kybotAuto: 'KyBot Auto Upload', openSuccess: 'open successfully', closeSuccess: 'close successfully', Manual: 'KAP Manual', kybotService: 'KyBot Service', updateLicense: 'Update License', aboutKap: 'About KAP', kybot: "By analyzing diagnostic package, <a href='https://kybot.io/#/home?src=kap240'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly.", signIn: 'Kyligence Account | Sign In', ok: 'OK', cancel: 'Cancel', save: 'Save', license: 'Update License'},
      'zh-cn': {autoUpload: '自动上传', usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空', agreeAndOpen: '同意协议并开启自动服务', kybotAuto: 'KyBot自动上传', openSuccess: '成功开启', closeSuccess: '成功关闭', Manual: 'KAP手册', kybotService: 'KyBot服务', updateLicense: '更新许可证', aboutKap: '关于KAP', kybot: '<a href="https://kybot.io/#/home?src=kap240">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传', signIn: 'Kyligence 帐号 | 登录', ok: '确定', cancel: '取消', save: '保存', license: '更新许可证'}
    }
  }
</script>
<style lang="less">
  @import '../../less/config.less';
  .help_box {
    line-height: 30px;
    .el-dialog--small{
      width: 700px;
    }
    .el-dropdown {
  	cursor: pointer;
  	svg{
  	  vertical-align: middle;
  	}
    }
    .el-dialog__header {padding:0 20px;text-align:left;}
    .el-dialog__title {color:#red;font-size:14px;}
    .el-dialog__body {padding:20px 50px;}
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
  #about-kap{
    .el-dialog__header{
      height: 55px;
      line-height: 55px;
    }
    .el-icon-close{
      margin-top: 15px;
    }
    .header{
      margin-top: 20px;
      border-color: #424860;
    }
    .container{
      border-color: #424860;
    }
    .buttonLink{
      width: 300px!important;
      display: block;
      margin: 0 auto;
      background: @base-color;
    }
  }
  #header-switch{
    transform: scale(0.9);
  }
  #login-kybotAccount{
    .el-dialog__header{
      height: 50px;
      line-height: 50px;
    }
    .el-dialog__body{
      .el-form{
        margin-top: 30px;
      }
      .el-input{
        padding: 0;
      }
    }
  }
</style>
