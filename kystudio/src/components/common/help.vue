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
    <el-dialog v-model="aboutKapVisible" :title="$t('aboutKap')" id="about-kap" :close-on-click-modal="false">
      <about_kap :about="serverAbout" :aboutKapVisible="aboutKapVisible">
      </about_kap>
    </el-dialog>
    <el-dialog id="login-kybotAccount" v-model="kyBotUploadVisible" :title="$t('signIn')" size="tiny" @close="resetLoginKybotForm" :close-on-click-modal="false">
      <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginOpenKybot"></login_kybot>
    </el-dialog>
    <el-dialog v-model="infoKybotVisible" :title="$t('kybotAuto')" size="tiny" :close-on-click-modal="false">
      <start_kybot @closeStartLayer="closeStartLayer" @openSwitch="openSwitch" :propAgreement="infoKybotVisible"></start_kybot>
    </el-dialog>
    <el-dialog v-model="alertkybot" :title="$t('autoUpload')" size="tiny">
      <div v-if="$lang=='en'"  >
        <div class="ksd-left">By analyzing diagnostic package, <a href='https://kybot.io/'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly</div>
        <el-button type="primary" @click="alertkybot = false">{{$t('ok')}}</el-button>
      </span>
      </div>
      <div v-if="$lang=='zh-cn'" >
        <div class="ksd-left"><a href="https://kybot.io/#/home?src=kap250">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传</div>
        <el-button type="primary" @click="alertkybot = false">{{$t('ok')}}</el-button>
      </span>
      </div>
    </el-dialog>

    <el-dialog :title="$t('license')" v-model="updateLicenseVisible" :close-on-click-modal="false" class="updateKAPLicense" size="tiny">
      <span style="float: left;font-size: 14px;font-color:#9095AB;">{{$t('validPeriod')}} {{license(serverAboutKap && serverAboutKap['kap.dates'])}}</span>
      <update_license ref="licenseEnter" :updateLicenseVisible="updateLicenseVisible" v-on:validSuccess="licenseValidSuccess"></update_license>
      <p @click="apply" v-if="!isPlusVersion">{{$t('applyLicense')}}</p>
      <div slot="footer" class="dialog-footer">
        <el-button @click="updateLicenseVisible = false">{{$t('cancel')}}</el-button>
        <el-button type="primary" :loading="loadCheck" @click="licenseForm">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="applyLicense" @close="closeApplyLicense" :title="$t('applyLicense')" v-model="applyLicense" size="tiny" v-if="!isPlusVersion" :close-on-click-modal="false">
      <el-form label-position="top" :model="userMessage" :rules="userRules" ref="applyLicenseForm">
        <el-form-item prop="email">
          <el-input v-model="userMessage.email" :placeholder="$t('businessEmail')"></el-input>
        </el-form-item>
        <el-form-item prop="company">
          <el-input v-model="userMessage.company" :placeholder="$t('companyName')"></el-input>
        </el-form-item>
        <el-form-item prop="userName">
          <el-input v-model="userMessage.userName" :placeholder="$t('yourName')"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="submitApply" type="primary" :loading="applyLoading">{{$t('kylinLang.common.submit')}}</el-button>
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
  import { personalEmail } from '../../config/index'
  export default {
    name: 'help',
    props: ['isLogin'],
    data () {
      return {
        userRules: {
          email: [
            { required: true, message: this.$t('noEmail'), trigger: 'blur' },
            { type: 'email', message: this.$t('noEmailStyle'), trigger: 'blur' },
            { validator: this.validateEmail, trigger: 'blur' }
          ],
          company: [{ required: true, message: this.$t('noCompany'), trigger: 'blur' }],
          userName: [{ required: true, message: this.$t('noName'), trigger: 'blur' }]
        },
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
        loadCheck: false,
        applyLicense: false,
        userMessage: {
          email: '',
          company: '',
          userName: '',
          lang: 'en'
        },
        applyLoading: false,
        changeDialog: true
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
        trialLicenseFile: 'TRIAL_LICENSE_FILE'
      }),
      handleCommand (val) {
        var _this = this
        if (val === 'kapmanual') {
          this.url = 'http://manual.kyligence.io/'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'kybotservice') {
          this.url = 'https://kybot.io/#/home?src=kap250'
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
        this.loadCheck = true
      },
      licenseValidSuccess: function (license) {
        if (license === true) {
          this.updateLicenseVisible = false
          this.$alert(this.$t('evaluationPeriod') + this.$store.state.system.serverAboutKap['kap.dates'], this.$t('evaluationLicense'), {
            cancelConfirmButton: true,
            type: 'success'
          })
        }
        this.loadCheck = false
      },
      license (obj) {
        if (!obj) {
          return 'N/A'
        } else {
          return obj
        }
      },
      apply: function () {
        this.updateLicenseVisible = false
        this.applyLicense = true
        this.changeDialog = true
      },
      closeApplyLicense: function () {
        this.$refs.applyLicenseForm.resetFields()
        if (this.changeDialog) {
          this.updateLicenseVisible = true
        }
      },
      submitApply: function () {
        this.$refs['applyLicenseForm'].validate((valid) => {
          if (valid) {
            this.applyLoading = true
            this.trialLicenseFile(this.userMessage).then((res) => {
              handleSuccess(res, (data) => {
                if (data && data['kap.dates']) {
                  if (this.lastTime(data['kap.dates']) > 0) {
                    this.$alert(this.$t('evaluationPeriod') + data['kap.dates'], this.$t('evaluationLicense'), {
                      cancelConfirmButton: true,
                      type: 'success'
                    })
                  } else {
                    var splitTime = data['kap.dates'].split(',')
                    var endTime = splitTime[1]
                    this.$alert(this.$t('expiredOn') + endTime, this.$t('evaluationLicense'), {
                      cancelConfirmButton: true,
                      type: 'warning'
                    })
                  }
                  this.changeDialog = false
                  this.applyLicense = false
                  this.updateLicenseVisible = false
                  this.applyLoading = false
                  this.$store.state.system.serverAboutKap['kap.dates'] = data['kap.dates']
                }
              })
            }, (res) => {
              handleError(res)
              this.applyLoading = false
            })
          }
        })
      },
      validateEmail: function (rule, value, callback) {
        if (value) {
          for (let key in personalEmail) {
            if (value.indexOf(key) !== -1) {
              callback(new Error(this.$t('enterpriseEmail')))
            }
          }
          callback()
        } else {
          callback()
        }
      },
      lastTime (date) {
        var splitTime = date.split(',')
        if (splitTime.length >= 2) {
          var endTime = splitTime[1]
          var lastTimes = (new Date(endTime + ' 23:59:59')) - (new Date())
          var days = Math.ceil(lastTimes / 1000 / 60 / 60 / 24)
          if (days >= 0) {
            days = Math.ceil(Math.abs(days))
          } else {
            days = 0
          }
          return days
        }
        return 0
      }
    },
    computed: {
      serverAbout () {
        return this.$store.state.system.serverAboutKap
      },
      kyStatus () {
        return this.$store.state.kybot.kyStatus
      },
      serverAboutKap () {
        return this.$store.state.system.serverAboutKap
      },
      isPlusVersion () {
        var kapVersionInfo = this.$store.state.system.serverAboutKap
        return kapVersionInfo && kapVersionInfo['kap.version'] && kapVersionInfo['kap.version'].indexOf('Plus') !== -1
      }
    },
    components: {
      'about_kap': aboutKap,
      'login_kybot': loginKybot,
      'start_kybot': startKybot,
      'update_license': updateLicense
    },
    locales: {
      'en': {autoUpload: 'Auto Upload', usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required', agreeAndOpen: 'agree the protocol and open the automatic service', kybotAuto: 'KyBot Auto Upload', openSuccess: 'open successfully', closeSuccess: 'close successfully', Manual: 'KAP Manual', kybotService: 'KyBot Service', updateLicense: 'Update License', aboutKap: 'About KAP', kybot: "By analyzing diagnostic package, <a href='https://kybot.io/#/home?src=kap250'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly.", signIn: 'Kyligence Account | Sign In', ok: 'OK', cancel: 'Cancel', save: 'Save', license: 'Update License', validPeriod: 'Valid Period:', applyLicense: 'Apply Evaluation License', evaluationLicense: 'Evaluation License', evaluationPeriod: 'Evaluation Period:', noEmail: 'Please enter your email.', noEmailStyle: 'Please enter a usable email.', noCompany: 'Please enter your company name.', enterpriseEmail: 'Please enter your enterprise email.', businessEmail: 'Business Mail', companyName: 'Company Name', yourName: 'Your Name', expiredOn: 'Expired On:', noName: 'Please enter your name.'},
      'zh-cn': {autoUpload: '自动上传', usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空', agreeAndOpen: '同意协议并开启自动服务', kybotAuto: 'KyBot自动上传', openSuccess: '成功开启', closeSuccess: '成功关闭', Manual: 'KAP手册', kybotService: 'KyBot服务', updateLicense: '更新许可证', aboutKap: '关于KAP', kybot: '<a href="https://kybot.io/#/home?src=kap250">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传', signIn: 'Kyligence 帐号 | 登录', ok: '确定', cancel: '取消', save: '保存', license: '更新许可证', validPeriod: '有效期限：', applyLicense: '申请许可证', evaluationLicense: '有效许可证', evaluationPeriod: '有效期限：', noEmail: '请输入邮箱。', noEmailStyle: '请输入一个可用邮箱。', noCompany: '请输入公司名称。', enterpriseEmail: '请输入企业邮箱。', businessEmail: '企业邮箱', companyName: '公司名称', yourName: '用户名称', expiredOn: '过期时间：', noName: '请输入用户名称。'}
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
  .updateKAPLicense {
    .el-dialog {
      .el-dialog__header {
        .el-dialog__title {
          font-size: 14px;
        }
      }
      .el-dialog__body {
        padding: 0px 20px 0px 20px;
      }
      .el-input__inner {
        font-size: 14px;
      }
      p {
        margin-bottom: 50px;
        cursor:pointer;
        color:#218fea;
        font-size:14px;
        text-align: left;
        text-decoration-line: underline;
      }
      .el-button {
        margin: 0px 0px 0px 0px;
        padding: 10px 15px 10px 15px;
      }
      .el-button--primary {
        border: rgb(28, 122, 216) 1px solid;
      }
    }
  }
  .applyLicense {
    .el-dialog {
      .el-dialog__title {
        font-size: 14px;
      }
    }
    .el-input {
      margin-right: 0px;
    }
    .el-input__inner {
      font-size: 14px;
    }
    .el-form-item {
      margin-bottom: 0px;
      text-align: left;
      .el-form-item__error {
        position: relative;
      }
      .el-input {
        padding: 3px 0px 3px 0px;
      }
    }
    .dialog-footer {
      .el-button {
        width: 100%;
      }
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
