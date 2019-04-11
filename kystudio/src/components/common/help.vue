<template>
  <div class="help-box">
    <el-dropdown @visible-change="dropHelp" @command="handleCommand">
      <!-- <span class="el-dropdown-link ky-a-like">
        {{$t('kylinLang.common.help')}} <i class="el-icon-caret-bottom"></i>
      </span> -->
      <el-button size="small" plain>
         {{$t('kylinLang.common.help')}}<i class="el-icon-arrow-down el-icon--right"></i>
      </el-button>
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item command="guide"><a class="ksd-block-a">{{$t('userGuide')}}</a></el-dropdown-item>
        <!-- <el-dropdown-item command="kapmanual"><a class="ksd-block-a" target="_blank" href="http://manual.kyligence.io/">{{$t('Manual')}}</a></el-dropdown-item>
        <el-dropdown-item command="kybotservice"><a class="ksd-block-a" target="_blank" :href="'https://kybot.io/#/home'+kapVersionPara"> {{$t('kybotService')}}</a></el-dropdown-item> -->
      </el-dropdown-menu>
    </el-dropdown>
  </div>
</template>
<script>
  import { mapActions } from 'vuex'
  import aboutKap from '../common/about_kap.vue'
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
        showGuideBox: false,
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
          lang: 'en',
          productType: 'kap',
          category: '2.x'
        },
        applyLoading: false,
        changeDialog: true,
        showLicenseCheck: false
      }
    },
    methods: {
      ...mapActions({
        getKyStatus: 'GET_KYBOT_STATUS',
        getAgreement: 'GET_AGREEMENT',
        trialLicenseFile: 'TRIAL_LICENSE_FILE',
        getCurKybotAccount: 'GET_CUR_ACCOUNTNAME'
      }),
      handleCommand (val) {
        if (val === 'aboutkap') {
          this.getCurKybotAccount().then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              this.$store.state.kybot.hasLoginAccount = data
            }, (errResp) => {
              this.$store.state.kybot.hasLoginAccount = ''
              handleError(errResp)
            })
          })
          this.aboutKapVisible = true
        } else if (val === 'updatelicense') {
          this.updateLicenseVisible = true
        } else if (val === 'guide') {
          this.$store.state.system.guideConfig.guideModeCheckDialog = true
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
      handleClose () {
        this.showLicenseCheck = false
      },
      licenseValidSuccess: function (license) {
        if (license === true) {
          this.updateLicenseVisible = false
          if (this.$store.state.system.serverAboutKap && (this.$store.state.system.serverAboutKap['code'] === '001' || this.$store.state.system.serverAboutKap['code'] === '002')) {
            this.showLicenseCheck = true
          } else {
            this.$alert(this.$t('evaluationPeriod') + this.$store.state.system.serverAboutKap['kap.dates'], this.$t('evaluationLicense'), {
              cancelConfirmButton: true,
              type: 'success'
            })
          }
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
            this.userMessage.productType = this.isPlusVersion ? 'kapplus' : 'kap'
            this.userMessage.category = `${this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['version']}.x`
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
      kapVersionPara () {
        var _kapV = this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.version'] || null
        return _kapV ? '?src=' + _kapV : ''
      },
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
      'update_license': updateLicense
    },
    locales: {
      'en': {autoUpload: 'Auto Upload', usernameEmpty: 'Please enter username', usernameRule: 'username contains only numbers, letters and character "_"', noUserPwd: 'password required', agreeAndOpen: 'agree the protocol and open the automatic service', kybotAuto: 'KyBot Auto Upload', openSuccess: 'open successfully', closeSuccess: 'close successfully', Manual: 'KAP Manual', kybotService: 'KyBot Service', updateLicense: 'Update License', aboutKap: 'About Kyligence Enterprise', kybot: "By analyzing diagnostic package, <a href='https://kybot.io/#/home?src=kap250'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly.", ok: 'OK', cancel: 'Cancel', save: 'Save', license: 'Update License', validPeriod: 'Valid Period:', applyLicense: 'Apply Evaluation License', evaluationLicense: 'Evaluation License', evaluationPeriod: 'Evaluation Period:', noEmail: 'Please enter your email.', noEmailStyle: 'Please enter a usable email.', noCompany: 'Please enter your company name.', enterpriseEmail: 'Please enter your enterprise email.', businessEmail: 'Business Mail', companyName: 'Company Name', yourName: 'Your Name', expiredOn: 'Expired On:', noName: 'Please enter your name.', userGuide: 'User Guide'},
      'zh-cn': {autoUpload: '自动上传', usernameEmpty: '请输入用户名', usernameRule: '名字只能包含数字字母下划线', noUserPwd: '密码不能为空', agreeAndOpen: '同意协议并开启自动服务', kybotAuto: 'KyBot自动上传', openSuccess: '成功开启', closeSuccess: '成功关闭', Manual: 'KAP手册', kybotService: 'KyBot服务', updateLicense: '更新许可证', aboutKap: '关于Kyligence Enterprise', kybot: '<a href="https://kybot.io/#/home?src=kap250">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传', ok: '确定', cancel: '取消', save: '保存', license: '更新许可证', validPeriod: '有效期限：', applyLicense: '申请许可证', evaluationLicense: '有效许可证', evaluationPeriod: '有效期限：', noEmail: '请输入邮箱。', noEmailStyle: '请输入一个可用邮箱。', noCompany: '请输入公司名称。', enterpriseEmail: '请输入企业邮箱。', businessEmail: '企业邮箱', companyName: '公司名称', yourName: '用户名称', expiredOn: '过期时间：', noName: '请输入用户名称。', userGuide: '新手指引'}
    }
  }
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .help-box {
    .errMsgBox {
      .el-dialog__body {
        padding: 30px 20px;
        line-height: normal;
        text-align:left;
      }
    }
    .license-msg {
      .el-dialog__body {
        padding: 30px 20px;
        line-height: normal;
        text-align:left;
      }
    }
  }
  .updateKAPLicense {
    .el-dialog__body {
      padding: 26px 172px 66px;
      .license-pic {
        width: 100%;
        text-align: center;
      }
    }
  }
  .applyLicense {
    .el-input {
      margin-right: 0px;
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
    .el-dialog__footer {
      padding: 15px 50px 15px 50px;
      .dialog-footer {
        .el-button {
          margin: 0px 0px 0px 0px;
          width: 100%
        }
      }
    }
  }
</style>
