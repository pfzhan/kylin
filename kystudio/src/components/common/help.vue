<template>
  <div class="help-box">
    <el-dropdown @command="handleCommand">
      <el-button plain size="small">
         {{$t('kylinLang.common.help')}}<i class="el-icon-arrow-down el-icon--right"></i>
      </el-button>
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item command="kapmanual"><a class="ksd-block-a" target="_blank" :href="$t('manualUrl')">{{$t('Manual')}}</a></el-dropdown-item>
        <el-dropdown-item command="kybotservice"><a class="ksd-block-a" target="_blank" :href="supportUrl"> {{$t('keService')}}</a></el-dropdown-item>
        <!-- <el-dropdown-item command="guide" v-if="!isLogin&&userGuideActions.includes('userGuide')"><a class="ksd-block-a">{{$t('userGuide')}}</a></el-dropdown-item> -->
        <el-dropdown-item command="updatelicense" divided>{{$t('updateLicense')}}</el-dropdown-item>
        <el-dropdown-item command="aboutkap" >{{$t('aboutKap')}}</el-dropdown-item>
      </el-dropdown-menu>
    </el-dropdown>

    <el-dialog :visible.sync="aboutKapVisible" top="5vh" :title="$t('aboutKap')" width="720px" limited-area :close-on-click-modal="false" :append-to-body="true">
      <about_kap :about="serverAboutKap" :aboutKapVisible="aboutKapVisible">
      </about_kap>
    </el-dialog>

    <el-dialog class="license-msg" :append-to-body="true" width="480px"
      :before-close="handleClose"
      :title="$t('kylinLang.common.license')"
      :close-on-click-modal="false"
      :visible.sync="showLicenseCheck"
      :close-on-press-escape="false">
      <el-alert
        :title="$store.state.system.serverAboutKap.msg"
        :icon="$store.state.system.serverAboutKap.code === '002' ? 'el-icon-ksd-error_01' : 'el-icon-ksd-alert'"
        :show-background="false"
        :closable="false"
        :type="$store.state.system.serverAboutKap.code === '002' ? 'error' : 'warning'">
      </el-alert>
      <span slot="footer" class="dialog-footer">
        <div>
          <a class="el-button  el-button--primary el-button--medium is-plain" style="text-decoration:none;" href="mailto:info@kyligence.io">{{$t('kylinLang.common.contactTech')}}</a>
          <el-button size="medium" type="primary" plain @click="handleClose">{{$t('kylinLang.common.IKnow')}}</el-button>
        </div>
      </span>
    </el-dialog>

    <el-dialog :title="$t('license')" :append-to-body="true" :visible.sync="updateLicenseVisible" :close-on-click-modal="false" class="updateKAPLicense" width="480px" @close="resetUpdate">
      <update_license v-if="updateLicenseVisible" ref="licenseEnter" :updateLicenseVisible="updateLicenseVisible" v-on:validSuccess="licenseValidSuccess" @requestLicense="apply"></update_license>
      <div slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="resetUpdate">{{$t('cancel')}}</el-button>
        <el-button size="medium" :loading="loadCheck" @click="licenseForm">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="applyLicense" :append-to-body="true" @close="closeApplyLicense" :title="$t('applyLicense')" :visible.sync="applyLicense" :close-on-click-modal="false" width="480px">
      <el-form label-position="top" :model="userMessage" :rules="userRules" ref="applyLicenseForm">
        <el-form-item prop="email" :label="$t('businessEmail')">
          <el-input v-model="userMessage.email" :placeholder="$t('businessEmail')"></el-input>
        </el-form-item>
        <el-form-item prop="company" :label="$t('companyName')">
          <el-input v-model="userMessage.company" :placeholder="$t('companyName')"></el-input>
        </el-form-item>
        <el-form-item prop="username" :label="$t('yourName')">
          <el-input v-model="userMessage.username" :placeholder="$t('yourName')"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="closeApplyLicense">{{$t('cancel')}}</el-button>
        <el-button @click="submitApply" :loading="applyLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="import-ssb" :append-to-body="true" :title="$t('importSSB')" :visible.sync="importSSBvisible" :close-on-click-modal="false" width="660px" @close="resetImportSSB">
      <div class="ksd-center" v-if="!isImportSuccess&&!isShowImportError">
        <div class="ksd-mb-20">{{$t('importTips')}}</div>
        <div class="progress-bar"><el-progress :stroke-width="12" :percentage="percent"></el-progress></div>
        <div class="inprogress ksd-fs-12">{{$t('importInprogress')}}</div>
      </div>
      <div class="ksd-center" v-if="isImportSuccess&&!isShowImportError">
        <div class="ksd-mb-20">{{$t('importSuccessTips')}}</div>
        <i class="el-icon-ksd-good_health"></i>
        <div class="ksd-mb-10"><el-button type="primary" size="small" @click="startGuide">{{$t('startGuide')}}</el-button></div>
      </div>
      <el-alert
        v-if="!isImportSuccess&&isShowImportError"
        :show-background="false"
        :closable="false"
        class="ksd-mb-10"
        show-icon
        type="warning">
        <span slot="title" class="ksd-fs-14">{{$t('importFailed1')}}<a target="_blank" :href="$t('manualLinkURL')">{{$t('manualLink')}}</a>{{$t('importFailed2')}}</span>
      </el-alert>
      <div slot="footer" class="dialog-footer" v-if="!isImportSuccess&&isShowImportError">
        <el-button plain size="medium" @click="importSSBvisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
  import { mapActions, mapGetters, mapMutations } from 'vuex'
  import aboutKap from '../common/about_kap.vue'
  import updateLicense from '../user/license'
  import { handleSuccess, handleError } from '../../util/business'
  import { personalEmail } from '../../config/index'
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
          username: '',
          lang: 'en',
          product_type: 'kap',
          category: '4.x'
        },
        applyLoading: false,
        changeDialog: true,
        showLicenseCheck: false,
        importSSBvisible: false,
        isImportSuccess: false,
        isShowImportError: false,
        percent: 0,
        ST: null
      }
    },
    methods: {
      ...mapActions({
        getAboutKap: 'GET_ABOUTKAP',
        trialLicenseFile: 'TRIAL_LICENSE_FILE',
        checkSSB: 'CHECK_SSB',
        importSSBDatabase: 'IMPORT_SSB_DATABASE'
      }),
      ...mapMutations({
        showLicenseSuccessDialog: 'TOGGLE_LICENSE_DIALOG'
      }),
      handleCommand (val) {
        if (val === 'aboutkap') {
          this.getAboutKap(() => {}, (res) => {
            handleError(res)
          })
          this.aboutKapVisible = true
        } else if (val === 'updatelicense') {
          this.updateLicenseVisible = true
        } else if (val === 'guide') {
          this.checkSSB().then((res) => {
            handleSuccess(res, (data) => {
              if (data) {
                this.$store.state.system.guideConfig.guideModeCheckDialog = true
              } else {
                this.isShowImportError = true
                this.importSSBvisible = true
                // this.importLoading()
                // this.importSSBDatabase().then((res) => {
                //   handleSuccess(res, (data) => {
                //     this.percent = 100
                //     this.$nextTick(() => {
                //       this.isImportSuccess = true
                //     })
                //   })
                // }, (r) => {
                //   this.isShowImportError = true
                // })
              }
            })
          }, (errResp) => {
            handleError(errResp)
          })
        }
      },
      startGuide () {
        this.importSSBvisible = false
        this.$store.state.system.guideConfig.guideModeCheckDialog = true
      },
      importLoading () {
        var _this = this
        this.percent = 0
        clearInterval(this.ST)
        this.ST = setInterval(() => {
          var randomPlus = Math.round(5 * Math.random())
          if (_this.percent + randomPlus < 99) {
            _this.percent += randomPlus
          } else {
            clearInterval(_this.ST)
          }
        }, 800)
      },
      resetImportSSB () {
        this.importSSBvisible = false
        this.isImportSuccess = false
        this.isShowImportError = false
      },
      resetUpdate () {
        this.loadCheck = false
        this.updateLicenseVisible = false
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
      licenseForm: function () {
        this.loadCheck = true
        this.$refs['licenseEnter'].$emit('licenseFormValid')
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
            this.showLicenseSuccessDialog(true)
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
          this.applyLicense = false
        }
      },
      submitApply: function () {
        this.$refs['applyLicenseForm'].validate((valid) => {
          if (valid) {
            this.applyLoading = true
            this.userMessage.category = `${this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['version']}.x`
            this.trialLicenseFile(this.userMessage).then((res) => {
              handleSuccess(res, (data) => {
                if (data && data['ke.dates']) {
                  if (this.lastTime(data['ke.dates']) > 0) {
                    this.$alert(this.$t('evaluationPeriod') + data['ke.dates'], this.$t('evaluationLicense'), {
                      cancelConfirmButton: true,
                      type: 'success'
                    })
                  } else {
                    var splitTime = data['ke.dates'].split(',')
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
                  this.$store.state.system.serverAboutKap['ke.dates'] = data['ke.dates']
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
          if (value.length > 50) {
            callback(new Error(this.$t('emailLength')))
          } else {
            for (let key in personalEmail) {
              if (value.indexOf(key) !== -1) {
                callback(new Error(this.$t('enterpriseEmail')))
              }
            }
            callback()
          }
        } else {
          callback()
        }
      },
      validateName: function (rule, value, callback) {
        const regex = /^\S[a-zA-Z\s\d\u4e00-\u9fa5]+\S$/
        if (value.length > 50 && !regex.test(regex)) {
          callback(new Error(this.$t('enterpriseName')))
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
      ...mapGetters([
        'supportUrl',
        'userGuideActions'
      ]),
      serverAboutKap () {
        return this.$store.state.system.serverAboutKap
      },
      userRules () {
        return {
          email: [
            { required: true, message: this.$t('noEmail'), trigger: 'blur' },
            { type: 'email', message: this.$t('noEmailStyle'), trigger: 'blur' },
            { validator: this.validateEmail, trigger: 'blur' }
          ],
          company: [
            { required: true, message: this.$t('noCompany'), trigger: 'blur' },
            { validator: this.validateName, trigger: 'blur' }
          ],
          username: [
            { required: true, message: this.$t('noName'), trigger: 'blur' },
            { validator: this.validateName, trigger: 'blur' }
          ]
        }
      }
    },
    components: {
      'about_kap': aboutKap,
      'update_license': updateLicense
    },
    locales: {
      'en': {
        autoUpload: 'Auto Upload',
        usernameEmpty: 'Please enter username',
        usernameRule: 'username contains only numbers, letters and character "_"',
        noUserPwd: 'password required',
        agreeAndOpen: 'agree the protocol and open the automatic service',
        kybotAuto: 'KyBot Auto Upload',
        openSuccess: 'open successfully',
        closeSuccess: 'close successfully',
        Manual: 'Kyligence Enterprise Manual',
        kybotService: 'KyBot Service',
        keService: 'Kyligence Support Portal',
        updateLicense: 'Update License',
        aboutKap: 'About Kyligence Enterprise',
        kybot: "By analyzing diagnostic package, <a href='https://kybot.io/#/home?src=kap250'>KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages at 24:00 o'clock everyday regularly.",
        ok: 'OK',
        cancel: 'Cancel',
        save: 'Save',
        license: 'Update License',
        validPeriod: 'Valid Period:',
        applyLicense: 'Apply Evaluation License',
        evaluationLicense: 'Evaluation License',
        evaluationPeriod: 'Evaluation Period:',
        noEmail: 'Please enter your email.',
        noEmailStyle: 'Please enter a usable email.',
        noCompany: 'Please enter your company name.',
        enterpriseEmail: 'Please enter your enterprise email.',
        businessEmail: 'Business Mail',
        companyName: 'Company Name',
        yourName: 'Your Name',
        expiredOn: 'Expired On:',
        noName: 'Please enter your name.',
        userGuide: 'User Guide',
        enterpriseName: 'Only Chinese characters, letters, digits and space are supported. The maximum is 50 characters.',
        manualUrl: 'https://docs.kyligence.io/books/v4.0/en/index.html',
        emailLength: 'The maximum is 50 characters',
        importSSB: 'Import SSB Dataset',
        importTips: 'The complete SSB sample dataset is not detected in the current system and is being imported automatically.',
        importInprogress: 'This process will take approximately 2 minutes, please wait for a while.',
        importSuccessTips: 'Imported SSB sample dataset successfully. Now you can start the user guide.',
        startGuide: 'Start user guide',
        importFailed1: 'The complete SSB sample dataset is not detected in the current system. Please refer to ',
        manualLink: 'the manual',
        manualLinkURL: 'https://docs.kyligence.io/books/v4.0/en/datasource/import_hive.en.html',
        importFailed2: ' to restart the user guide after importing the SSB sample dataset manually.'
      },
      'zh-cn': {
        autoUpload: '自动上传',
        usernameEmpty: '请输入用户名',
        usernameRule: '名字只能包含数字字母下划线',
        noUserPwd: '密码不能为空',
        agreeAndOpen: '同意协议并开启自动服务',
        kybotAuto: 'KyBot 自动上传',
        openSuccess: '成功开启',
        closeSuccess: '成功关闭',
        Manual: 'Kyligence Enterprise 手册',
        kybotService: 'KyBot 服务',
        keService: 'Kyligence 支持中心',
        updateLicense: '更新许可证',
        aboutKap: '关于 Kyligence Enterprise',
        kybot: '<a href="https://kybot.io/#/home?src=kap250">KyBot</a> 通过分析生产的诊断包，提供 KAP 在线诊断、优化及服务，启动自动上传服务后，每天零点定时自动上传，无需自行打包和上传',
        ok: '确定',
        cancel: '取消',
        save: '保存',
        license: '更新许可证',
        validPeriod: '有效期限：',
        applyLicense: '申请许可证',
        evaluationLicense: '有效许可证',
        evaluationPeriod: '有效期限：',
        noEmail: '请输入邮箱。',
        noEmailStyle: '请输入一个可用邮箱。',
        noCompany: '请输入公司名称。',
        enterpriseEmail: '请输入企业邮箱。',
        businessEmail: '企业邮箱',
        companyName: '公司名称',
        yourName: '用户名称',
        expiredOn: '过期时间：',
        noName: '请输入用户名称。',
        userGuide: '新手指引',
        enterpriseName: '支持中英文、数字、空格，最大值为 50 个字符',
        manualUrl: 'https://docs.kyligence.io/books/v4.0/zh-cn/index.html',
        emailLength: '最大值为 50 个字符',
        importSSB: '导入 SSB 数据集',
        importTips: '当前系统中未检测到完整的 SSB 样例数据集，正在自动导入。',
        importInprogress: '该过程需要大约 2 分钟，请稍候。',
        importSuccessTips: '导入成功，现在可以开始新手指引了。',
        startGuide: '进入新手指引',
        importFailed1: '当前系统中未检测到完整的 SSB 样例数据集，请参照',
        manualLink: '手册',
        manualLinkURL: 'https://docs.kyligence.io/books/v4.0/zh-cn/datasource/import_hive.cn.html',
        importFailed2: '手动导入 SSB 样例数据集后重新开始新手指引。'
      }
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
        line-height: normal;
        text-align:left;
        .el-icon-ksd-error_01 {
          color: @error-color-1;
        }
        .el-icon-ksd-alert {
          color: @warning-color-1;
        }
      }
    }
  }
  .updateKAPLicense {
    .el-dialog__body {
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
      text-align: left;
      .el-form-item__error {
        position: relative;
      }
      .el-input {
        padding: 3px 0px 3px 0px;
      }
    }
  }
  .import-ssb {
    .progress-bar {
      margin: 0 40px;
      .el-progress__text {
        font-size: 14px !important;
      }
    }
    .el-alert a {
      text-decoration: underline;
    }
    .inprogress {
      color: @text-disabled-color;
      margin-bottom: 60px;
      margin-top: 5px;
    }
    .el-icon-ksd-good_health {
      color: @normal-color-1;
      font-size: 40px;
      margin-bottom: 20px;
    }
  }
</style>
