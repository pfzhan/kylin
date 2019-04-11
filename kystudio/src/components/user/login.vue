<template>
  <div>
    <div class="login-header">
      <img src="../../assets/img/logo/big_logo.png">
      <ul class="ksd-fright">
        <!-- 在登录页不显示onoff -->
        <li></li>
        <li><kap-change-lang></kap-change-lang></li>
      </ul>
    </div>

    <div class="login-box">
      <el-row :gutter="0">
        <el-col :span="12">
          <div class="grid-content login-msg">
            <img src="../../assets/img/logo/logo_login.png">
            <p>{{$t('welcome')}}</p>
            <p>{{$t('kapMsg')}}</p>
            <div class="ky-line"></div>
            <ul>
              <li><i class="el-icon-ksd-login_intro ksd-fs-12"></i><a href="http://kyligence.io/enterprise/#analytics" target="_blank">{{$t('introduction')}}</a></li>
              <li><i class="el-icon-ksd-login_manual ksd-fs-12"></i><a href="http://docs.kyligence.io" target="_blank">{{$t('manual')}}</a></li>
              <li><i class="el-icon-ksd-login_email ksd-fs-12"></i><a href="mailto:info@Kyligence.io">{{$t('contactUs')}}</a></li>
            </ul>
          </div>
        </el-col>
        <el-col :span="12">
          <div class="login-form">
            <el-form   @keyup.native.enter="onLoginSubmit" :model="user" ref="loginForm" :rules="rules">
              <div class="input_group">
                <el-form-item label="" prop="username">
                  <el-input v-model="user.username" auto-complete="on" :autofocus="true"  :placeholder="$t('userName')" name="username"></el-input>
                </el-form-item>
                <el-form-item label="" prop="password" class="password">
                  <el-input  type="password" v-model="user.password" name="password" :placeholder="$t('password')"></el-input>
                </el-form-item>
                <p class="forget-pwd ksd-mt-5" v-show="user.username==='ADMIN'">
                  <common-tip :content="$t('adminTip')" >
                    {{$t('forgetPassword')}}
                  </common-tip>
                </p>
              </div>
              <el-form-item class="ksd-pt-40">
                <kap-icon-button type="primary" class="ksd-mt-10"  @keyup.native.enter="onLoginSubmit" @click.native="onLoginSubmit" :useload="false" ref="loginBtn">{{$t('loginIn')}}</kap-icon-button>
              </el-form-item>
            </el-form>

          </div>
        </el-col>
      </el-row>
    </div>

    <p class="login-footer">&copy;2019 <a href="http://kyligence.io/" target="_blank">Kyligence Inc.</a> All rights reserved.</p>

    <el-dialog class="updateKAPLicense" @close="closeDialog" :title="$t('license')" :visible.sync="hasLicense" :close-on-press-escape="false" :close-on-click-modal="false" width="720px">
      <div class="ksd-mb-40 license-pic">
        <img src="../../assets/img/license.png">
      </div>
      <license ref="licenseEnter" v-on:validSuccess="licenseValidSuccess"></license>
      <div slot="footer" class="dialog-footer">
        <span @click="apply" class="ksd-fleft ksd-lineheight-36 ky-a-like" style="text-decoration: underline;">{{$t('applyLicense')}}</span>       
        <el-button size="medium" @click="closeDialog">{{$t('cancel')}}</el-button>
        <el-button size="medium" type="primary" plain :loading="loadCheck" @click="licenseForm">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="applyLicense" @close="closeApplyLicense" :title="$t('applyLicense')" :visible.sync="applyLicense" :close-on-press-escape="false" :close-on-click-modal="false" width="480px">
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
        <el-button @click="submitApply" type="primary" plain :loading="applyLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="license-msg" width="480px"
      :before-close="handleClose"
      :title="$t('kylinLang.common.license')"
      :close-on-click-modal="false"
      :visible.sync="showLicenseCheck"
      :close-on-press-escape="false">
      <el-alert
        show-icon
        :title="$store.state.system.serverAboutKap.msg"
        :type="$store.state.system.serverAboutKap.code === '002' ? 'error' : 'warning'"
        :closable="false">
      </el-alert>
      <span slot="footer" class="dialog-footer">
        <div>
          <a class="el-button el-button--primary el-button--medium is-plain" style="text-decoration:none;"  href="mailto:g-ent-lic@kyligence.io">{{$t('kylinLang.common.contactTech')}}</a>
          <el-button size="medium" type="primary" plain @click="handleClose">{{$t('kylinLang.common.IKnow')}}</el-button>
        </div>
      </span>
    </el-dialog>
  </div>
</template>
<script>
import { mapActions, mapMutations } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import changeLang from '../common/change_lang'
import license from './license'
import help from '../common/help'
import Vue from 'vue'
import { Base64 } from 'js-base64'
import { personalEmail } from '../../config/index'
export default {
  name: 'login',
  data () {
    return {
      // rules: {
      //   username: [{ required: true, message: this.$t('noUserName'), trigger: 'blur' }],
      //   password: [{ required: true, message: this.$t('noUserPwd'), trigger: 'blur' }]
      // },
      userRules: {
        email: [
          { required: true, message: this.$t('noEmail'), trigger: 'blur' },
          { type: 'email', message: this.$t('noEmailStyle'), trigger: 'blur' },
          { validator: this.validateEmail, trigger: 'blur' }
        ],
        company: [{ required: true, message: this.$t('noCompany'), trigger: 'blur' }],
        userName: [{ required: true, message: this.$t('noName'), trigger: 'blur' }]
      },
      user: {
        username: '' || localStorage.getItem('username'),
        password: ''
      },
      btnLock: false,
      needLicense: false,
      loadCheck: false,
      applyLicense: false,
      userMessage: {
        email: '',
        company: '',
        userName: '',
        lang: 'en',
        productType: 'kap',
        category: `${this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['version']}.x`
      },
      applyLoading: false,
      hasLicense: false,
      changeDialog: true,
      showLicenseCheck: false,
      loginSuccess: false,
      hasLicenseMsg: false
    }
  },
  methods: {
    ...mapActions({
      login: 'LOGIN',
      getAboutKap: 'GET_ABOUTKAP',
      trialLicenseFile: 'TRIAL_LICENSE_FILE'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER'
    }),
    handleClose () {
      if (this.$store.state.system.serverAboutKap['code'] === '001' && this.loginSuccess === true) {
        this.showLicenseCheck = false
        // 一期先屏蔽该逻辑
        // if (!SystemPwdRegex.test(this.user.password) && this.user.username === 'ADMIN') {
        //   this.$router.push('/security/user')
        // } else {
        //   this.$router.push('/overview')
        // }
        this.$router.push('/overview')
      } else {
        this.showLicenseCheck = false
      }
    },
    onLoginSubmit () {
      this.$refs['loginForm'].validate((valid) => {
        if (valid) {
          var baseStr = 'Basic ' + Base64.encode(this.user.username + ':' + this.user.password)
          Vue.http.headers.common['Authorization'] = baseStr
          this.$refs['loginBtn'].loading = true
          this.login(this.user).then((res) => {
            handleSuccess(res, (data) => {
              this.loginEnd()
              this.setCurUser({ user: data })
              this.loginSuccess = true
              if (this.$store.state.system.serverAboutKap['code'] === '001' && this.hasLicenseMsg === false) {
                this.showLicenseCheck = true
              } else {
                // 一期先屏蔽该逻辑
                // if (!SystemPwdRegex.test(this.user.password) && this.user.username === 'ADMIN') {
                //   this.$router.push('/security/user')
                // } else {
                //   this.$router.push('/overview')
                // }
                this.$router.push('/dashboard')
              }
              localStorage.setItem('username', this.user.username)
              this.$store.state.config.overLock = false
            })
          }, (res) => {
            if (this.$store.state.system.serverAboutKap['code'] === '002') {
              this.showLicenseCheck = true
            } else {
              handleError(res)
            }
            this.loginEnd()
          })
        }
      })
    },
    loginEnd () {
      Vue.http.headers.common['Authorization'] = ''
      this.$refs['loginBtn'].loading = false
    },
    checkLicense () {
      // this.getAboutKap().then((result) => {
      //   handleSuccess(result, (data) => {
      //     if (this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.dates'] === null) {
      //       this.hasLicense = true
      //     }
      //   }, (resp) => {
      //     handleError(resp)
      //   })
      // })
    },
    licenseForm: function () {
      this.$refs['licenseEnter'].$emit('licenseFormValid')
      this.loadCheck = true
    },
    licenseValidSuccess: function (license) {
      if (license === true) {
        this.hasLicense = false
        this.hasLicenseMsg = true
        if (this.$store.state.system.serverAboutKap['code'] === '001' || this.$store.state.system.serverAboutKap['code'] === '002') {
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
    closeDialog: function () {
      this.hasLicense = false
    },
    apply: function () {
      this.hasLicense = false
      this.applyLicense = true
      this.changeDialog = true
    },
    closeApplyLicense: function () {
      this.$refs['applyLicenseForm'].resetFields()
      if (this.changeDialog) {
        this.hasLicense = true
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
                this.hasLicense = false
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
  components: {
    'kap-change-lang': changeLang,
    'kap-help': help,
    'license': license
  },
  created () {
    this.checkLicense()
  },
  computed: {
    isPlusVersion () {
      var kapVersionInfo = this.$store.state.system.serverAboutKap
      return kapVersionInfo && kapVersionInfo['kap.version'] && kapVersionInfo['kap.version'].indexOf('Plus') !== -1
    },
    rules () {
      return {
        username: [{ required: true, message: this.$t('noUserName'), trigger: 'blur' }],
        password: [{ required: true, message: this.$t('noUserPwd'), trigger: 'blur' }]
      }
    }
  },
  locales: {
    'en': {
      welcome: 'Welcome to Kyligence Enterprise',
      kapMsg: 'Speed up Mission Critical Analytics Intelligently',
      loginIn: 'Login',
      userName: 'Username',
      password: 'Password',
      forgetPassword: 'Forget Password',
      noUserName: 'Please enter your username.',
      noUserPwd: 'Please enter your password.',
      adminTip: 'Apply the reset password command "kylin.sh admin-password-reset" in the "$KYLIN_HOME/bin" , <br/>the ADMIN account password will back to the initial password, <br/>and the other account password will remain unchanged.',
      license: 'Update License',
      cancel: 'Cancel',
      save: 'Save',
      enterLicensePartOne: 'Please upload the license file from local or enter license content to the box below.To get the evaluation license for free, please visit ',
      enterLicensePartTwo: '.',
      applyLicense: 'Apply Evaluation License',
      evaluationLicense: 'Evaluation License',
      evaluationPeriod: 'Evaluation Period:',
      noEmail: 'Please enter your email.',
      noEmailStyle: 'Please enter a usable email.',
      noCompany: 'Please enter your company name.',
      noName: 'Please enter your name.',
      enterpriseEmail: 'Please enter your enterprise email.',
      businessEmail: 'Business Mail',
      companyName: 'Company Name',
      yourName: 'Your Name',
      expiredOn: 'Expired On:',
      introduction: 'Introduction',
      manual: 'Manual',
      contactUs: 'Contact Us'
    },
    'zh-cn': {
      welcome: '欢迎使用Kyligence Enterprise',
      kapMsg: '智能加速关键业务分析',
      loginIn: '登录',
      userName: '用户名',
      password: '密码',
      forgetPassword: '忘记密码',
      noUserName: '请输入用户名',
      noUserPwd: '请输入密码',
      adminTip: '在"$KYLIN_HOME/bin"使用重置密码命令"kylin.sh admin-password-reset"，<br/>将ADMIN账户密码恢复为初始密码，<br/>其他账户密码将保持不变。',
      license: '更新许可证',
      cancel: '取消',
      save: '保存',
      enterLicensePartOne: '请从本地选择许可证上传或者输入许可证内容。注册',
      enterLicensePartTwo: '后，可自助申请免费的试用许可证。',
      applyLicense: '申请许可证',
      evaluationLicense: '有效许可证',
      evaluationPeriod: '有效期限：',
      noEmail: '请输入邮箱。',
      noEmailStyle: '请输入一个可用邮箱。',
      noCompany: '请输入公司名称。',
      noName: '请输入用户名称。',
      enterpriseEmail: '请输入企业邮箱',
      businessEmail: '企业邮箱',
      companyName: '公司名称',
      yourName: '用户名称',
      expiredOn: '过期时间：',
      introduction: '产品介绍',
      manual: '用户手册',
      contactUs: '联系我们'
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .applyLicense {
    .el-form-item {
      margin-bottom: 0px;
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
  .login-header{
    height: 52px;
    background-color: @color-menu-color1;
    ul {
      margin-top: 14px;
      li{
        vertical-align: middle;
        display: inline-block;
        margin-right: 20px;
      }
    }
    img {
      height: 28px;
      z-index: 999;
      margin: 12px 20px;
    }
    .help-box span{
      color: @fff;
    }
  }
  .login-footer{
    color: @text-darkbg-color;
    text-align: center;
    bottom: 0px;
    position: fixed;
    width: 100%;
    height: 22px;
    padding: 19px 0px;
    background-color: @color-menu-color1;
  }
  .login-box{
    width: 840px;
    border: 1px solid @line-split-color;
    position: absolute;
    background:@fff;
    top: 50%;
    margin-top: -208px;
    left: 50%;
    margin-left: -420px;
    .grid-content {
      text-align: center;
      height: 416px;
    }
    .login-msg {
      border-right: 1px solid @line-split-color;
      img{
        height: 71px;
        margin: 40px 0 31px;
      }
      p:first-of-type {
        color: @text-title-color;
        font-size: 20px;
        margin-bottom: 11px;
        font-weight: bold;
        height: 28px;
      }
      p:last-of-type {
        color: @text-title-color;
        height: 20px;
      }
      .ky-line{
        background: @line-split-color;
        width: 370px;
        margin: 20px 24px 0;
      }
      ul{
        margin: 20px 0 0 140px;
        li {
          list-style: none;
          text-align: left;
          margin-bottom: 2px;
          height: 22px;
          color: @text-normal-color;
          i {
            margin-right: 7px;
          }
          a {
            color: @text-normal-color;
          }
          a:hover {
            color: @base-color-1;
          }
        }        
        li:hover {
          color: @base-color-1;
        }
      }
    }
    .login-form {
      .el-form {
        width: 320px;
        padding: 116px 50px 0px;
      }
      .forget-pwd{
        text-align: right;
        // margin-top: -10px;
        font-size: 12px;
        color: @text-disabled-color;
        height: 16px;
        cursor: pointer;
      }
      .forget-pwd:hover{
        color:@base-color-2;
        text-decoration: underline;
      }
      .el-button {
        width: 320px;
      }
    }
  }
  .license-msg {
    .el-dialog__footer {
      div {
        cursor: pointer;
      }
    }
  }
</style>
