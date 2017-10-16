<template>
  <div>
    <div id="loginPage">
      <ul class="ksd-fright">
        <!-- 在登录页不显示onoff -->
        <li><kap-help isLogin="login"></kap-help></li>
        <li><kap-change-lang isLogin="login"></kap-change-lang></li>
      </ul>
    </div>
    <div id="loginBox">
      <el-row :gutter="0">
        <el-col :span="12">
          <div class="grid-content bg-purple content_part1">
            <img src="../../assets/img/logo.png" class="logo">
            <p class="welcome">{{$t('welcome')}}</p>
            <div class="line"></div>
            <ul>
              <li><i class="icon_introduction"></i><a href="http://kyligence.io/kap/" target="_blank">KAP Introduction</a></li>
              <li><i class="icon_manual"></i><a href="http://docs.kyligence.io" target="_blank">KAP Manual</a></li>
              <li><i class="icon_contact"></i><a href="mailto:support@kyligence.io" target="_blank">Contact us</a></li>
            </ul>
          </div>
        </el-col>
        <el-col :span="12" class="loginform_box">
          <div class="grid-content bg-purple content_part2">
             <h2>{{$t('loginIn')}}</h2>
             <el-form   @keyup.native.enter="onLoginSubmit" class="login_form" :model="user" ref="loginForm" :rules="rules">
             <div class="input_group">
              <el-form-item label="" prop="username">
                <el-input v-model="user.username" auto-complete="on" :autofocus="true"  :placeholder="$t('userName')" name="username"></el-input>
              </el-form-item>
              <el-form-item label="" prop="password" class="password">
                <el-input  type="password" v-model="user.password" name="password" :placeholder="$t('password')"></el-input>
              </el-form-item>
              </div>
              <el-form-item>
                <kap-icon-button type="primary"  @keyup.native.enter="onLoginSubmit" @click.native="onLoginSubmit" :useload="false" ref="loginBtn">{{$t('loginIn')}}</kap-icon-button>
              </el-form-item>
            </el-form>
            <a class="forget_pwd" v-show="user.username==='ADMIN'">{{$t('forgetPassword')}}
              <common-tip :content="$t('adminTip')" >
                <icon name="question-circle" class="ksd-question-circle"></icon>
              </common-tip>
            </a>
          </div>
        </el-col>
      </el-row>
       <p class="ksd_footer">&copy;2016 <a href="http://kyligence.io/" target="_blank">Kyligence Inc.</a> All rights reserved.</p>
    </div>

    <el-dialog class="kapLicense" @close="closeDialog" :title="$t('license')" v-model="hasLicense" size="small" v-if="isPlusVersion" :close-on-click-modal="false">
      <el-alert  title="" type="info" show-icon :closable="false">
        <p class="ksd-left">{{$t('enterLicensePartOne')}}
           <a href="http://account.kyligence.io" target="_blank">Kyligence Account</a>
          {{$t('enterLicensePartTwo')}}
        </p>
      </el-alert>
      <license ref="licenseEnter" v-on:validSuccess="licenseValidSuccess"></license>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeDialog">{{$t('cancel')}}</el-button>
        <el-button type="primary" :loading="loadCheck" @click="licenseForm">{{$t('save')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="updateKAPLicense" @close="closeDialog" :title="$t('license')" v-model="hasLicense" size="tiny" v-if="!isPlusVersion" :close-on-click-modal="false">
      <license ref="licenseEnter" v-on:validSuccess="licenseValidSuccess"></license>
      <p @click="apply">{{$t('applyLicense')}}</p>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeDialog">{{$t('cancel')}}</el-button>
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
import { mapActions, mapMutations } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import changeLang from '../common/change_lang'
import license from './license'
import help from '../common/help'
import Vue from 'vue'
import { Base64 } from 'js-base64'
import { SystemPwdRegex, personalEmail } from '../../config/index'
export default {
  name: 'login',
  data () {
    return {
      rules: {
        username: [{ required: true, message: this.$t('noUserName'), trigger: 'blur' }],
        password: [{ required: true, message: this.$t('noUserPwd'), trigger: 'blur' }]
      },
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
        lang: 'en'
      },
      applyLoading: false,
      hasLicense: false,
      changeDialog: true
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
              if (!SystemPwdRegex.test(this.user.password) && this.user.username === 'ADMIN') {
                this.$router.push('/system/user')
              } else {
                this.$router.push('/dashboard')
              }
              localStorage.setItem('username', this.user.username)
              this.$store.state.config.overLock = false
            })
          }, (res) => {
            handleError(res)
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
      this.getAboutKap().then((result) => {
        handleSuccess(result, (data) => {
          if (this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.dates'] === null) {
            this.hasLicense = true
          }
        }, (resp) => {
          handleError(resp)
        })
      })
    },
    licenseForm: function () {
      this.$refs['licenseEnter'].$emit('licenseFormValid')
      this.loadCheck = true
    },
    licenseValidSuccess: function (license) {
      if (license === true) {
        this.hasLicense = false
        this.$alert(this.$t('evaluationPeriod') + this.$store.state.system.serverAboutKap['kap.dates'], this.$t('evaluationLicense'), {
          cancelConfirmButton: true,
          type: 'success'
        })
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
          this.trialLicenseFile(this.userMessage).then((res) => {
            handleSuccess(res, (data) => {
              if (data && data['kap.dates']) {
                console.log(this.lastTime(data['kap.dates']), 7777)
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
    }
  },
  locales: {
    'en': {
      welcome: 'Welcome to Kyligence Analytics Platform(KAP)',
      loginIn: 'Login',
      userName: 'Username',
      password: 'Password',
      forgetPassword: 'Forget your password?',
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
      expiredOn: 'Expired On:'
    },
    'zh-cn': {
      welcome: '欢迎使用Kyligence Analytics Platform(KAP)',
      loginIn: '登录',
      userName: '用户名',
      password: '密码',
      forgetPassword: '忘记密码？',
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
      expiredOn: '过期时间：'
    }
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .kapLicense {
    .el-dialog {
      .el-dialog__header {
        .el-dialog__title {
          font-size: 14px;
        }
      }
      .el-dialog__body {
        padding: 0px 20px 20px 20px;
      }
      .el-input__inner {
        font-size: 14px;
      }
      .el-alert {
        margin-top: 10px;
        border: #218fea solid 1px;
        background-color: rgba(33,143,234,0.1);
        p {
          font-weight: bold;
          cursor:pointer;
          color:#218fea;
          font-size:14px;
          a {
            color:#fff;
            text-decoration-line: underline;
          }
        }
      }
    }
  }
  .applyLicense {
    .el-dialog {
      .el-dialog__title {
        font-size: 14px;
      }
    }
    .el-input__inner {
      font-size: 14px;
    }
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
  #loginPage{
    height: 60px;
    ul {
      li{
        display: inline-block;
        height: 60px;
        margin-right: 40px;
      }
    }
    .logobox{
      width: 100px;
      height: 60px;
      background-color:@base-color;
    }
    .logo {
      height: 40px;
      // width: 40px;
      vertical-align: middle;
      z-index:999;
      margin: 10px 10px 13px 30px;
    }
  }
  .ksd_footer{
    font-size: 14px;
    text-align: center;
    bottom: -54px;
    position: absolute;
    width: 100%;
    color:#c0ccda;

  }
   #loginBox{
    .icon_introduction,.icon_manual,.icon_contact{
      display: inline-block;
      width: 12px;
      height: 9px;
    }
    .icon_introduction {
      background-image: url('../../assets/img/loginintroduction.png');
      background-size:cover;
    }
    .icon_manual {
      width: 10px;
      height:12px;
      background-image: url('../../assets/img/loginmanual.png');
      background-size: cover;
    }
    .icon_contact {
      background-image: url('../../assets/img/logincontactus.png');
      background-size: cover;
    }
    border-radius: 4px;
    position: absolute;
    width: 586px;
    height: 300px;
    top:50%;
    left: 50%;
    margin-left: -293px;
    margin-top: -150px;
    box-shadow: 0 0 10px #222;
    background: @grey-color;
    .logo{
      height: 65px;
      margin-top: 25px;
      margin-bottom: 15px;
    }
    .welcome{
      color:#fff;
      font-size: 18px;
    }
    .content_part1{
       text-align: center;
       height: 300px;
       width: 293px;
       background-color:@base-color;
       .line{
        height: 1px;
        background: #4fa6ee;
        width: 90%;
        margin: 0 auto;
        margin-top: 15px;
       }
       ul{
        margin: 20px 0 0 30px;
         li{
           i{
            margin-right: 10px;
           }
           a{
            color: #fff;
           }
           color: #fff;
           list-style: none;
           text-align: left;
           font-size: 14px;
           margin-bottom: 10px;
         }
       }
     }
     .content_part2 {
       h2{
        font-size:18px;
        color: @fff;
        margin-top: 45px;
        margin-left: 25px;
        margin-bottom: 10px;
       }
       .forget_pwd{
         margin-left: 25px;
         font-size: 12px;
         color:#c0ccda;
         cursor: pointer;
         text-decoration: none;
       }
       .login_form{
        padding: 15px 25px 0px 25px;
       }
       .el-button{
        width: 100%;
       }
       .el-form-item{
        margin-bottom: 20px;
         &.password{
          .el-input__inner{
             border-radius: 0 0 2px 2px;
           }
         }
       }
       .el-input__inner{
         border-radius: 2px 2px 0 0;
         background: @input-bg;
         color: @fff;
         border-color: @input-bg;
       }
     }
   }
</style>
