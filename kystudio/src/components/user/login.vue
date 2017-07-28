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
            <li><i class="icon_introduction"></i> <a href="http://kyligence.io/kap/" target="_blank">KAP Introduction</a></li>
            <li><i class="icon_manual"></i> <a href="http://docs.kyligence.io" target="_blank">KAP Manual</a></li>
            <li><i class="icon_contact"></i> <a href="mailto:support@kyligence.io" target="_blank">Contact us</a></li>
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
              <el-input  type="password" v-model="user.password" :placeholder="$t('password')"></el-input>
            </el-form-item>
            </div>
            <el-form-item>
              <kap-icon-button type="primary"  @keyup.native.enter="onLoginSubmit" @click.native="onLoginSubmit" :useload="false" ref="loginBtn">{{$t('loginIn')}}</kap-icon-button>
            </el-form-item>
          </el-form>
          <a class="forget_pwd" v-show="user.username==='ADMIN'">{{$t('forgetPassword')}}
            <common-tip :content="$t('adminTip')" >
              <icon name="question-circle-o"></icon>
            </common-tip>
          </a>
        </div>
      </el-col>
    </el-row>
     <p class="ksd_footer">&copy;2016 <a href="http://kyligence.io/" target="_blank">Kyligence</a> Inc. All rights reserved.</p>
  </div>
  <el-dialog @close="closeDialog" :title="$t('license')" v-model="hasLicense" size="small">
    <el-alert 
    title=""
    type="info"
    show-icon
    :closable="false">
    <p style="text-align: left;">{{$t('enterLicensePartOne')}}
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
export default {
  name: 'login',
  data () {
    return {
      rules: {
        username: [{ required: true, message: this.$t('noUserName'), trigger: 'blur' }],
        password: [{required: true, message: this.$t('noUserPwd'), trigger: 'blur'}]
      },
      user: {
        username: '' || localStorage.getItem('username'),
        password: ''
      },
      btnLock: false,
      needLicense: false,
      loadCheck: false
    }
  },
  methods: {
    ...mapActions({
      login: 'LOGIN',
      getAboutKap: 'GET_ABOUTKAP',
      saveLicenseContent: 'SAVE_LICENSE_CONTENT',
      saveLicenseFile: 'SAVE_LICENSE_FILE'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER'
    }),
    onLoginSubmit () {
      this.$refs['loginForm'].validate((valid) => {
        if (valid) {
          Vue.http.headers.common['Authorization'] = 'Basic ' + Base64.encode(this.user.username + ':' + this.user.password)
          this.$refs['loginBtn'].loading = true
          this.login(this.user).then((res) => {
            handleSuccess(res, (data) => {
              Vue.http.headers.common['Authorization'] = ''
              this.$refs['loginBtn'].loading = false
              this.setCurUser({ user: data })
              if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(this.user.password) && this.user.username === 'ADMIN') {
                this.$router.push('/system/user')
              } else {
                this.$router.push('/dashboard')
              }
              localStorage.setItem('username', this.user.username)
              this.$store.state.config.overLock = false
            })
          }, (res) => {
            handleError(res)
            Vue.http.headers.common['Authorization'] = ''
            this.$refs['loginBtn'].loading = false
          })
        }
      })
    },
    checkLicense () {
      this.getAboutKap().then((result) => {
      }, (resp) => {
      })
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
            this.loadCheck = false
          })
        }, (res) => {
          handleError({data: res})
        })
      } else {
        this.saveLicenseContent(license.content).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.loadCheck = false
          })
        }, (res) => {
          handleError(res)
        })
      }
      this.loadCheck = false
    },
    closeDialog: function () {
      this.$store.state.system.serverAboutKap['kap.dates'] = ''
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
    hasLicense () {
      if (this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.dates'] !== null) {
        return false
      } else {
        return true
      }
    }
  },
  locales: {
    'en': {
      welcome: 'Welcome to Kyligence Analytics Platform(KAP)',
      loginIn: 'Login',
      userName: 'Username',
      password: 'Password',
      forgetPassword: 'Forget your password?',
      noUserName: 'please enter your username',
      noUserPwd: 'please enter your password',
      adminTip: 'Apply the reset password command "kylin.sh admin-password-reset" in the "KYLIN_HOME/" , <br/>the ADMIN account password will back to the initial password, <br/>and the other account password will remain unchanged.',
      license: 'Update License',
      cancel: 'Cancel',
      save: 'Save',
      enterLicensePartOne: 'Please upload the license file from local or enter license content to the box below.To get the evaluation license for free, please visit ',
      enterLicensePartTwo: '.'
    },
    'zh-cn': {
      welcome: '欢迎使用Kyligence Analytics Platform(KAP)',
      loginIn: '登录',
      userName: '用户名',
      password: '密码',
      forgetPassword: '忘记密码？',
      noUserName: '请输入用户名',
      noUserPwd: '请输入密码',
      adminTip: '在"KYLIN_HOME/"使用重置密码命令"kylin.sh admin-password-reset"，<br/>将ADMIN账户密码恢复为初始密码，<br/>其他账户密码将保持不变。',
      license: '更新许可证',
      cancel: '取消',
      save: '保存',
      enterLicensePartOne: '请从本地选择许可证上传或者输入许可证内容。注册',
      enterLicensePartTwo: '后，可自助申请免费的试用许可证。'
    }
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  #loginPage{
    height: 60px;
    ul {
      li{
        display: inline-block;
        height: 60px;
        line-height: 60px;
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
     .el-row{
       // background:url(../../assets/img/login_bg.png);
       background-size: contain;
       background-position: right top;
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
