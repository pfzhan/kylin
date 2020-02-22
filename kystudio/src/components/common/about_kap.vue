<template>
  <div class="about-kap">
    <div class="header">
      <el-row class="logo text-center">
        <a href="http://kyligence.io/" target="_blank">
          <img src="../../assets/img/kyligence-logo.png" alt="" />
        </a>
      </el-row>
      <el-row>
        <label for="">{{$t('version')}}</label>{{license(serverAboutKap && serverAboutKap['ke.version'])}}
      </el-row>
      <el-row>
        <label for="">{{$t('validPeriod')}}</label>{{licenseRange}}
      </el-row>
      <el-row>
        <label for="">{{$t('dataVolume')}}</label>
        {{serverAboutKap['ke.license.volume']}}
        <!-- <span v-if="license(serverAboutKap && serverAboutKap['ke.license.source.total']) !== 'Unlimited'">{{license(serverAboutKap && serverAboutKap['ke.license.source.used'])}} TB / {{license(serverAboutKap && serverAboutKap['ke.license.source.total'])}} TB</span>
        <span v-else>Unlimited</span> -->
      </el-row>
      <el-row>
        <label for="">{{$t('level')}}</label>{{license(serverAboutKap && serverAboutKap['ke.license.level'])}}
      </el-row>
      <el-row>
        <label for="">{{$t('licenseStatement')}}</label>
        {{license(serverAboutKap&&serverAboutKap['ke.license.statement'])}}
      </el-row>	
	  </div>
	  <div class="container">
	    <h3>{{$t('statement')}}</h3>
      <p v-if="serverAboutKap['ke.license.isEvaluation']" v-html="$t('kylinLang.system.evaluationStatement')"></p>
      <p v-else v-html="$t('kylinLang.system.statement')"></p>
      <div class="margin-split"></div>
      <el-row>
        <label for="">{{$t('serviceEnd')}}</label>{{license(serverAboutKap&&serverAboutKap['ke.license.serviceEnd'])}}
      </el-row>
      <el-row>
        <label for="">Kyligence Enterprise Commit: </label>{{license(serverAboutKap&&serverAboutKap['ke.commit'])}}
      </el-row>
      <!-- <el-row>
        <label for="">Kyligence Account: </label>
        <span v-if="$store.state.kybot.hasLoginAccount">{{$store.state.kybot.hasLoginAccount}} <a href="#" @click.prevent="logOut">{{$t('quit')}}</a></span>
        <a v-if="!$store.state.kybot.hasLoginAccount" href="#" @click.prevent="loginKyaccount">{{$t('login')}}</a>
      </el-row> -->
    </div>
    <div class="footer">
      <p class="details" v-html="$t('sendFile')"></p>
      <!-- <a class="buttonLink" target="_blank" href="api/system/license/info">{{$t('generateLicense')}}</a> -->
      <el-row class="text-center">
        <el-button type="primary" @click="requestLicense">{{$t('generateLicense')}}</el-button>
      </el-row>
      <el-row class="gray text-center">Copyright 2020 Kyligence Inc. All rights reserved.</el-row>
    </div>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
// import loginKybot from '../common/login_kybot.vue'
// import { handleSuccess, handleError, kapConfirm } from '../../util/business'
export default {
  name: 'about_kap',
  props: ['about'],
  data () {
    return {
      aboutKap: this.about,
      hasLoginAccount: false,
      kyBotLoginVisible: false
    }
  },
  computed: {
    serverAboutKap () {
      return this.$store.state.system.serverAboutKap
    },
    kyAccount () {
      return this.$store.state.system.kyAccount
    },
    statement () {
      // kapService.evaluationStatement
      // console.log('this.$store.state.system.statement   ', this.$store.state.system.statement)
      return this.$store.state.system.statement
    },
    licenseRange () {
      let range = ''
      if (this.license(this.serverAboutKap && this.serverAboutKap['ke.dates']) !== 'N/A') {
        const dates = this.serverAboutKap['ke.dates'].split(',')
        range = dates[0] + ' ' + this.$t('kylinLang.query.to') + ' ' + dates[1]
      }
      return range
    }
  },
  components: {
    // 'login_kybot': loginKybot
  },
  created () {
    // this.getKyAccountStatus()
  },
  methods: {
    ...mapActions({
      // logOutKyAccount: 'LOGOUT_KYBOT',
      // getKybotAccount: 'GET_CUR_ACCOUNTNAME'
    }),
    license (obj) {
      if (!obj) {
        return 'N/A'
      } else {
        return obj
      }
    },
    requestLicense () {
      location.href = 'api/system/license/info'
    }
    // getLicense () {
    //   let newWinLicense = window.open()
    //   newWinLicense.location.href = 'api/kap/system/requestLicense'
    // },
    // logOut () {
    //   kapConfirm(this.$t('logOutConfirm')).then(() => {
    //     this.logOutKyAccount().then(() => {
    //       this.getKyAccountStatus()
    //     }, (res) => {
    //       handleError(res)
    //     })
    //   })
    // },
    // loginKyaccount () {
    //   this.$store.state.kybot.loginKyaccountDialog = true
    // },
    // getKyAccountStatus () {
    //   this.getKybotAccount().then((res) => {
    //     handleSuccess(res, (data, code, status, msg) => {
    //       this.$store.state.kybot.hasLoginAccount = data
    //     }, (errResp) => {
    //       this.$store.state.kybot.hasLoginAccount = ''
    //       handleError(errResp)
    //     })
    //   })
    // }
  },
  locales: {
    'en': {
      version: 'Version: ',
      validPeriod: 'Valid Period: ',
      serviceEnd: 'Service End Time: ',
      enterLicense: 'Select License File Or Enter Your License',
      upload: 'Upload',
      license: 'License',
      statement: 'Service Statement',
      licenseStatement: 'License Statement: ',
      sendFile: 'You can apply EVALUATION license from <a target="_blank" href="https://account.kyligence.io">Kyligence account</a>.<br/>To apply for the ENTERPRISE license, please contact Kyligence sales with the License Request file.',
      noAccount: 'No account is configured in Kylin properties',
      generateLicense: 'Generate License Request File',
      updateLicense: 'Update License',
      logOutConfirm: 'Comfirm quit?',
      login: 'Login',
      quit: 'Quit',
      dataVolume: 'Data Source Volume: ',
      noLimitation: 'No limitation',
      level: 'Level: '
    },
    'zh-cn': {
      version: '版本：',
      validPeriod: '使用期限：',
      serviceEnd: '服务截止日期：',
      enterLicense: '请选择许可证文件或手动输入许可证',
      upload: '上传',
      license: '许可证',
      statement: '服务申明',
      licenseStatement: '许可声明：',
      sendFile: '申请试用许可证，请访问 <a target="_blank" href="https://account.kyligence.io">Kyligence account</a>。<br/>申请企业版许可证，请将下面生成的许可证申请文件发送给销售人员。',
      noAccount: '未在Kylin properties中配置KyAccount账号',
      generateLicense: '生成许可申请文件',
      updateLicense: '更新许可证',
      logOutConfirm: '确认要退出吗？',
      login: '登录',
      quit: '退出',
      dataVolume: '数据量额度：',
      noLimitation: '无限制',
      level: '产品等级：'
    }
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
  .about-kap {
    .logo {
      margin-top: 10px;
      margin-bottom: 40px;
    }
    .text-center {
      text-align: center;
    }
    .el-dialog__header{
      height: 55px;
      line-height: 55px;
    }
    .el-icon-close{
      margin-top: 15px;
    }
    a:-webkit-any-link {
      text-decoration: underline;
    }
    // .buttonLink{
    //   width: 300px!important;
    //   display: block;
    //   margin: 0 auto;
    //   background: @base-color;
    // }
	line-height:20px;
	font-size:14px;
	text-align: left;
	img {width: 88px;}
	label {font-weight: @font-medium;}
	// .header, 
	// .container {padding-bottom:20px;border-bottom:1px solid #424860;}
	h3 {margin-top:20px;font-size:14px;}
	.details {line-height:24px;margin:20px 0 20px;}
	.gray {margin-top:30px;color:#a2a2a2;font-size:12px;}
	// .buttonLink {padding:10px;color: #fff;border-radius:2px;background: #35a8fe;text-decoration: none;}
  .el-input{
    width: 100%;
    display: inline-table;
    margin-top: 20px;
    position: relative;
  }
  .el-input-group__append {
    width: 8%;
    background-color:#a2a2a2;
    border-style:none;
    position: static;
    .el-button {
      margin: 0 0 0 0;
      padding: 0 0 0 0;
      border-style: none;
    }
  }
  .el-upload-list {
    position: absolute;
    margin-top: 5px;
    left: 0px;
    width: 100%;
    li {
      width: 500px;
    }
  }
  .uploadButton {
    position: absolute;
    display: block;
  }
  .margin-split {
    margin-top: 20px;
  }
}
</style>
