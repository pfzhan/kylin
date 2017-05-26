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
        v-model="isopend"
        on-color="#13ce66"
        off-color="#ff4949"
        @change
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
<el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" size="tiny">
  <el-form :model="kyBotAccount">
    <el-form-item prop="username">
      <el-input v-model="kyBotAccount.username" placeholder="username"></el-input>
    </el-form-item>
    <el-form-item prop="password">
      <el-input v-model="kyBotAccount.password" placeholder="password"></el-input>
    </el-form-item>
    <el-form-item>
      <el-button @click="loginKyBot" class="btn-loginKybot">Login</el-button>  
    </el-form-item>
  </el-form>
  <p class="no-account">No account? <a href="javascript:;">Sign up</a> now</p>
</el-dialog>
<el-dialog v-model="infoKybotVisible" title="KyBot自动上传" size="tiny">
  <p>KyBot通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。</p>
  <p>
    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">我已阅读并同意遵守《KyBot用户协议》</el-checkbox>
  </p>
  <el-button @click="afterAgree" type="primary" :disabled="!agreeKyBot" class="btn-agree">同意协议并开启KyBot自动服务</el-button>
</el-dialog>
</div>
</template>
<script>
  import { mapActions } from 'vuex'

  import aboutKap from '../common/about_kap.vue'

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
        isopend: false
      }
    },
    methods: {
      ...mapActions({
        getAboutKap: 'GET_ABOUTKAP'
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
            console.log(resp)
          })
          this.aboutKapVisible = true
        } else if (val === 'kybot') {
          this.kyBotUploadVisible = true
        }
      },
      // 登录kybot
      loginKyBot () {
        this.infoKybotVisible = true
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
      }
    },
    computed: {
      serverAbout () {
        return this.$store.state.system.serverAboutKap
      }
    },
    components: {
      'about_kap': aboutKap
    },
    locales: {
      'en': {},
      'zh-cn': {}
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
