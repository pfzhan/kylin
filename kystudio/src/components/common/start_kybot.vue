<template>
  <div class="start_kybot">
	  	<p><a href="https://kybot.io" target="_blank" class="blue">KyBot</a> {{$t('protocol')}}</p>
	    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">{{$t('hasAgree')}}</el-checkbox>
	  </p>
	  <el-button @click="startService" :loading="startLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeAndOpen')}}</el-button>
	</div>
</template>
<script>
  import { mapActions } from 'vuex'
  import { handleSuccess, handleError } from '../../util/business'

  export default {
    name: 'help',
    props: ['propAgreement'],
    data () {
      return {
        agreeKyBot: false,
        startLoading: false
      }
    },
    methods: {
      ...mapActions({
        startKybot: 'START_KYBOT',
        getAgreement: 'GET_AGREEMENT',
        setAgreement: 'SET_AGREEMENT'
      }),
      // 同意协议并开启自动服务
      startService () {
        this.startLoading = true
        // 开启自动服务
        this.startKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            if (data) {
              this.$message({
                type: 'success',
                message: this.$t('openSuccess')
              })
              this.startLoading = false
              this.$emit('closeStartLayer')
              this.$emit('openSwitch')
            }
          })
        }).catch((res) => {
          handleError(res)
        })
        // 同意协议
        this.setAgreement().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
          })
        }, (res) => {
        })
      }
    },
    watch: {
      propAgreement: function (val) {
        if (!val) {
          this.agreeKyBot = false
        }
      }
    },
    locales: {
      'en': {agreeAndOpen: 'Enable Auto Upload', hasAgree: 'I have read and agree《KyBot Term of Service》', protocol: 'By analyzing your diagnostic package, KyBot can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages everyday regularly.', openSuccess: 'open successfully'},
      'zh-cn': {agreeAndOpen: '开启自动上传', hasAgree: '我已阅读并同意《KyBot用户协议》', protocol: '通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。', openSuccess: '开启成功'}
    }
  }
</script>
<style lang="less">
  @import url(../../less/config.less);
  .start_kybot {
    .btn-agree {
      display: block;
      margin: 20px auto;
    }
    .blue {
      color: #20a0ff;
    }
    .el-checkbox{
      color: @content-color;
    }
  }
</style>

