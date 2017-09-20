<template>
  <div class="start_kybot">
      <p v-if="$lang==='en'">
        By analyzing your diagnostic package, <a href="https://kybot.io" target="_blank" class="blue">KyBot</a> can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages everyday regularly.
      </p>
      <p v-if="$lang==='zh-cn'">
        <a href="https://kybot.io/#/home?src=kap250" target="_blank" class="blue">KyBot</a> {{$t('protocol')}}
      </p>
	    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot"></el-checkbox>
      <el-button type="text" style="font-size: 12px; margin-left: -8px;" @click="openAgreement">{{$t('hasAgree')}}</el-button>
	  </p>
	  <el-button id="start-kybot" @click="startService" :loading="startLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeAndOpen')}}</el-button>
	</div>
</template>
<script>
  import { mapActions } from 'vuex'
  import { handleSuccess, handleError } from '../../util/business'
  // import Vue from 'vue'

  export default {
    name: 'help',
    props: ['propAgreement'],
    data () {
      return {
        agreeKyBot: false,
        startLoading: false,
        lang: ''
      }
    },
    methods: {
      openAgreement () {
        const h = this.$createElement
        this.$msgbox({
          title: this.$t('kybotAgreement'),
          message: h('p', {style: 'height: 500px; overflow: scroll'}, [
            h('pre', {}, this.$t('kylinLang.kybotXY.agreement'))
          ]),
          showCancelButton: false
        }).then(action => {

        })
      },
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
    created () {
      console.log(this)
    },
    watch: {
      propAgreement: function (val) {
        if (!val) {
          this.agreeKyBot = false
        }
      }
    },
    locales: {
      'en': {agreeAndOpen: 'Enable Auto Upload', kybotAgreement: 'KyBot User Agreement', hasAgree: 'I have read and agree《KyBot Term of Service》', protocol: 'By analyzing your diagnostic package, KyBot can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages everyday regularly.', openSuccess: 'open successfully'},
      'zh-cn': {agreeAndOpen: '开启自动上传', kybotAgreement: 'KyBot用户协议', hasAgree: '我已阅读并同意《KyBot用户协议》', protocol: '通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。', openSuccess: '开启成功'}
    }
  }

</script>
<style lang="less">
  @import url(../../less/config.less);
  .start_kybot {
    text-align: left;
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
    pre {
      white-space: pre-wrap; /* css-3 */
      white-space: -moz-pre-wrap; /* Mozilla, since 1999 */
      white-space: -pre-wrap; /* Opera 4-6 */
      white-space: -o-pre-wrap; /* Opera 7 */
      word-wrap: break-word; /* Internet Explorer 5.5+ */
    }
  }
  .el-button--primary{
    &.is-disabled{
      color: #e1e1e1!important;
      background: #999!important;
    }
    &.is-disabled:hover{
      background: #999!important;
      span{
        color: #e1e1e1;
      }
    }
  }
</style>
