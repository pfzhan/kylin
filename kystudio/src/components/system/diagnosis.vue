<template>
<div class="diagnosis-wrap">
  <div class="dia-title" style="border-bottom:solid 1px #ddd;">
    <p style="font-size:14px" v-if="job">{{$t('contentOneForJob')}}
      <a href="https://kybot.io/#/home?src=kap250" target="_blank" style="font-size:20px;">KyBot</a>
      {{$t('contentTwo')}}
    </p>
    <p style="font-size:14px" v-else="job">{{$t('contentOne')}}
      <a href="https://kybot.io/#/home?src=kap250" target="_blank" style="font-size:20px;">KyBot</a>
      {{$t('contentTwo')}}
    </p>

  </div>

  <!-- <p>{{$t('contentTip')}}</p> -->
  <div class="select-time" v-if="selectTimer">
    <div class="choices">
      <p class="hd" style="font-size: 13px;">{{$t('selectTime')}}&nbsp;&nbsp;( {{$t('timeLimits')}} )</p>
      <el-radio-group v-model="radio" @change="changeRange">
        <el-radio :label="1" size="small">{{$t('last1')}}</el-radio>
        <el-radio :label="2" size="small">{{$t('last2')}}</el-radio>
        <el-radio :label="3" size="small">{{$t('last3')}}</el-radio>
        <el-radio :label="4" size="small">{{$t('last4')}}</el-radio>
      </el-radio-group>
      <div class="date-picker">
        <el-date-picker
          v-model="startTime"
          type="datetime"
          :placeholder="$t('chooseDate')"
          size="small"
          format="yyyy-MM-dd HH:mm"
          @change="changeStartTime"
          :picker-options="pickerOptionsStart">
        </el-date-picker>
        <span class="line" style="margin-top: 0px;"></span>
        <el-date-picker
          v-model="endTime"
          type="datetime"
          :placeholder="$t('chooseDate')"
          size="small"
          format="yyyy-MM-dd HH:mm"
          @change="changeEndTime"
          :picker-options="pickerOptionsEnd">
        </el-date-picker>
      </div>
      <p v-if="hasErr" class="err-msg">{{errMsgPick}}</p>
    </div>
  </div>
  <div class="footer">
    <el-button type="primary" @click="upload" :loading="uploadLoading" :class="{'notAllowed' : hasErr}">{{$t('kybotUpload')}}</el-button>
    <br/>
    <fake_progress class="ksd-mt-10" ref="fpro" :step="2" :speed="2000" :stroke="2" :width="50">
      <span slot="underlabel">{{$t('uploading')}}</span>
    </fake_progress>
    <br />
    <p class="upload-wrap">
      <a @click="dump" class="uploader" :class="{'notAllowed' : hasErr}" href="javascript:;" target="_blank">{{$t('kybotDumpOne')}}</a>
      {{$t('kybotDumpTwo')}}
      <el-tooltip content="slot#content" placement="right" effect="dark">
        <el-button class="ques">?</el-button>
        <div slot="content" class="system-upload-tips">
          <p class="tips">{{$t('tipTitle')}}</p>
          <p class="tips">{{$t('tipStep1')}}</p>
          <p class="tips">{{$t('tipStep2')}}</p>
          <p class="tips">{{$t('tipStep3')}}</p>
        </div>
      </el-tooltip>
    </p>
  </div>
  <!-- 登录弹层 -->
  <el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" @close="resetLoginKybotForm" :modal="false" size="large">
    <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm"></login_kybot>
  </el-dialog>

  <!-- <el-dialog v-model="infoKybotVisible" title="KyBot自动上传" size="tiny">
    <start_kybot @onStart="closeStartLayer" :propAgreement="infoKybotVisible"></start_kybot>
  </el-dialog> -->
  <!-- 协议弹层 -->
  <el-dialog v-model="protocolVisible" :title="$t('kybotAutoUpload')" class="agree-protocol" :modal="false" @close="agreeKyBot = false" size="large">
    <div v-if="job">
      <p v-if="$lang==='en'" style="font-size:14px;">
        By analyzing your job diagnostic package, <a href="https://kybot.io/#/home?src=kap250" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a> can provide online diagnostic, tuning and support service for KAP
      </p>
      <p v-if="$lang==='zh-cn'" style="font-size:14px;">
        <a href="https://kybot.io" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a>通过分析生产的任务诊断包，提供KAP在线诊断、优化及服务。
      </p>
    </div>
    <div v-else>
      <p v-if="$lang==='en'" style="font-size:14px;">
        By analyzing your diagnostic package, <a href="https://kybot.io/#/home?src=kap250" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a> can provide online diagnostic, tuning and support service for KAP
      </p>
      <p v-if="$lang==='zh-cn'" style="font-size:14px;">
        <a href="https://kybot.io/#/home?src=kap250" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务。
      </p>
    </div>
    <div>
      <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">
        <a @click="showProtocol" class="btn-showProtocol" style="font-size:14px;">{{$t('protocol')}}</a>
      </el-checkbox>
    </div>
    <el-button @click="agreeProtocol" :loading="agreeLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeProtocol')}}</el-button>
  </el-dialog>
  <!-- 协议内容弹层 -->
    <el-dialog v-model='proContentVisivle' :title="$t('kybotXY')" class="pro-content" size="large" :modal="false" style="top: -30%;">
      <protocol_content style="overflow: scroll; height: 400px;"></protocol_content>
      <span slot="footer" class="dialog-footer">
        <el-button @click="proContentVisivle = false">{{$t('close')}}</el-button>
        <!-- <el-button type="primary" @click="dialogVisible = false">确 定</el-button> -->
      </span>
    </el-dialog>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm } from '../../util/business'
import $ from 'jquery'
import { apiUrl } from '../../config'
import loginKybot from '../common/login_kybot.vue'
import protocolContent from '../system/protocol.vue'

export default {
  name: 'diagnosis',
  props: ['targetId', 'selectTimer', 'job', 'show'],
  data () {
    return {
      newConfig: {
        key: '',
        value: ''
      },
      radio: 2,
      startTime: '',
      endTime: '',
      pickerOptionsStart: {},
      pickerOptionsEnd: {
      },
      canChangePickStart: true,
      canChangePickEnd: true,
      hasErr: false,
      errMsgPick: '',
      maxTime: 0,
      uploadLoading: false,
      kyBotUploadVisible: false,
      // infoKybotVisible: false,
      protocolVisible: false,
      agreeKyBot: false,
      agreeLoading: false,
      proContentVisivle: false
    }
  },
  methods: {
    ...mapActions({
      getKybotUpload: 'GET_KYBOT_UPLOAD',
      getKybotDump: 'GET_KYBOT_DUMP',
      getKybotAccount: 'GET_KYBOT_ACCOUNT',
      getAgreement: 'GET_AGREEMENT',
      setAgreement: 'SET_AGREEMENT',
      loginKybot: 'LOGIN_KYBOT',
      getJobKybot: 'GET_JOB_KYBOT'
    }),
    checkLogin () {
    },
    upload: function () {
      this.uploadLoading = true
      this.startTime = +new Date(this.startTime)
      this.endTime = +new Date(this.endTime)
      // let _this = this
      //
      // _this.switchVisible = true
      //
      // this.getKybotUpload({startTime: this.startTime, endTime: this.endTime}).then((res) => {
      //   handleSuccess(res, (data, code, status, msg) => {
      //   })
      //   this.uploadLoading = false
      // }).catch((res) => {
      //   this.uploadLoading = false
      //   handleError(res, (data, code, status, msg) => {
      //     this.$message({
      //       type: 'error',
      //       message: msg
      //     })
      //   })
      // })
      this.getKybotAccount().then((resp) => {
        handleSuccess(resp, (data, code, status, msg) => {
          if (!data) {
            this.kyBotUploadVisible = true
            this.uploadLoading = false
          } else {
            this.getAgreement().then((res) => {
              handleSuccess(res, (data, code, status, msg) => {
                this.uploadLoading = false
                if (!data) { // 没有同意过协议 开协议层
                  // this.$emit('closeLoginOpenKybot')
                  this.protocolVisible = true
                } else {
                  // b)
                  if (this.targetId) {
                    this.uploadingJob(this.targetId)
                  } else {
                    // 上传
                    this.uploading()
                  }
                }
              })
            }).catch((res) => {
              handleError(res)
            })
          }
        })
      })
    },
    uploadingJob (id) {
      this.uploadLoading = true
      this.$refs.fpro.start()
      this.getJobKybot(id).then((resp) => {
        handleSuccess(resp, (data, code, status, msg) => {
          if (data) {
            this.uploadLoading = false
            this.diagnosisVisible = false
            // 关闭
            this.protocolVisible = false
            this.kyBotUploadVisible = false
            var h = this.$createElement
            kapConfirm(this.$t('uploaded'), {
              type: 'success',
              showCancelButton: false,
              message: h('p', null, [
                h('span', null, this.$t('uploadSuccess')),
                h('a', {attrs: {target: '_blank', href: 'https://kybot.io'}, style: {color: '#218fea'}}, 'KyBot'),
                h('span', null, this.$t('see'))
              ])
            })
          }
          this.$refs.fpro.stop()
        })
      }).catch((res) => {
        this.uploadLoading = false
        this.$refs.fpro.stop()
        handleError(res)
      })
    },
    resetLoadProgress () {
      this.$refs.fpro.stop()
    },
    resetLoginKybotForm () {
      this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
      this.uploadLoading = false
    },
    uploading () {
      this.uploadLoading = true
      this.$refs.fpro.start()
      this.getKybotUpload({startTime: this.startTime, endTime: this.endTime}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          if (data) {
            this.uploadLoading = false
            this.diagnosisVisible = false
            // 关闭
            this.protocolVisible = false
            this.kyBotUploadVisible = false
            var h = this.$createElement
            kapConfirm(this.$t('uploaded'), {
              type: 'success',
              showCancelButton: false,
              message: h('p', null, [
                h('span', null, this.$t('uploadSuccess')),
                h('a', {attrs: {target: '_blank', href: 'https://kybot.io/#/home?src=kap250'}, style: {color: '#218fea'}}, 'KyBot'),
                h('span', null, this.$t('see'))
              ])
            })
          } else {
            handleError(res)
          }
        })
        this.$refs.fpro.stop()
      }).catch((res) => {
        this.uploadLoading = false
        this.$refs.fpro.stop()
        handleError(res)
      })
    },
    closeLoginForm () {
      this.kyBotUploadVisible = false
      this.$refs.fpro.stop()
      // this.infoKybotVisible = true
    },
    dump: function () {
      let href = ''
      if (this.selectTimer) {
        this.startTime = +new Date(this.startTime)
        href = apiUrl + 'kybot/dump?startTime=' + this.startTime + '&endTime=' + this.endTime + '&currentTime=' + (new Date()).getTime()
      } else {
        href = apiUrl + 'kybot/dump?target=' + this.targetId
      }
      $('.uploader').attr('href', href)
    },
    changeRange (radio) {
      if (this.radio === '') {
        return
      }
      this.canChangePickStart = false
      this.canChangePickEnd = false
      this.hasErr = false
      radio = radio || this.radio // typeof radio is number
      let cur = new Date()
      let now = +cur
      this.endTime = now
      this.maxTime = now
      let onehour = 60 * 60 * 1000
      if (this.radio === 1) { // 过去1小时
        this.startTime = now - onehour
      } else if (this.radio === 2) { // 过去1天
        this.startTime = now - onehour * 24
      } else if (this.radio === 3) { // 过去3天
        this.startTime = now - onehour * 24 * 3
      } else if (this.radio === 4) { // 过去一个月
        this.startTime = now - onehour * 24 * 30
        cur.setMonth(cur.getMonth() - 1)
        this.startTime = +cur
      }
    },
    changeStartTime () {
      // 如果选择的时间超过一个月就令结束时间为开始时间加一个月
      let endTime = new Date(this.endTime)
      let lastMounth = endTime.setMonth(endTime.getMonth() - 1)
      // let _this = this
      if (this.canChangePickStart) {
        this.radio = ''
      }
      this.hasErr = false // default everything is ok
      this.startTime = +new Date(this.startTime)
      this.endTime = +new Date(this.endTime)
      if (isNaN(this.startTime) || isNaN(this.endTime)) {
        this.hasErr = true
        this.errMsgPick = this.$t('noTime')
      } else if (this.startTime > this.endTime) {
        // 提示：选择的时间不能小于5分钟
        const h = this.$createElement
        this.$msgbox({
          title: this.$t('kylinLang.common.tip'),
          message: h('p', {}, [
            h('div', {}, this.$t('lessThanTime'))
          ]),
          showCancelButton: false
        }).then(action => {
          // alert(1)
        })
        this.startTime = this.endTime - 5 * 60 * 1000
      } else if (this.startTime + 5 * 60 * 1000 > this.endTime) {
        // 选择的时间小于5分钟
        this.startTime = this.endTime - 5 * 1000 * 60
      } else if (this.maxTime < this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err3')
      } else if (this.startTime < lastMounth) {
        let startTime = new Date(this.startTime)
        let nextMounth = startTime.setMonth(startTime.getMonth() + 1)
        this.endTime = nextMounth
      }

      this.canChangePickStart = true
    },
    changeEndTime () {
      let endTime = new Date(this.endTime)
      let lastMounth = endTime.setMonth(endTime.getMonth() - 1)
      if (this.canChangePickEnd) {
        this.radio = ''
      }
      this.hasErr = false // default everything is ok
      this.canChangePickEnd = true
      this.startTime = +new Date(this.startTime)
      this.endTime = +new Date(this.endTime)
      // console.log('this.startTime + 5 * 60 * 1000 > this.endTime --', this.startTime, this.endTime, this.startTime + 5 * 60 * 1000 > this.endTime)
      // console.log('endTime ;;', this.startTime, this.endTime)
      let nowDate = +new Date()
      let expectMinEndTime = this.startTime + 5 * 60 * 1000
      // 控制用户不能选择当前时间之后的
      if (nowDate < this.endTime) {
        this.endTime = nowDate
      }
      // 控制用户至少选择开始时间后的5分钟之后
      if (this.endTime < expectMinEndTime) {
        this.endTime = expectMinEndTime
      }
      if (isNaN(this.startTime) || isNaN(this.endTime)) {
        this.hasErr = true
        this.errMsgPick = this.$t('noTime')
      } else if (this.startTime > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err1')
      } else if (this.startTime + 5 * 60 * 1000 > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err2')
      } else if (this.startTime < lastMounth) {
        // 如果选择的时间超过一个月就令结束时间为开始时间加一个月
        let startTime = new Date(this.startTime)
        let nextMounth = startTime.setMonth(startTime.getMonth() + 1)
        this.endTime = nextMounth
        // 提示：选择的时间不能超过一个月
        const h = this.$createElement
        this.$msgbox({
          title: this.$t('kylinLang.common.tip'),
          message: h('p', {}, [
            h('div', {}, this.$t('moreThanTime'))
          ]),
          showCancelButton: false
        }).then(action => {
          // alert(1)
        })
      }
    },
    closeStartLayer () {
      // this.infoKybotVisible = false
    },
    showProtocol () {
      this.proContentVisivle = true
    },
    agreeProtocol () {
      // 同意协议
      this.setAgreement().then((resp) => {
        handleSuccess(resp, (data, code, status, msg) => {
          if (data) {
            this.agreeLoading = true
            this.protocolVisible = false
            this.uploading()
          }
        })
      }, (res) => {
        // console.log('同意失败')
      })
    }
  },
  watch: {
    'show' (v) {
      if (!v) {
        this.$refs.fpro.stop()
        this.uploadLoading = false
      }
    }
  },
  computed: {
  },
  components: {
    'login_kybot': loginKybot,
    'protocol_content': protocolContent
  },
  mounted () {
    this.canChangePick = false
    this.changeRange(1)
    this.radio = 2
    this.pickerOptionsEnd.disabledDate = (time) => { // set date-picker endTime
      let nowTime = new Date().getTime()
      return time > nowTime
    }
    this.pickerOptionsStart.disabledDate = (time) => { // set date-picker endTime
      let nowTime = new Date().getTime()
      return time > nowTime
    }
  },
  locales: {
    'en': {kybotUpload: 'Generate and sync package to KyBot', contentOne: 'By analyzing your diagnostic package, ', contentOneForJob: 'By analyzing your job diagnostic package ', contentTwo: 'can provide online diagnostic, tuning and support service for KAP.', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: 'Only generate', kybotDumpTwo: ', Manual upload ', selectTime: 'Select Time Range', last1: 'Last one hour', last2: 'Last one day', last3: 'Last three days', last4: 'Last one month', chooseDate: 'Choose Date', tipTitle: 'If there is no public network access, diagnostic package can be upload manually as following:', tipStep1: '1. Download diagnostic package', tipStep2: '2. Login on KyBot', tipStep3: '3. Click upload button on the top left of KyBot home page, and select the diagnostic package desired on the upload page to upload', err1: 'start time must less than end time', err2: 'at least 5 mins', err3: 'most one month', uploaded: 'uploaded successfully', protocol: 'I have read and agree《KyBot Term of Service》', agreeProtocol: 'Enable One Click Upload', noTime: 'Please choose the startTime or endTime', timeLimits: 'More than five minutes and less than one month', moreThanTime: 'Can not more than one month', lessThanTime: 'The choice should not be less than five minutes', kybot: 'By analyzing your diagnostic package, KyBot can provide online diagnostic, tuning and support service for KAP', kybotAutoUpload: 'Kybot Diagnostic Pack Upload', kybotXY: 'Kybot User agreement', close: 'Close', uploading: 'uploading...', 'uploadSuccess': 'Upload successful, please sign in ', 'see': ' ', signIn: 'KyAccount | Sign In'},
    'zh-cn': {kybotUpload: '生成诊断包并上传至KyBot', contentOne: '通过分析生成的诊断包，', contentOneForJob: '通过分析生成的Job诊断包，', contentTwo: '提供在线诊断，优化服务。', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: '下载诊断包', kybotDumpTwo: ', 手动上传 ', selectTime: '选择时间范围', last1: '最近一小时', last2: '最近一天', last3: '最近三天', last4: '最近一个月', chooseDate: '选择日期', tipTitle: '如无公网访问权限，可选择手动上传，操作步骤如下：', tipStep1: '1. 点击下载诊断包', tipStep2: '2. 登录KyBot', tipStep3: '3. 在首页左上角点击上传按钮，在上传页面选择已下载的诊断包上传', err1: '开始时间必须小于结束时间', err2: '至少选择5分钟之后', err3: '至多选择一个月之内', uploaded: '上传成功', protocol: '我已阅读并同意《KyBot 用户协议》', agreeProtocol: '开启一键上传', noTime: '开始时间，结束时间不能为空', timeLimits: '大于五分钟小于一个月', moreThanTime: '时间不能超过一个月', lessThanTime: '选择的时间不能小于5分钟', kybot: 'Kybot通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传', kybotAutoUpload: 'KyBot 诊断包上传', kybotXY: 'Kybot用户协议', close: '确定', uploading: '上传中...', 'uploadSuccess': '上传成功，请登录 ', 'see': ' 查看', signIn: 'KyAccount | 登录'}
  }
}
</script>
<style lang="less">
.diagnosis-wrap {
  .blue{
    font-size: 15px;
  }
  .notAllowed {
    cursor: not-allowed;
    pointer-events: none;
  }
  .pro-content {
    .el-dialog {
      width: 500px;
    }
  }
  a {
    text-decoration: none;
  }
  .btn-showProtocol {
    font-size: 14px;
    color: #fff;
  }
  .dia-title {
    position: relative;
    line-height:20px;
    padding-bottom: 10px;
    border-bottom: 1px solid #ddd;
    text-align: center;
    p {
      // width: 400px;
      margin: 0 auto;
    }
  }
  .dia-title:after {
    position: absolute;
    bottom: -7px;
    left: 50%;
    content: '';
    width: 12px;
    height:12px;
    background: #393e53;
    border-left: 1px solid #ddd;
    border-bottom: 1px solid #ddd;
    transform: rotate(-45deg);
    opacity: 0;
  }
  .select-time {
    padding: 10px 100px 20px;
    .hd {
      height: 30px;
      line-height: 30px;
    }
    .el-radio__label {
      color: #d4d7e3;
    }
    .choices {
      width: 440px;
      margin: 0 auto;
      .el-radio__label {
        font-size: 12px;
      }
      .el-radio__inner {
        width: 14px;
        height:14px;
      }
      .el-radio-group {
        height: 40px;
        line-height: 40px;
      }
      .line {
        display: block;
        width: 12px;
        height: 1px;
        margin: 14px 4px;
        background: #aaa;
        transform: translateY(15px);
      }
      .date-picker {
        display: flex;
        display: -webkit-flex;
        display: -webkit-box;
        justify-content: middle;
      }
    }
  }
  .footer {
    text-align: center;
    .upload-wrap {
      height: 50px;
      line-height: 50px;
      a:hover {
        text-decoration: none;
      }
      .uploader {
        font-size: 14px;
      }
    }
  }
  .err-msg {
    height: 30px;
    line-height: 30px;
    color: red;
    font-size: 12px;
  }

}
.system-upload-tips {
  max-width: 500px;
}
.ques.el-button {
  border-radius: 50%;
  width: 16px;
  height: 16px;
  box-sizing: border-box;
  padding: 0;
  border: 1px solid #20a0ff;
  color: #20a0ff;
}
</style>
