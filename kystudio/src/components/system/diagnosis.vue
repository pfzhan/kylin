<template>
<div class="diagnosis-wrap">
  <div class="dia-title">
    <p>{{$t('contentOne')}}
      <a href="https://kybot.io/" target="_blank">KyBot</a>
      {{$t('contentTwo')}}
    </p>
  </div>
  
  <!-- <p>{{$t('contentTip')}}</p> -->
  <div class="select-time" v-if="selectTimer">
    <div class="choices">
      <p class="hd">{{$t('selectTime')}}</p>
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
        <span class="line"></span>
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
    <el-button type="primary" @click="upload" :loading="uploadLoading">{{$t('kybotUpload')}}</el-button>
    <br />
    <p class="upload-wrap">
      <a @click="dump" class="uploader" href="javascript:;" target="_blank">{{$t('kybotDumpOne')}}</a>
      {{$t('kybotDumpTwo')}}
      <el-tooltip content="slot#content" placement="right" effect="light">
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
  <el-dialog v-model="kyBotUploadVisible" title="KyAccount | Sign in" @close="resetLoginKybotForm" :modal="false">
    <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm"></login_kybot>
  </el-dialog>
  
  <!-- <el-dialog v-model="infoKybotVisible" title="KyBot自动上传" size="tiny">
    <start_kybot @onStart="closeStartLayer" :propAgreement="infoKybotVisible"></start_kybot>
  </el-dialog> -->
  <!-- 协议弹层 -->
  <el-dialog v-model="protocolVisible" class="agree-protocol" :modal="false" @close="agreeKyBot = false">
    <p>{{$t('contentOne')}}
      <a href="https://kybot.io/" target="_blank">KyBot</a>
      {{$t('contentTwo')}}
    </p>
    <div>
      <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">
        <a @click="showProtocol" class="btn-showProtocol">{{$t('protocol')}}</a>
      </el-checkbox>
    </div>
    <el-button @click="agreeProtocol" :loading="agreeLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeProtocol')}}</el-button>
  </el-dialog> 
  <!-- 协议内容弹层 -->
    <el-dialog v-model='proContentVisivle' class="pro-content" size="large" :modal="false">
      <protocol_content></protocol_content>
    </el-dialog>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import $ from 'jQuery'
import { apiUrl } from '../../config'
import loginKybot from '../common/login_kybot.vue'
import protocolContent from '../system/protocol.vue'

export default {
  name: 'diagnosis',
  props: ['targetId', 'selectTimer'],
  data () {
    return {
      newConfig: {
        key: '',
        value: ''
      },
      radio: 1,
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
      this.getJobKybot(id).then((resp) => {
        handleSuccess(resp, (data, code, status, msg) => {
          if (data) {
            this.uploadLoading = false
            this.diagnosisVisible = false
            // 关闭
            this.protocolVisible = false
            this.kyBotUploadVisible = false
            this.$message({
              type: 'success',
              message: this.$t('uploaded')
            })
          }
        })
      }).catch((res) => {
        this.uploadLoading = false
        handleError(res)
      })
    },
    resetLoginKybotForm () {
      this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
      this.uploadLoading = false
    },
    uploading () {
      this.uploadLoading = true
      this.getKybotUpload({startTime: this.startTime, endTime: this.endTime}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          if (data) {
            this.uploadLoading = false
            this.diagnosisVisible = false
            // 关闭
            this.protocolVisible = false
            this.kyBotUploadVisible = false
            this.$message({
              type: 'success',
              message: this.$t('uploaded')
            })
          } else {
            handleError(res)
          }
        })
      }).catch((res) => {
        this.uploadLoading = false
        handleError(res)
      })
    },
    closeLoginForm () {
      this.kyBotUploadVisible = false
      // this.infoKybotVisible = true
    },
    dump: function () {
      let href = ''
      if (this.selectTimer) {
        this.startTime = +new Date(this.startTime)
        href = apiUrl + 'kybot/dump?startTime=' + this.startTime + '&endTime=' + this.endTime
      } else {
        href = apiUrl + 'kybot/dump?target=' + this.targetId
      }
      $('.uploader').attr('href', href)
    },
    changeRange (radio) {
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
      let _this = this
      if (this.canChangePickStart) {
        this.radio = ''
      }
      this.hasErr = false // default everything is ok
      this.pickerOptionsEnd.disabledDate = (time) => { // set date-picker endTime
        let nowDate = new Date(_this.startTime)
        nowDate.setMonth(nowDate.getMonth() + 1)// 后一个月
        // let v1 = time.getTime() > +new Date(_this.startTime) + 30 * 24 * 60 * 60 * 1000
        let v1 = time.getTime() > +nowDate
        let v2 = time.getTime() < +new Date(_this.startTime) - 8.64e7
        this.maxTime = +nowDate // 缓存最大值 endTime
        return (v1 || v2)
      }
      if (this.startTime > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err1')
      } else if (this.startTime + 5 * 60 * 1000 > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err2')
      } else if (this.maxTime < this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err3')
      }
      this.canChangePickStart = true
    },
    changeEndTime () {
      if (this.canChangePickEnd) {
        this.radio = ''
      }
      this.hasErr = false // default everything is ok
      this.canChangePickEnd = true
      if (this.startTime > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err1')
      } else if (this.startTime + 5 * 60 * 1000 > this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err2')
      } else if (this.maxTime < this.endTime) {
        this.hasErr = true
        this.errMsgPick = this.$t('err3')
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
  computed: {
  },
  components: {
    'login_kybot': loginKybot,
    'protocol_content': protocolContent
  },
  mounted () {
    this.canChangePick = false
    this.changeRange(1)
    this.radio = 1
  },
  locales: {
    'en': {kybotUpload: 'Generate and sync package to KyBot', contentOne: 'By analyzing your diagnostic package, ', contentTwo: 'can provide online diagnostic, tuning and support service for KAP.', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: 'Only generate', kybotDumpTwo: ', Manual upload ', selectTime: 'Select Time Range', last1: 'Last one hour', last2: 'Last one day', last3: 'Last three days', last4: 'Last one month', chooseDate: 'Choose Date', tipTitle: 'If there is no public network access, diagnostic package can be upload manually as following:', tipStep1: '1. Download diagnostic package', tipStep2: '2. Login on KYBOT', tipStep3: '3. Click upload button on the top left of KyBot home page, and select the diagnostic package desired on the upload page to upload', err1: 'start time must less than end time', err2: 'at least 5 mins', err3: 'most one month', uploaded: 'uploaded successfully', protocol: '《KyBot Term of Service》', agreeProtocol: 'I have read and agree'},
    'zh-cn': {kybotUpload: '一键生成诊断包至KyBot', contentOne: '通过分析生成的诊断包，', contentTwo: '提供在线诊断，优化服务。', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: '下载诊断包', kybotDumpTwo: ', 手动上传 ', selectTime: '选择时间范围', last1: '上一小时', last2: '上一天', last3: '过去3天', last4: '最近一个月', chooseDate: '选择日期', tipTitle: '如无公网访问权限，可选择手动上传，操作步骤如下：', tipStep1: '1. 点击下载诊断包', tipStep2: '2. 登录KYBOT', tipStep3: '3. 在首页左上角点击上传按钮，在上传页面选择已下载的诊断包上传', err1: '开始时间必须小于结束时间', err2: '至少选择5分钟之后', err3: '至多选择一个月之内', uploaded: '上传成功', protocol: '《KyBot用户协议》', agreeProtocol: '我已阅读并同意'}
  }
}
</script>
<style lang="less">
.diagnosis-wrap {
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
  }
  .dia-title {
    position: relative;
    line-height:20px;
    padding-bottom: 10px;
    border-bottom: 1px solid #ddd;
    text-align: center;
    p {
      width: 400px;
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
    background: #fff;
    border-left: 1px solid #ddd;
    border-bottom: 1px solid #ddd;
    transform: rotate(-45deg);
  }
  .select-time {
    padding: 10px 100px 20px;
    .hd {
      height: 30px;
      line-height: 30px;
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
