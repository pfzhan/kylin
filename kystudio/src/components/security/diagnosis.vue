<template>
<div class="diagnosis-wrap">
  <el-dialog class="kybot_diagnosis" :close-on-click-modal="false" :title="$t('diagnosis')" :visible.sync="diagnosisVisible" @close="closeDialog">
    <div class="dia-title">
      <p v-if="job">{{$t('contentOneForJob')}}
        <a :href="kybotUrl" target="_blank" class="ksd-fs-20">KyBot</a>
        {{$t('contentTwo')}}
      </p>
      <p v-else="job">{{$t('contentOne')}}
        <a :href="kybotUrl" target="_blank" class="ksd-fs-20">KyBot</a>
        {{$t('contentTwo')}}
      </p>
    </div>

    <!-- <p>{{$t('contentTip')}}</p> -->
    <div class="select-time" v-if="selectTimer">
      <div class="choices">
        <p class="hd">{{$t('selectTime')}}&nbsp;&nbsp;( {{$t('timeLimits')}} )</p>
        <el-radio-group v-model="radio" @change="changeRange" class="ksd-mtb-10">
          <el-radio :label="1" size="small">{{$t('last1')}}</el-radio>
          <el-radio :label="2" size="small">{{$t('last2')}}</el-radio>
          <el-radio :label="3" size="small">{{$t('last3')}}</el-radio>
          <el-radio :label="4" size="small">{{$t('last4')}}</el-radio>
        </el-radio-group>
        <el-row class="date-picker">
          <el-col :span="24">
            <el-date-picker 
              v-model="startTime"
              type="datetime"
              :placeholder="$t('chooseDate')"
              size="medium"
              format="yyyy-MM-dd HH:mm"
              @change="changeStartTime"
              :picker-options="pickerOptionsStart">
            </el-date-picker>
            <span class="ksd-mrl-6"> — </span>
            <el-date-picker
              v-model="endTime"
              type="datetime"
              :placeholder="$t('chooseDate')"
              size="medium"
              format="yyyy-MM-dd HH:mm"
              @change="changeEndTime"
              :picker-options="pickerOptionsEnd">
            </el-date-picker>
          </el-col>
        </el-row>
       <!--  <div class="date-picker">
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
        </div> -->
        <p v-if="hasErr" class="err-msg">{{errMsgPick}}</p>
      </div>
    </div>
    <div class="select_condition">
     <el-form :model="diagnosisFormModel" :rules="rules" ref="diagnosisForm">
        <el-form-item class="ksd-mb-6 ksd-mt-6" prop="category" v-if="!targetId">
          <span slot="label" style="font-size:13px;">{{$t('checkQuestionTip')}}</span>
          <el-select  multiple filterable style="width:100%" v-model="diagnosisFormModel.category">
            <el-option :value="x.id" :key="x.id" :label="$t(x.name)" v-for="x in categoryList"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item  class="ksd-mb-20 ksd-mt-10" prop="server">
          <span slot="label" style="font-size:13px;">{{$t('checkServeiceTip')}}</span>
          <el-select  multiple filterable style="width:100%" v-model="diagnosisFormModel.server">
            <el-option :value="x.name" :key="x.id" :label="x.name" v-for="x in kybotServersList"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
    </div>
    <div slot="footer" class="dialog-footer footer">
      <el-tooltip placement="top-start">
        <el-button type="info" text size="medium" @click="dump" :class="{'notAllowed' : hasErr}" style="font-weight:400;">{{$t('kybotDumpOne')}} <span v-show="loadCount">{{loadCount}}/{{diagnosisFormModel.server.length}}</span></el-button>
        <div slot="content" class="system-upload-tips">
          <p class="tips">{{$t('tipTitle')}}</p>
          <p class="tips">{{$t('tipStep1')}}</p>
          <p class="tips">{{$t('tipStep2')}}</p>
          <p class="tips">{{$t('tipStep3')}}</p>
        </div>
      </el-tooltip>
      <el-button type="primary" plain size="medium" @click="upload" :loading="uploadLoading" :class="{'notAllowed' : hasErr}">{{$t('kybotUpload')}}</el-button>
      <div class="progress-block">
        <fake_progress class="ksd-mt-20 ksd-mb-30" ref="fpro" :step="2" :speed="2000">
          <span slot="underlabel">{{loadCount}}/{{diagnosisFormModel.server.length}}</span>
        </fake_progress>
        <fake_progress class="ksd-mt-20 ksd-mb-30" ref="fproDownload" :step="2" :speed="400">
        </fake_progress>
      </div>
    </div>
  </el-dialog>
  <!-- 登录弹层 -->
  <el-dialog :visible.sync="kyBotUploadVisible" :title="$t('login')" @close="resetLoginKybotForm" width="30%" append-to-body>
    <login_kybot ref="loginKybotForm" @closeLoginOpenKybot="closeLoginForm"></login_kybot>
  </el-dialog>

  <!-- 协议弹层 -->
  <el-dialog :visible.sync="protocolVisible" :title="$t('kybotAutoUpload')" :close-on-click-modal="false" class="agree-protocol" @close="agreeKyBot = false" append-to-body>
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
        By analyzing your diagnostic package, <a :href="kybotUrl" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a> can provide online diagnostic, tuning and support service for KAP
      </p>
      <p v-if="$lang==='zh-cn'" style="font-size:14px;">
        <a :href="kybotUrl" target="_blank" class="blue" style="font-size:14px;color:#218fea">KyBot</a>通过分析生产的诊断包，提供KAP在线诊断、优化及服务。
      </p>
    </div>
    <div class="ksd-mt-20">
      <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot">
        <a @click="showProtocol" class="btn-showProtocol" style="font-size:14px;">{{$t('protocol')}}</a>
      </el-checkbox>
    </div>
    <span slot="footer" class="dialog-footer">
      <el-button type="info" size="medium" plain disabled v-if="!agreeKyBot">{{$t('agreeProtocol')}}</el-button>
      <el-button @click="agreeProtocol" :loading="agreeLoading" plain type="primary" size="medium" class="btn-agree" v-else>{{$t('agreeProtocol')}}</el-button>
    </span>
  </el-dialog>
  <!-- 协议内容弹层 -->
  <el-dialog :visible.sync='proContentVisivle' :title="$t('kybotXY')" class="pro-content" append-to-body>
    <protocol_content style="overflow: scroll; height: 400px;"></protocol_content>
    <span slot="footer" class="dialog-footer">
      <el-button size="medium" @click="proContentVisivle = false">{{$t('close')}}</el-button>
      <!-- <el-button type="primary" @click="dialogVisible = false">确 定</el-button> -->
    </span>
  </el-dialog>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm } from '../../util/business'
import { downloadByFrame } from '../../util/index'
import loginKybot from '../common/login_kybot.vue'
import protocolContent from '../security/protocol.vue'
import { apiUrl } from '../../config'
export default {
  name: 'diagnosis',
  props: ['targetId', 'selectTimer', 'job', 'show'],
  data () {
    return {
      newConfig: {
        key: '',
        value: ''
      },
      diagnosisFormModel: {
        category: [1],
        server: []
      },
      categoryList: [
        {id: 1, name: 'categorylist.c1'},
        {id: 2, name: 'categorylist.c2'},
        {id: 3, name: 'categorylist.c3'},
        {id: 4, name: 'categorylist.c4'},
        {id: 5, name: 'categorylist.c5'}
      ],
      rules: {
        category: [
          {type: 'array', required: true, message: this.$t('checkQuestionValidTip'), trigger: 'blur'}
        ],
        server: [
          { type: 'array', required: true, message: this.$t('checkServerValidTip'), trigger: 'blur' }
        ]
      },
      kybotServersList: [],
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
      diagnosisVisible: false,
      kyBotUploadVisible: false,
      // infoKybotVisible: false,
      protocolVisible: false,
      agreeKyBot: false,
      agreeLoading: false,
      proContentVisivle: false,
      loadCount: 0
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
      getJobKybot: 'GET_JOB_KYBOT',
      getKybotServers: 'GET_KYBOT_SERVERS'
    }),
    checkLogin () {
    },
    closeDialog: function () {
      this.diagnosisVisible = false
      this.$emit('closeModal')
    },
    upload: function () {
      this.$refs.diagnosisForm.validate((valid) => {
        if (valid) {
          this.uploadLoading = true
          this.startTime = +new Date(this.startTime)
          this.endTime = +new Date(this.endTime)
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
        }
      })
    },
    uploadingJob (id) {
      this.uploadLoading = true
      this.$refs.fpro.start()
      this.loadCount = 0
      this.diagnosisFormModel.server.forEach((i, index) => {
        var serverName = location.protocol + '//' + i
        this.getJobKybot({target: id, host: serverName}).then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            if (data) {
              this.loadCount++
              if (this.loadCount === this.diagnosisFormModel.server.length) {
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
            }
          })
        }, (res) => {
          this.uploadLoading = false
          this.$refs.fpro.stop()
          handleError(res)
        })
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
      var count = 0
      this.diagnosisFormModel.server.forEach((i, index) => {
        var serverName = location.protocol + '//' + i
        // var serverName = 'http://10.1.2.103:7070'
        this.getKybotUpload({
          startTime: this.startTime,
          endTime: this.endTime,
          host: serverName,
          'types[]': this.diagnosisFormModel.category.join(','),
          currentTime: (new Date()).getTime()
        }).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            if (data) {
              count++
              if (count === this.diagnosisFormModel.server.length) {
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
                    h('a', {attrs: {target: '_blank', href: this.kybotUrl}, style: {color: '#218fea'}}, 'KyBot'),
                    h('span', null, this.$t('see'))
                  ])
                })
                this.$refs.fpro.stop()
              }
            } else {
              handleError(res)
            }
          })
        }, (res) => {
          this.uploadLoading = false
          this.$refs.fpro.stop()
          handleError(res)
        })
      })
    },
    closeLoginForm () {
      this.kyBotUploadVisible = false
      this.$refs.fpro.stop()
      // this.infoKybotVisible = true
    },
    dump () {
      this.$refs.diagnosisForm.validate((valid) => {
        if (valid) {
          var hasErr = false
          setTimeout(() => {
            if (hasErr) {
              return
            }
            this.$refs.fproDownload.stop()
            var instance = kapConfirm(this.$t('downloadTip', {count: this.diagnosisFormModel.server.length}), {
              type: 'info',
              showCancelButton: false
            }).then(() => {
            })
            instance.confirmButtonLoading = true
          }, 2000)
          this.$refs.fproDownload.start()
          this.diagnosisFormModel.server.forEach((server) => {
            var serverName = location.protocol + '//' + server
            // var serverName = 'http://10.1.2.103:7070'
            let href = ''
            let urlPre = location.protocol + '//' + location.host + apiUrl
            if (this.selectTimer) {
              this.startTime = +new Date(this.startTime)
              href = urlPre + 'kybot/dump_remote?host=' + serverName + '&types[]=' + this.diagnosisFormModel.category.join(',') + '&startTime=' + this.startTime + '&endTime=' + this.endTime + '&currentTime=' + (new Date()).getTime()
            } else {
              href = urlPre + 'kybot/dump_remote?host=' + serverName + '&target=' + this.targetId
            }
            downloadByFrame(href, () => {}, (err) => {
              hasErr = true
              this.$refs.fproDownload.stop()
              handleError({status: 500, data: err})
            })
          })
        }
      })
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
      this.diagnosisVisible = v
      if (!v) {
        this.$refs.fpro.stop()
        this.uploadLoading = false
      }
    }
  },
  computed: {
    kapVersionPara () {
      var _kapV = this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.version'] || null
      return _kapV ? '?src=' + _kapV : ''
    },
    kybotUrl () {
      return 'https://kybot.io/#/home' + this.kapVersionPara
    }
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
  created () {
    this.getKybotServers().then((res) => {
      handleSuccess(res, (data) => {
        data = data || []
        data.forEach((s, i) => {
          this.kybotServersList.push({id: i + 1, name: s})
        })
        if (this.kybotServersList && this.kybotServersList[0].name) {
          this.diagnosisFormModel.server.push(this.kybotServersList[0].name)
        }
      })
    })
  },
  locales: {
    'en': {kybotUpload: 'Generate and sync package to Kyligence Robot', contentOne: 'By analyzing your diagnostic package, ', contentOneForJob: 'By analyzing your job diagnostic package ', contentTwo: 'can provide online diagnostic, tuning and support service for Kyligence Enterprise.', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: 'Only generate', kybotDumpTwo: ', Manual upload ', selectTime: 'Select Time Range', last1: 'Last one hour', last2: 'Last one day', last3: 'Last three days', last4: 'Last one month', chooseDate: 'Choose Date', tipTitle: 'If there is no public network access, diagnostic package can be upload manually as following:', tipStep1: '1. Download diagnostic package', tipStep2: '2. Login on Kyligence Robot', tipStep3: '3. Click upload button on the top left of Kyligence Robot home page, and select the diagnostic package desired on the upload page to upload', err1: 'start time must less than end time', err2: 'at least 5 mins', err3: 'most one month', uploaded: 'uploaded successfully', protocol: 'I have read and agree《Kyligence Robot Term of Service》', agreeProtocol: 'Enable One Click Upload', noTime: 'Please choose the startTime or endTime', timeLimits: 'More than five minutes and less than one month', moreThanTime: 'Can not more than one month', lessThanTime: 'The choice should not be less than five minutes', kybot: 'By analyzing your diagnostic package, Kyligence Robot can provide online diagnostic, tuning and support service for Kyligence Enterprise', kybotAutoUpload: 'Kyligence Robot Diagnostic Pack Upload', kybotXY: 'Kyligence Robot User agreement', close: 'Close', uploading: 'uploading...', 'uploadSuccess': 'Upload successful, please sign in ', 'see': ' ', signIn: 'Kyligence Account | Sign In', checkQuestionTip: 'Package Type', checkServeiceTip: 'Server', checkQuestionValidTip: 'package type is required', checkServerValidTip: 'server is required', categorylist: {c0: 'Basic package', c5: 'Full package'}, 'checkFailedJobTip': 'Click the "Operation" button in "Monitor" page and then select "Diagnosis" to generate diagnosis packages.', downloadTip: 'The system is downloading the diagnosis package of the selected %{count} nodes for you. This process may take a few minutes, please be patient.', diagnosis: 'Diagnosis', packageNodeTip: 'Please choose right node based on diagnose content.', login: 'Login'},
    'zh-cn': {kybotUpload: '生成诊断包并上传至 Kyligence Robot', contentOne: '通过分析生成的诊断包，', contentOneForJob: '通过分析生成的Job诊断包，', contentTwo: '提供在线诊断，优化服务。', contentTip: '(Generated diagnostic package would cover 72 hours using history ahead)', kybotDumpOne: '下载诊断包', kybotDumpTwo: ', 手动上传 ', selectTime: '选择时间范围', last1: '最近一小时', last2: '最近一天', last3: '最近三天', last4: '最近一个月', chooseDate: '选择日期', tipTitle: '如无公网访问权限，可选择手动上传，操作步骤如下：', tipStep1: '1. 点击下载诊断包', tipStep2: '2. 登录 Kyligence Robot', tipStep3: '3. 在首页左上角点击上传按钮，在上传页面选择已下载的诊断包上传', err1: '开始时间必须小于结束时间', err2: '至少选择5分钟之后', err3: '至多选择一个月之内', uploaded: '上传成功', protocol: '我已阅读并同意《Kyligence Robot 用户协议》', agreeProtocol: '开启一键上传', noTime: '开始时间，结束时间不能为空', timeLimits: '大于五分钟小于一个月', moreThanTime: '时间不能超过一个月', lessThanTime: '选择的时间不能小于5分钟', kybot: 'Kyligence Robot 通过分析生产的诊断包，提供 Kyligence Enterprise 在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传', kybotAutoUpload: 'Kyligence Robot 诊断包上传', kybotXY: 'Kyligence Robot 用户协议', close: '确定', uploading: '上传中...', 'uploadSuccess': '上传成功，请登录 ', 'see': ' 查看', signIn: 'Kyligence Account | 登录', checkQuestionTip: '诊断包类型', checkServeiceTip: '服务器', checkQuestionValidTip: '请选择打包类型', checkServerValidTip: '请选择服务器', categorylist: {c0: '基础诊断包', c5: '全量诊断包'}, 'checkFailedJobTip': '请到监控页面找到需要打诊断包的任务，点击“操作“按钮，选择”诊断“来生成任务诊断包。', downloadTip: '系统正在为您下载这%{count}个节点的诊断包，此过程可能需要持续几分钟，请耐心等待。', diagnosis: '诊断', packageNodeTip: '请根据打包的类别，选择正确的节点。', login: '登录'}
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
.diagnosis-wrap {
  .btn-agree {
    display: block;
    margin: 20px auto;
   }
  ._download{
    width:1px;
    height:1px;
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
    padding-bottom: 20px;
    // text-align: center;
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
  .select_condition {
    .el-icon-question {
      color: @border-color-base;
    }
  }
  .select-time {
    padding: 6px 0px;
    .hd {
      height: 16px;
      line-height: 16px;
    }
    .el-date-editor {
      width: 190px;
    }
  }
  .footer {
    // text-align: right;
    .el-button--info.is-text {
      color: @color-text-regular;
      font-family: 'Helvetia New';
      float: left;
    }
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
    .progress-block {
      text-align: center;
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
</style>
