<template>
  <el-dialog
    custom-class="diagnostic-dialog"
    :visible.sync="isShow"
    width="720px"
    :close-on-click-modal="false"
    :before-close="() => handleClose('header')"
    :title="$t('dialogTitle')"
    :limited-area="true"
  >
    <div class="body">
      <template v-if="$route.name !== 'Job'">
        <div class="time-range">{{$t('timeRange')}}<el-tooltip :content="$t('timeRangeTip')" effect="dark" placement="top"><i class="el-icon-ksd-what"></i></el-tooltip>：</div>
        <el-radio-group v-model="timeRangeValue" class="time-range-radio" @change="changeTimeRange" :disabled="isRunning">
          <el-radio :label="item.label" v-for="(item, index) in timeRange" :key="index">{{item.text}}</el-radio>
        </el-radio-group>
        <div class="datapicket-layout">
          <p class="custom-time-tip" v-if="timeRangeValue === 'custom'">{{$t('customTimeTip')}}</p>
          <el-date-picker
            :class="{'is-disabled': timeRangeValue !== 'custom', 'is-error': validDateTime && getDateTimeValid}"
            v-model="dateTime.prev"
            type="datetime"
            :placeholder="$t('selectDatePlaceholder')"
            align="right"
            :disabled="timeRangeValue !== 'custom' || isRunning"
            :clearable="timeRangeValue === 'custom'"
            @blur="onBlur"
          />
          <span>&#8211;</span>
          <el-date-picker
            :class="{'is-disabled': timeRangeValue !== 'custom', 'is-error': validDateTime && getDateTimeValid}"
            v-model="dateTime.next"
            type="datetime"
            :placeholder="$t('selectDatePlaceholder')"
            align="right"
            :disabled="timeRangeValue !== 'custom' || isRunning"
            :clearable="timeRangeValue === 'custom'"
          />
          <p class="error-text" v-if="validDateTime && getDateTimeValid">{{$t('timeErrorMsg')}}</p>
        </div>
      </template>
      <div :class="['server', $route.name !== 'Job' && 'ksd-mt-15']">
        <p class="title">{{$t('server')}}</p>
        <el-select :class="{'no-selected': isServerChange && !servers.length}" v-model="servers" multiple :placeholder="$t('selectServerPlaceHolder')" :disabled="isRunning" @change="isServerChange = true">
          <el-option
            v-for="item in serverOptions"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
        <p class="error-text" v-if="isServerChange && !servers.length">{{$t('selectServerTip')}}</p>
      </div>
      <div class="download-layout" v-if="isShowDiagnosticProcess">
        <p>{{$t('downloadTip')}}</p>
        <div class="download-progress">
          <div class="progress-item clearfix" v-for="item in diagDumpIds" :key="item.id">
            <el-checkbox v-model="item.isCheck" :disabled="item.stage !== 'DONE'" v-if="showManualDownloadLayout && isManualDownload" @change="changeCheckItems"></el-checkbox>
            <div class="download-details">
              <p class="title">{{ getTitle(item) }}</p>
              <el-progress class="progress" :percentage="Math.ceil(+item.progress * 100)" v-bind="setProgressColor(item)" ></el-progress>
              <template v-if="item.status === '001'">
                <span :class="['retry-btn', {'ksd-ml-20': isManualDownload}]" @click="retryJob(item)">{{$t('retry')}}</span>
                <p class="error-text">{{$t('requireOverTime1')}}<a class="user-manual" :href="$route.name !== 'Job' ? 'https://sso.kyligence.com/uaa/login.html?lang=en&source=docs' : ($lang === 'en' ? 'https://docs.kyligence.io/books/v4.0/en/monitor/job_diagnosis.en.html' : 'https://docs.kyligence.io/books/v4.0/zh-cn/monitor/job_diagnosis.cn.html')" target="_blank">{{$t('manual')}}</a>{{$t('requireOverTime2')}}</p>
              </template>
              <template v-if="['002', '999'].includes(item.status)">
                <span class="error-text">{{item.status === '002' ? $t('noAuthorityTip') : $t('otherErrorMsg')}}</span><span class="detail-text" @click="item.showErrorDetail = !item.showErrorDetail">{{$t('details')}}<i :class="item.showErrorDetail ? 'el-icon-arrow-up' : 'el-icon-arrow-down'"></i></span>
                <div class="dialog-detail" v-if="item.showErrorDetail">
                  <el-input class="details-content" type="textarea" v-model.trim="item.error" :rows="4" readonly></el-input>
                  <el-button class="copyBtn" size="mini" v-clipboard:copy="item.error" v-clipboard:success="onCopy" v-clipboard:error="onError">{{$t('kylinLang.common.copy')}}</el-button>
                </div>
              </template>
            </div>
          </div>
          <div class="checkbox-group" v-if="showManualDownloadLayout && isManualDownload">
            <el-checkbox v-model="checkAll" :indeterminate="indeterminate" @change="changeCheckAllType">{{$t('selectAll')}}</el-checkbox>
            <div class="download-msg">
              <span @click="downloadEvent">{{`${$t('download')}(${getDownloadNum})`}}</span>
              <span class="cancel" @click="cancelManualDownload">{{$t('cancel')}}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div slot="footer">
      <span class="manual-download"><span :class="['manual', {'is-disable': !showManualDownloadLayout || isManualDownload}]" @click="isManualDownload = true">{{$t('manualDownload')}}</span><el-tooltip :content="$t('manualDownloadTip')" effect="dark" placement="top"><i class="el-icon-ksd-what"></i></el-tooltip></span>
      <el-popover
        ref="closePopover"
        placement="top"
        width="326"
        :popper-class="closeFromHeader ? 'popover-running' : ''"
        v-model="showPopoverTip">
        <p>{{$t('closeModelTip')}}</p>
        <div style="text-align: right; margin: 0">
          <el-button size="mini" type="info" text @click="showPopoverTip = false">{{$t('cancelBtn')}}</el-button>
          <el-button type="primary" size="mini" @click="closeDialog">{{$t('confrimBtn')}}</el-button>
        </div>
      </el-popover>
      <el-button v-popover="'closePopover'" plain size="medium" @click="handleClose">{{$t('kylinLang.common.close')}}</el-button>
      <el-button size="medium" @click="generateDiagnostic" :loading="isRunning" :disabled="getDateTimeValid || !servers.length || isManualDownload">{{$t('generateBtn')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import locales from './locales'
import { getPrevTimeValue } from '../../../util/business'
// import { handleSuccess } from 'util/business'
import vuex from '../../../store'
import store, { types } from './store'
import { mapActions, mapState, mapMutations } from 'vuex'
vuex.registerModule(['diagnosticModel'], store)

@Component({
  props: {
    jobId: {
      type: String,
      default: ''
    }
  },
  computed: {
    ...mapState('diagnosticModel', {
      host: state => state.host,
      diagDumpIds: state => state.diagDumpIds
    })
  },
  methods: {
    ...mapActions('diagnosticModel', {
      getDumpRemote: types.GET_DUMP_REMOTE,
      getServers: types.GET_SERVERS,
      downloadDumps: types.DOWNLOAD_DUMP_DIAG
    }),
    ...mapMutations('diagnosticModel', {
      updateCheckType: types.UPDATE_CHECK_TYPE,
      delDumpid: types.DEL_DUMP_ID_LIST,
      resetDumpData: types.RESET_DUMP_DATA,
      stopInterfaceCall: types.STOP_INTERFACE_CALL
    })
  },
  locales
})

export default class Diagnostic extends Vue {
  isShow = true
  isRunning = false
  isManualDownload = false
  checkAll = false
  indeterminate = false
  isShowDiagnosticProcess = false
  showError = false
  showPopoverTip = false
  closeFromHeader = false
  validDateTime = false
  isServerChange = false
  timeRange = [
    {text: this.$t('lastHour'), label: 'lastHour'},
    {text: this.$t('lastDay'), label: 'lastDay'},
    {text: this.$t('lastThreeDay'), label: 'lastThreeDay'},
    {text: this.$t('lastMonth'), label: 'lastMonth'},
    {text: this.$t('custom'), label: 'custom'}
  ]
  serverOptions = []
  timeRangeValue = 'lastDay'
  dateTime = {
    prev: '',
    next: ''
  }
  servers = []

  @Watch('diagDumpIds')
  onChangeDumpList (newVal, oldVal) {
    if (JSON.stringify(oldVal) === '{}' || JSON.stringify(newVal) === '{}') return
    let list = Object.keys(newVal).length && Object.keys(newVal).filter(it => newVal[it].running)
    if (!list.length) {
      this.isRunning = false
    }
  }
  // 获取诊断包可下载数/总数
  get getDownloadNum () {
    const totalList = Object.keys(this.diagDumpIds)
    const checkList = totalList.filter(item => this.diagDumpIds[item] && this.diagDumpIds[item].isCheck)
    return `${checkList.length}/${totalList.length}`
  }
  // 日期在5分钟～1个月内
  get getDateTimeValid () {
    return !this.dateTime.prev || !this.dateTime.next || this.getTimes(this.dateTime.prev) > this.getTimes(this.dateTime.next) || (this.getTimes(this.dateTime.next) - this.getTimes(this.dateTime.prev)) < 300000 || (this.getTimes(getPrevTimeValue({ date: this.dateTime.next, m: 1 })) - this.getTimes(this.dateTime.prev)) > 0
  }
  // 是否展示手动下载提示
  get showManualDownloadLayout () {
    return Object.keys(this.diagDumpIds).filter(it => this.diagDumpIds[it].stage === 'DONE').length > 0
  }
  // 进度条title
  getTitle (item) {
    let [{label}] = this.serverOptions.filter(it => it.value === item.host.replace(/http:\/\//, ''))
    return `http://${label}`
  }
  getTimes (date) {
    return new Date(date).getTime()
  }
  // 更改进度条颜色
  setProgressColor (item) {
    const { progress = +progress, status } = item
    if (status === '000') {
      let color = ''
      let type = null
      switch (true) {
        case progress >= 0 && progress < 0.3:
          color = '#0988DE' // 蓝色进度条
          break
        case progress >= 0.3 && progress < 1:
          color = '#0988DE'
          break
        case progress >= 1:
          type = 'success'
          break
      }
      return type ? {status: type} : {color}
    } else {
      return {status: 'exception'}
    }
  }
  created () {
    this.getServers(this).then((data) => {
      if (data) {
        data.forEach(item => {
          Object.prototype.toString.call(item) === '[object Object]' && this.serverOptions.push({label: `${item.host}(${item.mode && item.mode.toLocaleUpperCase()})`, value: item.host})
        })
        this.servers = this.serverOptions.length ? [this.serverOptions[0].value] : []
      }
    })
  }
  onBlur () {
    this.validDateTime = true
  }
  // 更改时间选择操作
  changeTimeRange (val) {
    this.validDateTime = false
    const date = new Date()
    let dt = date.getTime()
    let t = dt

    switch (val) {
      case 'lastHour':
        t = dt - 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastDay':
        t = dt - 24 * 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastThreeDay':
        t = dt - 3 * 24 * 60 * 60 * 1000
        this.dateTime.prev = new Date(t)
        this.dateTime.next = date
        break
      case 'lastMonth':
        this.dateTime.prev = new Date(getPrevTimeValue({ date, m: 1 }))
        this.dateTime.next = date
        break
      case 'custom':
        this.dateTime.prev = ''
        this.dateTime.next = new Date(date)
        break
      default:
        this.dateTime.prev = ''
        this.dateTime.next = ''
    }
  }
  // 有诊断包在生成中关闭弹窗时的popover提示
  handleClose (para) {
    if (this.isRunning) {
      this.showPopoverTip = true
      this.closeFromHeader = para === 'header'
      return
    }
    this.closeDialog()
  }
  // 关闭弹窗
  closeDialog () {
    this.$refs['closePopover'] && this.$refs['closePopover'].doClose()
    this.resetDumpData(true)
    this.stopInterfaceCall(true)
    this.$emit('close')
  }
  // 生成诊断包
  generateDiagnostic () {
    if (this.getDateTimeValid || !this.servers.length) return
    if (this.$route.name === 'Job' && !this.jobId) {
      console.error('no job_id')
      return
    }
    this.resetDumpData(false)
    this.isRunning = true
    this.isShowDiagnosticProcess = true
    let apiErrorNum = 0
    let data = {}
    if (this.$route.name === 'Job') {
      data = {
        job_id: this.jobId
      }
    } else {
      data = {
        start: new Date(this.dateTime.prev).getTime(),
        end: new Date(this.dateTime.next).getTime()
      }
    }
    this.servers.forEach(async (host) => {
      await this.getDumpRemote({
        host: `http://${host.trim()}`,
        ...data
      }).then(() => {
        // apiErrorNum += 1
      }).catch(() => {
        apiErrorNum += 1
        if (apiErrorNum === this.servers.length) {
          this.isShowDiagnosticProcess = false
          this.isRunning = false
        }
      })
    })
  }
  // 生成超时，重新生成
  retryJob (item) {
    const { host, start, end, id } = item
    this.delDumpid(id)
    this.getDumpRemote({ host, start, end })
  }
  changeCheckAllType (val) {
    this.indeterminate = false
    this.updateCheckType(val)
  }
  // 取消手动下载
  cancelManualDownload () {
    this.isManualDownload = false
    this.checkAll = false
    this.indeterminate = false
    this.updateCheckType(false)
  }
  onCopy () {
    this.$message.success(this.$t('kylinLang.common.copySuccess'))
  }
  onError () {
    this.$message.error(this.$t('kylinLang.common.copyfail'))
  }
  changeCheckItems () {
    let checkList = Object.keys(this.diagDumpIds).filter(it => this.diagDumpIds[it].isCheck)
    this.indeterminate = checkList.length > 0 && checkList.length !== Object.keys(this.diagDumpIds).length
    this.checkAll = checkList.length === Object.keys(this.diagDumpIds).length
  }
  // 手动下在诊断包
  downloadEvent () {
    let dumps = Object.keys(this.diagDumpIds).filter(it => this.diagDumpIds[it].isCheck)
    dumps.forEach(item => {
      const { host, id } = this.diagDumpIds[item]
      this.downloadDumps({host, id})
    })
  }
  mounted () {
    this.changeTimeRange('lastDay')
    this.stopInterfaceCall(false)
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .diagnostic-dialog {
    .el-dialog__body {
      max-height: 464px !important;
    }
    .body {
      color: @text-title-color;
      .time-range {
        margin-bottom: 14px;
        .el-icon-ksd-what {
          margin-left: 5px;
        }
      }
      .time-range-radio {
        .el-radio {
          height: 30px;
          line-height: 30px;
          padding: 0 8px;
          box-sizing: border-box;
          &.is-checked {
            background: @background-disabled-color;
          }
        }
      }
      .datapicket-layout {
        width: 100%;
        background: @background-disabled-color;
        padding: 10px 10px;
        box-sizing: border-box;
        .custom-time-tip {
          font-size: 12px;
          margin-bottom: 5px;
          color: @text-normal-color;
        }
        .el-date-editor.is-disabled {
          width: 175px;
          input {
            border: none;
            padding-right: 0;
            color: @text-normal-color;
            border-color: @line-border-color;
          }
        }
        .el-date-editor.is-error {
          input {
            border: 1px solid @error-color-1;
          }
        }
      }
      .server {
        .el-select {
          margin-top: 10px;
          width: 100%;
        }
        .no-selected {
          input {
            border: 1px solid @error-color-1;
          }
        }
      }
      .error-text {
        font-size: 12px;
        color: @error-color-1;
        max-width: 650px;
        .user-manual {
          text-decoration: underline;
        }
      }
      .download-layout {
        margin-top: 15px;
        font-size: 14px;
        .download-progress {
          // margin-top: 10px;
          .progress-item {
            position: relative;
            margin-top: 0;
            .el-checkbox {
              float: left;
              margin-top: 24px;
              margin-right: 10px;
            }
            .download-details {
              display: inline-block;
            }
            .title {
              font-size: 12px;
              margin-top: 8px;
            }
            .progress {
              width: 450px;
            }
            .retry-btn {
              position: absolute;
              left: 430px;
              top: 25px;
              color: @base-color;
              cursor: pointer;
              font-size: 12px;
            }
            .detail-text {
              font-size: 12px;
              color: @base-color;
              cursor: pointer;
            }
            .dialog-detail{
              // border:solid 1px @line-border-color;
              // background:@background-disabled-color;
              position: relative;
              margin-top: 10px;
              .details-content {
                border: solid 1px @line-border-color;
                border-radius: 2px;
                textarea {
                  min-height: 95px;
                  background: @aceditor-bg-color;
                }
              }
              .copyBtn{
                position: absolute;
                right:5px;
                top:5px;
              }
            }
          }
        }
        .checkbox-group {
          height: 30px;
          background-color: @background-disabled-color;
          position: relative;
          font-size: 14px;
          margin-top: 15px;
          line-height: 30px;
          padding: 0 10px;
          .download-msg {
            color: @base-color;
            position: absolute;
            right: 10px;
            top: 0;
            cursor: pointer;
            .cancel {
              color: @text-normal-color;
              margin-left: 10px;
              cursor: pointer;
            }
          }
        }
      }
    }
    .manual-download {
      font-size: 12px;
      position: absolute;
      left: 20px;
      line-height: 30px;
      .manual {
        color: @text-normal-color;
        cursor: pointer;
        &.is-disable {
          color: @text-disabled-color;
          pointer-events: none;
        }
        &:hover {
          color: @base-color;
        }
      }
      .el-icon-ksd-what {
        margin-left: 5px;
      }
    }
  }
  .el-popover.popover-running {
    top: 179px !important;
    left: 50% !important;
    .popper__arrow {
      display: none;
    }
  }
</style>
