<template>
  <div id="dashboard">
    <el-alert class="quota_tips" type="error" :closable="false" show-icon v-if="isNoQuota">
      <span slot="title">
        <span>{{noMoreQuotaTips1}}</span><a @click="gotoSetting">{{$t('quotaTips3')}}</a><span>{{$t('quotaTips4')}}</span>
      </span>
    </el-alert>
    <div class="dashboard-content">
      <el-row :gutter="10" class="ratio-row">
        <el-col :span="8">
          <div class="dash-card">
            <div class="cart-title clearfix">
              <span>{{$t('storageQuota')}}
                <el-tooltip placement="right">
                  <div slot="content">{{$t('storageQuotaDesc')}}</div>
                  <i class="el-icon-ksd-what ksd-fs-14"></i>
                </el-tooltip>
              </span>
              <a class="ky-a-like ksd-fright ksd-fs-12" @click="gotoSetting">{{$t('viewDetail')}}</a>
            </div>
            <div class="quota-row">
              <div class="img-block ksd-fleft ksd-mr-15">
                <el-popover
                  ref="popover"
                  placement="right-end"
                  trigger="hover"
                  popper-class="quota-popover"
                  :disabled="!(trashRatio*quotaHeight<14 || useageRatio*quotaHeight<14)"
                  v-model="popoverVisible">
                  <p class="info-block">
                    <span class="info-title">{{$t('useageMana')}}</span>
                    <span class="useage" v-if="quotaInfo.total_storage_size>=0">{{useageRatio*100 | fixed(2)}}%</span>
                    <span v-else>--</span>
                  </p>
                  <p class="info-block">
                    <span  class="info-title">{{$t('trash')}}</span>
                    <span class="trash" v-if="quotaInfo.garbage_storage_size>=0">{{trashRatio*100 | fixed(2)}}%</span>
                    <span v-else>--</span>
                  </p>
                </el-popover>
                <div class="quota-chart" :class="{'is_no_quota': isNoQuota}" v-popover:popover>
                  <div class="useage-block" :style="{'height': useageBlockHeight+'px'}">
                    <div class="text" v-if="useageRatio*quotaHeight>=14">{{useageRatio*100 | fixed(2)}}%</div>
                  </div>
                  <div class="trash-block" :style="{'height': trashBlockHeight+'px'}">
                  </div>
                </div>
              </div>
              <div>
                <div class="quota-info">
                  <div class="info-title">{{$t('totalStorage')}}</div>
                  <div class="total-quota">
                    <span v-if="quotaInfo.storage_quota_size>=0">
                      <span class="ksd-fs-28">{{quotaTotalSize.size}}</span> <span class="ksd-fs-18">{{quotaTotalSize.unit}}</span>
                    </span>
                    <span class="ksd-fs-28" v-else>--</span>
                  </div>
                  <div class="info-title ksd-mt-15">{{$t('useageMana')}}</div>
                  <div class="useage">
                    <span v-if="quotaInfo.total_storage_size>=0">{{quotaInfo.total_storage_size | dataSize}}</span>
                    <span v-else>--</span>
                  </div>
                  <div class="info-title ksd-mt-15">{{$t('trash')}}</div>
                  <div class="trash">
                    <span v-if="quotaInfo.garbage_storage_size>=0">{{quotaInfo.garbage_storage_size | dataSize}}</span>
                    <span v-else>--</span><common-tip :content="$t('clear')">
                      <i class="el-icon-ksd-clear ksd-ml-10 clear-btn"
                      :class="{'is_no_quota': isNoQuota}"
                    @click="clearStorage" v-if="quotaInfo.garbage_storage_size>0"></i>
                    </common-tip>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="dash-card">
            <div class="cart-title clearfix">
              <span>{{$t('acceImpact')}}
                <el-tooltip placement="right">
                  <div slot="content">{{$t('acceImpactDesc')}}</div>
                  <i class="el-icon-ksd-what ksd-fs-14"></i>
                </el-tooltip>
              </span>
              <a class="ky-a-like ksd-fright ksd-fs-12" @click="gotoFavorite">{{$t('viewDetail')}}</a>
            </div>
            <svg id="ruleImpact" width="100%" height="168" class="ksd-mt-20"></svg>
          </div>
        </el-col>
      </el-row>
      <hr class="divider"/>
      <div class="clearfix ksd-mb-10">
        <div class="ksd-fleft">
          <el-date-picker v-model="daterange"
            type="daterange"
            size="small"
            unlink-panels
            range-separator="-"
            :start-placeholder="$t('kylinLang.common.startTime')"
            :end-placeholder="$t('kylinLang.common.endTime')"
            :default-time="['00:00:00', '23:59:59']"
            :picker-options="pickerOptions"></el-date-picker>
        </div>
      </div>
      <el-row :gutter="10" class="count-row">
        <el-col :span="6">
          <div class="dash-card" :class="{'isActive': showQueryChart}" @click="loadQueryChart">
            <div class="inner-card">
              <div class="cart-title">
                {{$t('queryCount')}}
                <a class="ky-a-like ksd-fright ksd-fs-12" @click.stop="gotoQueryHistory">{{$t('viewDetail')}}</a>
              </div>
              <div class="content">
                <span class="num">{{queryCount}}</span>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="dash-card" :class="{'isActive': showLatencyChart}" @click="loadLatencyChart">
            <div class="inner-card">
              <div class="cart-title">
                {{$t('avgQueryLatency')}}
                <a class="ky-a-like ksd-fright ksd-fs-12" @click.stop="gotoQueryHistory">{{$t('viewDetail')}}</a>
              </div>
              <div class="content">
                <span class="num">{{queryMean}}</span>
                <span class="unit">{{$t('sec')}}</span>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="dash-card" :class="{'isActive': showJobChart}" @click="loadJobChart">
            <div class="inner-card">
              <div class="cart-title">
                {{$t('jobCount')}}
                <a class="ky-a-like ksd-fright ksd-fs-12" @click.stop="gotoJoblist">{{$t('viewDetail')}}</a>
              </div>
              <div class="content">
                <span class="num">{{jobCount}}</span>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="dash-card" :class="{'isActive': showBulidChart}" @click="loadBulidChart">
            <div class="inner-card">
              <div class="cart-title">
                {{$t('avgBulidTime')}}
                <a class="ky-a-like ksd-fright ksd-fs-12" @click.stop="gotoJoblist">{{$t('viewDetail')}}</a>
              </div>
              <div class="content" v-if="noEnoughData">
                <span class="no-data">{{$t('noEnoughData')}}</span>
              </div>
              <div class="content" v-else>
                <span class="num" v-if="avgBulidTime+''==='0.00'">&lt; 0.01</span>
                <span class="num" v-else>{{avgBulidTime}}</span>
                <span class="unit">{{$t('sec')}}</span>
              </div>
            </div>
          </div>
        </el-col>
      </el-row>
      <el-row class="ksd-mt-10 chart-row dash-card">
        <el-col :span="12" class="chart-block">
          <div>
            <div class="cart-title" v-if="isAutoProject">{{$t('queryByIndex', {type: chartTitle})}}</div>
            <div class="cart-title" v-else>{{$t('queryByModel', {type: chartTitle})}}</div>
            <vn-bar :model="traffics"
              :x-format="formatLabel"
              :y-format="formatYAxis"
              id="barChart"
              :content-generator="contentGenerator">
            </vn-bar>
          </div>
        </el-col>
        <el-col :span="12" class="chart-block">
          <div>
            <div class="cart-title">{{$t('queryByDay', {type: chartTitle, zone: getLocalTimezone()})}}</div>
            <el-select v-model="dateUnit" size="small" class="line-chart-select" @change="loadLineChartData">
              <el-option
                v-for="item in unitOptions"
                :key="item"
                :label="$t(`${item}`)"
                :value="item">
              </el-option>
            </el-select>
            <vn-line :model="traffics2"
            id="lineChart"
            :x-format="formatDate"
            :y-format="formatYAxis"
            :content-generator="contentGenerator">
            </vn-line>
          </div>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, getLocalTimezone } from '../../util/business'
import { handleSuccessAsync, handleError } from '../../util/index'
import { loadLiquidFillGauge, liquidFillGaugeDefaultSettings } from '../../util/liquidFillGauge'
import $ from 'jquery'
import * as d3 from 'd3'
import moment from 'moment-timezone'
import BarChart from './BarChart'
import LineChart from './LineChart'
@Component({
  methods: {
    getLocalTimezone: getLocalTimezone,
    ...mapActions({
      getRulesImpact: 'GET_RULES_IMPACT',
      getQuotaInfo: 'GET_QUOTA_INFO',
      clearTrash: 'CLEAR_TRASH',
      loadDashboardQueryInfo: 'LOAD_DASHBOARD_QUERY_INFO',
      loadQueryChartData: 'LOAD_QUERY_CHART_DATA',
      loadQueryDuraChartData: 'LOAD_QUERY_DURA_CHART_DATA',
      loadDashboardJobInfo: 'LOAD_DASHBOARD_JOB_INFO',
      loadJobChartData: 'LOAD_JOB_CHART_DATA',
      loadJobBulidChartData: 'LOAD_JOB_BULID_CHART_DATA'
    })
  },
  components: {
    'vn-bar': BarChart,
    'vn-line': LineChart
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isAutoProject'
    ])
  },
  locales: {
    'en': {
      storageQuota: 'Storage Quota',
      acceImpact: 'Acceleration Ratio',
      totalStorage: 'Total Storage',
      useageMana: 'Used Storage',
      trash: 'Low usage Storage',
      clear: 'Clear',
      queryCount: 'Query Count',
      viewDetail: 'View Detail',
      avgQueryLatency: 'Avg. Query Latency',
      queryLatency: 'Query Latency',
      jobCount: 'Job Count',
      avgBulidTime: 'Avg. Job Duration per MB',
      jobDuration: 'Job Duration',
      queryByModel: '{type} by Model',
      queryByDay: '{type} by Time ({zone})',
      queryByIndex: '{type} by Index Group',
      lastWeek: 'Last Week',
      lastMonth: 'Last Month',
      thisMonth: 'This Month',
      day: 'Day',
      week: 'Week',
      month: 'Month',
      storageQuotaDesc: 'In the project, the total storage can be used.',
      acceImpactDesc: 'In the project, accelerated queries ratio.',
      noEnoughData: 'Not enough data yet',
      sec: 's',
      quotaTips1: 'The project only has 10% storage quota. Some new jobs will be terminated when no storage quota is available. Please clean up low-efficient storage in time, increase the',
      quotaTips2: 'No storage quota available. The system will terminate the new load data job and build index job, while the query engine will still serve. Please clean up low-efficient storage in time, increase the',
      quotaTips3: ' low-efficient storage threshold',
      quotaTips4: ', or notify the system administrator to increase the storage quota for this project.'
    },
    'zh-cn': {
      storageQuota: '存储配额',
      acceImpact: '加速比例',
      totalStorage: '总空间',
      useageMana: '已使用的存储',
      trash: '低效存储',
      clear: '清除',
      queryCount: '查询次数',
      viewDetail: '查看详情',
      avgQueryLatency: '平均查询延迟',
      queryLatency: '查询延迟',
      jobCount: '任务数',
      avgBulidTime: '构建 1MB 数据的平均时间',
      jobDuration: '任务时间',
      queryByModel: '按模型统计{type}',
      queryByDay: '按时间 ({zone}) 统计{type}',
      queryByIndex: '按索引组统计{type}',
      lastWeek: '最近一周',
      lastMonth: '上个月',
      thisMonth: '当前月',
      day: '天',
      week: '周',
      month: '月',
      storageQuotaDesc: '本项目可使用的存储空间总量。',
      acceImpactDesc: '本项目中，已经加速的查询的比例。',
      noEnoughData: '尚无足够数据统计',
      sec: '秒',
      quotaTips1: '只有10%存储配额可用，当没有可用存储配额时系统将终止部分新增的任务。请及时清理低效存储，提高',
      quotaTips2: '已无可用的存储配额。系统将终止新增的数据加载任务和索引构建任务，查询引擎依然正常服务。请及时清理低效存储，提高',
      quotaTips3: '低效存储阈值',
      quotaTips4: '，或者通知系统管理员提高本项目的存储配额。'
    }
  }
})
export default class Dashboard extends Vue {
  daterange = [new Date(new Date().getTime() - 3600 * 1000 * 24 * 7), new Date()]
  impactRatio = 0
  quotaInfo = {
    storage_quota_size: -1,
    total_storage_size: -1,
    garbage_storage_size: -1
  }
  useageRatio = 0
  trashRatio = 0
  useageBlockHeight = 0
  trashBlockHeight = 0
  popoverVisible = false
  queryCount = 0
  queryMean = 0
  jobCount = 0
  avgBulidTime = 0
  noEnoughData = false
  quotaHeight = 170
  showQueryChart = true
  showLatencyChart = false
  showJobChart = false
  showBulidChart = false
  barChartData = {}
  lineChartDara = {}
  dateUnit = 'day'
  unitOptions = ['day', 'week', 'month']
  isNoQuota = false
  get chartTitle () {
    if (this.showQueryChart) {
      return this.$t('queryCount')
    } else if (this.showLatencyChart) {
      return this.$t('queryLatency')
    } else if (this.showJobChart) {
      return this.$t('jobCount')
    } else if (this.showBulidChart) {
      return this.$t('jobDuration')
    }
  }
  gotoQueryHistory () {
    this.$router.push('/query/queryhistory')
  }
  gotoJoblist () {
    this.$router.push('/monitor/job')
  }
  gotoSetting () {
    this.$router.push('/setting')
  }
  gotoFavorite () {
    this.$router.push('/studio/acceleration')
  }
  resetShow () {
    this.showQueryChart = false
    this.showLatencyChart = false
    this.showJobChart = false
    this.showBulidChart = false
  }
  loadLineChartData () {
    if (this.showQueryChart) {
      this.getQueryLineChartData()
    } else if (this.showLatencyChart) {
      this.getQueryDuraLineChartData()
    } else if (this.showJobChart) {
      this.getJobLineChartData()
    } else if (this.showBulidChart) {
      this.getJobBulidLineChartData()
    }
  }
  @Watch('daterange')
  onDatepickerChange () {
    if (this.showQueryChart) {
      this.getQueryBarChartData()
      this.getQueryLineChartData()
    } else if (this.showLatencyChart) {
      this.getQueryDuraBarChartData()
      this.getQueryDuraLineChartData()
    } else if (this.showJobChart) {
      this.getJobBarChartData()
      this.getJobLineChartData()
    } else if (this.showBulidChart) {
      this.getJobBulidBarChartData()
      this.getJobBulidLineChartData()
    }
  }
  loadQueryChart () {
    this.resetShow()
    this.showQueryChart = true
    this.getQueryBarChartData()
    this.getQueryLineChartData()
  }
  async getQueryBarChartData () {
    const res = await this.loadQueryChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    this.barChartData = Object.entries(resData).map(([key, value]) => ({ label: key, value: value }))
  }
  async getQueryLineChartData () {
    const resLine = await this.loadQueryChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: this.dateUnit})
    const resDataLine = await handleSuccessAsync(resLine)
    this.lineChartDara = resDataLine
  }
  loadLatencyChart () {
    this.resetShow()
    this.showLatencyChart = true
    this.getQueryDuraBarChartData()
    this.getQueryDuraLineChartData()
  }
  async getQueryDuraBarChartData () {
    const res = await this.loadQueryDuraChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    Object.keys(resData).forEach(k => {
      resData[k] = resData[k] / 1000
    })
    this.barChartData = Object.entries(resData).map(([key, value]) => ({ label: key, value: value }))
  }
  async getQueryDuraLineChartData () {
    const resLine = await this.loadQueryDuraChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: this.dateUnit})
    const resDataLine = await handleSuccessAsync(resLine)
    Object.keys(resDataLine).forEach(k => {
      resDataLine[k] = (resDataLine[k] / 1000).toFixed(2)
    })
    this.lineChartDara = resDataLine
  }
  loadJobChart () {
    this.resetShow()
    this.showJobChart = true
    this.getJobBarChartData()
    this.getJobLineChartData()
  }
  async getJobBarChartData () {
    const res = await this.loadJobChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    this.barChartData = Object.entries(resData).map(([key, value]) => ({ label: key, value: value }))
  }
  async getJobLineChartData () {
    const resLine = await this.loadJobChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: this.dateUnit})
    const resDataLine = await handleSuccessAsync(resLine)
    this.lineChartDara = resDataLine
  }
  loadBulidChart () {
    this.resetShow()
    this.showBulidChart = true
    this.getJobBulidBarChartData()
    this.getJobBulidLineChartData()
  }
  async getJobBulidBarChartData () {
    const res = await this.loadJobBulidChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    Object.keys(resData).forEach(k => {
      resData[k] = resData[k] * 1024 * 1024 / 1000
    })
    this.barChartData = Object.entries(resData).map(([key, value]) => ({ label: key, value: value }))
  }
  async getJobBulidLineChartData () {
    const resLine = await this.loadJobBulidChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: this.dateUnit})
    const resDataLine = await handleSuccessAsync(resLine)
    Object.keys(resDataLine).forEach(k => {
      resDataLine[k] = (resDataLine[k] * 1024 * 1024 / 1000).toFixed(2)
    })
    this.lineChartDara = resDataLine
  }
  drawImpactChart () {
    $(this.$el.querySelector('#ruleImpact')).empty()
    const config1 = liquidFillGaugeDefaultSettings()
    config1.circleColor = '#15BDF1'
    config1.textColor = '#263238'
    config1.waveAnimateTime = 1000
    loadLiquidFillGauge('ruleImpact', this.impactRatio, config1)
  }
  mounted () {
    this.$nextTick(() => {
      window.onresize = () => {
        const targetDom = this.$el.querySelector('#ruleImpact')
        if (targetDom) {
          $(targetDom).empty()
          this.drawImpactChart()
        }
      }
    })
  }
  get pickerOptions () {
    return {
      shortcuts: [{
        text: this.$t('lastWeek'),
        onClick (picker) {
          const end = new Date()
          const start = new Date()
          start.setTime(start.getTime() - 3600 * 1000 * 24 * 7)
          picker.$emit('pick', [start, end])
        }
      }, {
        text: this.$t('lastMonth'),
        onClick (picker) {
          const end = new Date()
          const start = new Date()
          const year = new Date().getFullYear()
          const month = new Date().getMonth()
          start.setTime(new Date(start.setMonth(month - 1)).setDate(1))
          end.setTime(new Date(year, month, 0))
          picker.$emit('pick', [start, end])
        }
      }, {
        text: this.$t('thisMonth'),
        onClick (picker) {
          const end = new Date()
          const start = new Date()
          start.setTime(start.setDate(1))
          picker.$emit('pick', [start, end])
        }
      }]
    }
  }
  get traffics () {
    return [
      {
        key: this.chartTitle,
        area: true,
        values: this.barChartData
      }
    ]
  }
  get categoryArr () {
    const dataNums = this.barChartData.length
    let splitNum = 1
    if (dataNums > 3 && dataNums < 6) {
      splitNum = parseInt(dataNums / 2)
    } else if (dataNums >= 6) {
      splitNum = parseInt(dataNums / 3)
    }
    const categorys = this.barChartData.filter((item, index) => (index + parseInt(splitNum / 2)) % splitNum === 0).map((item) => item.label)
    return categorys
  }
  formatLabel (d) {
    return this.categoryArr.indexOf(d) !== -1 ? (d.length > 25 ? d.substring(0, 25) + '...' : d) : ''
  }
  formatYAxis (d) {
    if (d < 1000) {
      if (parseFloat(d) === d) {
        return d3.format('.1')(d)
      } else {
        return d3.format('.2f')(d)
      }
    } else {
      var prefix = d3.formatPrefix(d)
      return prefix.scale(d) + prefix.symbol
    }
  }
  contentGenerator (d) {
    let valueFormate
    const formatPattern = this.dateUnit === 'month' ? '%Y-%m' : '%Y-%m-%d'
    const label = (d.data && d.data.label) || (d.point && d3.time.format(formatPattern)(moment.unix(d.point.x / 1000).toDate()))
    const value = (d.data && d.data.value) || (d.point && d.point.y)
    if (this.showQueryChart || this.showJobChart) {
      valueFormate = value || 0
    } else {
      valueFormate = value ? (Number(value) > 0 && Number(value) < 0.01 ? Number(value).toFixed(4) + 's' : Number(value).toFixed(2) + this.$t('sec')) : '0.00' + this.$t('sec')
    }
    return `<table>
      <tr>
        <td class="key">${label}</td>
        <td class="value">${valueFormate}</td>
      </tr>
    </table>`
  }
  get traffics2 () {
    return [
      {
        key: this.chartTitle,
        area: true,
        values: Object.entries(this.lineChartDara).map(([key, value]) => ({ x: new Date(key).getTime(), y: value })).sort((a, b) => {
          return b.x - a.x
        })
      }
    ]
  }
  formatDate (d) {
    const formatPattern = this.dateUnit === 'month' ? '%Y-%m' : '%Y-%m-%d'
    return d3.time.format(formatPattern)(moment.unix(d / 1000).toDate())
  }
  beforeDestroy () {
    window.onresize = null
  }
  clearStorage () {
    if (this.currentSelectedProject) {
      this.clearTrash({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, () => {
          this.loadQuotaInfo()
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
  created () {
    if (this.currentSelectedProject) {
      this.loadRuleImpactRatio()
      this.loadQuotaInfo()
      this.loadDashInfo()
      this.loadQueryChart()
    }
  }
  get noMoreQuotaTips1 () {
    let noMoreQuotaTips1 = ''
    if (+this.useageRatio >= 0.9 && +this.useageRatio < 0.99) {
      noMoreQuotaTips1 = this.$t('quotaTips1')
    } else if (+this.useageRatio >= 0.99) {
      noMoreQuotaTips1 = this.$t('quotaTips2')
    } else {
      noMoreQuotaTips1 = ''
    }
    return noMoreQuotaTips1
  }
  async loadQuotaInfo () {
    const res = await this.getQuotaInfo({project: this.currentSelectedProject})
    const resData = await handleSuccessAsync(res)
    this.quotaInfo = resData
    this.useageRatio = (resData.total_storage_size / resData.storage_quota_size).toFixed(4)
    this.trashRatio = (resData.garbage_storage_size / resData.storage_quota_size).toFixed(4)
    if (+this.useageRatio >= 0.9 && +this.useageRatio < 0.99) {
      this.isNoQuota = true
    } else if (+this.useageRatio >= 0.99) {
      this.isNoQuota = true
    } else {
      this.isNoQuota = false
    }
    setTimeout(() => {
      this.useageBlockHeight = this.useageRatio >= 1 ? this.quotaHeight : this.useageRatio * this.quotaHeight
      this.trashBlockHeight = this.trashRatio >= 1 ? this.quotaHeight : this.trashRatio * this.quotaHeight
    }, 0)
  }
  get quotaTotalSize () {
    const totalSize = {size: 0.00, unit: 'KB'}
    if (this.quotaInfo.storage_quota_size) {
      totalSize.size = Vue.filter('dataSize')(this.quotaInfo.storage_quota_size).split(' ')[0]
      totalSize.unit = Vue.filter('dataSize')(this.quotaInfo.storage_quota_size).split(' ')[1]
    }
    return totalSize
  }
  loadRuleImpactRatio () {
    this.getRulesImpact({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.impactRatio = data.toFixed(2) * 100
        this.drawImpactChart()
      })
    }, (res) => {
      handleError(res)
    })
  }
  async loadDashInfo () {
    const res = await this.loadDashboardQueryInfo({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime()})
    const resData = await handleSuccessAsync(res)
    this.queryCount = resData.count || 0
    this.queryMean = resData.mean ? (resData.mean / 1000).toFixed(2) : 0
    const resJob = await this.loadDashboardJobInfo({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime()})
    const resDataJob = await handleSuccessAsync(resJob)
    this.jobCount = resDataJob.count || 0
    this.avgBulidTime = resDataJob.total_duration && resDataJob.total_byte_size ? (resDataJob.total_duration * 1024 * 1024 / (resDataJob.total_byte_size * 1000)).toFixed(2) : 0
    if (resDataJob.total_byte_size && resDataJob.total_byte_size < 1024 * 1024) {
      this.noEnoughData = true
    } else {
      this.noEnoughData = false
    }
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #dashboard {
    .quota_tips {
      a {
        color: @base-color;
      }
    }
    .dashboard-content {
      margin: 20px;
    }
    .el-date-editor--daterange.el-input__inner {
      width: 230px;
    }
    .dash-card {
      border: 1px solid @line-border-color;
      border-radius: 2px;
      background-color: @fff;
      box-sizing: border-box;
      &:hover {
        box-shadow: 1px 1px 2px 0 @line-split-color;
      }
      .inner-card {
        cursor: pointer;
      }
      .cart-title {
        color: @text-title-color;
        font-size: 14px;
        font-weight: @font-medium;
        height: 36px;
        line-height: 36px;
        padding: 0 15px;
        background-color: @regular-background-color;
      }
      .content {
        margin: 15px;
        color: @text-normal-color;
        .num {
          color: @text-title-color;
          font-size: 24px;
          line-height: 24px;
          font-weight: @font-medium;
        }
        .no-data {
          font-size: 14px;
          color: @text-disabled-color;
        }
      }
      .quota-row {
        margin: 15px;
        clear: both;
        overflow: hidden;
        .img-block {
          width: 33%;
          min-width: 100px;
        }
        .quota-chart {
          margin: 0 auto;
          height: 170px;
          width: 90px;
          border-radius: 4px;
          border: 2px solid #15bdf1;
          box-shadow: 0px 0px 2px 0px #3AA0E5;
          position: relative;
          &.is_no_quota {
            border-color: @color-danger;
          }
          .text {
            font-size: 12px;
            line-height: 14px;
            color: @fff;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            z-index: 2;
          }
          .unUseage-block {
            background-color: @fff;
            position: absolute;
            top: 0;
            width: 100%;
            .text {
              color: @text-secondary-color;
            }
          }
          .useage-block {
            background-image: linear-gradient(-202deg, #6EDAAF 0%, #3BB477 100%);
            position: absolute;
            bottom: 0;
            width: 100%;
            height: 0px;
            -moz-transition: height .5s ease;
            -webkit-transition: height .5s ease;
            -o-transition: height .5s ease;
            transition: height .5s ease;
          }
          .trash-block {
            background-image: linear-gradient(-194deg, #FCDE54 0%, #F7BA2A 100%);
            position: absolute;
            bottom: 0;
            width: 100%;
            height: 0px;
            z-index: 1;
            -moz-transition: height .5s ease;
            -webkit-transition: height .5s ease;
            -o-transition: height .5s ease;
            transition: height .5s ease;
          }
        }
        .quota-info {
          float: left;
          text-align: left;
          padding: 5px 0px 0px 15px;
          border-left: 1px solid @line-split-color;
          height: 174px;
          box-sizing: border-box;
          .info-title {
            color: @text-normal-color;
            font-size: 14px;
            line-height: 14px;
            font-weight: @font-medium;
            &:first-child {
              font-weight: @font-medium;
            }
          }
          .total-quota {
            font-weight: @font-medium;
            height: 40px;
            line-height: 40px;
            color: @text-title-color;
          }
          .useage {
            font-weight: @font-medium;
            font-size: 18px;
            line-height: 28px;
            color: #3bb477;
          }
          .trash {
            font-weight: @font-medium;
            font-size: 18px;
            line-height: 28px;
            color: @warning-color-1;
            .clear-btn {
              font-size: 16px;
              color: @base-color;
              &.is_no_quota {
                font-size: 18px;
              }
              &:hover {
                color: @base-color-2;
              }
            }
          }
        }
      }
    }
    .ratio-row .dash-card {
      height: 242px;
    }
    .count-row .el-col {
      position: relative;
      height: 93px;
      .dash-card {
        position: absolute;
        height: 93px;
        width: calc(~"100% - 10px");
        padding: 0;
        &.isActive {
          height: 138px;
          box-shadow: 1px 1px 2px 0 @line-split-color;
          .inner-card {
            height: 103px;
            width: 100%;
            border: 1px solid @line-border-color;
            border-bottom: none;
            position: absolute;
            top: -1px;
            left: -1px;
            z-index: 1;
            background-color: @fff;
          }
        }
      }
    }
    .chart-row.dash-card {
      padding: 15px;
      .cart-title {
        padding: 0;
        background-color: @fff;
      }
      .chart-block {
        position: relative;
        .nvd3.nv-noData {
          font-size: 16px;
          font-weight: normal;
          font-family: Helvetica Neue, Helvetica, PingFang SC, Hiragino Sans GB, Microsoft YaHei, SimSun, sans-serif;
          fill: @text-disabled-color;
        }
        .nvd3 text {
          font: normal 10px Helvetica Neue;
        }
        .nv-point {
          stroke-opacity: 1;
          stroke-width: 3px;
          fill-opacity: 1;
        }
        > div {
          height: 300px;
        }
       &:first-child {
        border-right: 1px solid @line-split-color;
        padding-right: 15px;
       }
       &:last-child {
         padding-left: 15px;
       }
       svg {
         width: 100%;
       }
       .line-chart-select {
         position: absolute;
         top: 6px;
         right: 0;
         width: 80px;
       }
      }
    }
    .divider {
      margin: 20px 0;
      border-top: 1px solid @table-stripe-color;
    }
  }
  .quota-popover {
    min-width: 130px !important;
    .info-block {
      display: table-row;
      font-weight: @font-medium;
      font-size: 12px;
      line-height: 18px;
      span {
        display: table-cell;
        &.info-title {
          text-align: right;
          padding-right: 5px;
          color: @text-title-color;
        }
        &.useage {
          color: #3bb477;
        }
        &.trash {
          color: @warning-color-1;
        }
      }
    }
  }
</style>
