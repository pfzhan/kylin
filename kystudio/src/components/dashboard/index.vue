<template>
  <div id="dashboard">
    <el-row :gutter="10" class="ratio-row">
      <el-col :span="8">
        <div class="dash-card">
          <div class="cart-title clearfix">
            <span>{{$t('storageQuota')}}</span>
            <el-button plain size="mini" class="ksd-fright">{{$t('kylinLang.common.setting')}}</el-button>
          </div>
          <el-row :gutter="35" class="quota-row">
            <el-col :span="12">
              <el-popover
                ref="popover"
                placement="left-end"
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
              <div class="quota-chart" v-popover:popover>
                <div class="useage-block" :style="{'height': useageBlockHeight+'px'}">
                  <div class="text" v-if="useageRatio*quotaHeight>=14">{{useageRatio*100 | fixed(2)}}%</div>
                </div>
                <div class="trash-block" :style="{'height': trashBlockHeight+'px'}">
                </div>
              </div>
            </el-col>
            <el-col :span="12">
              <div class="quota-info">
                <div class="info-title ksd-mt-10">{{$t('totalStorage')}}</div>
                <div class="total-quota">
                  <span v-if="quotaInfo.storage_quota_size>=0">
                    <span class="ksd-fs-28">{{quotaInfo.storage_quota_size | dataSize('GB', true)}}</span><span class="ksd-fs-18">G</span>
                  </span>
                  <span class="ksd-fs-28" v-else>--</span>
                </div>
                <div class="info-title ksd-mt-16">{{$t('useageMana')}}</div>
                <div class="useage">
                  <span v-if="quotaInfo.total_storage_size>=0">{{quotaInfo.total_storage_size | dataSize}}</span>
                  <span v-else>--</span>
                </div>
                <div class="info-title ksd-mt-16">{{$t('trash')}}</div>
                <div class="trash">
                  <span v-if="quotaInfo.garbage_storage_size>=0">{{quotaInfo.garbage_storage_size | dataSize}}</span>
                  <span v-else>--</span>
                  <el-button type="primary" size="mini" class="ksd-ml-10" @click="clearStorage" v-if="quotaInfo.garbage_storage_size>0">{{$t('clear')}}</el-button>
                </div>
              </div>
            </el-col>
          </el-row>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="dash-card">
          <div class="cart-title clearfix">
            <span>{{$t('acceImpact')}}</span>
            <el-button plain size="mini" class="ksd-fright">{{$t('ruleSetting')}}</el-button>
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
            <div class="cart-title">{{$t('queryCount')}}</div>
            <div class="content">
              <span class="num">{{queryCount}}</span>
            </div>
            <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
          </div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card" :class="{'isActive': showLatencyChart}" @click="loadLatencyChart">
          <div class="inner-card">
            <div class="cart-title">{{$t('avgQueryLatency')}}</div>
            <div class="content">
              <span class="num">{{queryMean}}</span>
              <span class="unit">sec</span>
            </div>
            <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
          </div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card" :class="{'isActive': showJobChart}" @click="loadJobChart">
          <div class="inner-card">
            <div class="cart-title">{{$t('jobCount')}}</div>
            <div class="content">
              <span class="num">5</span>
            </div>
            <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
          </div>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card" :class="{'isActive': showBulidChart}" @click="loadBulidChart">
          <div class="inner-card">
            <div class="cart-title">{{$t('avgBulidTime')}}</div>
            <div class="content">
              <span class="num">11.81</span>
              <span class="unit">sec</span>
            </div>
            <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
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
          <div class="cart-title">{{$t('queryByDay', {unit: $t(dateUnit), type: chartTitle})}}</div>
          <el-select v-model="dateUnit" size="mini" class="line-chart-select" @change="loadLineChartData">
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
          :y-format="formatYAxis">
          </vn-line>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess } from '../../util/business'
import { handleSuccessAsync, handleError } from '../../util/index'
import { loadLiquidFillGauge, liquidFillGaugeDefaultSettings } from '../../util/liquidFillGauge'
import $ from 'jquery'
import * as d3 from 'd3'
import moment from 'moment-timezone'
import BarChart from './BarChart'
import LineChart from './LineChart'
@Component({
  methods: {
    ...mapActions({
      getRulesImpact: 'GET_RULES_IMPACT',
      getQuotaInfo: 'GET_QUOTA_INFO',
      clearTrash: 'CLEAR_TRASH',
      loadDashboardQueryInfo: 'LOAD_DASHBOARD_QUERY_INFO',
      loadQueryChartData: 'LOAD_QUERY_CHART_DATA',
      loadQueryDuraChartData: 'LOAD_QUERY_DURA_CHART_DATA'
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
    'en': {storageQuota: 'Storage Quota', acceImpact: 'Acceleration Impact', ruleSetting: 'Rules Setting', totalStorage: 'Total Storage', useageMana: 'Useage Manage', trash: 'Trash', clear: 'Clear', queryCount: 'Query Count', viewDetail: 'View Detail', avgQueryLatency: 'Avg. Query Latency', jobCount: 'Job Count', avgBulidTime: 'Avg Build Time Per MB', queryByModel: '{type} by Model', queryByDay: '{type} by {unit}', queryByIndex: '{type} by Index', lastWeek: 'Last Week', lastMonth: 'Last Month', thisMonth: 'This Month', day: 'Day', week: 'Week', month: 'Month'},
    'zh-cn': {storageQuota: '储存配额', acceImpact: '加速规则影响力', ruleSetting: '规则设置', totalStorage: '总储存容量', useageMana: '占用资源管理', trash: '系统垃圾', clear: '清除垃圾', queryCount: '查询次数', viewDetail: '查看详情', avgQueryLatency: '平均查询延迟', jobCount: '任务次数', avgBulidTime: '每兆平均构建时间', queryByModel: '以模型{type}', queryByDay: '以{unit}{type}', queryByIndex: '以索引{type}', lastWeek: '最近一周', lastMonth: '上个月', thisMonth: '当前月', day: '天', week: '周', month: '月'}
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
  quotaHeight = 170
  showQueryChart = true
  showLatencyChart = false
  showJobChart = false
  showBulidChart = false
  chartTitle = this.$t('queryCount')
  barChartData = {
    customer_cube_hbase_spark: 6,
    customer_vorder_cube_hbase_MR: 6,
    lineitem_cube_hbase_MR: 1175,
    partsupp_cube_hbase_spark: 8,
    partsupp_cube_hbase_spark2: 18,
    partsupp_cube_hbase_spark3: 28
  }
  lineChartDara = {
    '2018-12-11': 0,
    '2018-12-12': 0,
    '2018-12-13': 0,
    '2018-12-14': 0,
    '2018-12-15': 0,
    '2018-12-16': 35,
    '2018-12-17': 1,
    '2018-12-18': 3
  }
  dateUnit = 'day'
  unitOptions = ['day', 'week', 'month']
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
    }
  }
  loadQueryChart () {
    this.resetShow()
    this.showQueryChart = true
    this.chartTitle = this.$t('queryCount')
    this.getQueryBarChartData()
    this.getQueryLineChartData()
  }
  async getQueryBarChartData () {
    const res = await this.loadQueryChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    this.barChartData = resData
  }
  async getQueryLineChartData () {
    const resLine = await this.loadQueryChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: this.dateUnit})
    const resDataLine = await handleSuccessAsync(resLine)
    this.lineChartDara = resDataLine
  }
  loadLatencyChart () {
    this.resetShow()
    this.showLatencyChart = true
    this.chartTitle = this.$t('avgQueryLatency')
    this.getQueryDuraBarChartData()
    this.getQueryDuraLineChartData()
  }
  async getQueryDuraBarChartData () {
    const res = await this.loadQueryDuraChartData({project: this.currentSelectedProject, start_time: this.daterange[0].getTime(), end_time: this.daterange[1].getTime(), dimension: 'model'})
    const resData = await handleSuccessAsync(res)
    Object.keys(resData).forEach(k => {
      resData[k] = resData[k] / 1000
    })
    this.barChartData = resData
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
    this.chartTitle = this.$t('jobCount')
  }
  loadBulidChart () {
    this.resetShow()
    this.showBulidChart = true
    this.chartTitle = this.$t('avgBulidTime')
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
        key: 'QUERY',
        area: true,
        values: Object.entries(this.barChartData).map(([key, value]) => ({ label: key, value: value }))
      }
    ]
  }
  formatLabel (d) {
    return d.length > 7 ? d.substring(0, 7) + '...' : d
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
    return `<table>
      <tr>
        <td class="key">${d.data.label}</td>
        <td class="value">${d.data.value.toFixed(2)}</td>
      </tr>
    </table>`
  }
  get traffics2 () {
    return [
      {
        key: 'QUERY',
        area: true,
        values: Object.entries(this.lineChartDara).map(([key, value]) => ({ x: new Date(key).getTime(), y: value })).sort((a, b) => {
          return b.x - a.x
        })
      }
    ]
  }
  formatDate (d) {
    const formatPattern = '%Y-%m-%d'
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
      this.loadQueryInfo()
      this.loadQueryChart()
    }
  }
  async loadQuotaInfo () {
    const res = await this.getQuotaInfo({project: this.currentSelectedProject})
    const resData = await handleSuccessAsync(res)
    this.quotaInfo = resData
    this.useageRatio = (resData.total_storage_size / resData.storage_quota_size).toFixed(4)
    this.trashRatio = (resData.garbage_storage_size / resData.storage_quota_size).toFixed(4)
    setTimeout(() => {
      this.useageBlockHeight = this.useageRatio >= 1 ? this.quotaHeight : this.useageRatio * this.quotaHeight
      this.trashBlockHeight = this.trashRatio >= 1 ? this.quotaHeight : this.trashRatio * this.quotaHeight
    }, 0)
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
  async loadQueryInfo () {
    const res = await this.loadDashboardQueryInfo({project: this.currentSelectedProject, start_time: this.daterange[0], end_time: this.daterange[1]})
    const resData = await handleSuccessAsync(res)
    this.queryCount = resData.count
    this.queryMean = (resData.mean / 1000).toFixed(2)
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #dashboard {
    margin: 20px;
    .dash-card {
      box-shadow: 0px 0px 4px 0px @line-border-color;
      border: 1px solid @table-stripe-color;
      background-color: @fff;
      padding: 15px;
      text-align: center;
      box-sizing: border-box;
      &:hover {
        box-shadow: 0px 0px 8px 0px @line-border-color;
      }
      .cart-title {
        color: @text-title-color;
        font-size: 14px;
        font-weight: 500;
        line-height: 14px;
        text-align: left;
      }
      .content {
        margin: 30px 0 25px auto;
        .num {
          color: @base-color;
          font-size: 36px;
          line-height: 43px;
        }
      }
      .quota-row {
        margin-top: 20px;
        .quota-chart {
          height: 170px;
          width: 90px;
          border-radius: 4px;
          border: 2px solid #15bdf1;
          box-shadow: 0px 0px 2px 0px #3AA0E5;
          float: right;
          position: relative;
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
          .info-title {
            color: @text-normal-color;
            font-size: 12px;
            line-height: 14px;
            font-weight: 500;
          }
          .total-quota {
            font-weight: 500px;
            color: @text-title-color;
          }
          .useage {
            font-weight: 500px;
            font-size: 18px;
            color: #3bb477;
          }
          .trash {
            font-weight: 500px;
            font-size: 18px;
            color: @warning-color-1;
          }
        }
      }
    }
    .ratio-row .dash-card {
      height: 253px;
    }
    .count-row .el-col {
      position: relative;
      height: 176px;
      .dash-card {
        position: absolute;
        height: 176px;
        width: calc(~"100% - 10px");
        padding: 0;
        .inner-card {
          padding: 15px;
        }
        &.isActive {
          height: 188px;
          border-top: 2px solid @base-color-1;
          .inner-card {
            height: 160px;
            width: calc(~"100% - 28px");
            border-bottom: none;
            position: absolute;
            top: 0px;
            left: -1px;
            z-index: 1;
            background-color: @fff;
          }
        }
      }
    }
    .chart-row.dash-card {
      .chart-block {
        position: relative;
        > div {
          height: 355px;
        }
       &:first-child {
        border-right: 1px solid @line-border-color;
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
         top: 0;
         right: 15px;
         width: 80px;
       }
      }
    }
    .divider {
      margin: 25px 0;
      border-top: 1px solid @table-stripe-color;
    }
  }
  .quota-popover {
    min-width: 130px !important;
    .info-block {
      display: table-row;
      font-weight: 500;
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
