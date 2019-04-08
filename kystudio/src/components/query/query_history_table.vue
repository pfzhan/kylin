<template>
  <div id="queryHistoryTable">
    <div class="clearfix ksd-mb-10">
      <div class="btn-group ksd-fleft">
        <div class="ksd-title-label ksd-mt-10">{{$t('kylinLang.menu.queryhistory')}}</div>
      </div>
      <div class="ksd-fright ksd-inline searchInput ksd-ml-10">
        <el-input v-model="filterData.sql" @input="onSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')+$t('kylinLang.query.sqlContent_th')" size="medium"></el-input>
      </div>
    </div>
    <el-table
      :data="queryHistoryData"
      border
      class="history-table"
      style="width: 100%">
      <el-table-column type="expand" width="34">
        <template slot-scope="props">
          <div class="detail-title">
            <span class="ksd-fleft ksd-fs-16">{{$t('queryDetails')}}</span>
          </div>
          <div class="detail-content">
            <el-row :gutter="30">
              <el-col :span="14">
                <kap-editor height="320" width="100%" lang="sql" theme="chrome" ref="historySqlEditor" :readOnly="true" :isFormatter="true" v-model="props.row.sql_text" dragbar="#393e53">
                </kap-editor>
              </el-col>
              <el-col :span="10">
                <table class="ksd-table">
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.query_id')}}</th>
                    <td>{{props.row.query_id}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.duration')}}</th>
                    <td>{{props.row.duration / 1000 | fixed(2)}}s</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.answered_by')}}</th>
                    <td style="padding: 5px 10px;">
                      <div v-if="props.row.realizations && props.row.realizations.length" class="realization-tags">
                        <el-tag size="small" style="cursor:pointer;" v-for="item in props.row.realizations" :key="item.modelId" @click.native="openAgg(item.modelId)">{{item.modelAlias}}</el-tag>
                      </div>
                      <div v-else class="realization-tags"><el-tag type="warning" size="small" v-if="props.row.engine_type">{{props.row.engine_type}}</el-tag></div>
                    </td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query. total_scan_count')}}</th>
                    <td>{{props.row.total_scan_count}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query. total_scan_bytes')}}</th>
                    <td>{{props.row.total_scan_bytes}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.result_row_count')}}</th>
                    <td>{{props.row.result_row_count}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.cache_hit')}}</th>
                    <td>{{props.row.cache_hit}}</td>
                  </tr>
                </table>
              </el-col>
            </el-row>
          </div>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="query_time" header-align="center" width="207">
        <template slot-scope="props">
          {{transToGmtTime(props.row.query_time)}}
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn2" prop="duration" header-align="center" align="right" width="133">
        <template slot-scope="props">
          <span v-if="props.row.duration < 1000 && props.row.query_status === 'SUCCEEDED'">&lt; 1s</span>
          <span v-if="props.row.duration >= 1000 && props.row.query_status === 'SUCCEEDED'">{{props.row.duration / 1000 | fixed(2)}}s</span>
          <span v-if="props.row.query_status === 'FAILED'">Failed</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_text" header-align="center" show-overflow-tooltip>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn3" prop="realizations" header-align="center" width="250" show-overflow-tooltip>
        <template slot-scope="props">
          <div class="tag-ellipsis">
            <template v-if="props.row.realizations && props.row.realizations.length">
              <el-tag v-for="item in props.row.realizations" size="small" :key="item.modelId">{{item.modelAlias}}</el-tag>
            </template>
            <template v-else>
              <el-tag type="warning" size="small" v-if="props.row.engine_type">{{props.row.engine_type}}</el-tag>
            </template>
          </div>
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.submitter')" prop="submitter" align="center" width="145">
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import { transToUtcTimeFormat, handleSuccess, handleError, transToGmtTime } from '../../util/business'
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import '../../util/fly.js'
import $ from 'jquery'
@Component({
  props: ['queryHistoryData'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      markFav: 'MARK_FAV'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'briefMenuGet'
    ])
  },
  locales: {
    'en': {queryDetails: 'Query Details', ruleDesc: 'Favorite Condition:<br/>Query Frequency (default by daily);<br/>Query Duration;<br/>From user/ user group;<br/>Pushdown Query.', toAcce: 'Click to Accelerate'},
    'zh-cn': {queryDetails: '查询执行详情', ruleDesc: '加速规则条件包括：<br/>查询频率(默认是每日的频率)；<br/>查询响应时间；<br/>特定用户(组)；<br/>所有下压查询。', toAcce: '去加速'}
  }
})
export default class QueryHistoryTable extends Vue {
  datetimerange = ''
  startSec = 0
  endSec = 10
  latencyFilterPopoverVisible = false
  statusFilteArr = [{name: 'el-icon-ksd-acclerate_all', value: 'FULLY_ACCELERATED', status: 'fullyAcce'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED', status: 'partlyAcce'}, {name: 'el-icon-ksd-negative', value: 'UNACCELERATED', status: 'unAcce1'}]
  realFilteArr = [{name: 'Pushdown', value: 'pushdown'}, {name: 'Model Name', value: 'modelName'}]
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: null
  }
  timer = null
  showCopyStatus = false

  @Watch('datetimerange')
  onDateRangeChange (val) {
    if (val) {
      this.filterData.startTimeFrom = new Date(val[0]).getTime()
      this.filterData.startTimeTo = new Date(val[1]).getTime()
    } else {
      this.filterData.startTimeFrom = null
      this.filterData.startTimeTo = null
    }
    this.filterList()
  }

  onCopy () {
    this.showCopyStatus = true
    setTimeout(() => {
      this.showCopyStatus = false
    }, 3000)
  }

  onError () {
    this.$message(this.$t('kylinLang.common.copyfail'))
  }

  flyEvent (event) {
    const targetArea = $('#studio')
    const targetDom = this.briefMenuGet ? targetArea.find('.menu-icon') : targetArea.find('#favo-menu-item')
    const offset = targetDom.offset()
    const flyer = $('<span class="fly-box"></span>')
    flyer.fly({
      start: {
        left: event.pageX,
        top: event.pageY
      },
      end: {
        left: offset.left,
        top: offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        $('#favo-menu-item').css('opacity', 1)
        targetDom.addClass('rotateY')
        flyer.remove()
        setTimeout(() => {
          targetDom.fadeTo('slow', 0.5, function () {
            targetDom.removeClass('rotateY')
            targetDom.fadeTo('fast', 1)
          })
        }, 3000)
      }
    })
  }
  getAnsweredByList (answeredBy) {
    return answeredBy ? answeredBy.split(',') : []
  }

  toAcce (event, row) {
    this.markFav({project: this.currentSelectedProject, sql: row.sql_text, sqlPattern: row.sql_pattern, queryTime: row.query_time, queryStatus: row.query_status}).then((res) => {
      handleSuccess(res, () => {
        if (this._isDestroyed) {
          return
        }
        this.flyEvent(event)
      })
    }, (res) => {
      handleError(res)
    })
  }

  onSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.filterList()
    }, 500)
  }

  filterList () {
    this.$emit('loadFilterList', this.filterData)
  }

  openAgg (modelId) {
    this.$emit('openAgg', modelId)
  }
  renderColumn (h) {
    if (this.filterData.startTimeFrom && this.filterData.startTimeTo) {
      const startTime = transToUtcTimeFormat(this.filterData.startTimeFrom)
      const endTime = transToUtcTimeFormat(this.filterData.startTimeTo)
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTime_th')}</span>
        <el-tooltip placement="top" class="ksd-fright">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {startTime} To {endTime}</span>
            </span>
          </div>
          <el-date-picker
            value={this.datetimerange}
            onInput={val => (this.datetimerange = val)}
            type="datetimerange"
            popper-class="table-filter-datepicker"
            toggle-icon="el-icon-ksd-data_range"
            is-only-icon={true}>
          </el-date-picker>
        </el-tooltip>
      </span>)
    } else {
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTime_th')}</span>
        <el-date-picker
          value={this.datetimerange}
          onInput={val => (this.datetimerange = val)}
          popper-class="table-filter-datepicker"
          type="datetimerange"
          toggle-icon="el-icon-ksd-data_range"
          is-only-icon={true}>
        </el-date-picker>
      </span>)
    }
  }
  resetLatency () {
    this.startSec = null
    this.endSec = null
    this.filterData.latencyFrom = this.startSec
    this.filterData.latencyTo = this.endSec
    this.latencyFilterPopoverVisible = false
    this.filterList()
  }
  saveLatencyRange () {
    this.filterData.latencyFrom = this.startSec
    this.filterData.latencyTo = this.endSec
    this.latencyFilterPopoverVisible = false
    this.filterList()
  }
  renderColumn2 (h) {
    if (this.filterData.latencyFrom && this.filterData.latencyTo) {
      return (<span>
        <span>{this.$t('kylinLang.query.latency_th')}</span>
        <el-tooltip placement="top" class="ksd-fright">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {this.filterData.latencyFrom}s To {this.filterData.latencyTo}s</span>
            </span>
          </div>
          <el-popover
            ref="latencyFilterPopover"
            placement="bottom"
            width="320"
            value={this.latencyFilterPopoverVisible}
            onInput={val => (this.latencyFilterPopoverVisible = val)}>
            <div class="latency-filter-pop">
              <el-input-number
                size="medium"
                value={this.startSec}
                onInput={val1 => (this.startSec = val1)}></el-input-number>
              <span>&nbsp;S&nbsp;&nbsp;To</span>
              <el-input-number
                size="medium"
                value={this.endSec}
                onInput={val2 => (this.endSec = val2)}></el-input-number>
              <span>&nbsp;S</span>
            </div>
            <div class="latency-filter-footer">
              <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
              <el-button type="primary" onClick={this.saveLatencyRange} plain size="small">{this.$t('kylinLang.common.save')}</el-button>
            </div>
            <i class="el-icon-ksd-data_range" onClick={e => (e.stopPropagation())} slot="reference"></i>
          </el-popover>
        </el-tooltip>
      </span>)
    } else {
      return (<span>
        <span>{this.$t('kylinLang.query.latency_th')}</span>
        <el-popover
          ref="latencyFilterPopover"
          placement="bottom"
          width="320"
          class="ksd-fright"
          value={this.latencyFilterPopoverVisible}
          onInput={val => (this.latencyFilterPopoverVisible = val)}>
          <div class="latency-filter-pop">
            <el-input-number
              size="medium"
              value={this.startSec}
              min={0}
              onInput={val1 => (this.startSec = val1)}></el-input-number>
            <span>&nbsp;S&nbsp;&nbsp;To</span>
            <el-input-number
              size="medium"
              value={this.endSec}
              min={0}
              onInput={val2 => (this.endSec = val2)}></el-input-number>
            <span>&nbsp;S</span>
          </div>
          <div class="latency-filter-footer">
            <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
            <el-button type="primary" onClick={this.saveLatencyRange} plain size="small">{this.$t('kylinLang.common.save')}</el-button>
          </div>
          <i class="el-icon-ksd-data_range" onClick={e => (e.stopPropagation())} slot="reference"></i>
        </el-popover>
      </span>)
    }
  }
  renderColumn3 (h) {
    let items = []
    for (let i = 0; i < this.realFilteArr.length; i++) {
      items.push(<el-checkbox label={this.realFilteArr[i].value} key={this.realFilteArr[i].value}>{this.realFilteArr[i].name}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.realization_th')}</span>
      <el-popover
        ref="realFilterPopover"
        placement="bottom"
        width="200">
        <el-checkbox-group class="filter-groups" value={this.filterData.realization} onInput={val => (this.filterData.realization = val)} onChange={this.filterList}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn5 (h) {
    let items = []
    for (let i = 0; i < this.statusFilteArr.length; i++) {
      items.push(<el-checkbox label={this.statusFilteArr[i].value} key={this.statusFilteArr[i].value}><i class={this.statusFilteArr[i].name}></i> {this.$t('kylinLang.query.' + this.statusFilteArr[i].status)}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.acceleration_th')}</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.filterData.accelerateStatus} onInput={val => (this.filterData.accelerateStatus = val)} onChange={this.filterList}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryHistoryTable {
    margin-top: 20px;
    .el-table__expanded-cell {
      padding: 20px;
      .copy-btn {
        margin-right: 9%;
        .copyStatusMsg {
          display: inline-block;
          color: @text-normal-color;
          .el-icon-circle-check {
            color: @normal-color-1;
          }
        }
      }
      .detail-title {
        border-bottom: 1px solid @line-border-color;
        overflow: hidden;
        padding: 10px 0;
        span:first-child {
          line-height: 24px;
          font-weight: bold;
        }
        span:last-child {
          color: @text-normal-color;
        }
      }
      .detail-content {
        padding: 10px 0;
        line-height: 1.8;
      }
    }
    .searchInput {
      width: 400px;
    }
    .history-table {
      .tag-ellipsis {
        width: 100%;
        text-overflow: ellipsis;
        overflow: hidden;
        font-size: 0;
        line-height: 1;
        .el-tag:not(:last-child) {
          margin-right: 5px;
        }
      }
      .realization-tags {
        display: flex;
        flex-wrap: wrap;
        .el-tag {
          margin: 2.5px 5px 2.5px 0;
        }
      }
      .el-date-editor {
        line-height: inherit;
        padding: 0;
        position: absolute;
        right: 10px;
      }
      .el-icon-ksd-filter {
        float: right;
        position: relative;
        top: 5px;
      }
      .el-icon-ksd-negative {
        color: @text-normal-color;
        font-size: 20px;
        &:hover {
          color: @base-color;
        }
      }
      .status-icon {
        font-size: 20px;
        &.el-icon-ksd-acclerate_all,
        &.el-icon-ksd-acclerate_portion {
          color: @normal-color-1;
        }
      }
    }
  }
  &.el-icon-ksd-acclerate_all,
  &.el-icon-ksd-acclerate_portion {
    color: @normal-color-1;
  }
  .to_acce {
    font-size: 12px;
    line-height: 1.5;
    color: @text-title-color;
  }
  .latency-filter-pop {
    display: inline-flex;
    align-items: center;
    padding: 5px 20px;
    .el-input-number--medium {
      width: 120px;
      margin-left: 10px;
      &:first-child {
        margin-left: 0;
      }
    }
  }
  .latency-filter-footer {
    border-top: 1px solid @line-border-color;
    padding: 10px 10px 0;
    text-align: right;
    margin: 10px -10px 0;
  }
  .filter-groups .el-checkbox {
    display: block;
    margin-bottom: 8px;
    margin-left: 5px !important;
    &:last-child {
      margin-bottom: 0;
    }
  }
</style>
