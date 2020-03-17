<template>
  <div id="queryHistoryTable">
    <div class="clearfix ksd-mb-10">
      <div class="btn-group ksd-fleft">
        <div class="ksd-title-label ksd-mt-10">{{$t('kylinLang.menu.queryhistory')}}</div>
      </div>
      <div class="ksd-fright ksd-inline searchInput ksd-ml-10">
        <el-input v-model="filterData.sql" v-global-key-event.enter.debounce="onSqlFilterChange" @clear="onSqlFilterChange()" prefix-icon="el-icon-search" :placeholder="$t('searchSQL')" size="medium"></el-input>
      </div>
    </div>
    <div class="filter-tags" v-show="filterTags.length">
      <div class="filter-tags-layout"><el-tag size="small" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{$t(item.source) + '：' + item.label}}</el-tag></div>
      <span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span>
    </div>
    <el-table
      :data="queryHistoryData"
      border
      v-scroll-shadow
      class="history-table"
      :empty-text="emptyText"
      :expand-row-keys="toggleExpandId"
      @expand-change="expandChange"
      :row-key="val => val.query_id"
      ref="queryHistoryTable"
      style="width: 100%">
      <el-table-column type="expand" width="34">
        <template slot-scope="props">
          <div class="detail-title">
            <span class="ksd-fleft ksd-fs-14">{{$t('queryDetails')}}</span>
          </div>
          <div class="detail-content">
            <el-row :gutter="15" type="flex">
              <el-col :span="14" :style="{height: props.row.flexHeight + 'px'}">
                <div class="loading" v-if="currentExpandId === props.row.query_id"><i class="el-icon-loading"></i></div>
                <kap-editor
                  width="100%"
                  :height="props.row.editorH"
                  lang="sql"
                  theme="chrome"
                  v-if="props.row.editorH"
                  :ref="'historySqlEditor' + props.row.query_id"
                  :key="props.row.query_id"
                  :readOnly="true"
                  :needFormater="true"
                  :dragable="false"
                  :isAbridge="true"
                  :value="props.row.sql_text">
                </kap-editor>
              </el-col>
              <el-col :span="10">
                <table class="ksd-table history_detail_table" :id="'detailTable_' + props.row.query_id">
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.query_id')}}</th>
                    <td>{{props.row.query_id}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.duration')}}</th>
                    <td>{{props.row.duration / 1000 | fixed(2)}}s</td>
                  </tr>
                  <tr class="ksd-tr" :class="{'active': props.row.hightlight_realizations}">
                    <th class="label">{{$t('kylinLang.query.answered_by')}}</th>
                    <td style="padding: 3px 10px;">
                      <div v-if="props.row.realizations && props.row.realizations.length" class="realization-tags">
                        <span v-for="(item, index) in props.row.realizations" :key="item.modelId">
                          <template v-if="'visible' in item && !item.visible">
                            <span @click="openAuthorityDialog(item)" class="no-authority-model"><i class="el-icon-ksd-lock"></i>{{item.modelAlias}}</span><span class="split" v-if="index < props.row.realizations.length-1">,</span>
                          </template>
                          <template v-else>
                            <span @click="openIndexDialog(item)" :class="{'model-tag': item.valid, 'disable': !item.valid}">{{item.modelAlias}}</span><span class="split" v-if="index < props.row.realizations.length-1">,</span>
                          </template>
                        </span>
                      </div>
                      <div v-else class="realization-tags"><el-tag type="warning" size="small" v-if="props.row.engine_type">{{props.row.engine_type}}</el-tag></div>
                    </td>
                  </tr>
                  <tr class="ksd-tr" v-if="props.row.realizations && props.row.realizations.length">
                    <th class="label">{{$t('kylinLang.query.index_id')}}</th>
                    <td>{{getLayoutIds(props.row.realizations)}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.total_scan_count')}}</th>
                    <td>{{props.row.total_scan_count | filterNumbers}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.total_scan_bytes')}}</th>
                    <td>{{props.row.total_scan_bytes | filterNumbers}}</td>
                  </tr>
                  <tr class="ksd-tr">
                    <th class="label">{{$t('kylinLang.query.result_row_count')}}</th>
                    <td>{{props.row.result_row_count | filterNumbers}}</td>
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
      <el-table-column :renderHeader="renderColumn" prop="query_time" width="218">
        <template slot-scope="props">
          {{transToGmtTime(props.row.query_time)}}
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn2" prop="duration" align="right" width="100">
        <template slot-scope="props">
          <span v-if="props.row.duration < 1000 && props.row.query_status === 'SUCCEEDED'">&lt; 1s</span>
          <span v-if="props.row.duration >= 1000 && props.row.query_status === 'SUCCEEDED'">{{props.row.duration / 1000 | fixed(2)}}s</span>
          <!-- <span v-if="props.row.query_status === 'FAILED'">Failed</span> -->
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.query_id')" prop="query_id" width="120" show-overflow-tooltip>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_limit" min-width="120">
        <template slot-scope="props">
          <el-popover
            ref="sql-popover"
            placement="top"
            trigger="hover"
            popper-class="col-sql-popover">
            <div class="sql-column" slot="reference" @click="handleExpandType(props)">{{props.row.sql_limit}}</div>
            <template>
              <span>{{props.row.sql_limit}}</span>
              <div class="sql-tip" v-if="sqlOverLimit(props.row.sql_limit)">{{$t('sqlDetailTip')}}</div>
            </template>
          </el-popover>
        </template>
      </el-table-column>
      <el-table-column
        :filters="realFilteArr"
        :filtered-value="filterData.realization"
        :label="$t('kylinLang.query.realization_th')"
        filter-icon="el-icon-ksd-filter"
        :show-multiple-footer="false"
        :filter-change="(v) => filterContent(v, 'realization')"
        prop="realizations"
        width="250">
        <template slot-scope="props">
          <div class="tag-ellipsis" :class="{'hasMore': checkIsShowMore(props.row.realizations)}">
            <template v-if="props.row.realizations && props.row.realizations.length">
              <el-tag v-for="(item, index) in props.row.realizations" :class="{'disabled': 'visible' in item && !item.visible}" v-if="index < checkShowCount(props.row.realizations)" :type="'visible' in item && !item.visible ? 'info' : item.valid ? 'success' : 'info'" size="small" :key="item.modelId"><i class="el-icon-ksd-lock" v-if="'visible' in item && !item.visible"></i>{{item.modelAlias}}</el-tag>
              <a v-if="checkIsShowMore(props.row.realizations)" href="javascript:;" @click="handleExpandType(props, true)" class="showMore el-tag el-tag--small">{{$t('showDetail', {count: props.row.realizations.length - checkShowCount(props.row.realizations)})}}</a>
            </template>
            <template v-else>
              <el-tag type="warning" size="small" v-if="props.row.engine_type">{{props.row.engine_type}}</el-tag>
            </template>
          </div>
        </template>
      </el-table-column>
      <el-table-column :filters="statusList.map(item => ({text: item, value: item}))" :filtered-value="filterData.query_status" :label="$t('kylinLang.query.query_status')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'query_status')" show-overflow-tooltip prop="query_status" width="130">
        <template slot-scope="scope">
          {{$t('kylinLang.query.' + scope.row.query_status)}}
        </template>
      </el-table-column>
      <el-table-column :filterMultiple="false" :show-all-select-option="false" :filters="queryNodes.map(item => ({text: item, value: item}))" :filtered-value="filterData.server" :label="$t('kylinLang.query.queryNode')" filter-icon="el-icon-ksd-filter" :filter-change="(v) => filterContent(v, 'server')"  show-overflow-tooltip prop="server" width="145">
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.submitter')" prop="submitter" show-overflow-tooltip width="90">
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import { transToUtcTimeFormat, handleSuccess, handleError, transToGmtTime, getStringLength } from '../../util/business'
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import '../../util/fly.js'
import $ from 'jquery'
import { sqlRowsLimit, sqlStrLenLimit } from '../../config/index'
// import sqlFormatter from 'sql-formatter'
@Component({
  name: 'QueryHistoryTable',
  props: ['queryHistoryData', 'queryNodes'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      markFav: 'MARK_FAV'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'briefMenuGet'
    ])
  },
  locales: {
    'en': {
      queryDetails: 'Query Details',
      ruleDesc: 'Favorite Condition:<br/>Query Frequency (default by daily);<br/>Query Duration;<br/>From user/ user group;<br/>Pushdown Query.',
      toAcce: 'Click to Accelerate',
      searchSQL: 'Search one keyword or query ID',
      noSpaceTips: 'Invalide entering: cannot search space',
      sqlDetailTip: 'Please click sql to get more informations',
      taskStatus: 'Task Status',
      realization: 'Query',
      clearAll: 'Clear All',
      showDetail: 'More {count}'
    },
    'zh-cn': {
      queryDetails: '查询执行详情',
      ruleDesc: '加速规则条件包括：<br/>查询频率(默认是每日的频率)；<br/>查询响应时间；<br/>特定用户(组)；<br/>所有下压查询。',
      toAcce: '去加速',
      searchSQL: '搜索单个关键词或查询 ID',
      noSpaceTips: '无法识别输入中的空格',
      sqlDetailTip: '更多信息请点击 SQL 语句',
      taskStatus: '任务状态',
      realization: '查询对象',
      clearAll: '清除所有',
      showDetail: '更多 {count}'
    }
  },
  filters: {
    filterNumbers (num) {
      if (num > 0) return num
    }
  }
})
export default class QueryHistoryTable extends Vue {
  datetimerange = ''
  startSec = 0
  endSec = 10
  latencyFilterPopoverVisible = false
  statusFilteArr = [{name: 'el-icon-ksd-acclerate_all', value: 'FULLY_ACCELERATED', status: 'fullyAcce'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED', status: 'partlyAcce'}, {name: 'el-icon-ksd-negative', value: 'UNACCELERATED', status: 'unAcce1'}]
  realFilteArr = [{text: 'Pushdown', value: 'pushdown'}, {text: 'Model Name', value: 'modelName'}]
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    server: [],
    sql: null,
    query_status: []
  }
  timer = null
  showCopyStatus = false
  currentExpandId = ''
  toggleExpandId = []
  sqlLimitRows = 40 * 10
  statusList = ['SUCCEEDED', 'FAILED']
  filterTags = []

  @Watch('datetimerange')
  onDateRangeChange (val) {
    if (val) {
      this.filterData.startTimeFrom = new Date(val[0]).getTime()
      this.filterData.startTimeTo = new Date(val[1]).getTime()
      this.clearDatetimeRange()
      this.filterTags.push({label: `${this.transToGmtTime(this.filterData.startTimeFrom)} To ${this.transToGmtTime(this.filterData.startTimeTo)}`, source: 'kylinLang.query.startTime_th', key: 'datetimerange'})
    } else {
      this.filterData.startTimeFrom = null
      this.filterData.startTimeTo = null
      this.clearDatetimeRange()
    }
    this.filterList()
  }

  @Watch('queryHistoryData')
  onQueryHistoryDataChange (val) {
    val.forEach(element => {
      const sql = element.sql_text
      element['sql_limit'] = this.sqlOverLimit(sql) ? `${sql.slice(0, this.sqlLimitRows)}...` : sql
      element['server'] = [element['server']]
      element['flexHeight'] = 0
      element['editorH'] = 0
    })
    this.toggleExpandId = []
  }

  // 清除查询开始事件筛选项
  clearDatetimeRange () {
    if (this.filterTags.filter(item => item.key === 'datetimerange').length) {
      let idx = null
      for (let index in this.filterTags) {
        if (this.filterTags[index].key === 'datetimerange') {
          idx = index
          break
        }
      }
      this.filterTags.splice(idx, 1)
    }
  }

  // 清除响应时间筛选项
  clearLatencyRange () {
    if (this.filterTags.filter(item => item.key === 'latency').length) {
      let idx = null
      for (let index in this.filterTags) {
        if (this.filterTags[index].key === 'latency') {
          idx = index
          break
        }
      }
      idx !== null && this.filterTags.splice(idx, 1)
    }
  }

  get isHasFilterValue () {
    return this.filterData.sql || this.filterData.startTimeFrom || this.filterData.startTimeTo || this.filterData.latencyFrom || this.filterData.latencyTo || this.filterData.realization.length || this.filterData.query_status.length || this.filterData.server
  }

  get emptyText () {
    return this.isHasFilterValue ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  // 控制是否显示查看更多
  checkIsShowMore (arr) {
    let str = ''
    if (arr && arr.length) {
      // 如果将全部的模型合并后，超过21个字符，就肯定要显示更多
      for (let i = 0; i < arr.length; i++) {
        let item = arr[i]
        str = str + item.modelAlias
      }
      return arr.length > 1 && getStringLength(str) >= 21
    } else {
      return false
    }
  }
  checkShowCount (arr) {
    let str = ''
    let idx = 1
    if (arr && arr.length) {
      for (let i = 0; i < arr.length; i++) {
        let item = arr[i]
        str = str + item.modelAlias
        // 如果第一个就超21个字符了，就直接返回下标1
        if (i === 0 && getStringLength(str) >= 21) {
          idx = 1
          break
        } else {
          if (arr.length > 1 && getStringLength(str) >= 21) {
            idx = i
            break
          } else { // 如果每次加上名字后，还是可以放得下，就让下标+1，保证都正常渲染
            idx = i + 1
          }
        }
      }
    }
    return idx
  }

  sqlOverLimit (sql) {
    return sql.length > this.sqlLimitRows
  }

  expandChange (e) {
    if (this.toggleExpandId.includes(e.query_id)) {
      const index = this.toggleExpandId.indexOf(e.query_id)
      this.toggleExpandId.splice(index, 1)
      e.hightlight_realizations = false
      return
    }
    this.currentExpandId = e.query_id
    e.flexHeight = 0
    e.editorH = 0
    this.toggleExpandId.push(e.query_id)
    this.$nextTick(() => {
      const tableHeigth = $('#detailTable_' + e.query_id) && $('#detailTable_' + e.query_id).height()
      if (tableHeigth) {
        e.flexHeight = e.flexHeight + tableHeigth
        let showLimitTip = false
        let sqlTextArr = e.sql_text.split('\n')
        // 要手动传入高度
        if ((sqlTextArr.length > 0 && sqlTextArr.length > sqlRowsLimit) || (sqlTextArr.length === 0 && e.sql_text.length > sqlStrLenLimit)) {
          showLimitTip = true
        }
        e.editorH = showLimitTip ? (e.flexHeight - 32) : (e.flexHeight - 2)
      }
      this.currentExpandId = ''
    })
  }

  handleExpandType (props, realizations) {
    // 点击的时候根据当前是展开还是收起，进行展开收起
    let flag = this.toggleExpandId.indexOf(props.row.query_id) === -1
    if (realizations) {
      props.row.hightlight_realizations = flag
    }
    this.$refs.queryHistoryTable.toggleRowExpansion(props.row, flag)
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
  getLayoutIds (realizations) {
    if (realizations && realizations.length) {
      let firstSnapshot = false
      let filterIds = []
      for (let i of realizations) {
        if (i.layoutId === -1 && !firstSnapshot) {
          filterIds.push('Snapshot')
          firstSnapshot = true
        } else if (i.layoutId !== -1) {
          filterIds.push(i.layoutId)
        }
      }
      return filterIds.join(', ')
    } else {
      return ''
    }
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
    if (this.filterData.sql.trim().match(/\s/)) {
      this.$message({
        message: this.$t('noSpaceTips'),
        type: 'warning',
        duration: 0,
        showClose: true
      })
    }
    this.filterList()
  }

  filterServer (server) {
    this.filterData.server = this.filterData.server === server ? '' : server
    this.filterList()
  }
  filterList () {
    this.toggleExpandId = []
    this.$emit('loadFilterList', {...this.filterData, server: this.filterData.server.join('')})
  }

  openIndexDialog (realization) {
    if (!realization.valid) return
    if (realization.indexType === 'Agg Index') {
      this.$emit('openIndexDialog', realization.modelId)
    } else if (realization.indexType === 'Table Index') {
      this.$emit('openIndexDialog', realization.modelId, realization.layoutId)
    }
  }
  renderColumn (h) {
    if (this.filterData.startTimeFrom && this.filterData.startTimeTo) {
      const startTime = transToUtcTimeFormat(this.filterData.startTimeFrom)
      const endTime = transToUtcTimeFormat(this.filterData.startTimeTo)
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTime_th')}</span>
        <el-tooltip placement="top">
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
            toggle-icon="el-icon-ksd-data_range isFilter"
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
    this.clearLatencyRange()
    this.filterTags.push({label: `${this.startSec}s To ${this.endSec}s`, source: 'kylinLang.query.latency_th', key: 'latency'})
    this.filterList()
  }

  renderColumn2 (h) {
    if (this.filterData.latencyTo) {
      return (<span>
        <span style="margin-right:5px;">{this.$t('kylinLang.query.latency_th')}</span>
        <el-tooltip placement="top">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {this.filterData.latencyFrom}s To {this.filterData.latencyTo}s</span>
            </span>
          </div>
          <el-popover
            ref="latencyFilterPopover"
            placement="bottom"
            width="315"
            value={this.latencyFilterPopoverVisible}
            onInput={val => (this.latencyFilterPopoverVisible = val)}>
            <div class="latency-filter-pop">
              <el-input-number
                size="small"
                min={0}
                max={this.endSec}
                value={this.startSec}
                onInput={val1 => (this.startSec = val1)}></el-input-number>
              <span>&nbsp;S&nbsp;&nbsp;To</span>
              <el-input-number
                size="small"
                min={0}
                class="ksd-ml-10"
                value={this.endSec}
                onInput={val2 => (this.endSec = val2)}></el-input-number>
              <span>&nbsp;S</span>
            </div>
            <div class="latency-filter-footer">
              <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
              <el-button type="primary" onClick={this.saveLatencyRange} plain size="small">{this.$t('kylinLang.common.save')}</el-button>
            </div>
            <i class="el-icon-ksd-data_range isFilter" onClick={e => (e.stopPropagation())} slot="reference"></i>
          </el-popover>
        </el-tooltip>
      </span>)
    } else {
      return (<span>
        <span style="margin-right:5px;">{this.$t('kylinLang.query.latency_th')}</span>
        <el-popover
          ref="latencyFilterPopover"
          placement="bottom"
          width="315"
          value={this.latencyFilterPopoverVisible}
          onInput={val => (this.latencyFilterPopoverVisible = val)}>
          <div class="latency-filter-pop">
            <el-input-number
              size="small"
              value={this.startSec}
              min={0}
              max={this.endSec}
              onInput={val1 => (this.startSec = val1)}></el-input-number>
            <span>&nbsp;S&nbsp;&nbsp;To</span>
            <el-input-number
              size="small"
              class="ksd-ml-10"
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
        popperClass="history-filter">
        <el-checkbox-group class="filter-groups" value={this.filterData.realization} onInput={val => (this.filterData.realization = val)} onChange={this.filterList}>
          {items}
        </el-checkbox-group>
        <i class={this.filterData.realization.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumnStatus (h) {
    let statusList = ['SUCCEEDED', 'FAILED']
    let items = []
    for (let i = 0; i < statusList.length; i++) {
      items.push(<el-checkbox label={statusList[i]} key={statusList[i]}>{this.$t('kylinLang.query.' + statusList[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.query_status')}</span>
      <el-popover
        ref="statusFilterPopover"
        placement="bottom"
        popperClass="history-filter">
        <el-checkbox-group class="filter-groups" value={this.filterData.query_status} onInput={val => (this.filterData.query_status = val)} onChange={this.filterList}>
          {items}
        </el-checkbox-group>
        <i class={this.filterData.query_status.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn4 (h) {
    let items = []
    for (let i = 0; i < this.queryNodes.length; i++) {
      items.push(
        <div onClick={() => { this.filterServer(this.queryNodes[i]) }}>
          <el-dropdown-item class={this.queryNodes[i] === this.filterData.server ? 'active' : ''} key={i}>{this.queryNodes[i]}</el-dropdown-item>
        </div>
      )
    }
    return (<span>
      <span>{this.$t('kylinLang.query.queryNode')}</span>
      <el-dropdown hide-on-click={false} trigger="click">
        <i class={this.filterData.server ? 'el-icon-ksd-filter el-dropdown-link isFilter' : 'el-icon-ksd-filter el-dropdown-link'}></i>
        <template slot="dropdown">
          <el-dropdown-menu class="jobs-dropdown">
            {items}
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </span>)
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      realization: 'kylinLang.query.answered_by',
      query_status: 'taskStatus',
      server: 'kylinLang.query.queryNode'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filterData[type] = val
    this.filterList()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    if (tag.key === 'datetimerange') {
      this.datetimerange = ''
    } else if (tag.key === 'latency') {
      this.filterData.latencyFrom = null
      this.filterData.latencyTo = null
      this.clearLatencyRange()
    } else if (tag.key === 'server') {
      this.filterData.server.splice(0, 1)
      const index = this.filterTags.map(item => item.key).indexOf('server')
      this.filterTags.splice(index, 1)
    } else {
      const index = this.filterData[tag.key].indexOf(tag.label)
      index > -1 && this.filterData[tag.key].splice(index, 1)
      this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    }
    this.filterList()
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterData.query_status.splice(0, this.filterData.query_status.length)
    this.filterData.realization.splice(0, this.filterData.realization.length)
    this.filterData.server.splice(0, this.filterData.server.length)
    this.filterData.latencyFrom = null
    this.filterData.latencyTo = null
    this.datetimerange = ''
    this.filterTags = []
    this.filterList()
  }
  openAuthorityDialog (item) {
    const { unauthorized_tables, unauthorized_columns, modelAlias } = item
    let details = []
    if (unauthorized_tables && unauthorized_tables.length) {
      details.push({title: `Table (${unauthorized_tables.length})`, list: unauthorized_tables})
    }
    if (unauthorized_columns && unauthorized_columns.length) {
      details.push({title: `Columns (${unauthorized_columns.length})`, list: unauthorized_columns})
    }
    this.callGlobalDetailDialog({
      theme: 'plain-mult',
      title: this.$t('kylinLang.model.authorityDetail'),
      msg: this.$t('kylinLang.model.authorityMsg', {modelName: modelAlias}),
      showCopyBtn: true,
      showIcon: false,
      showDetailDirect: true,
      details,
      showDetailBtn: false,
      dialogType: 'error',
      customClass: 'no-acl-model',
      showCopyTextLeftBtn: true
    })
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryHistoryTable {
    margin-top: 20px;
    table.ksd-table{
      tr:nth-child(odd){
        background: @background-disabled-color;
      }
    }
    .el-table__expanded-cell {
      padding: 15px;
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
        /* border-bottom: 1px solid @line-border-color; */
        overflow: hidden;
        padding-bottom: 10px;
        span:first-child {
          line-height: 18px;
          font-weight: @font-medium;
        }
        span:last-child {
          color: @text-normal-color;
        }
      }
      .detail-content {
        padding-top: 15px;
        line-height: 1.8;
        .el-col {
          position: relative;
          .history_detail_table{
            tr.active{
              background-color: @base-color-9;
            }
          }
        }
      }
    }
    .searchInput {
      width: 400px;
    }
    .history-table {
      th .el-dropdown {
        padding: 0;
        line-height: 0;
        position: relative;
        left: 5px;
        top: 2px;
        .el-icon-ksd-filter {
          float: none;
          position: relative;
          left: 0px;
        }
      }
      .sql-column {
        cursor: pointer;
        width: calc(100%);
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 1;
        /*! autoprefixer: off */
        -webkit-box-orient: vertical;
        /* autoprefixer: on */
        white-space: nowrap\0;
        &:hover {
          color: @base-color;
        }
      }
      .ksd-table th {
        width: 140px;
        color:@text-normal-color;
      }
      .tag-ellipsis {
        width: 100%;
        text-overflow: ellipsis;
        overflow: hidden;
        font-size: 0;
        line-height: 1;
        display:flex;
        align-items: center;
        /* position:relative; */
        &.hasMore{
          .el-tag{
            max-width: calc(~"100% - 85px");
            &.showMore{
              width:auto;
              max-width: 80px;
              min-width: 60px;
            }
          }
        }
        .el-tag:not(:last-child) {
          margin-right: 5px;
        }
        .el-tag{
          max-width: 100%;
          overflow: hidden;
          text-overflow: ellipsis;
          &.disabled {
            border: solid 1px @text-placeholder-color;
            background-color: @background-disabled-color;
            color: @text-disabled-color;
            .el-icon-ksd-lock {
              margin-right: 3px;
            }
          }
        }
        .showMore{
          width:auto;
          max-width: 80px;
          min-width: 60px;
          text-overflow: ellipsis;
          font-size:12px;
          line-height: 20px;
          height:20px;
          border-radius: 10px;
          box-sizing: border-box;
          display:inline-block;
          border: none;
          &:hover{
            text-decoration: none;
            cursor:pointer;
          }
        }
        /* .el-tag.realizationsNumTag{
          &:hover{
            cursor:pointer;
          }
        } */
      }
      .realization-tags {
        display: flex;
        flex-wrap: wrap;
        /* .el-tag {
          margin: 2.5px 10px 2.5px 0;
          border: none;
          background: none;
          padding: 0;
          height: 16px;
          line-height: 16px;
          &.model-tag {
            cursor: pointer;
          }
        } */
        .model-tag {
          color: @base-color;
          cursor: pointer;
        }
        .disable{
          color: @text-disabled-color;
        }
        .split{
          margin-right:10px;
        }
        .no-authority-model {
          color: @text-disabled-color;
          cursor: pointer;
          &:hover {
            color: @base-color;
          }
          .el-icon-ksd-lock {
            margin-right: 3px;
          }
        }
      }
      .el-date-editor {
        line-height: 1;
        padding: 0;
        position: relative;
        top: 2px;
        left: 5px;
      }
      .el-icon-ksd-data_range {
        &.isFilter,
        &:hover {
          color: @base-color;
        }
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
      .el-icon-ksd-filter {
        position: relative;
        font-size: 17px;
        top: 1px;
        left: 5px;
        &:hover,
        &.filter-open {
          color: @base-color;
        }
      }
    }
  }
  .col-sql-popover {
    max-width: 400px;
    box-sizing: border-box
  }
  .sql-tip {
    text-align: center;
    margin-top: 5px;
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
    .el-input-number--medium {
      width: 120px;
      margin-left: 10px;
      &:first-child {
        margin-left: 0;
      }
    }
  }
  .latency-filter-footer {
    border-top: 1px solid @line-split-color;
    padding: 10px 10px 0;
    margin: 10px -10px 0;
    text-align: right;
  }
  .filter-groups .el-checkbox {
    display: block;
    margin-bottom: 8px;
    margin-left: 0px !important;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .el-popover.history-filter {
    min-width: 130px;
    box-sizing: border-box;
  }
  .loading {
    width: calc(~'100% - 13px');
    height: 254px;
    position: absolute;
    z-index: 10;
    background: @fff;
    .el-icon-loading {
      font-size: 20px;
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%)
    }
  }
  .filter-tags {
    margin-bottom: 10px;
    padding: 0px 5px 5px;
    box-sizing: border-box;
    position: relative;
    background: @background-disabled-color;
    .filter-tags-layout {
      display: inline-block;
      width: calc(~'100% - 80px')
    }
    .el-tag {
      margin-left: 5px;
      margin-top: 5px;
    }
    .clear-all-filters {
      position: absolute;
      top: 8px;
      right: 10px;
      font-size: 14px;
      color: @base-color;
      cursor: pointer;
    }
  }
</style>
