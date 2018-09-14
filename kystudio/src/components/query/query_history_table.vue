<template>
  <div id="queryHistoryTable">
    <div class="clearfix ksd-mb-16">
      <div class="btn-group ksd-fleft" v-if="isCandidate">
        <el-button type="primary" plain size="medium" icon="el-icon-ksd-mark_favorite" @click="markFavorite">Mark Favorite</el-button>
      </div>
      <div class="ksd-fright ksd-inline searchInput ksd-ml-10">
        <el-input v-model="filterData.sql" @input="onSqlFilterChange" prefix-icon="el-icon-search" placeholder="请输入内容" size="medium"></el-input>
      </div>
      <el-dropdown v-if="isCandidate" @command="handleCommand" class="fav-dropdown ksd-fright" trigger="click">
        <el-button size="medium" icon="el-icon-ksd-table_setting" plain type="primary">{{$t('kylinLang.query.applyRule')}}</el-button>
        <el-dropdown-menu slot="dropdown">
          <el-dropdown-item v-for="item in rules" :key="item.ruleId" class="fav-dropdown-item">
            <el-checkbox v-model="item.enabled" v-event-stop:click @change="toggleRule(item.uuid)">{{item.name}}</el-checkbox>
            <i class="el-icon-ksd-table_delete ksd-fright" v-event-stop:click @click="delRule(item.uuid)"></i>
            <i class="el-icon-ksd-table_edit ksd-fright" @click="editRule(item)"></i>
          </el-dropdown-item>
          <el-dropdown-item divided command="createRule">{{$t('createRule')}}</el-dropdown-item>
          <el-dropdown-item divided command="applyAll">{{$t('applyAll')}}</el-dropdown-item>
          <el-dropdown-item command="markAll">
            <span v-if="!isAutoMatic">{{$t('markAll')}}</span>
            <span v-else>{{$t('unMarkAll')}}</span>
          </el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
    </div>
    <el-table
      :data="queryHistoryData"
      border
      class="history-table"
      @selection-change="handleSelectionChange"
      style="width: 100%">
      <el-table-column type="expand" v-if="!isCandidate">
        <template slot-scope="props">
          <div class="detail-title">
            <span class="ksd-fleft ksd-fs-16">Query Details</span>
            <span class="ksd-fright">Help <i class="el-icon-question"></i></span>
          </div>
          <div class="detail-content">
            <el-row :gutter="20">
              <el-col :span="10">
                <div>
                  <span class="label">Query ID:</span>
                  <span>{{props.row.query_id}}</span>
                </div>
                <div>
                  <span class="label">Duration:</span>
                  <span>{{props.row.latency}}s</span>
                </div>
                <div>
                  <span class="label">Query Countent:</span>
                  <kap_editor height="130" width="80%" lang="sql" theme="chrome" v-model="props.row.sql" dragbar="#393e53">
                  </kap_editor>
                </div>
              </el-col>
              <el-col :span="10">
                <div>
                  <span class="label">Model Name:</span>
                  <span>{{props.row.model_name}}</span>
                </div>
                <div>
                  <span class="label">Realization:</span>
                  <span class="realization-detail" @click="openAgg(props.row)">{{props.row.realization | arrayToStr}}</span>
                </div>
                <div>
                  <span class="label">Content:</span>
                  <span>{{props.row.content | arrayToStr}}</span>
                </div>
                <div>
                  <span class="label">Total Scan Count:</span>
                  <span>{{props.row.total_scan_count}}</span>
                </div>
                <div>
                  <span class="label">Total Scan Bytes:</span>
                  <span>{{props.row.total_scan_bytes}}</span>
                </div>
                <br/>
                <div>
                  <span class="label">Result Row Count:</span>
                  <span>{{props.row.result_row_count}}</span>
                </div>
                <div>
                  <span class="label">If Hit Cache:</span>
                  <span>{{props.row.is_cubeHit}}</span>
                </div>
              </el-col>
            </el-row>
          </div>
        </template>
      </el-table-column>
      <el-table-column type="selection" width="55" align="center" v-if="isCandidate"></el-table-column>
      <el-table-column :renderHeader="renderColumn" sortable prop="start_time" header-align="center" width="160">
        <template slot-scope="props">
          {{props.row.start_time | gmtTime}}
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn2" sortable prop="latency" header-align="center" align="right" width="150">
        <template slot-scope="props">
          <span v-if="props.row.latency < 1 && props.row.query_status === 'SUCCEEDED'">< 1s</span>
          <span v-if="props.row.latency > 1 && props.row.query_status === 'SUCCEEDED'">{{props.row.latency / 1000}}s</span>
          <span v-if="props.row.query_status === 'FAILED'">Failed</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent')" prop="sql" header-align="center" show-overflow-tooltip>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn3" prop="realization" header-align="center" width="250">
      </el-table-column>
      <el-table-column label="IP" prop="query_node" header-align="center" width="200">
      </el-table-column>
      <el-table-column :renderHeader="renderColumn5" prop="accelerate_status" align="center" width="100" v-if="!isCandidate">
        <template slot-scope="props">
          <i class="status-icon" :class="{
            'el-icon-ksd-acclerate': props.row.accelerate_status === 'FULLY_ACCELERATED',
            'el-icon-ksd-acclerate_ready': props.row.accelerate_status === 'NEW'
          }"></i>
        </template>
      </el-table-column>
    </el-table>
    <div class="rule-block" v-if="ruleVisible"></div>
    <div class="ruleDiaglog translate-right transition-new" v-if="ruleVisible">
      <div class="el-dialog__header">
        <span class="el-dialog__title">{{ruleDiaglogTitle}}</span>
      </div>
      <div class="el-dialog__body">
        <el-form label-position="top" size="medium" :model="formRule" ref="formRule">
          <el-form-item :label="$t('ruleName')">
            <el-input v-model.trim="formRule.name"></el-input>
          </el-form-item>
          <hr></hr>
          <div class="ksd-mb-16">
            <span>When a new SQL query that meets all these conditions : </span>
            <i class="el-icon-ksd-what"></i>
          </div>
          <el-form-item v-for="(con, index) in formRule.conds" :key="index" class="con-form-item">
            <el-row :gutter="10">
              <el-col :span="6">
                <el-select v-model="con.field" placeholder="请选择" @change="fieldChanged(con)">
                  <el-option v-for="(item, key) in options" :key="key" :label="key" :value="item">
                  </el-option>
                </el-select>
              </el-col>
              <el-col :span="3" class="ksd-center">
                <span v-if="con.field=='latency' || con.field=='frequency'"> > </span>
                <span v-if="con.field=='user'"> is </span>
                <span v-if="con.field=='sql'"> Contains </span>
              </el-col>
              <el-col :span="5">
                <el-input v-model.trim="con.rightThreshold" v-if="con.field!=='user'"></el-input>
                <el-select v-model="con.rightThreshold" placeholder="请选择" v-else>
                  <el-option v-for="item in userGroups" :key="item" :label="item" :value="item">
                  </el-option>
                </el-select>
              </el-col>
              <el-col :span="4" style="width:105px;height:36px;">
                <span v-if="con.field=='latency'">Seconds</span>
                <span v-if="con.field=='frequency'">Times</span>
              </el-col>
              <el-col :span="6">
                <div class="action-group ksd-fright">
                  <el-button type="primary" icon="el-icon-ksd-add" plain circle size="medium" @click="addCon" v-if="index==0"></el-button>
                  <el-button icon="el-icon-ksd-minus" plain circle size="medium" :disabled="formRule.conds.length == 1" @click="removeCon(index)"></el-button>
                </div>
              </el-col>
            </el-row>
          </el-form-item>
        </el-form>
      </div>
      <div class="el-dialog__footer">
        <span class="dialog-footer">
          <el-checkbox v-model="formRule.enabled" class="ksd-fleft ksd-mt-6">Enabled</el-checkbox>
          <el-button size="medium" @click="ruleVisible = false">取 消</el-button>
          <el-button size="medium" type="primary" plain @click="submitRuleFrom">{{$t('kylinLang.common.save')}}</el-button>
        </span>
      </div>
    </div>
  </div>
</template>

<script>
import { handleSuccessAsync } from '../../util/index'
import { transToUtcTimeFormat, handleSuccess, handleError } from '../../util/business'
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
@Component({
  props: ['queryHistoryData', 'isCandidate'],
  methods: {
    ...mapActions({
      getAllRules: 'GET_ALL_RULES',
      saveRule: 'SAVE_RULE',
      updateRule: 'UPDATE_RULE',
      deleteRule: 'DELETE_RULE',
      enableRule: 'ENABLE_RULE',
      applyRule: 'APPLY_RULE',
      automaticRule: 'AUTOMATIC_RULE',
      getAutoMaticStatus: 'GET_AUTO_MATIC_STATUS'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {createRule: 'Create Rule', editRule: 'Edit Rule', applyAll: 'Apply All', markAll: 'Mark Favorite Automatically', ruleName: 'Rule Name', unMarkAll: 'Unmark Favorite Automatically'},
    'zh-cn': {createRule: '新建规则', editRule: '编辑规则', applyAll: '全部应用', markAll: '全部标记为待加速', ruleName: '规则名称', unMarkAll: '取消全部标记为待加速'}
  }
})
export default class QueryHistoryTable extends Vue {
  datetimerange = ''
  startSec = 0
  endSec = 10
  latencyFilterPopoverVisible = false
  statusFilteArr = [{name: 'el-icon-ksd-acclerate', value: 'FULLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ready', value: 'NEW'}]
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
  multipleSelection = []
  userGroups = []
  ruleVisible = false
  isEditRule = false
  isAutoMatic = false
  rules = [
    {name: 'Rule_setting_01', enabled: true, ruleId: '1'},
    {name: 'Rule_setting_02', enabled: false, ruleId: '2'},
    {name: 'Rule_setting_03', enabled: false, ruleId: '3'},
    {name: 'Rule_setting_04', enabled: true, ruleId: '4'}
  ]
  formRule = {
    name: '',
    conds: [
      {field: 'latency', op: 'GREATER', rightThreshold: null}
    ],
    enabled: false
  }
  options = {Latency: 'latency', Frequency: 'frequency', 'User/Group': 'user', 'SQL Content': 'sql'}

  @Watch('datetimerange')
  onDateRangeChange (val) {
    if (val) {
      this.filterData.startTimeFrom = new Date(val[0]).getTime()
      this.filterData.startTimeTo = new Date(val[1]).getTime()
      this.filterList()
    }
  }

  async loadAllRules () {
    const res = await this.getAllRules({project: this.currentSelectedProject})
    const data = await handleSuccessAsync(res)
    this.rules = data && data.rules
  }

  getAutoStatus () {
    this.getAutoMaticStatus({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.isAutoMatic = data
      })
    }, (res) => {
      handleError(res)
    })
  }

  created () {
    if (this.isCandidate) {
      this.loadAllRules()
      this.getAutoStatus()
    }
  }

  get ruleDiaglogTitle () {
    if (this.isEditRule) {
      return this.$t('editRule')
    } else {
      return this.$t('createRule')
    }
  }

  markFavorite () {
    this.$emit('markToFav')
  }

  onSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.filterList()
    }, 500)
  }

  filterList () {
    console.log(this.filterData)
    this.$emit('loadFilterList', this.filterData)
  }

  addCon () {
    const con = {field: '', op: '', rightThreshold: null}
    this.formRule.conds.push(con)
  }
  removeCon (index) {
    if (index === 0 && this.formRule.conds.length === 1) {
      return
    }
    this.formRule.conds.splice(index, 1)
  }
  fieldChanged (con) {
    if (con.field === 'latency' || con.field === 'frequency') {
      con.op = 'GREATER'
    } else if (con.field === 'user') {
      con.op = 'EQUAL'
    } else if (con.field === 'sql') {
      con.op = 'CONTAINS'
    }
  }

  editRule (ruleObj) {
    this.formRule = ruleObj
    this.ruleVisible = true
    this.isEditRule = true
  }

  submitRuleFrom () {
    this.$refs['formRule'].validate((valid) => {
      if (valid) {
        if (this.isEditRule) {
          this.updateRule({project: this.currentSelectedProject, rule: this.formRule}).then((res) => {
            handleSuccess(res, () => {
              this.resetToList()
              this.filterList()
              this.loadAllRules()
            })
          }, (res) => {
            handleError(res)
          })
        } else {
          this.saveRule({project: this.currentSelectedProject, rule: this.formRule}).then((res) => {
            handleSuccess(res, () => {
              this.resetToList()
              this.filterList()
              this.loadAllRules()
            })
          }, (res) => {
            handleError(res)
          })
        }
      }
    })
  }

  resetToList () {
    this.isEditRule = false
    this.ruleVisible = false
  }

  delRule (ruleId) {
    this.deleteRule({project: this.currentSelectedProject, uuid: ruleId}).then((res) => {
      handleSuccess(res, () => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.loadAllRules()
        this.filterList()
      })
    }, (res) => {
      handleError(res)
    })
  }

  toggleRule (ruleId) {
    this.enableRule({project: this.currentSelectedProject, uuid: ruleId}).then((res) => {
      handleSuccess(res, () => {
        this.filterList()
      })
    }, (res) => {
      handleError(res)
    })
  }

  handleCommand (command) {
    if (command === 'createRule') {
      this.ruleVisible = true
      this.formRule = {
        name: '',
        conds: [
          {field: 'latency', op: 'GREATER', rightThreshold: null}
        ],
        enabled: false
      }
    } else if (command === 'applyAll') {
      this.applyRule({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, () => {
          this.loadAllRules()
          this.filterList()
        })
      }, (res) => {
        handleError(res)
      })
    } else if (command === 'markAll') {
      this.automaticRule({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, () => {
          this.getAutoStatus()
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
  handleSelectionChange (rows) {
    this.$emit('selectionChanged', rows)
  }
  openAgg (queryHistory) {
    if (queryHistory.realization.length && queryHistory.realization.includes('Aggregate Index')) {
      this.$emit('openAgg', queryHistory)
    }
  }
  renderColumn (h) {
    if (this.filterData.startTimeFrom && this.filterData.startTimeTo) {
      const startTime = transToUtcTimeFormat(this.filterData.startTimeFrom)
      const endTime = transToUtcTimeFormat(this.filterData.startTimeTo)
      return (<span onClick={e => (e.stopPropagation())}>
        <span>{this.$t('kylinLang.query.startTimeFilter')}</span>
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
        <span>{this.$t('kylinLang.query.startTimeFilter')}</span>
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
    this.startSec = -1
    this.endSec = -1
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
        <span>{this.$t('kylinLang.query.latency')}</span>
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
        <span>{this.$t('kylinLang.query.latency')}</span>
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
      </span>)
    }
  }
  renderColumn3 (h) {
    let items = []
    for (let i = 0; i < this.realFilteArr.length; i++) {
      items.push(<el-checkbox label={this.realFilteArr[i].value} key={this.realFilteArr[i].value}>{this.realFilteArr[i].name}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.realization')}</span>
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
      items.push(<el-checkbox label={this.statusFilteArr[i].value} key={this.statusFilteArr[i].value}><i class={this.statusFilteArr[i].name}></i></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.common.status')}</span>
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
    .el-table__expanded-cell {
      padding: 20px;
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
        .smyles_editor_wrap {
          margin-left: 12px;
        }
        .label {
          font-weight: bold;
          display: inline-block;
          width: 125px;
          text-align: right;
        }
        .realization-detail {
          color: @link-color;
          cursor: pointer;
        }
      }
    }
    .searchInput {
      width: 400px;
    }
    .history-table {
      .el-date-editor {
        line-height: inherit;
        padding: 0;
        position: absolute;
        right: 10px;
      }
      .el-icon-ksd-filter {
        position: relative;
        top: 2px;
      }
      .status-icon {
        font-size: 20px;
        &.el-icon-ksd-acclerate {
          color: @normal-color-1;
        }
        &.el-icon-ksd-acclerate_portion,
        &.el-icon-ksd-acclerate_ongoing {
          color: @base-color;
        }
      }
    }
  }
  .fav-dropdown-item {
    overflow: hidden;
    i {
      margin-left: 5px;
      display: inline-block;
      position: relative;
      top: 10px;
    }
    &:hover {
      i {
        color: @text-normal-color;
        &:hover {
          color: @base-color;
        }
      }
    }
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
  .rule-block {
    width: 100%;
    height: 78vh;
    position: absolute;
    top: 57px;
    left: 0;
    z-index: 99;
    background: #fff;
    opacity: 0.85;
    box-shadow: 0 1px 3px rgba(0,0,0,.3);
  }
  .translate-right {
    -webkit-transform: translateX(600px);
    -moz-transform: translateX(600px);
    -ms-transform: translateX(600px);
    -o-transform: translateX(600px);
    transform: translateX(600px);
    animation: rotate 1s forwards;
  }
  @keyframes rotate {
    0% {
      opacity: 0;
    }
    100% {
      opacity: 1;
      transform: rotate(360deg);
    }
  }
  .transition-new {
    -webkit-transition: all 1s ease-in;
    -moz-transition: all 1s ease-in;
    -ms-transition: all 1s ease-in;
    -o-transition: all 1s ease-in;
    transition: all 1s ease-in;
  }
  .ruleDiaglog {
    width: 660px;
    height: 78vh;
    position: absolute;
    top: 57px;
    right: 0;
    z-index: 999;
    background-color: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,.3);
    hr {
      height: 1px;
      border: none;
      background-color: @line-border-color;
      margin-bottom: 30px;
    }
    .el-dialog__body {
      height: calc(~"78vh - 160px");
      overflow-y: scroll;
    }
    .con-form-item {
      margin-bottom: 20px;
    }
    .action-group {
      .el-button i {
        display: block;
      }
      .is-disabled {
        background-color: @grey-4;
        color: @line-border-color;
        .el-icon-ksd-minus {
          cursor: not-allowed;
        }
        :hover {
          background-color: @grey-4;
          color: @line-border-color;
        }
      }
    }
  }
</style>
