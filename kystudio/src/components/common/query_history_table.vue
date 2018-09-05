<template>
  <div id="queryHistoryTable">
    <div class="clearfix ksd-mb-16">
      <div class="btn-group ksd-fleft" v-if="isCandidate">
        <el-button type="primary" plain size="medium" icon="el-icon-ksd-mark_favorite">Mark Favorite</el-button>
      </div>
      <div class="ksd-fright ksd-inline searchInput ksd-ml-10">
        <el-input v-model="inputHasVal" prefix-icon="el-icon-search" placeholder="请输入内容" size="medium"></el-input>
      </div>
      <el-dropdown v-if="isCandidate" @command="handleCommand" class="fav-dropdown ksd-fright" trigger="click">
        <el-button size="medium" icon="el-icon-ksd-table_setting" plain type="primary">{{$t('kylinLang.query.applyRule')}}</el-button>
        <el-dropdown-menu slot="dropdown">
          <el-dropdown-item v-for="item in rules" :key="item.ruleId" class="fav-dropdown-item">
            <el-checkbox v-model="item.enabled" v-event-stop:click>{{item.name}}</el-checkbox>
            <i class="el-icon-ksd-table_edit" v-event-stop:click></i>
            <i class="el-icon-ksd-table_delete"></i>
          </el-dropdown-item>
          <el-dropdown-item divided command="createRule">{{$t('createRule')}}</el-dropdown-item>
          <el-dropdown-item divided command="applyAll">{{$t('applyAll')}}</el-dropdown-item>
          <el-dropdown-item command="markAll">{{$t('markAll')}}</el-dropdown-item>
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
                  <span>{{props.row.queryId}}</span>
                </div>
                <div>
                  <span class="label">Duration:</span>
                  <span>{{props.row.duration}}</span>
                </div>
                <div>
                  <span class="label">Query Countent:</span>
                  <kap_editor height="130" width="80%" lang="sql" theme="chrome" v-model="props.row.queryContent" dragbar="#393e53">
                  </kap_editor>
                </div>
              </el-col>
              <el-col :span="10">
                <div>
                  <span class="label">Model Name:</span>
                  <span>{{props.row.modelName}}</span>
                </div>
                <div>
                  <span class="label">Realization:</span>
                  <span class="realization-detail" @click="openAgg">{{props.row.realization}}</span>
                </div>
                <div>
                  <span class="label">Content:</span>
                  <span>{{props.row.content}}</span>
                </div>
                <div>
                  <span class="label">Total Scan Count:</span>
                  <span>{{props.row.totalScanCount}}</span>
                </div>
                <div>
                  <span class="label">Total Scan Bytes:</span>
                  <span>{{props.row.totalScanBytes}}</span>
                </div>
                <br/>
                <div>
                  <span class="label">Result Row Count:</span>
                  <span>{{props.row.resultRowCount}}</span>
                </div>
                <div>
                  <span class="label">If Hit Cache:</span>
                  <span>{{props.row.ifHitCache}}</span>
                </div>
              </el-col>
            </el-row>
          </div>
        </template>
      </el-table-column>
      <el-table-column type="selection" width="55" align="center" v-if="isCandidate"></el-table-column>
      <el-table-column :renderHeader="renderColumn" sortable prop="startTime" header-align="center" width="160">
        <template slot-scope="props">
          {{props.row.startTime | gmtTime}}
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn2" sortable prop="duration" header-align="center" align="right" width="150">
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent')" prop="queryContent" header-align="center">
      </el-table-column>
      <el-table-column :renderHeader="renderColumn3" prop="realization" header-align="center" width="250">
      </el-table-column>
      <el-table-column :renderHeader="renderColumn4" prop="ip" header-align="center" width="150">
      </el-table-column>
      <el-table-column :renderHeader="renderColumn5" prop="status" align="center" width="100" v-if="!isCandidate">
        <template slot-scope="props">
          <i class="status-icon" :class="{
            'el-icon-ksd-acclerate': props.row.status === 'speed',
            'el-icon-ksd-acclerate_portion': props.row.status === 'partSpeed',
            'el-icon-ksd-acclerate_ready': props.row.status === 'unSpeed',
            'el-icon-ksd-acclerate_ongoing': props.row.status === 'speeding'
          }"></i>
        </template>
      </el-table-column>
    </el-table>
    <pager ref="queryHistoryPager" class="ksd-center" :totalSize="queryHistoryData.length"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
    <div class="rule-block" v-if="ruleVisible"></div>
    <div class="ruleDiaglog translate-right transition-new" v-if="ruleVisible">
      <div class="el-dialog__header">
        <span class="el-dialog__title">{{$t('createRule')}}</span>
      </div>
      <div class="el-dialog__body">
        <el-form label-position="top" size="medium" :model="formRule">
          <el-form-item :label="$t('ruleName')">
            <el-input v-model.trim="formRule.name"></el-input>
          </el-form-item>
          <hr></hr>
          <div class="ksd-mb-16">
            <span>When a new SQL query that meets all these conditions : </span>
            <i class="el-icon-ksd-what"></i>
          </div>
          <el-form-item v-for="(con, index) in formRule.conditions" :key="index" class="con-form-item">
            <el-row :gutter="10">
              <el-col :span="6">
                <el-select v-model="con.name" placeholder="请选择">
                  <el-option v-for="item in options" :key="item" :label="item" :value="item">
                  </el-option>
                </el-select>
              </el-col>
              <el-col :span="3">
                <el-select v-model="con.mark" placeholder="请选择">
                  <el-option v-for="item in markOptions" :key="item" :label="item" :value="item">
                  </el-option>
                </el-select>
              </el-col>
              <el-col :span="5">
                <el-input v-model.trim="con.value" placeholder="Number"></el-input>
              </el-col>
              <el-col :span="4">
                <span v-if="con.name=='Latency'">Times</span>
                <el-select v-model="con.unit" placeholder="请选择" v-else>
                  <el-option label="Days" value="Days">
                  </el-option>
                </el-select>
              </el-col>
              <el-col :span="6">
                <div class="action-group ksd-fright">
                  <el-button type="primary" icon="el-icon-ksd-add" plain circle size="medium" @click="addCon" v-if="index==0"></el-button>
                  <el-button icon="el-icon-ksd-minus" plain circle size="medium" :disabled="formRule.conditions.length == 1" @click="removeCon(index)"></el-button>
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
          <el-button size="medium" type="primary" plain @click="ruleVisible = false">{{$t('kylinLang.common.save')}}</el-button>
        </span>
      </div>
    </div>
  </div>
</template>

<script>
import { transToUtcTimeFormat } from '../../util/business'
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
@Component({
  props: ['queryHistoryData', 'isCandidate'],
  methods: {
    ...mapActions({})
  },
  locales: {
    'en': {createRule: 'Create Rule', applyAll: 'Apply All', markAll: 'Mark Favorite Automatically', ruleName: 'Rule Name'},
    'zh-cn': {createRule: '新建规则', applyAll: '全部应用', markAll: '全部标记为待加速', ruleName: '规则名称'}
  }
})
export default class QueryHistoryTable extends Vue {
  inputHasVal = ''
  datetimerange = ''
  startSec = 0
  endSec = 10
  latencyFilterPopoverVisible = false
  realFilteArr = [{name: 'Pushdown to Hive', value: 'pushdown1'}, {name: 'Pushdown to Greenplum', value: 'pushdown2'}, {name: 'Model Name', value: 'modelName'}]
  ipFilteArr= ['101.1.1.181']
  statusFilteArr = [{speed: 'el-icon-ksd-acclerate'}, {unSpeed: 'el-icon-ksd-acclerate_ready'}, {partSpeed: 'el-icon-ksd-acclerate_portion'}, {speeding: 'el-icon-ksd-acclerate_ongoing'}]
  filterData = {
    startTime: null,
    endTime: null,
    startSec: -1,
    endSec: -1,
    checkedRealization: [],
    checkedIP: [],
    checkedStatus: []
  }
  multipleSelection = []
  ruleVisible = false
  rules = [
    {name: 'Rule_setting_01', enabled: true, ruleId: ''},
    {name: 'Rule_setting_02', enabled: false, ruleId: ''},
    {name: 'Rule_setting_03', enabled: false, ruleId: ''},
    {name: 'Rule_setting_04', enabled: true, ruleId: ''}
  ]
  formRule = {
    name: '',
    conditions: [
      {name: 'Latency', mark: '>', value: null, unit: 'Times'}
    ],
    enabled: false
  }
  options = ['Latency', 'Last Period']
  markOptions = ['>', 'is']

  @Watch('datetimerange')
  onDateRangeChange (val) {
    if (val) {
      this.filterData.startTime = new Date(val[0]).getTime()
      this.filterData.endTime = new Date(val[1]).getTime()
    }
  }

  addCon () {
    const con = {name: '', mark: '', value: null, unit: ''}
    this.formRule.conditions.push(con)
  }
  removeCon (index) {
    if (index === 0 && this.formRule.conditions.length === 1) {
      return
    }
    this.formRule.conditions.splice(index, 1)
  }
  handleCommand (command) {
    if (command === 'createRule') {
      this.ruleVisible = true
    }
  }
  pageCurrentChange () {}
  handleSelectionChange (val) {
    this.multipleSelection = val
  }
  openAgg () {
    this.$emit('openAgg')
  }
  renderColumn (h) {
    if (this.filterData.startTime && this.filterData.endTime) {
      const startTime = transToUtcTimeFormat(this.filterData.startTime)
      const endTime = transToUtcTimeFormat(this.filterData.endTime)
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
    this.filterData.startSec = this.startSec
    this.filterData.endSec = this.endSec
    this.latencyFilterPopoverVisible = false
  }
  renderColumn2 (h) {
    if (this.filterData.startSec >= 0 && this.filterData.endSec >= 0) {
      return (<span>
        <span>{this.$t('kylinLang.query.latency')}</span>
        <el-tooltip placement="top" class="ksd-fright">
          <div slot="content">
            <span>
              <i class='el-icon-time'></i>
              <span> {this.filterData.startSec}s To {this.filterData.endSec}s</span>
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
      items.push(<el-checkbox label={this.realFilteArr[i].name} key={this.realFilteArr[i].value}>{this.realFilteArr[i].name}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.realization')}</span>
      <el-popover
        ref="realFilterPopover"
        placement="bottom"
        width="200">
        <el-checkbox-group class="filter-groups" value={this.filterData.checkedRealization} onInput={val => (this.filterData.checkedRealization = val)}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn4 (h) {
    let items = []
    for (let i = 0; i < this.ipFilteArr.length; i++) {
      items.push(<el-checkbox label={this.ipFilteArr[i]} key={this.ipFilteArr[i]}>{this.ipFilteArr[i]}</el-checkbox>)
    }
    return (<span>
      <span>IP</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover"
        width="100">
        <el-checkbox-group class="filter-groups" value={this.filterData.checkedIP} onInput={val => (this.filterData.checkedIP = val)}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn5 (h) {
    let items = []
    for (let i = 0; i < this.statusFilteArr.length; i++) {
      const keyName = Object.keys(this.statusFilteArr[i])[0]
      const labelClass = this.statusFilteArr[i][keyName]
      items.push(<el-checkbox key={keyName}><slot><i class={labelClass}></i></slot></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.common.status')}</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.filterData.checkedStatus} onInput={val => (this.filterData.checkedStatus = val)}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
}
// export default {
//   name: 'QueryHistoryTable',
//   props: ['queryHistoryData', 'isCandidate'],
//   data () {
//     return {
//       inputHasVal: '',
//       datetimerange: '',
//       startSec: 0,
//       endSec: 10,
//       latencyFilterPopoverVisible: false,
//       realFilteArr: [{name: 'Pushdown to Hive', value: 'pushdown1'}, {name: 'Pushdown to Greenplum', value: 'pushdown2'}, {name: 'Model Name', value: 'modelName'}],
//       ipFilteArr: ['101.1.1.181'],
//       statusFilteArr: [{speed: 'el-icon-ksd-acclerate'}, {unSpeed: 'el-icon-ksd-acclerate_ready'}, {partSpeed: 'el-icon-ksd-acclerate_portion'}, {speeding: 'el-icon-ksd-acclerate_ongoing'}],
//       filterData: {
//         startTime: null,
//         endTime: null,
//         startSec: -1,
//         endSec: -1,
//         checkedRealization: [],
//         checkedIP: [],
//         checkedStatus: []
//       },
//       multipleSelection: []
//     }
//   },
//   locales: {
//     'en': {createRule: 'Create Rule', applyAll: 'Apply All', markAll: 'Mark Favorite Automatically', ruleName: 'Rule Name'},
//     'zh-cn': {createRule: '新建规则', applyAll: '全部应用', markAll: '全部标记为待加速', ruleName: '规则名称'}
//   },
//   watch: {
//     datetimerange (val) {
//       if (val) {
//         this.filterData.startTime = new Date(val[0]).getTime()
//         this.filterData.endTime = new Date(val[1]).getTime()
//       }
//     }
//   },
//   methods: {
//     pageCurrentChange () {},
//     handleSelectionChange (val) {
//       this.multipleSelection = val
//     },
//     openAgg () {
//       this.$emit('openAgg')
//     },
//     renderColumn (h) {
//       if (this.filterData.startTime && this.filterData.endTime) {
//         const startTime = transToUtcTimeFormat(this.filterData.startTime)
//         const endTime = transToUtcTimeFormat(this.filterData.endTime)
//         return (<span onClick={e => (e.stopPropagation())}>
//           <span>{this.$t('kylinLang.query.startTimeFilter')}</span>
//           <el-tooltip placement="top" class="ksd-fright">
//             <div slot="content">
//               <span>
//                 <i class='el-icon-time'></i>
//                 <span> {startTime} To {endTime}</span>
//               </span>
//             </div>
//             <el-date-picker
//               value={this.datetimerange}
//               onInput={val => (this.datetimerange = val)}
//               type="datetimerange"
//               popper-class="table-filter-datepicker"
//               toggle-icon="el-icon-ksd-data_range"
//               is-only-icon={true}>
//             </el-date-picker>
//           </el-tooltip>
//         </span>)
//       } else {
//         return (<span onClick={e => (e.stopPropagation())}>
//           <span>{this.$t('kylinLang.query.startTimeFilter')}</span>
//           <el-date-picker
//             value={this.datetimerange}
//             onInput={val => (this.datetimerange = val)}
//             popper-class="table-filter-datepicker"
//             type="datetimerange"
//             toggle-icon="el-icon-ksd-data_range"
//             is-only-icon={true}>
//           </el-date-picker>
//         </span>)
//       }
//     },
//     resetLatency () {
//       this.startSec = -1
//       this.endSec = -1
//     },
//     saveLatencyRange () {
//       this.filterData.startSec = this.startSec
//       this.filterData.endSec = this.endSec
//       this.latencyFilterPopoverVisible = false
//     },
//     renderColumn2 (h) {
//       if (this.filterData.startSec >= 0 && this.filterData.endSec >= 0) {
//         return (<span>
//           <span>{this.$t('kylinLang.query.latency')}</span>
//           <el-tooltip placement="top" class="ksd-fright">
//             <div slot="content">
//               <span>
//                 <i class='el-icon-time'></i>
//                 <span> {this.filterData.startSec}s To {this.filterData.endSec}s</span>
//               </span>
//             </div>
//             <el-popover
//               ref="latencyFilterPopover"
//               placement="bottom"
//               width="320"
//               value={this.latencyFilterPopoverVisible}
//               onInput={val => (this.latencyFilterPopoverVisible = val)}>
//               <div class="latency-filter-pop">
//                 <el-input-number
//                   size="medium"
//                   value={this.startSec}
//                   onInput={val1 => (this.startSec = val1)}></el-input-number>
//                 <span>&nbsp;S&nbsp;&nbsp;To</span>
//                 <el-input-number
//                   size="medium"
//                   value={this.endSec}
//                   onInput={val2 => (this.endSec = val2)}></el-input-number>
//                 <span>&nbsp;S</span>
//               </div>
//               <div class="latency-filter-footer">
//                 <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
//                 <el-button type="primary" onClick={this.saveLatencyRange} plain size="small">{this.$t('kylinLang.common.save')}</el-button>
//               </div>
//               <i class="el-icon-ksd-data_range" onClick={e => (e.stopPropagation())} slot="reference"></i>
//             </el-popover>
//           </el-tooltip>
//         </span>)
//       } else {
//         return (<span>
//           <span>{this.$t('kylinLang.query.latency')}</span>
//           <el-popover
//             ref="latencyFilterPopover"
//             placement="bottom"
//             width="320"
//             class="ksd-fright"
//             value={this.latencyFilterPopoverVisible}
//             onInput={val => (this.latencyFilterPopoverVisible = val)}>
//             <div class="latency-filter-pop">
//               <el-input-number
//                 size="medium"
//                 value={this.startSec}
//                 onInput={val1 => (this.startSec = val1)}></el-input-number>
//               <span>&nbsp;S&nbsp;&nbsp;To</span>
//               <el-input-number
//                 size="medium"
//                 value={this.endSec}
//                 onInput={val2 => (this.endSec = val2)}></el-input-number>
//               <span>&nbsp;S</span>
//             </div>
//             <div class="latency-filter-footer">
//               <el-button size="small" onClick={this.resetLatency}>{this.$t('kylinLang.query.clear')}</el-button>
//               <el-button type="primary" onClick={this.saveLatencyRange} plain size="small">{this.$t('kylinLang.common.save')}</el-button>
//             </div>
//             <i class="el-icon-ksd-data_range" onClick={e => (e.stopPropagation())} slot="reference"></i>
//           </el-popover>
//         </span>)
//       }
//     },
//     renderColumn3 (h) {
//       let items = []
//       for (let i = 0; i < this.realFilteArr.length; i++) {
//         items.push(<el-checkbox label={this.realFilteArr[i].name} key={this.realFilteArr[i].value}>{this.realFilteArr[i].name}</el-checkbox>)
//       }
//       return (<span>
//         <span>{this.$t('kylinLang.query.realization')}</span>
//         <el-popover
//           ref="realFilterPopover"
//           placement="bottom"
//           width="200">
//           <el-checkbox-group class="filter-groups" value={this.filterData.checkedRealization} onInput={val => (this.filterData.checkedRealization = val)}>
//             {items}
//           </el-checkbox-group>
//           <i class="el-icon-ksd-filter" slot="reference"></i>
//         </el-popover>
//       </span>)
//     },
//     renderColumn4 (h) {
//       let items = []
//       for (let i = 0; i < this.ipFilteArr.length; i++) {
//         items.push(<el-checkbox label={this.ipFilteArr[i]} key={this.ipFilteArr[i]}>{this.ipFilteArr[i]}</el-checkbox>)
//       }
//       return (<span>
//         <span>IP</span>
//         <el-popover
//           ref="ipFilterPopover"
//           placement="bottom"
//           popperClass="filter-popover"
//           width="100">
//           <el-checkbox-group class="filter-groups" value={this.filterData.checkedIP} onInput={val => (this.filterData.checkedIP = val)}>
//             {items}
//           </el-checkbox-group>
//           <i class="el-icon-ksd-filter" slot="reference"></i>
//         </el-popover>
//       </span>)
//     },
//     renderColumn5 (h) {
//       let items = []
//       for (let i = 0; i < this.statusFilteArr.length; i++) {
//         const keyName = Object.keys(this.statusFilteArr[i])[0]
//         const labelClass = this.statusFilteArr[i][keyName]
//         items.push(<el-checkbox key={keyName}><slot><i class={labelClass}></i></slot></el-checkbox>)
//       }
//       return (<span>
//         <span>{this.$t('kylinLang.common.status')}</span>
//         <el-popover
//           ref="ipFilterPopover"
//           placement="bottom"
//           popperClass="filter-popover">
//           <el-checkbox-group class="filter-groups" value={this.filterData.checkedStatus} onInput={val => (this.filterData.checkedStatus = val)}>
//             {items}
//           </el-checkbox-group>
//           <i class="el-icon-ksd-filter" slot="reference"></i>
//         </el-popover>
//       </span>)
//     }
//   }
// }
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
    height: 710px;
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
    height: 710px;
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
      height: 560px;
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
