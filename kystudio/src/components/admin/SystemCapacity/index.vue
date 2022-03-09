<template>
  <div class="system-capacity">
    <p class="main-title">{{$t('mainTitle')}}
      <el-popover ref="systemCapacity" width="290" popper-class="system-capacity-popover" trigger="hover">
        <span v-if="$lang === 'en'">{{$t('capacityTip1')}}<a target="_blank" href="https://docs.kyligence.io/books/v4.5/en/Operation-and-Maintenance-Guide/license_capacity.en.html">{{$t('manual')}}</a>{{$t('capacityTip2')}}</span>
        <span v-else>{{$t('capacityTip1')}}<a target="_blank" href="https://docs.kyligence.io/books/v4.5/zh-cn/Operation-and-Maintenance-Guide/license_capacity.cn.html">{{$t('manual')}}</a></span>
      </el-popover>
      <i class="icon el-icon-ksd-what" v-popover:systemCapacity></i>
    </p>
    <p class="system-msg">
      <span>{{$t('usedData')}}：</span>
      <i class="icon el-icon-loading" v-if="systemCapacityInfo.isLoading"></i>
      <template v-else>
        <span :class="['used-data-number', getValueColor]">
          <span v-if="!systemCapacityInfo.isLoading && !systemCapacityInfo.fail">
            <template v-if="systemCapacityInfo.unlimited">
              {{systemCapacityInfo.current_capacity | dataSize}}
            </template>
            <template v-else>
              <span>{{getCapacityPrecent}}% ({{systemCapacityInfo.current_capacity | dataSize}}/{{systemCapacityInfo.capacity | dataSize}})</span>
            </template>
          </span>
        </span>
        <span>
          <span class="font-disabled" v-if="systemCapacityInfo.fail">
            {{$t('failApi')}}
            <el-tooltip :content="$t('refresh')" effect="dark" placement="top">
              <i class="icon el-icon-ksd-restart" @click="refreshSystemBuildJob"></i>
            </el-tooltip>
          </span>
          <!-- <span v-if="systemCapacityInfo.error_over_thirty_days">
            <el-tooltip :content="$t('failedTagTip')" effect="dark" placement="top">
              <el-tag class="over-thirty-days" size="mini" type="danger">{{$t('failApi')}}<i class="icon el-icon-ksd-what"></i></el-tag>
            </el-tooltip>
          </span> -->
          <el-tag size="mini" type="danger" v-if="systemCapacityInfo.capacity_status === 'OVERCAPACITY'">{{$t('excess')}}</el-tag>
        </span>
      </template>
      <span class="line">|</span>
      <span class="used-nodes">
        <span>{{$t('usedNodes')}}：</span>
        <i class="icon el-icon-loading" v-if="systemNodeInfo.isLoading"></i>
        <span v-else :class="['num', systemNodeInfo.node_status === 'OVERCAPACITY' && 'is-error']">
          <template v-if="!systemNodeInfo.isLoading && !systemNodeInfo.fail">
            <template v-if="systemNodeInfo.unlimited">
              {{systemNodeInfo.current_node}}
            </template>
            <span v-else>
              {{systemNodeInfo.current_node}}/{{systemNodeInfo.node}}
            </span>
          </template>
        </span>
        <span v-if="!systemNodeInfo.isLoading">
          <span class="font-disabled" v-if="systemNodeInfo.fail">{{$t('failApi')}}</span>
          <!-- <span><el-tooltip :content="$t('failedTagTip')" effect="dark" placement="top">
              <el-tag size="mini" type="danger">{{$t('failApi')}}<i class="icon el-icon-ksd-what"></i></el-tag>
            </el-tooltip></span> -->
          <el-tag size="mini" type="danger" v-if="systemNodeInfo.node_status === 'OVERCAPACITY'">{{$t('excess')}}</el-tag>
          <el-tooltip :content="$t('refresh')" effect="dark" placement="top">
            <i class="icon el-icon-ksd-restart" v-if="systemNodeInfo.fail" @click="refreshNodes"></i>
          </el-tooltip>
        </span>
      </span>
      <!-- <p class="alert-notice clearfix"><span>{{$t('alertNotice')}}</span><span :class="noticeType ? 'is-open' : 'is-error'">{{ noticeType ? $t('open') : $t('close') }}</span><i class="icon el-icon-ksd-table_edit" @click="changeNoitceType"></i></p> -->
    </p>
    <el-row :gutter="10" class="content">
      <el-col :span="16" class="data-map">
        <div class="col-board">
          <div class="col-title"><span>{{$t('usedDataline')}}</span>
            <el-tooltip :content="$t('usedDatalineTip')" effect="dark" placement="top">
              <i class="icon el-icon-ksd-what"></i>
            </el-tooltip>
            <span class="select-date-line">
              <el-select size="mini" v-model="selectedDataLine" @change="changeDataLine" popper-class="data-line-dropdown" :style="{'width': $lang === 'en' ? '115px' : '100px'}">
                <el-option v-for="(item, index) in dataOptions" :key="index" :value="item.value" :label="item.text">
                </el-option>
              </el-select>
            </span>
          </div>
          <div id="used-data-map" :class="[!lineOptions && 'hide-charts']"></div>
          <empty-data :showImage="false" v-show="!lineOptions" />
        </div>
      </el-col>
      <el-col class="node-table" :span="8">
        <div class="col-board">
          <div class="col-title"><span>{{$t('nodeList')}}</span></div>
          <div class="table-bg">
            <el-table
              ref="nodeListTable"
              :data="nodes"
              size="small"
              v-scroll-shadow
              class="node-list"
              :empty-text="$t('kylinLang.common.noData')"
            >
              <el-table-column prop="host" :label="$t('node')"></el-table-column>
              <el-table-column :label="$t('type')" width="100">
                <template slot-scope="scope">
                  <span>{{scope.row.mode === 'all' ? 'All' : $t(`kylinLang.common.${scope.row.mode}Node`)}}</span>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </div>
      </el-col>
    </el-row>
    <div class="project-data">
      <div class="col-title"><span>{{$t('projectDatas')}}</span></div>
      <div class="contain">
        <div class="data-tree-map">
          <div id="tree-map">
            <empty-data />
          </div>
        </div>
        <div class="project-usage-table">
          <el-input v-model="filterProject" prefix-icon="el-icon-search" v-global-key-event.enter.debounce="changeResult" :placeholder="$t('projectPlaceholder')" :clearable="true" size="small" @clear="changeResult"></el-input>
          <div class="list">
            <el-table
              :data="projectCapacity.list"
              size="small"
              v-scroll-shadow
              class="project-capacity"
              max-height="360"
              @sort-change="projectSortChange"
              :empty-text="$t('kylinLang.common.noData')"
            >
              <el-table-column prop="name" :label="$t('project')" align="left" show-overflow-tooltip></el-table-column>
              <el-table-column :render-header="renderUsageHeader" prop="capacity_ratio" sortable align="right" width="140">
                <template slot-scope="scope"><span v-if="!['ERROR', 'TENTATIVE'].includes(scope.row.status)">{{(scope.row.capacity_ratio * 100).toFixed(2)}}%</span><span class="font-disabled" v-else>{{$t('failApi')}}</span></template>
              </el-table-column>
              <el-table-column :label="$t('usedCapacity')" prop="capacity" sortable align="right" width="120">
                <template slot-scope="scope"><span v-if="!['ERROR', 'TENTATIVE'].includes(scope.row.status)">{{scope.row.capacity | dataSize}}</span><span class="font-disabled" v-else>{{$t('failApi')}}</span></template>
              </el-table-column>
              <el-table-column
                :label="$t('status')"
                prop="status"
                width="120"
                align='right'
                :filters="projectStatusFilter.map(item => ({text: $t(item), value: item}))"
                :filtered-value="projectCapacity.status"
                filter-icon="el-icon-ksd-filter"
                :show-multiple-footer="false"
                :filter-change="(v) => filterContent(v, 'project_status')"
              >
                <template slot-scope="scope">
                  {{scope.row.status === 'OK' ? $t('success') : $t('fail')}}
                </template>
              </el-table-column>
              <el-table-column :label="$t('tools')" align="left" class-name="tools-list" width="80">
                <template slot-scope="scope">
                  <common-tip :content="$t('viewDetail')">
                    <i :class="['sub-icon', 'el-icon-ksd-desc', {'is-disabled': ['ERROR'].includes(scope.row.status)}]" @click="!['ERROR'].includes(scope.row.status) && showDetail(scope.row)"></i>
                  </common-tip>
                  <common-tip :content="$t('kylinLang.common.refresh')" :disabled="scope.row.refresh">
                    <i :class="['sub-icon', scope.row.refresh ? 'el-icon-loading' : 'el-icon-ksd-restart']" v-if="['ERROR', 'TENTATIVE'].includes(scope.row.status)" @click="!scope.row.refresh && refreshProjectCapacity(scope.row)"></i>
                  </common-tip>
                </template>
              </el-table-column>
            </el-table>
          </div>
          <kap-pager class="ksd-center ksd-mtb-10" :perPageSize="projectCapacity.pageSize" :refTag="pageRefTags.capacityPager" layout="total, prev, pager, next, jumper" :curPage="projectCapacity.currentPage + 1" :totalSize="projectCapacity.totalSize"  @handleCurrentChange='pageCurrentChange'></kap-pager>
        </div>
      </div>
    </div>
    <el-dialog
      :title="$t('dataAlertTitle')"
      v-if="showAlert"
      :visible="true"
      width="400px"
      @close="showAlert = false"
      :close-on-click-modal="false"
      :is-dragable="true"
      custom-class="alert-dialog"
      ref="alert-msg-dialog">
      <div class="dialog-body">
        <p>
          <span>{{$t('noticeLabel')}}</span>
          <el-switch
            v-model="alertMsg.notify"
            active-text="OFF"
            inactive-text="ON">
          </el-switch>
        </p>
        <p :class="[!alertMsg.notify && 'is-disabled']">{{$t('alertCapacityText')}}</p>
        <el-form :model="alertMsg" ref="emailsValidate" size="small" @keydown.enter="() => { return false }">
          <el-form-item :rules="emailRules" prop="emails">
            <!-- <el-select
              v-model="alertMsg.emails"
              multiple
              filterable
              allow-create
              default-first-option
              popper-class="custom-email-content"
              class="change-input-emails"
              @change="changeEmails"
            ></el-select> -->
            <div :class="['input-emails', errorEmailTip && 'error-emails']" @click="alertMsg.notify && handlerClickEvent()">
              <div class="disable-mask" v-if="!alertMsg.notify"></div>
              <span>
                <el-tag size="small" closable v-for="(item, index) in alertMsg.emailTags" :key="index" @close.stop="removeCurrentTag(item)">{{ item }}</el-tag>
              </span>
              <el-input ref="emailInput" size="medium" v-if="showEmailInput" :clearable="false" v-model="alertMsg.emails" @focus="handleEventFocus" @blur="changeEmails"></el-input>
            </div>
          </el-form-item>
        </el-form>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="showAlert = false">{{$t('know')}}</el-button>
        <el-button type="primary" @click="submitEmailNotice" :disabled="!isChangeValue">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>
    <!-- project占比详情 -->
    <el-dialog
      :title="$t('projectDetailsTitle', {projectName: currentProjectName})"
      v-if="showProjectChargedDetails"
      :visible="true"
      width="720px"
      @close="showProjectChargedDetails = false"
      :close-on-click-modal="false"
      :is-dragable="false"
      custom-class="project-charged-details-dialog"
      ref="project-charged-details">
      <div class="dialog-body">
        <el-table
          :data="projectDetail.list"
          border
          v-scroll-shadow
          @sort-change="projectDetailsSortChange"
          class="charged-table"
          :empty-text="$t('kylinLang.common.noData')"
        >
          <el-table-column prop="name" :label="$t('tableName')" align="left" show-overflow-tooltip></el-table-column>
          <el-table-column :render-header="renderDetailUsageHeader" prop="capacity_ratio" sortable align="right">
            <template slot-scope="scope">
              <span class="font-disabled" v-if="scope.row.status === 'TENTATIVE' || scope.row.capacity === -1">{{$t('failApi')}}</span>
              <span v-else>{{(scope.row.capacity_ratio * 100).toFixed(2) + '%'}}</span>
            </template>
          </el-table-column>
          <el-table-column :label="$t('usedCapacity')" prop="capacity" sortable align="right">
            <template slot-scope="scope">
              <span class="font-disabled" v-if="scope.row.status === 'TENTATIVE' || scope.row.capacity === -1">{{$t('failApi')}}</span>
              <span v-else>{{scope.row.capacity | dataSize}}</span>
            </template>
          </el-table-column>
        </el-table>
        <kap-pager class="ksd-center ksd-mtb-10" :perPageSize="projectDetail.pageSize" :refTag="pageRefTags.projectDetail" layout="total, prev, pager, next, jumper" :curPage="projectDetail.currentPage + 1" :totalSize="projectDetail.totalSize"  @handleCurrentChange='pageCurrentChangeByDetail'></kap-pager>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" plain @click="showProjectChargedDetails = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import locales from './locales'
// import echartsLine from 'echarts/lib/chart/line'
import charts from 'util/charts'
import echarts from 'echarts'
import EmptyData from '../../common/EmptyData/EmptyData'
import { mapActions, mapState } from 'vuex'
import { transToUtcDateFormat } from '../../../util/business'
import filterElements from '../../../filter/index'
import { pageRefTags, pageCount } from 'config'

@Component({
  methods: {
    ...mapActions({
      getSystemCapacity: 'GET_SYSTEM_CAPACITY',
      getProjectDetails: 'GET_PROJECT_CAPACITY_DETAILS',
      getProjectCapacityList: 'GET_PROJECT_CAPACITY_LIST',
      refreshSingleProjectCapacity: 'REFRESH_SINGLE_PROJECT',
      refreshAll: 'REFRESH_ALL_SYSTEM',
      getNotifyStatus: 'GET_EMAIL_NOTIFY_STATUS',
      saveAlertEmails: 'SAVE_ALERT_EMAILS',
      getNodesInfo: 'GET_NODES_INFO'
    })
  },
  computed: {
    ...mapState({
      nodeList: state => state.capacity.nodeList,
      systemNodeInfo: state => state.capacity.systemNodeInfo,
      systemCapacityInfo: state => state.capacity.systemCapacityInfo
    })
  },
  locales,
  components: {
    EmptyData
  }
})
export default class SystemCapacity extends Vue {
  pageRefTags = pageRefTags
  alertMsg = {
    notify: false,
    emails: '',
    emailTags: []
  }
  oldAlert = {
    notify: false,
    emails: '',
    emailTags: []
  }
  errorEmailTip = false
  showEmailInput = false
  noticeType = false
  lineCharts = null
  lineOptions = null
  treeMapCharts = null
  filterProject = ''
  nodes = []
  projectStatusFilter = ['success', 'fail']
  projectCapacity = {
    list: [],
    pageSize: +localStorage.getItem(this.pageRefTags.capacityPager) || pageCount,
    currentPage: 0,
    totalSize: 10,
    sort_by: '',
    reverse: '',
    status: []
  }
  showAlert = false
  showProjectChargedDetails = false
  currentProjectName = ''
  projectDetail = {
    list: [],
    pageSize: +localStorage.getItem(this.pageRefTags.projectDetail) || pageCount,
    currentPage: 0,
    totalSize: 10,
    sort_by: '',
    reverse: ''
  }
  selectedDataLine = 'month'
  dalyTimer = null

  // @Watch('alertMsg.notify')
  // changeAlertEmail (newVal, oldVal) {
  //   if (newVal !== oldVal && !newVal) {
  //     this.errorEmailTip = false
  //     this.$refs.emailsValidate.resetFields()
  //   }
  // }

  @Watch('nodeList')
  changeNodes (newVal, oldVal) {
    if (newVal.length && !this.nodes.length) this.nodes = newVal
    this.renderNodeListTable()
  }

  created () {
    this.nodes = this.nodeList.length ? this.nodeList : []
    this.getProjectList()
    this.getTreeMapProjectList()
    // 获取邮箱提醒状态
    // this.getNotifyStatus().then(status => {
    //   this.noticeType = status
    // })
  }

  // 获取全量的项目占比绘制treeMap
  getTreeMapProjectList () {
    this.getProjectCapacityList({project_names: this.filterProject, page_offset: 0, page_size: 110, sort_by: this.projectCapacity.sort_by, reverse: this.projectCapacity.reverse}).then(data => {
      this.initTreeMapCharts(data.capacity_detail)
    })
  }

  renderNodeListTable () {
    this.$nextTick(() => {
      this.$refs.nodeListTable && this.$refs.nodeListTable.doLayout()
    })
  }

  // 获取项目占比
  getProjectList () {
    let status = [...this.projectCapacity.status]
    if (status.includes('fail')) {
      status.splice(status.indexOf('fail'), 1)
      status.push('ERROR', 'TENTATIVE')
    }
    if (status.includes('success')) {
      status.splice(status.indexOf('success'), 1)
      status.push('OK')
    }
    this.getProjectCapacityList({
      project_names: this.filterProject,
      page_offset: this.projectCapacity.currentPage,
      page_size: this.projectCapacity.pageSize,
      sort_by: this.projectCapacity.sort_by,
      reverse: this.projectCapacity.reverse,
      status: status.join(',')
    }).then(data => {
      this.projectCapacity = {...this.projectCapacity, totalSize: data.size, list: data.capacity_detail.map(it => ({...it, refresh: false}))}
    })
  }

  // 项目重排
  projectSortChange (sortBy) {
    this.projectCapacity = {...this.projectCapacity, ...{sort_by: sortBy.prop, reverse: sortBy.order === 'descending'}}
    this.getProjectList()
  }

  // 切换折线图数据范围
  changeDataLine () {
    this.getCapacityBySystem()
  }

  getCapacityBySystem () {
    this.getSystemCapacity({data_range: this.selectedDataLine}).then(data => {
      if (!Object.keys(data).length) {
        this.lineOptions = null
      } else {
        this.lineOptions = data
        this.initLineCharts()
      }
    })
  }

  get getCapacityPrecent () {
    return this.systemCapacityInfo.unlimited ? 0 : (((this.systemCapacityInfo.current_capacity / this.systemCapacityInfo.capacity) || 0) * 100.00).toFixed(2)
  }

  // 邮箱规则
  // get emailRules () {
  //   return [
  //     { validator: this.validateEmail, trigger: 'blur', required: true }
  //   ]
  // }

  // 折线图获取的数据集（当月、当季、当年）
  get dataOptions () {
    return [
      { text: this.$t('cMonth'), value: 'month' },
      { text: this.$t('cSeason'), value: 'quarter' }
    ]
  }

  // 判断数据量预警数值是否更改
  get isChangeValue () {
    return this.oldAlert.notify !== this.alertMsg.notify || this.oldAlert.emailTags.join(',') !== this.alertMsg.emailTags.join(',')
  }

  // 根据已使用的数据量判断显示的颜色
  get getValueColor () {
    const num = this.getCapacityPrecent
    if (num >= 80 && num <= 100) {
      return 'is-warn'
    } else if (num > 100) {
      return 'is-error'
    } else {
      return ''
    }
  }
  initLineCharts () {
    if (this.lineOptions) {
      const curDate = new Date().getTime()
      const objs1 = Object.keys(this.lineOptions).sort((a, b) => a - b)
      const xDates1 = objs1.map(it => (transToUtcDateFormat(+it)))
      let length = this.selectedDataLine === 'month' ? 30 : 90
      let newObj = {}
      for (let i = 0; i < length; i++) {
        let date = transToUtcDateFormat(curDate - 86400000 * i)
        if (xDates1.indexOf(date) === -1) {
          newObj[curDate - 86400000 * i] = 0
        }
      }
      let allObj = {...this.lineOptions, ...newObj}
      const objs = Object.keys(allObj).sort((a, b) => a - b)
      const xDates = objs.map(it => (transToUtcDateFormat(+it)))
      const yVol = objs.map(it => (+allObj[it] / 1024 / 1024 / 1024 / 1024).toFixed(2))
      this.lineCharts = echarts.init(document.getElementById('used-data-map'))
      this.lineCharts.setOption(charts.line(this, xDates, yVol, allObj))
    }
  }

  initTreeMapCharts (list) {
    if (!list.length) return
    const data = list.filter(item => item.status !== 'ERROR').map(it => ({name: it.name, value: it.capacity_ratio * 100, capacity: filterElements.dataSize(+it.capacity)}))
    const formatUtil = echarts.format
    this.treeMapCharts = echarts.init(this.$el.querySelector('#tree-map'))
    this.treeMapCharts.setOption(charts.treeMap(this, data, formatUtil))
    this.treeMapCharts.on('click', (params) => {
      this.filterProject = params.data.name
      this.getProjectList()
    })
  }

  // echart 随窗口变化
  resetChartsPosition () {
    this.lineCharts && this.lineCharts.resize()
    this.treeMapCharts && this.treeMapCharts.resize()
  }

  lisenceEvent () {
    window.addEventListener('resize', this.resetChartsPosition)
  }

  handleEventFocus () {
    window.addEventListener('keydown', this.delEmailEvent)
  }

  delEmailEvent (e) {
    if (!this.alertMsg.emails && e.key && e.key === 'Backspace' && this.alertMsg.emailTags.length) {
      this.alertMsg.emailTags.pop()
    }
  }

  changeResult () {
    this.getProjectList()
  }

  pageCurrentChange (page) {
    this.projectCapacity.currentPage = page
    this.getProjectList()
  }

  // table详情排序
  projectDetailsSortChange (sort) {
    this.projectDetail = {...this.projectDetail, ...{sort_by: sort.prop, reverse: sort.order === 'descending'}}
    this.getTableDetails()
  }

  // 切换项目详情页码
  pageCurrentChangeByDetail (val) {
    this.projectDetail.currentPage = val
    this.getTableDetails()
  }

  // changeNoitceType () {
  //   this.showAlert = true
  // }

  // async submitEmailNotice () {
  //   if (await this.$refs.emailsValidate.validate()) {
  //     // this.showAlert = false
  //     // todo 邮箱提醒接口
  //     this.saveAlertEmails({})
  //     this.noticeType = this.alertMsg.notify
  //     this.oldAlert = this.alertMsg
  //   }
  // }

  renderUsageHeader (h, { column, index }) {
    return <span class="usage-header">
      { this.$t('usage') }
      <el-tooltip content={ this.$t('usageTip') } effect="dark" placement="top">
        <span class="icon el-icon-ksd-what"></span>
      </el-tooltip>
    </span>
  }

  // 表占比tip
  renderDetailUsageHeader (h, { column, index }) {
    return <span class="usage-header">
      { this.$t('usage') }
      <el-tooltip content={ this.$t('usageTableTip') } effect="dark" placement="top">
        <span class="icon el-icon-ksd-what"></span>
      </el-tooltip>
    </span>
  }

  // 展示项目占比详情
  showDetail (row) {
    this.currentProjectName = row.name
    this.showProjectChargedDetails = true
    this.getTableDetails()
  }

  getTableDetails () {
    this.getProjectDetails({
      project: this.currentProjectName,
      page_offset: this.projectDetail.currentPage,
      page_size: this.projectDetail.pageSize,
      sort_by: this.projectDetail.sort_by,
      reverse: this.projectDetail.reverse
    }).then(data => {
      this.projectDetail = {...this.projectDetail, list: data.tables, totalSize: data.size}
    })
  }

  // 手动认证邮箱格式
  // validateEmail (rule, value, callback) {
  //   const emails = value.split(',')
  //   const reg = /^\w+((.\w+)|(-\w+))@[A-Za-z0-9]+((.|-)[A-Za-z0-9]+).[A-Za-z0-9]+$/
  //   if (!value && !this.alertMsg.emailTags.length) {
  //     this.errorEmailTip = true
  //     callback(new Error(this.$t('noEmail')))
  //   } else if (value && emails.length && emails.filter(it => !reg.test(it)).length) {
  //     this.errorEmailTip = true
  //     callback(new Error(this.$t('errorEmailFormat')))
  //   } else {
  //     this.errorEmailTip = false
  //     callback()
  //   }
  // }

  // 邮件预警
  // async changeEmails () {
  //   if (await this.$refs.emailsValidate.validate() && this.alertMsg.emails) {
  //     this.alertMsg.emailTags = [...this.alertMsg.emailTags, ...this.alertMsg.emails.split(',')]
  //     this.alertMsg.emails = ''
  //   }
  //   this.showEmailInput = false
  //   window.removeEventListener('keydown', this.delEmailEvent)
  // }

  // handlerClickEvent () {
  //   this.showEmailInput = true
  //   this.$nextTick(() => {
  //     this.$refs.emailInput.$el.querySelector('.el-input__inner').focus()
  //   })
  // }

  // 删除邮箱tag
  // removeCurrentTag (name) {
  //   const index = this.alertMsg.emailTags.findIndex(n => n === name)
  //   index >= 0 && this.alertMsg.emailTags.splice(index, 1)
  // }

  // 刷新单个项目占比及数据量
  refreshProjectCapacity (row) {
    row.refresh = true
    this.refreshSingleProjectCapacity({project: row.name}).then(() => {
      row.refresh = false
      this.getProjectList()
    }).catch(() => {
      row.refresh = false
    })
  }

  // 刷新构建 重新获取系统容量
  refreshSystemBuildJob () {
    this.refreshAll()
  }

  // 手动刷新节点信息
  refreshNodes () {
    this.getNodesInfo()
  }

  // 过滤项目或表的状态
  filterContent (v, type) {
    if (type === 'project_status') {
      this.projectCapacity.status = v
      this.getProjectList()
    } else if (type === 'project_detail_status') {
      this.projectDetail.status = v
      this.getTableDetails()
    }
  }

  mounted () {
    this.getCapacityBySystem()
    // this.initCharts()
    // this.initTreeMapCharts()
    this.lisenceEvent()
    this.renderNodeListTable()
  }

  beforeDestroy () {
    window.removeEventListener('resize', this.resetChartsPosition)
    window.removeEventListener('keydown', this.delEmailEvent)
  }
}
</script>

<style scoped lang="less">
@import '../../../assets/styles/variables.less';
.system-capacity {
  padding: 20px 15px;
  box-sizing: border-box;
  background: @base-background-color-1;
  .main-title {
    color: @text-title-color;
    font-weight: bold;
    font-size: @text-title-size;
  }
  .font-disabled {
    color: @text-disabled-color;
  }
  .icon {
    margin-left: 2px;
    color: @text-disabled-color;
    cursor: pointer;
  }
  .el-tag.el-tag--danger {
    .icon {
      color: @error-color-1;
    }
  }
  .alert-notice {
    float: right;
    margin-top: -17px;
    font-size: @text-assist-size;
    .is-open {
      color: @normal-color-1;
    }
    .icon {
      margin-left: 5px;
      cursor: pointer;
      &:hover {
        color: @base-color;
      }
    }
  }
  .system-msg {
    margin-top: 10px;
    font-size: @text-assist-size;
    .used-data-number {
      color: @text-title-color;
      font-weight: bold;
      .is-danger {
        color: @error-color-1;
      }
    }
    .over-thirty-days {
      cursor: pointer;
    }
    .line {
      margin: 0 15px;
      color: @line-border-color3;
    }
    .used-nodes {
      .num {
        color: @text-title-color;
        font-weight: bold;
      }
    }
    .el-icon-ksd-restart {
      color: @base-color;
    }
  }
  .col-board {
    width: 100%;
    height: 100%;
    border-radius:1px 1px 0px 0px;
    // border:1px solid @line-border-color3;
    background: @fff;
    // background:rgba(255,255,255,1);
    box-shadow:0px 0px 5px 0px rgba(240,240,240,1);
  }
  .col-title {
    height: 36px;
    // background: @table-stripe-color;
    line-height: 50px;
    font-size: @text-subtitle-size;
    color: @text-title-color;
    padding: 0 15px;
    font-weight: bold;
  }
  .content {
    margin-top: 15px;
    .hide-charts {
      opacity: 0;
    }
    .data-map {
      height: 320px;
      position: relative;
      #used-data-map {
        width: 100%;
        height: calc(~'100% - 36px');
      }
      .select-date-line {
        position: absolute;
        right: 17px;
        // .el-select {
        //   width: 72px;
        // }
      }
    }
    .node-table {
      height: 320px;
      position: relative;
      .table-bg {
        width: 100%;
        height: calc(~'100% - 36px');
        padding: 10px 10px 15px 10px;
        box-sizing: border-box;
        overflow: auto;
      }
    }
  }
  .project-data {
    height: 500px;
    margin-top: 10px;
    border-radius:2px;
    // border:1px solid @line-border-color3;
    box-shadow:0px 0px 5px 0px rgba(240,240,240,1);
    background: @fff;
    position: relative;
    // overflow: auto;
    .contain {
      height: calc(~'100% - 40px');
      display: flex;
      padding: 10px 15px;
      box-sizing: border-box;
      .data-tree-map {
        flex: 1;
        height: calc(~'100% - 30px');
        position: relative;
      }
      .project-usage-table {
        width: 50%;
        text-align: right;
        margin-left: 10px;
        .el-input {
          width: 220px;
          position: absolute;
          top: 10px;
          right: 15px;
        }
        .list {
          // margin-top: 10px;
        }
      }
      #tree-map {
        height: 100%;
      }
    }
    .sub-icon {
      cursor: pointer;
      &.is-disabled {
        color: @text-disabled-color;
        cursor: not-allowed;
      }
    }
  }
  .is-warn {
    color: @warning-color-1 !important;
  }
  .is-error {
    color: @error-color-1 !important;
  }
  .alert-dialog {
    .is-disabled {
      color: @text-disabled-color;
    }
    .input-emails {
      border: 1px solid @line-border-color3;
      padding: 0 10px;
      position: relative;
      min-height: 32px;
      .el-input {
        display: inline-block;
        max-width: 100%;
        width: inherit;
      }
      .el-tag {
        margin-right: 5px;
      }
      &.error-emails {
        border: 1px solid @error-color-1;
      }
      .el-input--medium {
        // width: inherit;
        // max-width: 100%;
      }
      .disable-mask {
        width: 100%;
        height: 100%;
        position: absolute;
        top: 0;
        left: 0;
        background: #f5f5f5;
        cursor: not-allowed;
        z-index: 10;
        opacity: .5;
      }
    }
  }
}
// .alert-dialog {
//   /deep/.el-input__inner {
//     padding-right: 10px;
//   }
//   /deep/.el-input__suffix {
//     display: none;
//   }
// }
// .change-input-emails /deep/ .el-input__suffix {
//   display: none;
// }
</style>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .project-capacity .el-table__header {
    .el-table__column-filter-trigger i {
      font-size: 14px;
    }
  }
  .input-emails {
    .el-input__inner {
      border: none;
      padding: 0;
    }
  }
  .project-data {
    .empty-data.empty-data-normal {
      img {
        height: 80px
      }
    }
  }
  .change-input-emails {
    width: 100%;
    .el-input__suffix {
      display: none;
    }
    .el-input__inner {
      cursor: text;
      padding-right: 10px;
    }
  }
  .custom-email-content {
    display: none;
  }
  .data-line-dropdown {
    .el-select-dropdown__item {
      font-size: 12px;
    }
  }
  .tools-list {
    .tip_box {
      margin-left: 5px;
      &:first-child {
        margin-left: inherit;
      }
    }
  }
  .usage-header {
    .icon {
      margin-left: 5px;
    }
  }
</style>
