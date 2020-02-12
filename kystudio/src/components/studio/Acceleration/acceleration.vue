<template>
  <div id="favoriteQuery">
    <div class="ksd-title-label ksd-mtb-20">
      <span>{{$t('acceleration')}}</span>
    </div>
    <div class="btn-groups ky-no-br-space ksd-mb-15" v-if="datasourceActions.includes('accelerationActions')">
      <span class="guide-checkData" v-if="!waitingSQLSize"></span>
      <el-button type="primary" v-guide.speedSqlNowBtn plain :disabled="!waitingSQLSize" :loading="isAcceSubmit" @click="applySpeed">{{$t('accelerateNow')}}</el-button>
      <el-button type="primary" plain @click="openImportSql">{{$t('importSql')}}</el-button>
    </div>
    <div class="img-groups" v-guide.speedProcess>
      <div class="label-groups">
        <span>{{$t('kylinLang.query.canBeAcce')}}: {{waitingSQLSize}}</span>
        <span v-if="showGif" class="ongoing-label">{{$t('kylinLang.query.ongoingAcce')}}</span>
        <div class="pattern-num">
          <p v-if="listSizes">{{listSizes.accelerated}}</p>
          <p class="">{{$t('acceleratedSQL')}}</p>
        </div>
      </div>
      <div v-if="showGif" class="img-block">
        <img src="../../../assets/img/merge1.gif" width="735px" alt=""><img src="../../../assets/img/acc_light.png" class="ksd-ml-10" width="85px" alt="">
      </div>
      <div v-else class="img-block">
        <img src="../../../assets/img/bg1.jpg" width="735px" alt=""><img src="../../../assets/img/acc_light.png" class="ksd-ml-10" width="85px" alt="">
      </div>
    </div>
    <div class="fav-tables ksd-mt-40">
      <div class="btn-group" v-if="datasourceActions.includes('accelerationActions')">
        <!-- <el-button size="small" type="primary" icon="el-icon-ksd-setting" plain @click="openRuleSetting">{{$t('ruleSetting')}}</el-button><el-button size="small" type="primary" icon="el-icon-ksd-table_discard" plain @click="openBlackList">{{$t('blackList')}}</el-button> -->
        <el-dropdown @command="handleCommand">
          <el-button size="small" type="primary" plain class="el-dropdown-link">
            {{$t('more')}}<i class="el-icon-arrow-down el-icon--right"></i>
          </el-button>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-if="datasourceActions.includes('acceRuleSettingActions')" command="ruleSetting">{{$t('ruleSetting')}}</el-dropdown-item>
            <el-dropdown-item command="blackList">{{$t('blackList')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </div>
      <el-tabs v-model="activeList" type="card" @tab-click="handleClick">
        <el-tab-pane name="waiting">
          <span slot="label">{{$t('waitingList', {num: listSizes.waiting})}}</span>
          <acceleration_table
            v-if="activeList=='waiting'"
            :favoriteTableData="favQueList.value"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="waiting"></acceleration_table>
          <kap-pager ref="favoriteQueryPager" v-if="activeList=='waiting'" class="ksd-center ksd-mtb-10" :curPage="filterData.offset+1" :totalSize="favQueList.total_size" v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="not_accelerated">
          <span slot="label">{{$t('not_accelerated', {num: listSizes.not_accelerated})}}</span>
          <acceleration_table
            v-if="activeList=='not_accelerated'"
            :favoriteTableData="favQueList.value"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="not_accelerated"></acceleration_table>
          <kap-pager ref="favoriteQueryPager2" v-if="activeList=='not_accelerated'" class="ksd-center ksd-mtb-10" :curPage="filterData.offset+1" :totalSize="favQueList.total_size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="accelerated">
          <span slot="label">{{$t('accelerated',  {num: listSizes.accelerated})}}</span>
          <acceleration_table
            v-if="activeList=='accelerated'"
            :favoriteTableData="favQueList.value"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="accelerated"></acceleration_table>
          <kap-pager ref="favoriteQueryPager3" v-if="activeList=='accelerated'" class="ksd-center ksd-mtb-10" :curPage="filterData.offset+1" :totalSize="favQueList.total_size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
      </el-tabs>
    </div>
    <el-dialog
      top="5vh"
      :visible.sync="blackListVisible"
      width="960px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="resetBlack"
      class="blackListDialog">
      <span slot="title" class="ky-list-title">{{$t('blackList')}}
        <el-tooltip placement="left">
          <div slot="content">{{$t('blackListDesc')}}</div>
          <i class="el-icon-ksd-what ksd-fs-16"></i>
        </el-tooltip>
      </span>
      <el-row :gutter="15">
        <el-col :span="16">
          <div class="clearfix ksd-mb-10">
            <span class="ksd-title-label ksd-fs-14 query-count">{{$t('blackList1', {size: blackSqlData.total_size})}}
            </span>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="blackSqlFilter" v-global-key-event.enter.debounce="onblackSqlFilterChange" @clear="onblackSqlFilterChange()" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table :data="blackSqlData.value" border :empty-text="emptyText" @row-click="viewBlackSql" :row-class-name="tableRowClassName" class="import-table" style="width: 100%">
            <el-table-column prop="sql_pattern" label="SQL" :resizable="false">
              <template slot-scope="props">
                <span class="ksd-nobr-text" style="width: 289px;">{{props.row.sql_pattern}}</span>
              </template>
            </el-table-column>
            <el-table-column prop="create_time" :label="$t('createdTime')" show-overflow-tooltip width="218">
              <template slot-scope="props">
                {{transToGmtTime(props.row.create_time)}}
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" width="80">
              <template slot-scope="props">
                <common-tip :content="$t('kylinLang.common.drop')">
                  <i class="el-icon-ksd-table_delete" @click.stop="delBlack(props.row.id)"></i>
                </common-tip>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-10" :curPage="blackFilter.offset+1" :totalSize="blackSqlData.total_size" layout="total, prev, pager, next, jumper" v-on:handleCurrentChange='blackSqlDatasPageChange' :perPageSize="10" v-if="blackSqlData.total_size"></kap-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-12 ksd-fs-14">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10" v-loading="sqlLoading" element-loading-spinner="el-icon-loading">
            <kap-editor
              :key="'editor_' + inputHeight"
              ref="blackInputBox"
              :height="inputHeight"
              :tipsHeight="sqlTipsHeight"
              :dragable="false"
              :readOnly="true"
              lang="sql"
              :needFormater="true"
              :isAbridge="true"
              theme="chrome"
              v-model="blackSql">
            </kap-editor>
          </div>
        </el-col>
      </el-row>
      <span slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="blackListVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      :visible.sync="ruleSettingVisible"
      width="720px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :title="$t('ruleSetting')"
      class="ruleSettingDialog">
      <el-form ref="rulesForm" :rules="rulesSettingRules" :show-message="false" :model="rulesObj" size="medium">
        <div class="conds">
          <div class="conds-title">
            <span>{{$t('queryFrequency')}}</span>
            <el-switch size="small" v-model="rulesObj.count_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-mt-10 ksd-fs-14">
              <el-form-item prop="count_value">
                <span>{{$t('AccQueryStart')}}</span>
                <el-input-number :min="1" :max="1000" v-model.trim="rulesObj.count_value" v-number="rulesObj.count_value" size="small" class="rule-setting-input count-input" :disabled="!rulesObj.count_enable" :controls="false"></el-input-number> 
                <span>{{$t('AccQueryEnd')}}</span>
              </el-form-item>
            </div>
          </div>
        </div>

        <div class="conds">
          <div class="conds-title">
            <span>{{$t('querySubmitter')}}</span>
            <el-switch size="small" v-model="rulesObj.submitter_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content">
            <div class="vip-users-block">
              <el-form-item prop="users">
                <div class="ksd-mt-10 conds-title"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
                <el-select v-model="rulesObj.users" v-event-stop :popper-append-to-body="false" filterable size="medium" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                  <span slot="prefix" class="el-input__icon el-icon-search" v-if="!rulesObj.users.length"></span>
                  <el-option v-for="item in allSubmittersOptions.user" :key="item" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
              <el-form-item prop="user_groups">
              <div class="ksd-mt-10 conds-title"><i class="el-icon-ksd-table_group"></i> VIP Group</div>
              <el-select v-model="rulesObj.user_groups" v-event-stop :popper-append-to-body="false" filterable size="medium" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                <span slot="prefix" class="el-input__icon el-icon-search" v-if="!rulesObj.user_groups.length"></span>
                <el-option v-for="item in allSubmittersOptions.group" :key="item" :label="item" :value="item"></el-option>
              </el-select>
              </el-form-item>
            </div>
          </div>
        </div>
        <el-form-item prop="latency">
        <div class="conds">
          <div class="conds-title">
            <span>{{$t('queryDuration')}}</span>
            <el-switch size="small" v-model="rulesObj.duration_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-mt-10 ksd-fs-14">
              {{$t('from')}}
              <el-form-item prop="min_duration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.min_duration" v-number="rulesObj.min_duration" size="small" class="rule-setting-input" :disabled="!rulesObj.duration_enable"></el-input>
              </el-form-item>
              {{$t('to')}}
              <el-form-item prop="max_duration" style="display: inline-block;">
              <el-input v-model.trim="rulesObj.max_duration" v-number="rulesObj.max_duration" size="small" class="rule-setting-input" :disabled="!rulesObj.duration_enable"></el-input>
              </el-form-item>
              {{$t('secondes')}}
            </div>
          </div>
        </div>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="cancelRuleSetting" size="medium">{{$t('kylinLang.common.cancel')}}</el-button><el-button
        @click="saveRuleSetting" size="medium" :loading="updateLoading">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>

    <UploadSqlModel v-on:reloadListAndSize="reloadListAndSize" />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapMutations, mapGetters, mapState } from 'vuex'
import $ from 'jquery'
import { handleSuccessAsync, handleError, objectClone } from '../../../util/index'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../../util/business'
import { sqlRowsLimit, sqlStrLenLimit } from '../../../config/index'
import accelerationTable from './acceleration_table'
import UploadSqlModel from '../../common/UploadSql/UploadSql.vue'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      getRules: 'GET_RULES',
      formatSql: 'FORMAT_SQL',
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      updateRules: 'UPDATE_RULES',
      loadBlackList: 'LOAD_BLACK_LIST',
      deleteBlack: 'DELETE_BLACK_SQL',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      getWaitingAcceSize: 'GET_WAITING_ACCE_SIZE'
    }),
    ...mapMutations({
      lockSpeedInfo: 'LOCK_SPEED_INFO'
    }),
    ...mapActions('UploadSqlModel', {
      showUploadSqlDialog: 'CALL_MODAL'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions',
      'isAutoProject'
    ]),
    ...mapState({
      isSemiAutomatic: state => state.project.isSemiAutomatic
    })
  },
  components: {
    'acceleration_table': accelerationTable,
    'UploadSqlModel': UploadSqlModel
  },
  locales: {
    'en': {
      more: 'More',
      acceleration: 'Acceleration',
      importSql: 'Import SQL',
      createdTime: 'Create Time',
      sqlBox: 'SQL Box',
      blackList: 'Black List',
      blackList1: 'Black List ({size})',
      ruleSetting: 'Acceleration Rule',
      favoriteRules: 'Favorite Rule',
      favRulesDesc: 'By filtering SQL\'s frequency, duration and submitter, favorite rule will catch up frequently used and business critical queries.',
      queryFrequency: 'Frequency Rule',
      querySubmitter: 'User Rule',
      queryDuration: 'Latency Rule',
      frequencyDesc: 'Optimize queries frequently used over last 24 hours',
      submitterDesc: 'Optimize queries from critical users and groups',
      durationDesc: 'Optimize queries with long duration',
      blackListDesc: 'Black list helps to manage SQLs which are undesired for accelerating, especially for those SQLs will require unreasonable large storage or computing resource to accelerate.',
      AccQueryStart: 'Accelerate queries whose usage is higher than ',
      AccQueryEnd: ' times',
      unit: 'Seconds / Job',
      inputSql: 'Add SQL',
      delSql: 'Are you sure to delete this sql?',
      delSqlTitle: 'Delete SQL',
      giveUpEdit: 'Are you sure to give up the edit?',
      thereAre: 'There are {waitingSQLSize} SQL(s) waiting for acceleration on the threshold of <span class="highlight">{threshold}</span>.',
      accelerateNow: 'Accelerate Now',
      from: 'Accelerate the queries whose latency range is between',
      to: 'second(s) to',
      secondes: 'second(s)',
      acceleratedSQL: 'Accelerated SQL',
      waitingList: 'Waiting List ({num})',
      not_accelerated: 'Not Accelerated ({num})',
      accelerated: 'Accelerated ({num})'
    },
    'zh-cn': {
      more: '更多',
      acceleration: '加速引擎',
      importSql: '导入 SQL 文件',
      createdTime: '创建时间',
      sqlBox: 'SQL 窗口',
      blackList: '禁用名单',
      blackList1: '禁用名单 ({size})',
      ruleSetting: '加速规则',
      favoriteRules: '加速规则',
      queryFrequency: '查询频率',
      querySubmitter: '查询用户',
      queryDuration: '查询延迟',
      frequencyDesc: '优化过去 24 小时内查询频率较高的查询',
      submitterDesc: '优化重要⽤用户或⽤用户组发出的查询',
      durationDesc: '优化慢查询',
      blackListDesc: '本列表管理用户不希望被加速的 SQL 查询。一般是指加速时对存储空间、计算力需求过大的查询。',
      AccQueryStart: '加速使用次数大于',
      AccQueryEnd: '的查询',
      unit: '秒 / 任务',
      inputSql: '新增查询语句',
      delSql: '确定删除这条查询语句吗？',
      delSqlTitle: '删除查询语句',
      giveUpEdit: '确定放弃本次编辑吗？',
      thereAre: '已有 {waitingSQLSize} 条 SQL 查询等待加速(阈值为 <span class="highlight">{threshold}</span> 条 SQL)',
      accelerateNow: '立即加速',
      from: '加速延迟的范围在',
      to: '秒到',
      secondes: '秒的查询',
      acceleratedSQL: '已加速 SQL',
      waitingList: '未加速 ({num})',
      not_accelerated: '加速失败 ({num})',
      accelerated: '加速完毕 ({num})'
    }
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  checkedStatus = []
  activeList = 'waiting'
  statusMap = {
    'waiting': ['TO_BE_ACCELERATED', 'ACCELERATING'],
    'not_accelerated': ['PENDING', 'FAILED'],
    'accelerated': ['ACCELERATED']
  }
  showGif = false
  listSizes = {waiting: 0, not_accelerated: 0, accelerated: 0}
  waitingSQLSize = 0
  isAcceSubmit = false
  importSqlVisible = false
  ruleSettingVisible = false
  blackListVisible = false
  inputHeight = 422
  sqlTipsHeight = this.$store.state.system.lang === 'en' ? 40 : 50
  isEditSql = false
  blackSqlFilter = ''
  sqlLoading = false
  activeSqlObj = null
  sqlFormatterObj = {}
  blackSqlData = {total_size: 0, value: []}
  blackSql = ''
  isReadOnly = true
  initTimer = null
  activeNames = ['rules']
  filterData = {
    sortBy: 'last_query_time',
    reverse: true,
    pageSize: 10,
    offset: 0
  }
  blackFilter = {
    offset: 0,
    pageSize: 10
  }
  updateLoading = false
  stCycle = null
  isPausePolling = false
  rulesObj = {
    count_enable: true,
    count_value: 0,
    submitter_enable: true,
    users: [],
    user_groups: [],
    duration_enable: false,
    min_duration: 0,
    max_duration: 0
  }
  allSubmittersOptions = {
    user: [],
    group: []
  }
  rulesSettingRules = {
    count_value: [{validator: this.validatePass, trigger: 'blur'}],
    min_duration: [{validator: this.validatePass, trigger: 'blur'}],
    max_duration: [{validator: this.validatePass, trigger: 'blur'}]
  }
  get emptyText () {
    return this.blackSqlFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  validatePass (rule, value, callback) {
    if ((!value && value !== 0) && (rule.field === 'count_value' && this.rulesObj.count_enable || rule.field.indexOf('duration') !== -1 && this.rulesObj.duration_enable)) {
      callback(new Error(null))
    } else {
      callback()
    }
  }
  tableRowClassName ({row, rowIndex}) {
    if (this.activeSqlObj && row.id === this.activeSqlObj.id) {
      return 'active-row'
    }
    return ''
  }
  handleClick () {
    this.checkedStatus = []
    this.filterData = {
      sortBy: 'last_query_time',
      reverse: true,
      pageSize: 10,
      offset: 0
    }
    this.reCallPolling()
    this.pageCurrentChange(0, 10)
  }
  handleCommand (command) {
    if (command === 'ruleSetting') {
      this.openRuleSetting()
    } else if (command === 'blackList') {
      this.openBlackList()
    }
  }
  applySpeed (event) {
    this.isAcceSubmit = true
    this.lockSpeedInfo({isLock: true})
    this.applySpeedInfo({size: this.waitingSQLSize, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.isAcceSubmit = false
        this.flyEvent(event)
        this.getSQLSizes()
        this.showGif = true
        this.lockSpeedInfo({isLock: false})
      })
    }, (res) => {
      handleError(res)
      this.isAcceSubmit = false
    })
  }
  flyEvent (event) {
    let navOpen = (element) => {
      let targetDom = ''
      let rotateIcon = false
      const dom = {
        monitor: {parent: $('#monitor').find('.menu-icon'), children: $('#monitorJobs')},
        studio: {parent: $('#studio').find('.menu-icon'), children: $('#studioModel')}
      }
      document.querySelector(element).classList.value.indexOf('is-opened') >= 0 ? (targetDom = dom[element.replace(/^[.|#]/g, '')].children) : (targetDom = dom[element.replace(/^[.|#]/g, '')].parent, rotateIcon = true)
      return { targetDom, offset: targetDom.offset(), rotateIcon, flyer: true }
    }
    const currentPattern = !this.isAutoProject && this.isSemiAutomatic ? navOpen('#studio') : navOpen('#monitor')
    const flyer = $('<span class="fly-box"></span>')
    let leftOffset = 80
    if (this.$lang === 'en') {
      leftOffset = 74
    }
    if (this.briefMenuGet) {
      leftOffset = 20
    }
    flyer.fly({
      start: {
        left: event.pageX,
        top: event.pageY
      },
      end: {
        left: currentPattern.offset.left + leftOffset,
        top: currentPattern.offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        if (currentPattern.rotateIcon && currentPattern.flyer) {
          currentPattern.targetDom.addClass('rotateY')
          setTimeout(() => {
            currentPattern.targetDom.fadeTo('slow', 0.5, function () {
              currentPattern.targetDom.removeClass('rotateY')
              currentPattern.targetDom.fadeTo('fast', 1)
            })
            flyer.fadeOut(1500, () => {
              flyer.remove()
            })
          }, 3000)
        } else {
          setTimeout(() => {
            flyer.fadeOut(1500, () => {
              flyer.remove()
            })
          }, 3000)
        }
      }
    })
  }

  sortFavoriteList (filterData) {
    this.filterData.reverse = filterData.reverse
    this.filterData.sortBy = filterData.sortBy
    this.pageCurrentChange(0, this.filterData.pageSize)
  }

  filterFav (checkedStatus) {
    this.checkedStatus = checkedStatus
    this.pageCurrentChange(0, this.filterData.pageSize)
    this.getSQLSizes()
  }

  get modelSpeedEvents () {
    return this.$store.state.model.modelSpeedEvents
  }
  @Watch('modelSpeedEvents')
  onSpeedEventsChange (val) {
    if (val) {
      clearTimeout(this.initTimer)
      this.initTimer = setTimeout(() => {
        this.init()
      }, 1000)
    }
  }

  openRuleSetting () {
    if (this.currentSelectedProject) {
      const loadRulesData = new Promise((resolve, reject) => {
        this.getRules({project: this.currentSelectedProject}).then((res) => {
          handleSuccess(res, (data) => {
            this.rulesObj = data
            // 换字段名了，也不用处理 百分比的切换了
            // this.rulesObj.freqValue = this.rulesObj.freqValue * 100
            resolve()
          })
        }, (res) => {
          handleError(res)
          reject()
        })
      })
      const loadAllSubmittersData = new Promise((resolve, reject) => {
        this.getUserAndGroups().then((res) => {
          handleSuccess(res, (data) => {
            this.allSubmittersOptions = data
            resolve()
          }, (res) => {
            handleError(res)
            reject()
          })
        })
      })
      Promise.all([loadRulesData, loadAllSubmittersData]).then((res) => {
        this.ruleSettingVisible = true
      }, (res) => {
        this.ruleSettingVisible = false
        handleError(res)
      })
    }
  }

  cancelRuleSetting () {
    this.ruleSettingVisible = false
  }

  saveRuleSetting () {
    this.$refs['rulesForm'].validate((valid) => {
      if (valid) {
        this.updateLoading = true
        const submitData = objectClone(this.rulesObj)
        // 换成次数字段了，不需要除于 100
        // submitData.freqValue = submitData.freqValue / 100
        this.updateRules({ ...submitData, ...{project: this.currentSelectedProject} }).then((res) => {
          handleSuccess(res, (data) => {
            this.updateLoading = false
            this.ruleSettingVisible = false
          })
        }, (res) => {
          handleError(res)
          this.updateLoading = false
          this.ruleSettingVisible = false
        })
      }
    })
  }
  async loadFavoriteList () {
    return new Promise(async resolve => {
      const res = await this.getFavoriteList({
        project: this.currentSelectedProject || null,
        limit: this.filterData.pageSize,
        offset: this.filterData.offset,
        status: !this.checkedStatus.length ? this.statusMap[this.activeList] : this.checkedStatus,
        sort_by: this.filterData.sortBy,
        reverse: this.filterData.reverse
      })
      const data = await handleSuccessAsync(res)
      this.favQueList = data
      resolve()
    })
  }

  openImportSql () {
    this.showUploadSqlDialog({})
  }

  async getSQLSizes () {
    return new Promise(async resolve => {
      if (this.currentSelectedProject) {
        const res = await this.getWaitingAcceSize({project: this.currentSelectedProject})
        const data = await handleSuccessAsync(res)
        this.listSizes = data
        this.waitingSQLSize = data.can_be_accelerated
      }
      resolve()
    })
  }
  refreshLists () {
    if (!this.isPausePolling) {
      return Promise.all([this.loadFavoriteList(), this.getSQLSizes()])
    } else {
      return new Promise((resolve) => {
        resolve()
      })
    }
  }
  pausePolling () {
    this.isPausePolling = true
  }
  reloadListAndSize () {
    this.loadFavoriteList()
    this.getSQLSizes()
  }
  reCallPolling () {
    this.isPausePolling = false
  }
  async init () {
    clearTimeout(this.stCycle)
    this.stCycle = setTimeout(() => {
      this.refreshLists().then((res) => {
        handleSuccess(res, (data) => {
          if (this._isDestroyed) {
            return
          }
          this.init()
        })
      }, (res) => {
        handleError(res)
      })
    }, 5000)
  }

  created () {
    if (this.currentSelectedProject) {
      this.loadFavoriteList()
      this.getSQLSizes()
      this.init()
    }
  }
  destroyed () {
    clearTimeout(this.stCycle)
  }
  mounted () {
    this.$nextTick(() => {
      $('#favo-menu-item').removeClass('rotateY').css('opacity', 0)
    })
  }

  pageCurrentChange (offset, pageSize) {
    this.filterData.offset = offset
    this.filterData.pageSize = pageSize
    this.loadFavoriteList()
  }

  openBlackList () {
    this.blackListVisible = true
    this.inputHeight = 422
    this.blackSql = ''
    this.isEditSql = false
    this.getBlackList()
  }

  async getBlackList () {
    const res = await this.loadBlackList({
      project: this.currentSelectedProject,
      limit: this.blackFilter.pageSize,
      offset: this.blackFilter.offset,
      sql: this.blackSqlFilter
    })
    const data = await handleSuccessAsync(res)
    if (data && data.total_size > 0) {
      this.blackSqlData = data
      this.$nextTick(() => {
        this.viewBlackSql(this.blackSqlData.value[0])
      })
    } else {
      this.blackSqlData = {total_size: 0, value: []}
      this.blackSql = ''
      this.activeSqlObj = null
    }
  }
  showLoading () {
    this.sqlLoading = true
  }
  hideLoading () {
    this.sqlLoading = false
  }
  resetBlack () {
    this.sqlFormatterObj = {}
  }

  /* async viewBlackSql (row) {
    this.showLoading()
    this.activeSqlObj = row
    let formatterSql
    if (this.blackSqlData[row.id]) {
      formatterSql = this.sqlFormatterObj[row.id]
      this.$refs.blackInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({sqls: [row.sql_pattern]})
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[row.id] = formatterSql
      this.$refs.blackInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
  } */
  viewBlackSql (row) {
    let showLimitTip = false
    let sqlTextArr = row.sql_pattern.split('\n')
    // 要手动传入高度
    if ((sqlTextArr.length > 0 && sqlTextArr.length > sqlRowsLimit) || (sqlTextArr.length === 0 && row.sql_pattern.length > sqlStrLenLimit)) {
      showLimitTip = true
    }
    this.inputHeight = showLimitTip ? (424 - this.sqlTipsHeight) : 422
    this.$nextTick(() => {
      if (this.$refs && this.$refs['blackInputBox']) {
        this.$refs.blackInputBox.$emit('input', row.sql_pattern)
        this.$refs.blackInputBox.editorResize()
      }
    })
  }

  delBlack (id) {
    kapConfirm(this.$t('delSql'), null, this.$t('delSqlTitle')).then(() => {
      this.deleteBlack({id: id, project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
        })
        this.getBlackList()
      }, (res) => {
        handleError(res)
      })
    })
  }

  onblackSqlFilterChange () {
    this.blackFilter.offset = 0
    this.getBlackList()
  }

  blackSqlDatasPageChange (offset, pageSize) {
    this.blackFilter.offset = offset
    this.blackFilter.pageSize = pageSize
    this.getBlackList()
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  #favoriteQuery {
    padding: 0px 20px 50px 20px;
    .batch-btn-groups {
      display: flex;
      > .el-button {
        z-index: 1;
      }
      .ksd-btn-groups {
        margin-left: -1px;
      }
    }
    .slide-enter-active, .slide-leave-active {
      transition: transform .1s linear;
      transform: translateX(0px);
    }
    .slide-enter, .slide-leave-to {
      transform: translateX(-200px);
    }
    .img-block {
      height: 46px;
    }
    .table-title {
      color: @text-title-color;
      font-size: 16px;
      line-height: 32px;
    }
    .highlight {
      color: @base-color;
    }
    .fav-tables {
      position: relative;
      .btn-group {
        position: absolute;
        right: 0;
        top: 2px;
        z-index: 9;
      }
    }
    .img-groups {
      margin: 0 auto;
      width: 850px;
      .label-groups {
        position: relative;
        margin-bottom: 5px;
        > span {
          display: inline-block;
          width: 245px;
          text-align: center;
          line-height: 18px;
          font-weight: @font-medium;
        }
        .pattern-num {
          position: absolute;
          right: 12px;
          top: 18px;
          display: inline-block;
          width: 98px;
          text-align: center;
          line-height: 18px;
          p {
            font-size: 18px;
            font-weight: @font-medium;
            &:last-child {
              font-size: 12px;
            }
          }
        }
      }
      .btn-groups {
        .btn-block {
          display: inline-block;
          width: 245px;
          text-align: center;
        }
      }
    }
    .ruleSettingDialog {
      .el-dialog__body {
        color: @text-title-color;
      }
      .conds-title {
        font-weight: @font-medium;
      }
      .conds > .conds-title {
        height: 18px;
        line-height: 18px;
        display: flex;
        align-items: flex-end;
        .el-switch--small {
          margin-left: 10px;
        }
      }
      .el-form-item--medium .el-form-item__content, .el-form-item--medium .el-form-item__label {
        line-height: 1;
      }
      .el-form-item {
        margin-bottom: 0;
      }
      .conds:not(:last-child) {
        margin-bottom: 15px;
        padding-bottom: 15px;
        border-bottom: 1px solid @line-split-color;
      }
    }
    .rule-setting-input {
      display: inline-block;
      width: 60px;
      &.count-input{
        width: 80px;
        &.el-input-number.is-without-controls .el-input__inner{
          text-align: left;
        }
      }
    }
    .favorite-table {
      .impored-row {
        background: @warning-color-2;
      }
      .status-icon {
        font-size: 20px;
        position: relative;
        top: 3px;
        &.el-icon-ksd-to_accelerated,
        &.el-icon-ksd-acclerating {
          color: @base-color;
        }
        &.el-icon-ksd-acclerated {
          color: @normal-color-1;
        }
        &.el-icon-ksd-negative {
          color: @warning-color-1;
        }
        &.el-icon-ksd-negative.failed {
          color: @error-color-1;
        }
      }
      .el-icon-ksd-table_delete,
      .el-icon-ksd-table_discard {
        &:hover {
          color: @base-color;
        }
      }
    }
    .fav-dropdown {
      .el-icon-ksd-table_setting {
        color: inherit;
      }
    }
  }
  .el-message-box__status.el-icon-info.primary {
    color: @base-color;
  }
  .importSqlDialog,
  .blackListDialog {
    .ksd-null-pic-text {
      margin: 122.5px 0;
    }
    .query-count {
      color: @text-title-color;
      line-height: 30px;
      height: 30px;
      display: inline-block;
    }
    .tips {
      span {
        margin-left: 5px;
        color: @text-normal-color;
      }
      i {
        color: @text-disabled-color;
      }
    }
    .el-icon-ksd-good_health {
      color: @normal-color-1;
      margin-right: 2px;
      font-size: 14px;
    }
    .el-icon-ksd-error_01 {
      color: @error-color-1;
      margin-right: 2px;
      font-size: 14px;
    }
    .el-dialog__body {
      // min-height: 460px;
      height: 460px;
      .model-table {
        .rename-error {
          color: @error-color-1;
          font-size: 12px;
          line-height: 1.2;
        }
        .name-error {
          .el-input__inner {
            border-color: @error-color-1;
          }
        }
      }
      .import-table {
        .cell {
          height: 23px;
        }
        .active-row {
          background-color: @base-color-9;
        }
        .el-table__row {
          cursor: pointer;
        }
        .el-icon-ksd-table_edit,
        .el-icon-ksd-table_delete {
          &:hover {
            color: @base-color;
          }
        }
      }
      .new_sql_status {
        border-color: @base-color;
        .ace-chrome {
          border-color: @base-color;
          border-bottom-color: @text-secondary-color;
        }
      }
      .operatorBox{
        margin-top: 0;
        padding: 10px;
        display: block;
        overflow: hidden;
      }
      .error_messages {
        height: 130px;
        border: 1px solid @line-border-color;
        border-radius: 2px;
        font-size: 12px;
        margin-top: 10px;
        padding: 10px;
        box-sizing: border-box;
        overflow-y: auto;
        .label {
          color: @error-color-1;
        }
      }
      .smyles_editor_wrap .smyles_dragbar {
        height: 0;
      }
      .upload-block {
        text-align: center;
        margin: 0 auto;
        img {
          margin-top: 100px;
        }
        .text {
          font-size: 14px;
          color: @text-title-color;
          line-height: 24px;
        }
        .el-upload {
          margin-top: 25px;
        }
        .el-upload-list {
          width: 300px;
          margin: 0 auto;
          text-align: left;
          .el-upload-list__item {
            .el-upload-list__item-name {
              margin-right: 20px;
              &:hover {
                text-decoration: none;
                color: inherit;
              }
            }
            &:hover {
              background-color: inherit;
            }
          }
        }
      }
    }
    .filter-tags {
      margin-bottom: 10px;
      padding: 6px 10px;
      box-sizing: border-box;
      position: relative;
      background: @background-disabled-color;
      .filter-tags-layout {
        width: calc(~'100% - 100px');
        display: inline-block;
        line-height: 34px;
      }
      .el-tag {
        margin-left: 5px;
        &:first-child {
          margin-left: 0;
        }
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
  }
  .cell.highlight {
    .el-icon-ksd-filter {
      color: @base-color;
    }
  }
  .el-icon-ksd-filter {
    position: relative;
    font-size: 17px !important;
    top: 2px;
    left: 5px;
    &:hover,
    &.filter-open {
      color: @base-color;
    }
  }
  .el-table-filter__content {
    .filter-icon {
      margin-right: 5px;
    }
  }
</style>
