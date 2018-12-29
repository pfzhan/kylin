<template>
  <div id="favoriteQuery">
    <div class="ksd-title-label ksd-mt-10 ksd-mb-10">
      <span>{{$t('kylinLang.menu.favorite_query')}}</span>
      <el-tooltip placement="right">
        <div slot="content" v-html="$t('favDesc')"></div>
        <i class="el-icon-ksd-what ksd-fs-14"></i>
      </el-tooltip>
    </div>
    <div class="img-groups">
      <div class="label-groups">
        <span>{{$t('kylinLang.query.wartingAcce')}} {{modelSpeedEvents}}</span>
        <span v-if="showGif" class="ongoing-label">{{$t('kylinLang.query.ongoingAcce')}}</span>
        <div class="pattern-num">
          <p>{{patternNum}}</p>
          <p class="">{{$t('acceleratedSQL')}}</p>
        </div>
      </div>
      <div v-if="!showGif">
        <img src="../../assets/img/acc_01.gif" width="245px" alt=""><img src="../../assets/img/acc_02.gif" width="245px" alt=""><img src="../../assets/img/acc_03.gif" width="245px" height="48px" alt=""><img src="../../assets/img/acc_light.png" width="85px" alt="">
      </div>
      <div v-else>
        <img src="../../assets/img/acc_01.jpg" width="245px" alt=""><img src="../../assets/img/acc_02.jpg" width="245px" alt=""><img src="../../assets/img/acc_03.jpg" width="245px" height="48px" alt=""><img src="../../assets/img/acc_light.png" width="85px" alt="">
      </div>
      <div class="btn-groups ksd-mt-10">
        <el-button size="mini" type="primary" plain @click="openImportSql">{{$t('importSql')}}</el-button>
        <el-button size="mini" type="primary" plain :disabled="!modelSpeedEvents" @click="applySpeed">{{$t('accelerateNow')}}</el-button>
      </div>
    </div>
    <div class="fav-tables">
      <div class="btn-group">
        <el-button size="mini" type="primary" icon="el-icon-ksd-setting" plain @click="openRuleSetting">{{$t('ruleSetting')}}</el-button>
        <el-button size="mini" type="primary" icon="el-icon-ksd-table_discard" plain @click="openBlackList">{{$t('blackList')}}</el-button>
      </div>
      <el-tabs v-model="activeList" @tab-click="handleClick">
        <el-tab-pane name="wartingAcce">
          <span slot="label">{{$t('kylinLang.query.unAcce1')}}({{unAcceListSize}})</span>
          <favorite_table :favoriteTableData="favQueList.favorite_queries" :sortTable="sortFavoriteList" :filterFav="filterFav"></favorite_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="accelerated">
          <span slot="label">{{$t('kylinLang.query.fullyAcce')}}({{patternNum}})</span>
          <favorite_table :favoriteTableData="favQueList.favorite_queries" :sortTable="sortFavoriteList" :filterFav="filterFav" :isAccelerated="true"></favorite_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
      </el-tabs>
    </div>
    <el-dialog
      :visible.sync="importSqlVisible"
      top="5vh"
      width="1180px"
      @closed="resetImport"
      class="importSqlDialog">
      <span slot="title" class="ky-list-title">{{$t('importSql')}}</span>
      <div class="upload-block" v-if="!isUploaded">
        <img src="../../assets/img/license.png" alt="">
        <div class="ksd-mt-10 text">{{$t('pleImport')}}</div>
        <el-upload
          :headers="uploadHeader"
          :action="actionUrl"
          :data="uploadData"
          ref="sqlUpload"
          :before-upload="beforeUpload"
          multiple :auto-upload="true"
          :on-success="uploadSuccess"
          :on-error="uploadError">
          <el-button type="primary" size="medium" :loading="importLoading">{{$t('sqlFiles')}}
          </el-button>
        </el-upload>
      </div>
      <el-row :gutter="20" v-else>
        <el-col :span="16">
          <div class="clearfix ksd-mb-10">
            <div class="ksd-fleft query-count">
              <span>{{$t('selectedQuery')}} (99) </span>
              <span><i class="el-icon-ksd-good_health"></i>97 <i class="el-icon-ksd-error_01"></i>2</span>
            </div>
            <div class="ksd-fright ksd-inline searchInput" v-if="whiteSqlList.size">
              <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table :data="whiteSqlList.sqls" border @row-click="activeSql" class="import-table" style="width: 100%">
            <el-table-column type="selection" align="center" width="44" :selectable="selectable"></el-table-column>
            <el-table-column prop="sql" label="SQL" header-align="center" show-overflow-tooltip min-width="350"></el-table-column>
            <el-table-column prop="createdTime" :label="$t('createdTime')" show-overflow-tooltip header-align="center" min-width="180"></el-table-column>
            <el-table-column prop="capable" :label="$t('kylinLang.common.status')" align="center" min-width="80">
              <template slot-scope="props">
                <i :class="{'el-icon-ksd-good_health': props.row.capable, 'el-icon-ksd-error_01': !props.row.capable}"></i>
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" align="center" min-width="80">
              <template slot-scope="props">
                <i class="el-icon-ksd-table_edit" @click.stop="editWhiteSql(props.row)"></i>
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delWhite(props.row.id)"></i>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20" :totalSize="whiteSqlList.size"  v-on:handleCurrentChange='whiteSqlListsPageChange' :perPageSize="10" v-if="whiteSqlList.size > 0"></kap-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-10 ksd-fs-16">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="whiteInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="whiteSql">
            </kap-editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
                <el-button size="medium" @click="cancelEdit(isWhiteErrorMessage)">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="medium" plain @click="saveWhiteSql()">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
            </div>
          </div>
          <div class="error_messages" v-if="isWhiteErrorMessage">
            <div v-for="(mes, index) in whiteMessages" :key="index">
              <div class="label">{{$t('messages')}}</div>
              <p>{{mes.incapableReason}}</p>
              <div class="label ksd-mt-10">{{$t('suggestion')}}</div>
              <p>{{mes.suggestion}}</p>
            </div>
          </div>
        </el-col>
      </el-row>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="importSqlVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" plain :disabled="!toShowList" @click="toImportList">{{$t('kylinLang.common.next')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      top="5vh"
      :visible.sync="blackListVisible"
      width="1180px"
      class="blackListDialog">
      <span slot="title" class="ky-list-title">{{$t('blackList')}}
        <el-tooltip placement="left">
          <div slot="content">{{$t('blackListDesc')}}</div>
          <i class="el-icon-ksd-what ksd-fs-12"></i>
        </el-tooltip>
      </span>
      <el-row :gutter="20">
        <el-col :span="16" v-if="blackSqlList.size">
          <div class="clearfix ksd-mb-10">
            <span class="ksd-title-label query-count">{{$t('blackList')}}
              <span v-if="blackSqlList.size">({{blackSqlList.size}})</span>
            </span>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="blackSqlFilter" @input="onblackSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table :data="blackSqlList.sqls" border @row-click="viewBlackSql" class="import-table" style="width: 100%">
            <el-table-column type="selection" align="center" width="44" :selectable="selectable"></el-table-column>
            <el-table-column prop="sql" label="SQL" header-align="center" show-overflow-tooltip min-width="350"></el-table-column>
            <el-table-column prop="createdTime" :label="$t('createdTime')" show-overflow-tooltip header-align="center" min-width="180"></el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" align="center" min-width="80">
              <template slot-scope="props">
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delBlack(props.row.id)"></i>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="blackSqlList.size"  v-on:handleCurrentChange='blackSqlListsPageChange' :perPageSize="10" v-if="blackSqlList.size"></kap-pager>
        </el-col>
        <el-col :span="8" v-if="blackSqlList.size">
          <div class="ky-list-title ksd-mt-10 ksd-fs-16">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="blackInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="blackSql">
            </kap-editor>
          </div>
        </el-col>
      </el-row>
      <div class="ksd-null-pic-text" v-if="!blackSqlList.size && !isEditSql">
        <img  src="../../assets/img/no_data.png" />
        <p>{{$t('kylinLang.common.noData')}}</p>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="blackListVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      :visible.sync="ruleSettingVisible"
      width="1180px"
      :title="$t('ruleSetting')"
      class="ruleSettingDialog">
      <div class="conds">
        <div class="conds-title">
          <span>{{$t('queryFrequency')}}</span>
          <el-switch class="ksd-switch" v-model="frequencyObj.enable" active-text="ON" inactive-text="OFF" @change="updateFre"></el-switch>
        </div>
        <div class="conds-content clearfix">
          <!-- <div class="desc">{{$t('frequencyDesc')}}</div> -->
          <div class="ksd-mt-10 ksd-fs-14">
            <span>TopX% {{$t('queryFrequency')}}</span>
            <el-input v-model.trim="frequencyObj.freqValue" @input="handleInputChangeFre" size="small" class="rule-setting-input"></el-input> %
          </div>
        </div>
      </div>
      <div class="conds">
        <div class="conds-title">
          <span>{{$t('querySubmitter')}}</span>
          <el-switch class="ksd-switch" v-model="submitterObj.enable" active-text="ON" inactive-text="OFF" @change="updateSub"></el-switch>
        </div>
        <div class="conds-content">
          <!-- <div class="desc">{{$t('submitterDesc')}}</div> -->
        </div>
        <div class="conds-footer">
          <el-select v-model="selectedUser" v-event-stop :popper-append-to-body="false" filterable size="medium" placeholder="VIP User" class="ksd-mt-10" @change="selectUserChange">
            <el-option-group v-for="group in options" :key="group.label" :label="group.label">
              <el-option v-for="item in group.options" :key="item" :label="item" :value="item"></el-option>
            </el-option-group>
          </el-select>
          <div class="vip-users-block ksd-mb-10">
            <div class="ksd-mt-10" v-if="submitterObj.users.length"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
            <div class="vip-users">
              <el-tag
                v-for="(user, index) in submitterObj.users"
                :key="index"
                closable
                class="user-label"
                size="small"
                @close="removeUser(index)">
                {{user}}
              </el-tag>
            </div>
            <div class="ksd-mt-10" v-if="submitterObj.groups.length"><i class="el-icon-ksd-table_group"></i> VIP Group</div>
            <div class="vip-users">
              <el-tag
                v-for="(userGroup, index) in submitterObj.groups"
                :key="index"
                closable
                class="user-label"
                size="small"
                @close="removeUserGroup(index)">
                {{userGroup}}
              </el-tag>
            </div>
          </div>
        </div>
      </div>
      <div class="conds">
        <div class="conds-title">
          <span>{{$t('queryDuration')}}</span>
          <el-switch class="ksd-switch" v-model="durationObj.enable" active-text="ON" inactive-text="OFF" @change="updateDura"></el-switch>
        </div>
        <div class="conds-content clearfix">
          <!-- <div class="desc">{{$t('durationDesc')}}</div> -->
          <div class="ksd-mt-16 ksd-fs-12">
            {{$t('from')}}
            <el-input v-model.trim="durationObj.durationValue[0]" @input="handleInputChangeDur1" size="small" class="rule-setting-input"></el-input>
            {{$t('to')}}
            <el-input v-model.trim="durationObj.durationValue[1]" @input="handleInputChangeDur2" size="small" class="rule-setting-input"></el-input>
            {{$t('secondes')}}
          </div>
        </div>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="cancelRuleSetting" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="saveRuleSetting" size="medium">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { apiUrl } from '../../config'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import $ from 'jquery'
import { handleSuccessAsync, handleError } from '../../util/index'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../util/business'
import sqlFormatter from 'sql-formatter'
import favoriteTable from './favorite_table'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      getFrequency: 'GET_FREQUENCY',
      getSubmitter: 'GET_SUBMITTER',
      getDuration: 'GET_DURATION',
      updateFrequency: 'UPDATE_FREQUENCY',
      updateSubmitter: 'UPDATE_SUBMITTER',
      updateDuration: 'UPDATE_DURATION',
      loadWhiteList: 'LOAD_WHITE_LIST',
      saveWhite: 'SAVE_WHITE_SQL',
      deleteWhite: 'DELETE_WHITE_SQL',
      loadBlackList: 'LOAD_BLACK_LIST',
      addBlack: 'ADD_BLACK_SQL',
      deleteBlack: 'DELETE_BLACK_SQL',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      getSpeedInfo: 'GET_SPEED_INFO'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'favorite_table': favoriteTable
  },
  locales: {
    'en': {importSql: 'Import SQL', pleImport: 'Please Import Files', sqlFiles: 'SQL Files', createdTime: 'Create Time', selectedQuery: 'Selected Query', sqlBox: 'SQL Box', blackList: 'Black List', ruleSetting: 'Rule-based Setting', favDesc: 'Favorite queries are from both favorite rule filtered query and user defined query.<br/> Favorite query represent your main business analysis scenarios and critical decision point.<br/> System will optimize its to max performance by auto-modeling and pre-calculating.', favoriteRules: 'Favorite Rules', favRulesDesc: 'By filtering SQL\'s frequency, duration and submitter, favorite rule will catch up frequently used and business critical queries.', queryFrequency: 'Query Frequency', querySubmitter: 'Query Submitter', queryDuration: 'Query Duration', frequencyDesc: 'Optimize queries frequently used over last 24 hours', submitterDesc: 'Optimize queries from critical users and groups', durationDesc: 'Optimize queries with long duration', unit: 'Seconds / Job', inputSql: 'Add SQL', delSql: 'Are you sure to delete this sql?', giveUpEdit: 'Are you sure to give up the edit?', whiteListDesc: 'White list helps to manage user manually defined favorite SQLs, especially for SQLs from query history list and imported SQL files.', blackListDesc: 'Black list helps to manage SQLs which are undesired for accelerating, especially for those SQLs will require unreasonable large storage or computing resource to accelerate.', ruleImpact: 'Rules Impact', ruleImpactDesc: 'Percentage of SQL queries selected by the favorite rule.', thereAre: 'There are {modelSpeedEvents} SQLs waiting for acceleration on the threshold of <span class="highlight">{threshold}</span>.', accelerateNow: 'Accelerate now', openTips: 'Expand this block to set the "Acceleration Rule"', messages: 'Error Messages:', suggestion: 'Suggestion:', from: 'From', to: 'to', secondes: 'secondes', acceleratedSQL: 'Accelerated SQL'},
    'zh-cn': {importSql: '导入SQL', pleImport: '请导入文件', sqlFiles: 'SQL文件', createdTime: '创建时间', selectedQuery: '已选择的SQL', sqlBox: 'SQL窗口', blackList: '禁用名单', ruleSetting: '规则设置', favDesc: '经过加速规则筛选或者用户主动选择的SQL查询将成为加速查询。<br/>这类查询可以代表最主要的业务分析和重要的业务决策点。<br/>系统将对其进行自动建模和预计算，确保查询效率得到提升。', favRulesDesc: '加速规则过滤不同SQL查询的频率、时长、用户等特征，筛选出高频使用的、对业务分析重要的SQL查询。', favoriteRules: '加速规则', queryFrequency: '查询频率', querySubmitter: '查询用户', queryDuration: '查询时长', frequencyDesc: '优化过去24小时内查询频率较高的查询', submitterDesc: '优化重要⽤用户或⽤用户组发出的查询', durationDesc: '优化慢查询', unit: '秒 / 任务', inputSql: '新增查询语句', delSql: '确定删除这条查询语句吗？', giveUpEdit: '确定放弃本次编辑吗？', whiteListDesc: '本列表管理用户人为指定加速的SQL查询。一般指用户从查询历史指定或导入的查询文件。', blackListDesc: '本列表管理用户不希望被加速的SQL查询。一般是指加速时对存储空间、计算力需求过大的查询。', ruleImpact: '加速规则影响⼒', ruleImpactDesc: '被加速规则选出的SQL查询的百分⽐。', thereAre: '已有{modelSpeedEvents}条SQL查询等待加速(阈值为<span class="highlight">{threshold}</span>条SQL)', accelerateNow: '立即加速', openTips: '展开此区块可设定"加速规则"', messages: '错误信息：', suggestion: '修改建议：', from: '从', to: '至', secondes: '秒', acceleratedSQL: '已加速SQL'}
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  checkedStatus = ['WAITING', 'ACCELERATING', 'BLOCKED']
  activeList = 'wartingAcce'
  showGif = false
  unAcceListSize = 0
  patternNum = 0
  importSqlVisible = false
  isUploaded = false
  toShowList = false
  ruleSettingVisible = false
  blackListVisible = false
  isShowInput = false
  inputHeight = 574
  isEditSql = false
  blackSqlFilter = ''
  whiteSqlFilter = ''
  activeIndex = 0
  activeSqlObj = null
  whiteSqlList = []
  blackSqlList = []
  blackSql = ''
  whiteSql = ''
  isWhiteErrorMessage = false
  whiteMessages = []
  importLoading = false
  favoriteCurrentPage = 1
  activeNames = ['rules']
  filterData = {
    sortBy: 'last_query_time',
    reverse: false
  }
  frequencyObj = {
    enable: true,
    freqValue: 0
  }
  submitterObj = {
    enable: true,
    users: [],
    groups: []
  }
  durationObj = {
    enable: true,
    durationValue: [0, 0]
  }
  selectedUser = ''
  options = [{
    label: this.$t('kylinLang.menu.user'),
    options: ['ADMIN', 'ANALYST', 'MODELER']
  }, {
    label: this.$t('kylinLang.menu.group'),
    options: ['ALL_USERS', 'ROLE_ADMIN', 'ROLE_ANALYST', 'ROLE_MODELER']
  }]
  handleClick () {
    this.checkedStatus = this.activeList === 'wartingAcce' ? ['WAITING', 'ACCELERATING', 'BLOCKED'] : ['FULLY_ACCELERATED']
    this.loadFavoriteList()
  }
  applySpeed (event) {
    this.applySpeedInfo({size: this.modelSpeedEvents, project: this.currentSelectedProject}).then(() => {
      this.flyEvent(event)
      this.getSpeedInfo(this.currentSelectedProject)
      this.showGif = true
    }, (res) => {
      handleError(res)
    })
  }
  flyEvent (event) {
    var targetArea = $('#monitor')
    var targetDom = targetArea.find('.menu-icon')
    var offset = targetDom.offset()
    var flyer = $('<span class="fly-box"></span>')
    let leftOffset = 64
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
        left: offset.left + leftOffset,
        top: offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        targetDom.addClass('rotateY')
        setTimeout(() => {
          targetDom.fadeTo('slow', 0.5, function () {
            targetDom.removeClass('rotateY')
            targetDom.fadeTo('fast', 1)
          })
          flyer.fadeOut(1500, () => {
            flyer.remove()
          })
        }, 3000)
      }
    })
  }

  sortFavoriteList (filterData) {
    this.filterData = filterData
    this.loadFavoriteList()
  }

  filterFav (checkedStatus) {
    this.checkedStatus = checkedStatus.length ? checkedStatus : ['WAITING', 'ACCELERATING', 'BLOCKED']
    this.loadFavoriteList()
  }

  get modelSpeedEvents () {
    return this.$store.state.model.modelSpeedEvents
  }

  openRuleSetting () {
    this.getFrequencyObj()
    this.getSubmitterObj()
    this.getDurationObj()
    this.ruleSettingVisible = true
  }

  cancelRuleSetting () {
    this.ruleSettingVisible = false
  }

  saveRuleSetting () {
    this.updateFre()
    this.updateFre()
    this.updateDura()
  }

  updateFre () {
    this.updateFrequency({
      project: this.currentSelectedProject,
      enable: this.frequencyObj.enable,
      freqValue: this.frequencyObj.freqValue / 100
    }).then((res) => {
    }, (res) => {
      handleError(res)
    })
  }

  updateSub () {
    this.updateSubmitter({
      project: this.currentSelectedProject,
      enable: this.submitterObj.enable,
      users: this.submitterObj.users,
      groups: null
    }).then((res) => {
    }, (res) => {
      handleError(res)
    })
  }

  selectUserChange (val) {
    const index = this.options[0].options.indexOf(val)
    if (index !== -1) {
      this.submitterObj.users.push(val)
      this.selectedUser = ''
      this.options[0].options.splice(index, 1)
    } else {
      const groupIndex = this.options[1].options.indexOf(val)
      this.submitterObj.groups.push(val)
      this.selectedUser = ''
      this.options[1].options.splice(groupIndex, 1)
    }
  }

  removeUser (index) {
    const user = this.submitterObj.users[index]
    this.submitterObj.usres.splice(index, 1)
    this.options[0].options.push(user)
  }
  removeUserGroup (index) {
    const userGroups = this.submitterObj.groups[index]
    this.submitterObj.groups.splice(index, 1)
    this.options[1].options.push(userGroups)
  }

  updateDura () {
    this.updateDuration({
      project: this.currentSelectedProject,
      enable: this.durationObj.enable,
      durationValue: this.durationObj.durationValue
    }).then((res) => {
    }, (res) => {
      handleError(res)
    })
  }
  handleInputChangeFre (value) {
    this.$nextTick(() => {
      this.frequencyObj.freqValue = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
  }
  handleInputChangeDur1 (value) {
    this.$nextTick(() => {
      this.durationObj.durationValue[0] = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
  }
  handleInputChangeDur2 (value) {
    this.$nextTick(() => {
      this.durationObj.durationValue[1] = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
  }

  async loadFavoriteList (pageIndex, pageSize) {
    const res = await this.getFavoriteList({
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      status: this.checkedStatus,
      sortBy: this.filterData.sortBy,
      reverse: this.filterData.reverse
    })
    const data = await handleSuccessAsync(res)
    this.favQueList = data
    if (this.activeList === 'wartingAcce') {
      this.unAcceListSize = data.size
    } else {
      this.patternNum = data.size
    }
  }

  formatTooltip (val) {
    return val * 100
  }

  openImportSql () {
    this.importSqlVisible = true
  }

  getFormatterSql (sql) {
    return sqlFormatter.format(sql)
  }

  getFrequencyObj () {
    if (this.currentSelectedProject) {
      this.getFrequency({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.frequencyObj = data
          this.frequencyObj.freqValue = data.freqValue * 100
        })
      }, (res) => {
        handleError(res)
      })
    }
  }

  getSubmitterObj () {
    if (this.currentSelectedProject) {
      this.getSubmitter({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.submitterObj = data
        })
      }, (res) => {
        handleError(res)
      })
    }
  }

  getDurationObj () {
    if (this.currentSelectedProject) {
      this.getDuration({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.durationObj = data
        })
      }, (res) => {
        handleError(res)
      })
    }
  }

  async created () {
    this.loadFavoriteList()
    const res = await this.getFavoriteList({
      project: this.currentSelectedProject || null,
      limit: 10,
      offset: 0,
      status: ['FULLY_ACCELERATED'],
      sortBy: this.filterData.sortBy,
      reverse: this.filterData.reverse
    })
    const data = await handleSuccessAsync(res)
    this.patternNum = data.size
    if (this.currentSelectedProject) {
      this.getSpeedInfo(this.currentSelectedProject)
    }
  }
  mounted () {
    this.$nextTick(() => {
      $('#favo-menu-item').removeClass('rotateY').css('opacity', 0)
    })
  }

  pageCurrentChange (offset, pageSize) {
    this.favoriteCurrentPage = offset + 1
    this.loadFavoriteList(offset, pageSize)
  }

  openBlackList () {
    this.blackListVisible = true
    this.inputHeight = 574
    this.blackSql = ''
    this.isEditSql = false
    this.getBlackList()
  }

  async getWhiteList (pageIndex, pageSize) {
    const res = await this.loadWhiteList({
      project: this.currentSelectedProject,
      limit: pageSize || 10,
      offset: pageIndex || 0
    })
    const data = await handleSuccessAsync(res)
    this.whiteSqlList = data
    if (this.whiteSqlList.size > 0) {
      this.activeSql(this.whiteSqlList.sqls[0])
    } else {
      this.whiteSql = ''
      this.activeSqlObj = null
      this.isEditSql = false
      this.whiteMessages = []
      this.isWhiteErrorMessage = false
      this.inputHeight = 574
    }
  }

  async getBlackList (pageIndex, pageSize) {
    const res = await this.loadBlackList({
      project: this.currentSelectedProject,
      limit: pageSize || 10,
      offset: pageIndex || 0
    })
    const data = await handleSuccessAsync(res)
    this.blackSqlList = data
    if (this.blackSqlList.size > 0) {
      this.viewBlackSql(this.blackSqlList.sqls[0])
    } else {
      this.blackSql = ''
    }
  }

  openWhiteList () {
    this.whiteListVisible = true
    this.isEditSql = false
    this.getWhiteList()
  }

  activeSql (sqlObj) {
    this.whiteSql = this.formatterSql(sqlObj.sql)
    this.isEditSql = false
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 574
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 574 - 150
      this.whiteMessages = sqlObj.sqlAdvices
    }
    setTimeout(() => {
      this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
    }, 0)
  }

  editWhiteSql (sqlObj) {
    this.isEditSql = true
    this.inputHeight = 522
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 522
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 522 - 150
    }
    this.whiteSql = this.formatterSql(sqlObj.sql)
    this.activeSqlObj = sqlObj
    this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(false)
  }

  get uploadHeader () {
    if (this.$store.state.system.lang === 'en') {
      return {'Accept-Language': 'en'}
    } else {
      return {'Accept-Language': 'cn'}
    }
  }
  get actionUrl () {
    return apiUrl + 'query/favorite_queries/whitelist'
  }
  get uploadData () {
    return {
      project: this.currentSelectedProject
    }
  }
  beforeUpload () {
    this.importLoading = true
  }
  uploadSuccess (response) {
    this.importLoading = false
    this.toShowList = true
  }
  uploadError (err, file, fileList) {
    handleError({
      data: JSON.parse(err.message),
      status: err.status
    })
    this.importLoading = false
    this.toShowList = true
  }

  selectable (row) {
    return row.capable ? 1 : 0
  }

  resetImport () {
    this.isUploaded = false
    this.toShowList = false
  }

  toImportList () {
    this.isUploaded = true
    this.getWhiteList()
  }

  cancelEdit (isErrorMes) {
    this.isEditSql = false
    this.inputHeight = isErrorMes ? 574 - 150 : 574
    setTimeout(() => {
      this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
    }, 0)
  }

  saveWhiteSql () {
    this.saveWhite({sql: this.whiteSql, id: this.activeSqlObj.id, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        if (data.capable) {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.actionSuccess')
          })
        } else {
          this.whiteMessages = data.sqlAdvices
          this.inputHeight = 522 - 150
          this.isWhiteErrorMessage = true
        }
      })
    }, (res) => {
      handleError(res)
    })
  }

  viewBlackSql (row) {
    this.blackSql = this.formatterSql(row.sql)
    setTimeout(() => {
      this.$refs.blackInputBox.$refs.kapEditor.editor.setReadOnly(true)
    }, 0)
  }

  delBlack (id) {
    kapConfirm(this.$t('delSql')).then(() => {
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

  delWhite (id) {
    kapConfirm(this.$t('delSql')).then(() => {
      this.deleteWhite({id: id, project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.getWhiteList()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }

  formatterSql (sql) {
    return this.getFormatterSql(sql)
  }

  transformSql (sql) {
    return sql.length > 350 ? sql.substr(0, 350) + '...' : sql
  }

  onblackSqlFilterChange () {}

  onWhiteSqlFilterChange () {}

  blackSqlListsPageChange (offset, pageSize) {
    this.getBlackList(offset, pageSize)
  }

  whiteSqlListsPageChange (offset, pageSize) {
    this.getWhiteList(offset, pageSize)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #favoriteQuery {
    padding: 0px 20px 50px 20px;
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
        top: 8px;
        z-index: 999;
      }
    }
    .img-groups {
      margin: 0 auto;
      width: 850px;
      .label-groups {
        position: relative;
        > span {
          margin-left: 105px;
          &.ongoing-label {
            margin-left: 175px;
          }
        }
        .pattern-num {
          position: absolute;
          right: 30px;
          top: 18px;
          display: inline-block;
          width: 85px;
          text-align: center;
          line-height: 18px;
          p {
            font-size: 18px;
            font-weight: 500;
            &:last-child {
              font-size: 12px;
            }
          }
        }
      }
      .btn-groups {
        .el-button {
          margin-left: 100px;
          &:last-child {
            margin-left: 160px;
          }
        }
      }
    }
    .importSqlDialog,
    .blackListDialog {
      .el-dialog__body {
        min-height: 600px;
        .import-table {
          .el-table__row {
            cursor: pointer;
          }
        }
        .new_sql_status {
          border-color: @base-color;
          .ace-chrome {
            border-color: @base-color;
            border-bottom-color: @text-secondary-color;
          }
        }
        .query-count {
          color: @text-title-color;
          font-size: 16px;
          line-height: 24px;
          position: relative;
          top: 10px;
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
          margin-top: 20px;
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
          width: 50%;
          margin-left: calc(~'50% - 35px');
          text-align: left;
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
            width: 200px;
            margin-left: -60px;
            .el-upload-list__item {
              .el-upload-list__item-name {
                display: inline-block;
                margin-right: 5px;
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
    }
    .ruleSettingDialog {
      .conds:not(:last-child) {
        margin-bottom: 20px;
        padding-bottom: 20px;
        border-bottom: 1px solid @line-border-color;
      }
      .vip-users {
        .user-label {
          font-size: 14px;
          margin-right: 8px;
          margin-top: 5px;
        }
      }
    }
    .rule-setting-input {
      display: inline-block;
      width: 56px;
    }
    .favorite-table {
      .impored-row {
        background: @warning-color-2;
      }
      .status-icon {
        font-size: 20px;
        position: relative;
        top: 3px;
        &.el-icon-ksd-acclerate_pendding,
        &.el-icon-ksd-acclerate_ongoing {
          color: @base-color;
        }
        &.el-icon-ksd-acclerate_portion,
        &.el-icon-ksd-acclerate_all {
          color: @normal-color-1;
        }
        &.el-icon-ksd-table_discard {
          color: @text-disabled-color;
        }
      }
      .el-icon-ksd-filter {
        position: relative;
        top: 1px;
      }
    }
    .fav-dropdown {
      .el-icon-ksd-table_setting {
        color: inherit;
      }
    }
  }
</style>
