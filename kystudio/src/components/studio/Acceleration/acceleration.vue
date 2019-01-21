<template>
  <div id="favoriteQuery">
    <div class="ksd-title-label ksd-mt-10 ksd-mb-10">
      <span>{{$t('acceleration')}}</span>
    </div>
    <div class="img-groups" v-guide.speedProcess>
      <div class="label-groups">
        <span>{{$t('kylinLang.query.wartingAcce')}}: {{modelSpeedEvents}}</span>
        <span v-if="showGif" class="ongoing-label">{{$t('kylinLang.query.ongoingAcce')}}</span>
        <div class="pattern-num">
          <p>{{patternNum}}</p>
          <p class="">{{$t('acceleratedSQL')}}</p>
        </div>
      </div>
      <div v-if="showGif">
        <img src="../../../assets/img/merge1.gif" width="735px" alt=""><img src="../../../assets/img/acc_light.png" class="ksd-ml-10" width="85px" alt="">
        <!-- <img src="../../assets/img/acc_01.gif" width="245px" alt=""><img src="../../assets/img/acc_02.gif" width="245px" alt=""><img src="../../assets/img/acc_03.gif" width="245px" height="48px" alt=""> -->
      </div>
      <div v-else>
        <img src="../../../assets/img/bg1.jpg" width="735px" alt=""><img src="../../../assets/img/acc_light.png" class="ksd-ml-10" width="85px" alt="">
        <!-- <img src="../../assets/img/acc_01.jpg" width="245px" alt=""><img src="../../assets/img/acc_02.jpg" width="245px" alt=""><img src="../../assets/img/acc_03.jpg" width="245px" height="48px" alt=""> -->
      </div>
      <div class="btn-groups ksd-mt-10">
        <span class="guide-checkData" v-if="!modelSpeedEvents"></span>
        <el-button size="mini" type="primary" plain @click="openImportSql">{{$t('importSql')}}</el-button>
        <el-button size="mini" type="primary" v-guide.speedSqlNowBtn plain :disabled="!modelSpeedEvents" @click="applySpeed">{{$t('accelerateNow')}}</el-button>
      </div>
    </div>
    <div class="fav-tables ksd-mt-30">
      <div class="btn-group">
        <el-button size="mini" type="primary" icon="el-icon-ksd-setting" plain @click="openRuleSetting">{{$t('ruleSetting')}}</el-button>
        <el-button size="mini" type="primary" icon="el-icon-ksd-table_discard" plain @click="openBlackList">{{$t('blackList')}}</el-button>
      </div>
      <el-tabs v-model="activeList" @tab-click="handleClick">
        <el-tab-pane name="wartingAcce">
          <span slot="label">{{$t('waitingList')}}({{unAcceListSize}})</span>
          <acceleration_table :favoriteTableData="favQueList.favorite_queries" :sortTable="sortFavoriteList" v-on:filterFav="filterFav"></acceleration_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="accelerated">
          <span slot="label">{{$t('accelerated')}}({{patternNum}})</span>
          <acceleration_table :favoriteTableData="favQueList.favorite_queries" :sortTable="sortFavoriteList" v-on:filterFav="filterFav" :isAccelerated="true"></acceleration_table>
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
        <img src="../../../assets/img/license.png" alt="" v-show="!uploadItems.length">
        <div class="ksd-mt-10 text" v-show="!uploadItems.length">{{$t('pleImport')}}</div>
        <el-upload
          ref="sqlUpload"
          :headers="uploadHeader"
          action=""
          :on-remove="handleRemove"
          :on-change="fileItemChange"
          :file-list="uploadItems"
          multiple
          :auto-upload="false">
          <el-button type="primary" size="medium">{{$t('sqlFiles')}}
          </el-button>
        </el-upload>
      </div>
      <el-row :gutter="20" v-else>
        <el-col :span="16">
          <div class="clearfix ksd-mb-10">
            <div class="ksd-fleft">
              <div v-if="pagerTableData.length&&whiteSqlData.capable_sql_num" class="ksd-fleft ksd-mr-10">
                <el-button type="primary" size="medium" plain @click="selectAll" v-if="selectSqls.length!==whiteSqlData.capable_sql_num">{{$t('checkAll')}}</el-button>
                <el-button type="primary" size="medium" plain @click="cancelSelectAll" v-else>{{$t('cancelAll')}}</el-button>
              </div>
              <el-button type="primary" size="medium" :disabled="!finalSelectSqls.length" :loading="submitSqlLoading" @click="addTofav">{{$t('addTofavorite')}}({{finalSelectSqls.length}})</el-button>
            </div>
            <div class="ksd-fright ksd-inline searchInput" v-if="whiteSqlData.size">
              <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table
            :data="pagerTableData"
            border
            ref="multipleTable"
            @row-click="activeSql"
            @select="handleSelectionChange"
            @select-all="handleSelectAllChange"
            :row-class-name="tableRowClassName"
            class="import-table"
            style="width: 100%">
            <el-table-column type="selection" align="center" width="44" :selectable="selectable"></el-table-column>
            <el-table-column prop="sql" label="SQL" header-align="center" :resizable="false">
              <template slot-scope="props">
                <span class="ksd-nobr-text">{{props.row.sql}}</span>
              </template>
            </el-table-column>
            <el-table-column prop="capable" :label="$t('kylinLang.common.status')" align="center" width="83">
              <template slot-scope="props">
                <i :class="{'el-icon-ksd-good_health': props.row.capable, 'el-icon-ksd-error_01': !props.row.capable}"></i>
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" align="center" width="83">
              <template slot-scope="props">
                <i class="el-icon-ksd-table_edit" @click.stop="editWhiteSql(props.row)"></i>
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delWhiteComfirm(props.row.id)"></i>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20" :totalSize="filteredDataSize"  v-on:handleCurrentChange='whiteSqlDatasPageChange' :perPageSize="whitePageSize" v-if="filteredDataSize > 0"></kap-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-10 ksd-fs-16">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="whiteInputBox" :height="inputHeight" :readOnly="this.isReadOnly" :isFormatter="true" lang="sql" theme="chrome" v-model="whiteSql">
            </kap-editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
                <el-button size="medium" @click="cancelEdit(isWhiteErrorMessage)">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="medium" plain :loading="validateLoading" @click="validateWhiteSql()">{{$t('kylinLang.common.submit')}}</el-button>
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
        <div class="ksd-fleft query-count">
          <span v-if="isUploaded"><i class="el-icon-ksd-good_health"></i>{{whiteSqlData.capable_sql_num}}
          <i class="el-icon-ksd-error_01"></i>{{whiteSqlData.size-whiteSqlData.capable_sql_num}}</span>
          <span v-else class="tips">
            <i class="el-icon-warning ksd-fs-16"></i><span class="ksd-fs-12">{{$t('uploadFileTips')}}</span>
          </span>
        </div>
        <el-button size="medium" @click="importSqlVisible = false">{{$t('kylinLang.common.close')}}</el-button>
        <el-button type="primary" size="medium" plain v-if="!isUploaded" :loading="importLoading" :disabled="!uploadItems.length||fileSizeError"  @click="submitFiles">{{$t('kylinLang.common.submit')}}</el-button>
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
        <el-col :span="16" v-if="blackSqlData&&blackSqlData.size">
          <div class="clearfix ksd-mb-10">
            <span class="ksd-title-label query-count">{{$t('blackList')}}
              <span v-if="blackSqlData.size">({{blackSqlData.size}})</span>
            </span>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="blackSqlFilter" @input="onblackSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table :data="blackSqlData.sqls" border @row-click="viewBlackSql" class="import-table" style="width: 100%">
            <el-table-column prop="sql_pattern" label="SQL" :resizable="false" header-align="center" show-overflow-tooltip></el-table-column>
            <el-table-column prop="create_time" :label="$t('createdTime')" show-overflow-tooltip header-align="center" width="207">
              <template slot-scope="props">
                {{transToGmtTime(props.row.create_time)}}
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" align="center" width="83">
              <template slot-scope="props">
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delBlack(props.row.id)"></i>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="blackSqlData.size"  v-on:handleCurrentChange='blackSqlDatasPageChange' :perPageSize="10" v-if="blackSqlData.size"></kap-pager>
        </el-col>
        <el-col :span="8" v-if="blackSqlData&&blackSqlData.size">
          <div class="ky-list-title ksd-mt-10 ksd-fs-16">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="blackInputBox" :height="inputHeight" :readOnly="true" :isFormatter="true" lang="sql" theme="chrome" v-model="blackSql">
            </kap-editor>
          </div>
        </el-col>
      </el-row>
      <div class="ksd-null-pic-text" v-if="blackSqlData&&!blackSqlData.size">
        <img  src="../../../assets/img/no_data.png" />
        <p>{{$t('kylinLang.common.noData')}}</p>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="blackListVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      :visible.sync="ruleSettingVisible"
      width="780px"
      :title="$t('ruleSetting')"
      class="ruleSettingDialog">
      <div class="conds">
        <div class="conds-title">
          <span>{{$t('queryFrequency')}}</span>
          <el-switch class="ksd-switch" v-model="frequencyObj.enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')" @change="updateFre"></el-switch>
        </div>
        <div class="conds-content clearfix">
          <!-- <div class="desc">{{$t('frequencyDesc')}}</div> -->
          <div class="ksd-mt-10 ksd-fs-14">
            <span>{{$t('AccQueryStart')}}</span>
            <el-input v-model.trim="frequencyObj.freqValue" @input="handleInputChangeFre" size="small" class="rule-setting-input"></el-input> %
            <span>{{$t('AccQueryEnd')}}</span>
          </div>
        </div>
      </div>
      <div class="conds">
        <div class="conds-title">
          <span>{{$t('querySubmitter')}}</span>
          <el-switch class="ksd-switch" v-model="submitterObj.enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')" @change="updateSub"></el-switch>
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
            <div class="ksd-mt-20 conds-title" v-if="submitterObj.users.length"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
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
            <div class="ksd-mt-20 conds-title" v-if="submitterObj.groups.length"><i class="el-icon-ksd-table_group"></i> VIP Group</div>
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
          <el-switch class="ksd-switch" v-model="durationObj.enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')" @change="updateDura"></el-switch>
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
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import $ from 'jquery'
import { handleSuccessAsync, handleError } from '../../../util/index'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../../util/business'
import accelerationTable from './acceleration_table'
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
      validateWhite: 'VALIDATE_WHITE_SQL',
      addTofavoriteList: 'ADD_TO_FAVORITE_LIST',
      loadBlackList: 'LOAD_BLACK_LIST',
      deleteBlack: 'DELETE_BLACK_SQL',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      getSpeedInfo: 'GET_SPEED_INFO',
      importSqlFiles: 'IMPORT_SQL_FILES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'acceleration_table': accelerationTable
  },
  locales: {
    'en': {
      acceleration: 'Acceleration',
      importSql: 'Import SQL',
      pleImport: 'Please Import Files',
      sqlFiles: 'SQL File',
      createdTime: 'Create Time',
      selectedQuery: 'Selected Query',
      sqlBox: 'SQL Box',
      blackList: 'Black List',
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
      AccQueryStart: 'Accelerate',
      AccQueryEnd: ' Queries (From the top usage to lower usage)',
      unit: 'Seconds / Job',
      inputSql: 'Add SQL',
      delSql: 'Are you sure to delete this sql?',
      delSqlTitle: 'Delete SQL',
      giveUpEdit: 'Are you sure to give up the edit?',
      thereAre: 'There are {modelSpeedEvents} SQLs waiting for acceleration on the threshold of <span class="highlight">{threshold}</span>.',
      accelerateNow: 'Accelerate now',
      messages: 'Error Messages:',
      suggestion: 'Suggestion:',
      from: 'Accelerate the queries whose latency range is between',
      to: 'second(s) to',
      secondes: 'second(s)',
      acceleratedSQL: 'Accelerated SQL',
      checkAll: 'Check All',
      cancelAll: 'Uncheck All',
      addTofavorite: 'Submit',
      filesSizeError: 'Files cannot exceed 20M.',
      fileTypeError: 'Invalid file format。',
      waitingList: 'Waiting List',
      accelerated: 'Accelerated',
      uploadFileTips: 'Supported file formats are txt and sql. Supported file size is up to 20 MB.'
    },
    'zh-cn': {
      acceleration: '加速引擎',
      importSql: '导入SQL文件',
      pleImport: '请导入文件',
      sqlFiles: 'SQL文件',
      createdTime: '创建时间',
      selectedQuery: '已选择的SQL',
      sqlBox: 'SQL窗口',
      blackList: '禁用名单',
      ruleSetting: '加速规则',
      favoriteRules: '加速规则',
      queryFrequency: '查询频率',
      querySubmitter: '查询用户',
      queryDuration: '查询延迟',
      frequencyDesc: '优化过去24小时内查询频率较高的查询',
      submitterDesc: '优化重要⽤用户或⽤用户组发出的查询',
      durationDesc: '优化慢查询',
      blackListDesc: '本列表管理用户不希望被加速的SQL查询。一般是指加速时对存储空间、计算力需求过大的查询。',
      AccQueryStart: '加速',
      AccQueryEnd: '的SQL查询（按照使用频率由高到低计算）',
      unit: '秒 / 任务',
      inputSql: '新增查询语句',
      delSql: '确定删除这条查询语句吗？',
      delSqlTitle: '删除查询语句',
      giveUpEdit: '确定放弃本次编辑吗？',
      thereAre: '已有{modelSpeedEvents}条SQL查询等待加速(阈值为<span class="highlight">{threshold}</span>条SQL)',
      accelerateNow: '立即加速',
      messages: '错误信息：',
      suggestion: '修改建议：',
      from: '加速延迟的范围在',
      to: '秒到',
      secondes: '的查询',
      acceleratedSQL: '已加速SQL',
      checkAll: '全选',
      cancelAll: '取消全选',
      addTofavorite: '提交',
      filesSizeError: '文件大小不能超过20M!',
      fileTypeError: '不支持的文件格式！',
      waitingList: '未加速',
      accelerated: '加速完毕',
      uploadFileTips: '支持的文件格式为 txt 和 sql，文件最大支持20 MB。'
    }
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
  ruleSettingVisible = false
  blackListVisible = false
  isShowInput = false
  inputHeight = 574
  isEditSql = false
  blackSqlFilter = ''
  whiteSqlFilter = ''
  activeIndex = 0
  activeSqlObj = null
  whiteSqlData = null
  blackSqlData = null
  blackSql = ''
  whiteSql = ''
  isReadOnly = true
  validateLoading = false
  fileSizeError = false
  isWhiteErrorMessage = false
  whiteMessages = []
  importLoading = false
  uploadItems = []
  pagerTableData = []
  multipleSelection = []
  selectSqls = []
  submitSqlLoading = false
  filteredDataSize = 0
  favoriteCurrentPage = 1
  whiteCurrentPage = 0
  timer = null
  initTimer = null
  whitePageSize = 10
  activeNames = ['rules']
  filterData = {
    sortBy: 'last_query_time',
    reverse: true
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
  tableRowClassName ({row, rowIndex}) {
    if (this.activeSqlObj && row.id === this.activeSqlObj.id) {
      return 'active-row'
    }
    return ''
  }
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
    this.checkedStatus = checkedStatus.length ? checkedStatus : this.activeList === 'wartingAcce' ? ['WAITING', 'ACCELERATING', 'BLOCKED'] : ['FULLY_ACCELERATED']
    this.loadFavoriteList()
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

  openImportSql () {
    this.importSqlVisible = true
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

  async init () {
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

  created () {
    this.init()
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

  async getBlackList (pageIndex, pageSize) {
    const res = await this.loadBlackList({
      project: this.currentSelectedProject,
      limit: pageSize || 10,
      offset: pageIndex || 0
    })
    const data = await handleSuccessAsync(res)
    this.blackSqlData = data
    if (this.blackSqlData.size > 0) {
      this.viewBlackSql(this.blackSqlData.sqls[0])
    } else {
      this.blackSql = ''
    }
  }

  activeSql (sqlObj) {
    this.whiteSql = sqlObj.sql
    this.isEditSql = false
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 574
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 574 - 150
      this.whiteMessages = sqlObj.sqlAdvices
    }
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
    this.whiteSql = sqlObj.sql
    this.activeSqlObj = sqlObj
    this.isReadOnly = false
  }

  get uploadHeader () {
    if (this.$store.state.system.lang === 'en') {
      return {'Accept-Language': 'en'}
    } else {
      return {'Accept-Language': 'cn'}
    }
  }
  fileItemChange (file, fileList) {
    let totalSize = 0
    this.uploadItems = fileList.filter((file) => {
      return file.name.indexOf('.txt') !== -1 || file.name.indexOf('.sql') !== -1
    }).map((item) => {
      totalSize = totalSize + item.size
      return item.raw ? item.raw : item
    })
    if (totalSize > 20 * 1024 * 1024) { // 附件不能大于20M
      this.$message.warning(this.$t('filesSizeError'))
      this.fileSizeError = true
    } else {
      this.fileSizeError = false
    }
    if (!(file.name.indexOf('.txt') !== -1 || file.name.indexOf('.sql') !== -1)) {
      this.$message.error(this.$t('fileTypeError'))
    }
  }
  handleRemove (file, fileList) {
    this.uploadItems = fileList
  }
  selectable (row) {
    return row.capable ? 1 : 0
  }

  resetImport () {
    this.isUploaded = false
    this.uploadItems = []
    this.whiteSqlData = null
    this.pagerTableData = []
    this.whiteSqlFilter = ''
  }

  submitFiles () {
    const formData = new FormData()   // 利用H5 FORMDATA 同时传输多文件和数据
    this.uploadItems.forEach(file => {
      formData.append('files', file)
    })
    this.importLoading = true
    this.importSqlFiles({project: this.currentSelectedProject, formData: formData}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.importLoading = false
        this.isUploaded = true
        this.whiteSqlData = data
        this.selectAll()
        this.whiteSqlDatasPageChange(0)
        if (msg) {
          this.$message.warning(msg)
        }
      })
    }, (res) => {
      handleError(res)
    })
  }

  get finalSelectSqls () {
    let finalSqls = []
    finalSqls = this.selectSqls.filter((item) => {
      return item.sql.indexOf(this.whiteSqlFilter) !== -1
    })
    return finalSqls
  }
  selectAll () {
    this.selectSqls = this.whiteSqlData.data.filter((item) => {
      return item.capable
    })
    this.selectPagerSqls(true)
  }
  cancelSelectAll () {
    this.selectSqls = []
    this.selectPagerSqls(false)
  }
  handleSelectionChange (val, row) {
    this.mergeSelectSqls(row)
  }
  handleSelectAllChange (val) {
    if (val.length) {
      val.forEach((item) => {
        this.mergeSelectSqls(item, 'batchAdd')
      })
    } else {
      this.pagerTableData.forEach((item) => {
        this.mergeSelectSqls(item, 'batchRemove')
      })
    }
  }
  // 单选一条时：toggle row; batchFlag有值时：批量添加rows或者批量去除rows
  mergeSelectSqls (row, batchFlag) {
    let index = -1
    for (const key in this.selectSqls) {
      if (this.selectSqls[key].id === row.id) {
        index = key
        break
      }
    }
    if (index === -1) {
      if (batchFlag !== 'batchRemove') {
        this.selectSqls.push(row)
      }
    } else {
      if (batchFlag !== 'batchAdd') {
        this.selectSqls.splice(index, 1)
      }
    }
  }
  whiteSqlDatasPageChange (currentPage, pageSize) {
    const size = pageSize || 10
    this.whiteCurrentPage = currentPage
    this.whitePageSize = size
    const filteredData = this.whiteFilter(this.whiteSqlData.data)
    this.filteredDataSize = filteredData.length
    this.pagerTableData = filteredData.slice(currentPage * size, (currentPage + 1) * size)
    if (this.filteredDataSize) {
      this.$nextTick(() => {
        this.activeSql(this.pagerTableData[0])
      })
      let targetSelectSqls = []
      this.pagerTableData.forEach((item) => {
        let index = -1
        for (const key in this.selectSqls) {
          if (this.selectSqls[key].id === item.id) {
            index = key
            break
          }
        }
        if (index !== -1) {
          targetSelectSqls.push(item)
        }
      })
      this.$nextTick(() => {
        this.toggleSelection(targetSelectSqls)
      })
    } else {
      this.whiteSql = ''
      this.activeSqlObj = null
      this.isEditSql = false
      this.whiteMessages = []
      this.isWhiteErrorMessage = false
      this.inputHeight = 574
    }
  }
  selectPagerSqls (isSelectAll) {
    const selectedRows = isSelectAll ? this.pagerTableData.filter((item) => {
      return item.capable
    }) : []
    this.$nextTick(() => {
      this.toggleSelection(selectedRows)
    })
  }
  whiteFilter (data) {
    return data.filter((sqlObj) => {
      return sqlObj.sql.indexOf(this.whiteSqlFilter) !== -1
    })
  }

  toggleSelection (rows) {
    if (rows && rows.length) {
      this.$refs.multipleTable.clearSelection()
      rows.forEach(row => {
        this.$refs.multipleTable.toggleRowSelection(row)
      })
    } else {
      this.$refs.multipleTable.clearSelection()
    }
  }
  addTofav () {
    this.submitSqlLoading = true
    const sqlsData = this.finalSelectSqls
    const sqls = sqlsData.map((item) => {
      return item.sql
    })
    this.addTofavoriteList({project: this.currentSelectedProject, sqls: sqls}).then((res) => {
      handleSuccess(res, (data) => {
        this.submitSqlLoading = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        sqlsData.forEach((item) => {
          this.delWhite(item.id)
        })
        this.loadFavoriteList()
      })
    }, (res) => {
      handleError(res)
      this.submitSqlLoading = false
    })
  }

  onWhiteSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.whiteSqlDatasPageChange(0)
    }, 500)
  }

  cancelEdit (isErrorMes) {
    this.isEditSql = false
    this.inputHeight = isErrorMes ? 574 - 150 : 574
    this.whiteSql = this.activeSqlObj.sql
    this.activeSqlObj = null
    this.isReadOnly = true
  }

  validateWhiteSql () {
    this.validateLoading = true
    this.validateWhite({sql: this.whiteSql, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.validateLoading = false
        if (data.capable) {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.actionSuccess')
          })
          this.whiteMessages = []
          this.inputHeight = 522
          this.isWhiteErrorMessage = false
          for (const key in this.whiteSqlData.data) {
            if (this.whiteSqlData.data[key].id === this.activeSqlObj.id) {
              this.whiteSqlData.data[key].sql = this.whiteSql
              this.whiteSqlDatasPageChange(this.whiteCurrentPage)
              if (!this.whiteSqlData.data[key].capable) {
                this.whiteSqlData.data[key].capable = true
                this.whiteSqlData.data[key].sqlAdvices = []
                this.whiteSqlData.capable_sql_num++
              }
              break
            }
          }
        } else {
          this.whiteMessages = data.sqlAdvices
          this.inputHeight = 522 - 150
          this.isWhiteErrorMessage = true
        }
      })
    }, (res) => {
      this.validateLoading = false
      handleError(res)
    })
  }

  delWhiteComfirm (id) {
    kapConfirm(this.$t('delSql'), null, this.$t('delSqlTitle')).then(() => {
      this.delWhite(id)
    })
  }

  delWhite (id) {
    for (const key in this.whiteSqlData.data) {
      if (this.whiteSqlData.data[key].id === id) {
        this.whiteSqlData.data.splice(key, 1)
        this.whiteSqlData.size--
        this.$nextTick(() => {
          this.whiteSqlDatasPageChange(this.whiteCurrentPage)
        })
        break
      }
    }
    for (const index in this.selectSqls) {
      if (this.selectSqls[index].id === id) {
        this.selectSqls.splice(index, 1)
        break
      }
    }
  }

  viewBlackSql (row) {
    this.blackSql = row.sql_pattern
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

  onblackSqlFilterChange () {}

  blackSqlDatasPageChange (offset, pageSize) {
    this.getBlackList(offset, pageSize)
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
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
        margin-bottom: 5px;
        > span {
          margin-left: 105px;
          line-height: 18px;
          &.ongoing-label {
            margin-left: 175px;
          }
        }
        .pattern-num {
          position: absolute;
          right: 12px;
          top: 18px;
          display: inline-block;
          width: 95px;
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
      .query-count {
        color: @text-title-color;
        font-size: 16px;
        line-height: 32px;
        height: 32px;
        display: inline-block;
      }
      .tips {
        span {
          position: relative;
          bottom: 2px;
          margin-left: 5px;
          color: @text-normal-color;
        }
        i {
          color: @text-secondary-color;
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
        min-height: 600px;
        .import-table {
          .active-row {
            background-color: @base-color-9;
          }
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
      .conds-title {
        font-weight: 500;
      }
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
        float: right;
        position: relative;
        top: 5px;
      }
    }
    .fav-dropdown {
      .el-icon-ksd-table_setting {
        color: inherit;
      }
    }
  }
</style>
