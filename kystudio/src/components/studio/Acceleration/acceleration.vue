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
            <el-dropdown-item command="ruleSetting">{{$t('ruleSetting')}}</el-dropdown-item>
            <el-dropdown-item command="blackList">{{$t('blackList')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </div>
      <el-tabs v-model="activeList" type="card" @tab-click="handleClick">
        <el-tab-pane name="waiting">
          <span slot="label">{{$t('waitingList', {num: listSizes.waiting})}}</span>
          <acceleration_table
            v-if="activeList=='waiting'"
            :favoriteTableData="favQueList.favorite_queries"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="waiting"></acceleration_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mtb-10" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="not_accelerated">
          <span slot="label">{{$t('not_accelerated', {num: listSizes.not_accelerated})}}</span>
          <acceleration_table
            v-if="activeList=='not_accelerated'"
            :favoriteTableData="favQueList.favorite_queries"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="not_accelerated"></acceleration_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mtb-10" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
        <el-tab-pane name="accelerated">
          <span slot="label">{{$t('accelerated',  {num: listSizes.accelerated})}}</span>
          <acceleration_table
            v-if="activeList=='accelerated'"
            :favoriteTableData="favQueList.favorite_queries"
            v-on:sortTable="sortFavoriteList"
            v-on:filterFav="filterFav"
            v-on:pausePolling="pausePolling"
            v-on:reCallPolling="reCallPolling"
            tab="accelerated"></acceleration_table>
          <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mtb-10" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
        </el-tab-pane>
      </el-tabs>
    </div>
    <el-dialog
      :visible.sync="importSqlVisible"
      top="5vh"
      width="960px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
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
      <el-row :gutter="15" v-else>
        <el-col :span="16">
          <div class="clearfix ksd-mb-10">
            <div class="ksd-fleft">
              <div v-if="pagerTableData.length&&whiteSqlData.capable_sql_num" class="ksd-fleft ksd-mr-10">
                <el-button type="primary" size="medium" plain @click="selectAll" v-if="selectSqls.length!==whiteSqlData.capable_sql_num">{{$t('checkAll')}}</el-button><el-button
                type="primary" size="medium" plain @click="cancelSelectAll" v-else>{{$t('cancelAll')}}</el-button>
              </div>

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
            <el-table-column type="selection" width="44" align="center" :selectable="selectable"></el-table-column>
            <el-table-column prop="sql" label="SQL" :resizable="false">
              <template slot-scope="props">
                <span class="ksd-nobr-text" style="width: 382px;">{{props.row.sql}}</span>
              </template>
            </el-table-column>
            <el-table-column prop="capable" :label="$t('kylinLang.common.status')" width="80">
              <template slot-scope="props">
                <i :class="{'el-icon-ksd-good_health': props.row.capable, 'el-icon-ksd-error_01': !props.row.capable}"></i>
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" width="80">
              <template slot-scope="props">
                <common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit" @click.stop="editWhiteSql(props.row)"></i>
                </common-tip>
                <common-tip :content="$t('kylinLang.common.drop')">
                  <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delWhiteComfirm(props.row.id)"></i>
                </common-tip>
               </template>
            </el-table-column>
          </el-table>
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-10" :totalSize="filteredDataSize" layout="total, prev, pager, next, jumper" v-on:handleCurrentChange='whiteSqlDatasPageChange' :perPageSize="whitePageSize" v-if="filteredDataSize > 0"></kap-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-10 ksd-fs-14">{{$t('sqlBox')}}</div>
          <div element-loading-spinner="el-icon-loading">
            <div v-loading="sqlLoading" class="query_panel_box ksd-mt-10">
              <kap-editor ref="whiteInputBox" :height="inputHeight" :dragable="false" :readOnly="this.isReadOnly" lang="sql" theme="chrome" v-model="whiteSql" v-if="isShowEditor">
              </kap-editor>
              <div class="operatorBox" v-if="isEditSql">
                <div class="btn-group ksd-fright ky-no-br-space">
                  <el-button size="small" @click="cancelEdit(isWhiteErrorMessage)">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" size="small" plain :loading="validateLoading" @click="validateWhiteSql()">{{$t('kylinLang.common.submit')}}</el-button>
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
          </div>
        </el-col>
      </el-row>
      <span slot="footer" class="dialog-footer">
        <div class="ksd-fleft query-count">
          <span v-if="isUploaded">
            <span><i class="el-icon-ksd-good_health"></i>{{whiteSqlData.capable_sql_num}}</span><span class="ksd-ml-10">
            <i class="el-icon-ksd-error_01"></i>{{whiteSqlData.size-whiteSqlData.capable_sql_num}}</span>
          </span>
          <span v-else class="tips">
            <i class="el-icon-ksd-info ksd-fs-14"></i><span class="ksd-fs-12">{{$t('uploadFileTips')}}</span>
          </span>
        </div>
        <el-button size="medium" @click="importSqlVisible = false">{{$t('kylinLang.common.close')}}</el-button><el-button
        type="primary" size="medium" plain v-if="!isUploaded" :loading="importLoading" :disabled="!uploadItems.length||fileSizeError"  @click="submitFiles">{{$t('kylinLang.common.submit')}}</el-button><el-button
        type="primary" size="medium" v-if="isUploaded" :disabled="!finalSelectSqls.length" :loading="submitSqlLoading" @click="addTofav">{{$t('addTofavorite')}}</el-button>
      </span>
    </el-dialog>
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
            <span class="ksd-title-label ksd-fs-14 query-count">{{$t('blackList1', {size: blackSqlData.size})}}
            </span>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="blackSqlFilter" @input="onblackSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table :data="blackSqlData.sqls" border @row-click="viewBlackSql" :row-class-name="tableRowClassName" class="import-table" style="width: 100%">
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
          <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-10" :totalSize="blackSqlData.size" layout="total, prev, pager, next, jumper" v-on:handleCurrentChange='blackSqlDatasPageChange' :perPageSize="10" v-if="blackSqlData.size"></kap-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-12 ksd-fs-14">{{$t('sqlBox')}}</div>
          <div class="query_panel_box ksd-mt-10" v-loading="sqlLoading" element-loading-spinner="el-icon-loading">
            <kap-editor ref="blackInputBox" :height="inputHeight" :dragable="false" :readOnly="true" lang="sql" theme="chrome" v-model="blackSql">
            </kap-editor>
          </div>
        </el-col>
      </el-row>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="blackListVisible = false">{{$t('kylinLang.common.close')}}</el-button>
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
            <el-switch size="small" v-model="rulesObj.freqEnable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-mt-10 ksd-fs-14">
              <el-form-item prop="freqValue">
                <span>{{$t('AccQueryStart')}}</span>
                <el-input v-model.trim="rulesObj.freqValue" v-number="rulesObj.freqValue" size="small" class="rule-setting-input" :disabled="!rulesObj.freqEnable"></el-input> %
                <span>{{$t('AccQueryEnd')}}</span>
              </el-form-item>
            </div>
          </div>
        </div>

        <div class="conds">
          <div class="conds-title">
            <span>{{$t('querySubmitter')}}</span>
            <el-switch size="small" v-model="rulesObj.submitterEnable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content">
            <div class="vip-users-block">
              <el-form-item prop="users">
                <div class="ksd-mt-10 conds-title"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
                <el-select v-model="rulesObj.users" v-event-stop :popper-append-to-body="false" filterable size="medium" placeholder="VIP User" class="ksd-mt-5" multiple style="width:100%">
                  <el-option v-for="item in allSubmittersOptions.user" :key="item" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
              <el-form-item prop="userGroups">
              <div class="ksd-mt-10 conds-title"><i class="el-icon-ksd-table_group"></i> VIP Group</div>
              <el-select v-model="rulesObj.userGroups" v-event-stop :popper-append-to-body="false" filterable size="medium" placeholder="VIP User Group" class="ksd-mt-5" multiple style="width:100%">
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
            <el-switch size="small" v-model="rulesObj.durationEnable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-mt-10 ksd-fs-14">
              {{$t('from')}}
              <el-form-item prop="minDuration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.minDuration" v-number="rulesObj.minDuration" size="small" class="rule-setting-input" :disabled="!rulesObj.durationEnable"></el-input>
              </el-form-item>
              {{$t('to')}}
              <el-form-item prop="maxDuration" style="display: inline-block;">
              <el-input v-model.trim="rulesObj.maxDuration" v-number="rulesObj.maxDuration" size="small" class="rule-setting-input" :disabled="!rulesObj.durationEnable"></el-input>
              </el-form-item>
              {{$t('secondes')}}
            </div>
          </div>
        </div>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="cancelRuleSetting" size="medium">{{$t('kylinLang.common.cancel')}}</el-button><el-button
        type="primary" plain @click="saveRuleSetting" size="medium" :loading="updateLoading">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapMutations, mapGetters } from 'vuex'
import $ from 'jquery'
import { handleSuccessAsync, handleError, objectClone } from '../../../util/index'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../../util/business'
import accelerationTable from './acceleration_table'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      getRules: 'GET_RULES',
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      updateRules: 'UPDATE_RULES',
      validateWhite: 'VALIDATE_WHITE_SQL',
      addTofavoriteList: 'ADD_TO_FAVORITE_LIST',
      loadBlackList: 'LOAD_BLACK_LIST',
      deleteBlack: 'DELETE_BLACK_SQL',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      importSqlFiles: 'IMPORT_SQL_FILES',
      getWaitingAcceSize: 'GET_WAITING_ACCE_SIZE',
      formatSql: 'FORMAT_SQL'
    }),
    ...mapMutations({
      lockSpeedInfo: 'LOCK_SPEED_INFO'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  components: {
    'acceleration_table': accelerationTable
  },
  locales: {
    'en': {
      more: 'More',
      acceleration: 'Acceleration',
      importSql: 'Import SQL',
      pleImport: 'Please Import Files',
      sqlFiles: 'SQL File',
      createdTime: 'Create Time',
      selectedQuery: 'Selected Query',
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
      AccQueryStart: 'Accelerate',
      AccQueryEnd: ' Queries (From the top usage to lower usage)',
      unit: 'Seconds / Job',
      inputSql: 'Add SQL',
      delSql: 'Are you sure to delete this sql?',
      delSqlTitle: 'Delete SQL',
      giveUpEdit: 'Are you sure to give up the edit?',
      thereAre: 'There are {waitingSQLSize} SQL(s) waiting for acceleration on the threshold of <span class="highlight">{threshold}</span>.',
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
      filesSizeError: 'Files cannot exceed 5M.',
      fileTypeError: 'Invalid file format。',
      waitingList: 'Waiting List ({num})',
      not_accelerated: 'Not Accelerated ({num})',
      accelerated: 'Accelerated ({num})',
      uploadFileTips: 'Supported file formats are txt and sql. Supported file size is up to 5 MB.',
      submitConfirm: '{unCheckedSQL} validated SQL statements are not selected, do you want to ignore them and submit anyway?',
      addSuccess: 'Suceess to imported {importedNum} new SQL statements to the waiting list',
      existedMsg: '({existedNum} SQL statements are duplicated)',
      end: '.'
    },
    'zh-cn': {
      more: '更多',
      acceleration: '加速引擎',
      importSql: '导入 SQL 文件',
      pleImport: '请导入文件',
      sqlFiles: 'SQL 文件',
      createdTime: '创建时间',
      selectedQuery: '已选择的 SQL',
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
      AccQueryStart: '加速',
      AccQueryEnd: '的 SQL 查询（按照使用频率由高到低计算）',
      unit: '秒 / 任务',
      inputSql: '新增查询语句',
      delSql: '确定删除这条查询语句吗？',
      delSqlTitle: '删除查询语句',
      giveUpEdit: '确定放弃本次编辑吗？',
      thereAre: '已有 {waitingSQLSize} 条 SQL 查询等待加速(阈值为 <span class="highlight">{threshold}</span> 条 SQL)',
      accelerateNow: '立即加速',
      messages: '错误信息：',
      suggestion: '修改建议：',
      from: '加速延迟的范围在',
      to: '秒到',
      secondes: '的查询',
      acceleratedSQL: '已加速 SQL',
      checkAll: '全选',
      cancelAll: '取消全选',
      addTofavorite: '提交',
      filesSizeError: '文件大小不能超过 5M!',
      fileTypeError: '不支持的文件格式！',
      waitingList: '未加速 ({num})',
      not_accelerated: '加速失败 ({num})',
      accelerated: '加速完毕 ({num})',
      uploadFileTips: '支持的文件格式为 txt 和 sql，文件最大支持 5 MB。',
      submitConfirm: '还有 {unCheckedSQL} 条正确的 SQL 未被选中，是否忽略并直接提交？',
      addSuccess: '导入的查询中，新增 {importedNum} 条进入待加速列表',
      existedMsg: '（{existedNum} 条已经存在）',
      end: '。'
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
  isUploaded = false
  ruleSettingVisible = false
  blackListVisible = false
  isShowInput = false
  inputHeight = 424
  isEditSql = false
  blackSqlFilter = ''
  whiteSqlFilter = ''
  sqlLoading = false
  activeIndex = 0
  activeSqlObj = null
  whiteSqlData = null
  sqlFormatterObj = {}
  blackSqlData = {size: 0, sqls: []}
  blackSql = ''
  whiteSql = ''
  isReadOnly = true
  validateLoading = false
  fileSizeError = false
  isWhiteErrorMessage = false
  whiteMessages = []
  importLoading = false
  messageInstance = null
  uploadItems = []
  pagerTableData = []
  multipleSelection = []
  selectSqls = []
  submitSqlLoading = false
  filteredDataSize = 0
  whiteCurrentPage = 0
  timer = null
  initTimer = null
  whitePageSize = 10
  activeNames = ['rules']
  filterData = {
    sortBy: 'last_query_time',
    reverse: true,
    pageSize: 10,
    offset: 0
  }
  updateLoading = false
  isShowEditor = true
  ST = null
  stCycle = null
  isPausePolling = false
  rulesObj = {
    freqEnable: true,
    freqValue: 0,
    submitterEnable: true,
    users: [],
    userGroups: [],
    durationEnable: false,
    minDuration: 0,
    maxDuration: 0
  }
  allSubmittersOptions = {
    user: [],
    group: []
  }
  rulesSettingRules = {
    freqValue: [{validator: this.validatePass, trigger: 'blur'}],
    minDuration: [{validator: this.validatePass, trigger: 'blur'}],
    maxDuration: [{validator: this.validatePass, trigger: 'blur'}]
  }
  validatePass (rule, value, callback) {
    if ((!value && value !== 0) && (rule.field === 'freqValue' && this.rulesObj.freqEnable || rule.field.indexOf('Duration') !== -1 && this.rulesObj.durationEnable)) {
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
    this.filterData.reverse = filterData.reverse
    this.filterData.sortBy = filterData.sortBy
    this.loadFavoriteList()
  }

  filterFav (checkedStatus) {
    this.checkedStatus = checkedStatus
    this.loadFavoriteList()
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
  @Watch('inputHeight')
  onHeightChange (val) {
    if (val) {
      this.isShowEditor = false
      this.$nextTick(() => {
        this.isShowEditor = true
      })
    }
  }

  openRuleSetting () {
    if (this.currentSelectedProject) {
      const loadRulesData = new Promise((resolve, reject) => {
        this.getRules({project: this.currentSelectedProject}).then((res) => {
          handleSuccess(res, (data) => {
            this.rulesObj = data
            this.rulesObj.freqValue = this.rulesObj.freqValue * 100
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
        submitData.freqValue = submitData.freqValue / 100
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
        sortBy: this.filterData.sortBy,
        reverse: this.filterData.reverse
      })
      const data = await handleSuccessAsync(res)
      this.favQueList = data
      resolve()
    })
  }

  openImportSql () {
    this.importSqlVisible = true
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
    return Promise.all([this.loadFavoriteList(), this.getSQLSizes()])
  }
  pausePolling () {
    this.isPausePolling = true
  }
  reCallPolling () {
    this.isPausePolling = false
    this.loadFavoriteList()
    this.getSQLSizes()
    this.init()
  }
  async init () {
    clearTimeout(this.stCycle)
    this.stCycle = setTimeout(() => {
      if (!this.isPausePolling) {
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
      }
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
    this.inputHeight = 424
    this.blackSql = ''
    this.isEditSql = false
    this.getBlackList()
  }

  async getBlackList (pageIndex, pageSize) {
    const res = await this.loadBlackList({
      project: this.currentSelectedProject,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      sql: this.blackSqlFilter
    })
    const data = await handleSuccessAsync(res)
    if (data && data.size > 0) {
      this.blackSqlData = data
      this.$nextTick(() => {
        this.viewBlackSql(this.blackSqlData.sqls[0])
      })
    } else {
      this.blackSqlData = {size: 0, sqls: []}
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
  async activeSql (sqlObj) {
    this.activeSqlObj = sqlObj
    this.isEditSql = false
    this.isReadOnly = true
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 424
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 424 - 140
      this.whiteMessages = sqlObj.sqlAdvices
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.$refs.whiteInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({sqls: [sqlObj.sql]})
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
  }

  async editWhiteSql (sqlObj) {
    this.isEditSql = true
    this.inputHeight = 382
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 382
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 382 - 140
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.$refs.whiteInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({sqls: [sqlObj.sql]})
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
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
      return file.name.toLowerCase().indexOf('.txt') !== -1 || file.name.toLowerCase().indexOf('.sql') !== -1
    }).map((item) => {
      totalSize = totalSize + item.size
      return item.raw ? item.raw : item
    })
    if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
      this.messageInstance = this.$message.warning(this.$t('filesSizeError'))
      this.fileSizeError = true
    } else {
      this.fileSizeError = false
    }
    if (!(file.name.toLowerCase().indexOf('.txt') !== -1 || file.name.toLowerCase().indexOf('.sql') !== -1)) {
      this.$message.error(this.$t('fileTypeError'))
    }
  }
  handleRemove (file, fileList) {
    this.messageInstance && this.messageInstance.close()
    this.uploadItems = fileList
    let totalSize = 0
    this.uploadItems.forEach((item) => {
      totalSize = totalSize + item.size
    })
    if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
      this.messageInstance = this.$message.warning(this.$t('filesSizeError'))
      this.fileSizeError = true
    } else {
      this.fileSizeError = false
    }
  }
  selectable (row) {
    return row.capable ? 1 : 0
  }

  resetImport () {
    this.isUploaded = false
    this.uploadItems = []
    this.whiteSqlData = null
    this.activeSqlObj = null
    this.pagerTableData = []
    this.whiteSqlFilter = ''
    this.importLoading = false
    this.sqlFormatterObj = {}
    this.messageInstance && this.messageInstance.close()
  }
  resetBlack () {
    this.sqlFormatterObj = {}
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
      this.importLoading = false
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
      this.inputHeight = 424
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
  submit () {
    this.submitSqlLoading = true
    const sqlsData = this.finalSelectSqls
    const sqls = sqlsData.map((item) => {
      return item.sql
    })
    this.addTofavoriteList({project: this.currentSelectedProject, sqls: sqls}).then((res) => {
      handleSuccess(res, (data) => {
        this.submitSqlLoading = false
        const importedMsg = this.$t('addSuccess', {importedNum: data.imported})
        const existedMsg = data.imported < sqls.length ? this.$t('existedMsg', {existedNum: sqls.length - data.imported}) : ''
        this.$alert(importedMsg + existedMsg + this.$t('end'), this.$t('kylinLang.common.notice'), {
          confirmButtonText: this.$t('kylinLang.common.ok'),
          iconClass: 'el-icon-info primary'
        })
        sqlsData.forEach((item) => {
          this.delWhite(item.id)
        })
        this.loadFavoriteList()
        this.getSQLSizes()
        this.importSqlVisible = false
      })
    }, (res) => {
      handleError(res)
      this.submitSqlLoading = false
      this.importSqlVisible = false
    })
  }
  addTofav () {
    const unCheckedSQL = this.whiteSqlData.capable_sql_num - this.finalSelectSqls.length
    if (unCheckedSQL) {
      kapConfirm(this.$t('submitConfirm', {unCheckedSQL: unCheckedSQL}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), type: 'warning'}).then(() => {
        this.submit()
      })
    } else {
      this.submit()
    }
  }

  onWhiteSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.whiteSqlDatasPageChange(0)
    }, 500)
  }

  cancelEdit (isErrorMes) {
    this.isEditSql = false
    this.inputHeight = isErrorMes ? 424 - 140 : 424
    this.whiteSql = this.sqlFormatterObj[this.activeSqlObj.id]
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
          this.inputHeight = 424
          this.isWhiteErrorMessage = false
          this.isEditSql = false
          this.isReadOnly = true
          for (const key in this.whiteSqlData.data) {
            if (this.whiteSqlData.data[key].id === this.activeSqlObj.id) {
              this.whiteSqlData.data[key].sql = this.whiteSql
              this.sqlFormatterObj[this.activeSqlObj.id] = this.whiteSql
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
          this.inputHeight = 424 - 140
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
        if (this.whiteSqlData.data[key].capable) {
          this.whiteSqlData.capable_sql_num--
        }
        this.whiteSqlData.size--
        this.whiteSqlData.data.splice(key, 1)
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

  async viewBlackSql (row) {
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
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.getBlackList()
    }, 500)
  }

  blackSqlDatasPageChange (offset, pageSize) {
    this.getBlackList(offset, pageSize)
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
    .importSqlDialog,
    .blackListDialog {
      .ksd-null-pic-text {
        margin: 122.5px 0;
      }
      .query-count {
        color: @text-title-color;
        line-height: 40px;
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
      .el-icon-ksd-filter {
        position: relative;
        left: 5px;
        &.isFilter,
        &:hover {
          color: @base-color;
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
</style>
