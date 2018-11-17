<template>
  <div id="favoriteQuery">
    <div class="clearfix ksd-mb-10 ksd-mt-20 rules_tools">
      <div class="ksd-fleft">
        <span class="ksd-fs-16" style="line-height:52px;">
          <span v-html="$t('thereAre', {threshold: preSettingObj.threshold, modelSpeedEvents: modelSpeedEvents})"></span>
          <el-button type="primary" class="ksd-ml-10" plain size="medium" v-if="modelSpeedEvents>0" @click="applySpeed">{{$t('accelerateNow')}}</el-button>
        </span>
      </div>
      <div class="ksd-fright btn-group ksd-mt-10">
        <el-button size="medium" icon="el-icon-ksd-acclerate_pendding" plain @click="openPreferrenceSetting">
          {{$t('preferrence')}}
        </el-button>
        <el-button size="medium" icon="el-icon-ksd-white_list" plain @click="openWhiteList">{{$t('whiteList')}}
          <el-tooltip placement="left">
            <div slot="content">{{$t('whiteListDesc')}}</div>
            <i class="el-icon-ksd-what el-icon--right"></i>
          </el-tooltip>
        </el-button>
        <el-button size="medium" icon="el-icon-ksd-black_list" plain @click="openBlackList">{{$t('blackList')}}
          <el-tooltip placement="left">
            <div slot="content">{{$t('blackListDesc')}}</div>
            <i class="el-icon-ksd-what el-icon--right"></i>
          </el-tooltip>
        </el-button>
      </div>
    </div>
    <el-collapse v-model="activeNames" class="favorite-rules">
      <el-collapse-item name="rules">
        <template slot="title">
          {{$t('favoriteRules')}}
          <el-tooltip placement="right">
            <div slot="content">{{$t('favRulesDesc')}}</div>
            <i class="el-icon-ksd-what"></i>
          </el-tooltip>
        </template>
        <el-row>
          <el-col :span="18" class="rules-conds">
            <div class="conds" :class="{'disabled': !frequencyObj.enable}">
              <div class="conds-title">
                <span>{{$t('queryFrequency')}}</span>
                <el-switch class="ksd-switch" v-model="frequencyObj.enable" active-text="ON" inactive-text="OFF" @change="updateFre"></el-switch>
                <el-button type="primary" size="medium" text class="ksd-fright edit-conds" @click="editFrequency">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content clearfix">
                <el-slider class="show-only" v-model="frequencyObj.freqValue" :step="0.1" button-type="sharp" :min="0" :max="1" show-dynamic-values disabled :format-tooltip="formatTooltip"></el-slider>
                <div class="ksd-fright ksd-mt-10 ksd-fs-12">TopX% {{$t('queryFrequency')}}</div>
              </div>
            </div>
            <div class="conds" :class="{'disabled': !submitterObj.enable}">
              <div class="conds-title">
                <span>{{$t('querySubmitter')}}</span>
                <el-switch class="ksd-switch" v-model="submitterObj.enable" active-text="ON" inactive-text="OFF" @change="updateSub"></el-switch>
                <el-button type="primary" size="medium" text class="ksd-fright edit-conds" @click="editSubmitter">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content">
                <div class="users">
                  <i class="el-icon-ksd-table_admin"></i> <span class="ksd-fs-24">{{submitterObj.users.length}}</span> Users
                  <i class="el-icon-ksd-table_group"></i> <span class="ksd-fs-24">{{submitterObj.groups.length}}</span> Groups
                </div>
              </div>
            </div>
            <div class="conds" :class="{'disabled': !durationObj.enable}">
              <div class="conds-title">
                <span>{{$t('queryDuration')}}</span>
                <el-switch class="ksd-switch" v-model="durationObj.enable" active-text="ON" inactive-text="OFF" @change="updateDura"></el-switch>
                <el-button type="primary" size="medium" text class="ksd-fright edit-conds" @click="editDuration">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content clearfix">
                <el-slider class="show-only" v-model="durationObj.durationValue" :step="1" range button-type="sharp" :min="0" :max="180" show-dynamic-values disabled></el-slider>
                <div class="ksd-fright ksd-mt-10 ksd-fs-12">{{$t('unit')}}</div>
              </div>
            </div>
          </el-col>
          <el-col :span="6" class="fillgauge-block">
            <div class="conds-title">
              <span>{{$t('ruleImpact')}}</span>
              <el-tooltip placement="left">
                <div slot="content">{{$t('ruleImpactDesc')}}</div>
                <i class="el-icon-ksd-what"></i>
              </el-tooltip>
            </div>
            <svg id="fillgauge" width="100%" height="150"></svg>
          </el-col>
        </el-row>
      </el-collapse-item>
    </el-collapse>
    <div class="open_tips" v-if="!activeNames[0]">{{$t('openTips')}}</div>
    <div class="ksd-title-label ksd-mt-10 ksd-mb-10">
      <span>{{$t('kylinLang.menu.favorite_query')}}</span>
      <el-tooltip placement="right">
        <div slot="content" v-html="$t('favDesc')"></div>
        <i class="el-icon-ksd-what"></i>
      </el-tooltip>
    </div>
    <el-table
      :data="favQueList.favorite_queries"
      border
      class="favorite-table"
      ref="favoriteTable"
      style="width: 100%">
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_pattern" header-align="center" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_query_time" sortable header-align="center" width="210">
        <template slot-scope="props">
          {{transToGmtTime(props.row.last_query_time)}}
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.rate')" prop="success_rate" sortable align="center" width="135">
        <template slot-scope="props">
          {{props.row.success_rate * 100 | number(2)}}%
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" prop="totalCount" sortable align="center" width="120"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.avgDuration')" prop="average_duration" sortable align="center" width="160">
        <template slot-scope="props">
          <span v-if="props.row.average_duration < 1000"> &lt; 1s</span>
          <span v-else>{{props.row.average_duration / 1000 | fixed(2)}}s</span>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" align="center" width="120">
        <template slot-scope="props">
          <el-tooltip class="item" effect="dark" :content="$t('kylinLang.query.fullyAcce')" placement="top" v-if="props.row.status === 'FULLY_ACCELERATED'">
            <i class="status-icon el-icon-ksd-acclerate_all"></i>
          </el-tooltip>
          <el-tooltip class="item" effect="dark" :content="$t('kylinLang.query.partlyAcce')" placement="top" v-if="props.row.status === 'PARTLY_ACCELERATED'">
            <i class="status-icon el-icon-ksd-acclerate_portion"></i>
          </el-tooltip>
          <el-tooltip class="item" effect="dark" :content="$t('kylinLang.query.ongoingAcce')" placement="top" v-if="props.row.status === 'ACCELERATING'">
            <i class="status-icon el-icon-ksd-acclerate_ongoing"></i>
          </el-tooltip>
          <el-tooltip class="item" effect="dark" :content="$t('kylinLang.query.wartingAcce')" placement="top" v-if="props.row.status === 'WAITING'">
            <i class="status-icon el-icon-ksd-acclerate_pendding"></i>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
    <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      :visible.sync="preferrenceVisible"
      width="440px"
      class="preferrenceDialog">
      <span slot="title" class="ky-list-title">{{$t('preferrence')}}</span>
      <div class="batch">
        <span class="ky-list-title">{{$t('acceThreshold')}}</span>
        <el-switch class="ksd-switch" v-model="preSettingObj.batch_enabled" active-text="ON" inactive-text="OFF"></el-switch>
        <div class="setting">
          <span>{{$t('notifyLeftTips')}}</span>
          <el-input size="small" class="acce-input" v-model="preSettingObj.threshold"></el-input>
          <span>{{$t('notifyRightTips')}}</span>
        </div>
      </div>
      <div class="divider-line"></div>
      <div class="resource">
        <span class="ky-list-title">{{$t('acceResource')}}</span>
        <div class="ksd-mt-10 ksd-mb-10">{{$t('reasourceDsec')}}</div>
        <el-radio-group v-model="preSettingObj.auto_apply">
          <el-radio :label="true">{{$t('ressourceYse')}}</el-radio>
          <el-radio :label="false">{{$t('ressourceNo')}}</el-radio>
        </el-radio-group>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="preferrenceVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" plain @click="savePreferrence">{{$t('kylinLang.common.save')}}</el-button>
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
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
      </span>
      <span class="ksd-title-label">{{$t('kylinLang.query.sqlContent_th')}}</span>
      <el-row :gutter="20">
        <el-col :span="16">
          <div class="clearfix ksd-mt-10">
            <div class="btn-group ksd-fleft">
              <el-button type="primary" size="medium" plain @click="newBlackSql">{{$t('inputSql')}}</el-button>
            </div>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="blackSqlFilter" @input="onblackSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <div class="sqlLists">
            <div class="ksd-null-pic-text" v-if="!blackSqlList.size">
              <img  src="../../assets/img/no_data.png" />
              <p>{{$t('kylinLang.common.noData')}}</p>
            </div>
            <div v-for="(sqlObj, index) in blackSqlList.sqls" :key="index" class="sqlBox" :class="{'active': index == activeIndex}" @click.stop="viewBlackSql(sqlObj.sql, index)" v-else>
              <span>{{transformSql(sqlObj.sql)}}</span>
              <div class="group-btn">
                <el-button type="primary" size="small" text @click.stop="delBlack(sqlObj.id)">{{$t('kylinLang.common.delete')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="blackInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="blackSql">
            </kap-editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
                <el-button size="medium" @click="clearSql">{{$t('kylinLang.query.clear')}}</el-button>
                <el-button type="primary" size="medium" plain @click="submitBlackSql()">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
            </div>
          </div>
          <div class="error_messages" v-if="isBlackErrorMessage">
            <div v-for="(mes, index) in blackMessages" :key="index">
              <div class="label">{{$t('messages')}}</div>
              <p>{{mes.incapableReason}}</p>
              <div class="label ksd-mt-10">{{$t('suggestion')}}</div>
              <p>{{mes.suggestion}}</p>
            </div>
          </div>
        </el-col>
      </el-row>
      <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="blackSqlList.size"  v-on:handleCurrentChange='blackSqlListsPageChange' :perPageSize="5" v-if="blackSqlList.size > 0"></kap-pager>
    </el-dialog>
    <el-dialog
      :visible.sync="whiteListVisible"
      top="5vh"
      width="1180px"
      class="whiteListDialog">
      <span slot="title" class="ky-list-title">{{$t('whiteList')}}
        <el-tooltip placement="left">
          <div slot="content">{{$t('whiteListDesc')}}</div>
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
      </span>
      <span class="ksd-title-label">{{$t('kylinLang.query.sqlContent_th')}}</span>
      <el-row :gutter="20">
        <el-col :span="16">
          <div class="clearfix ksd-mt-10">
            <div class="btn-group ksd-fleft">
              <el-upload class="ksd-fleft"
                :headers="uploadHeader"
                :action="actionUrl"
                :data="uploadData"
                multiple :auto-upload="true"
                :on-success="uploadSuccess"
                :on-error="uploadError"
                :show-file-list="false">
                <el-button type="primary" size="medium" plain icon="el-icon-ksd-query_import">{{$t('kylinLang.common.import')}}
                </el-button>
              </el-upload>
            </div>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <div class="sqlLists">
            <div class="ksd-null-pic-text" v-if="!whiteSqlList.size">
              <img  src="../../assets/img/no_data.png" />
              <p>{{$t('kylinLang.common.noData')}}</p>
            </div>
            <div v-for="(sqlObj, index) in whiteSqlList.sqls" :key="index" class="sqlBox" :class="{'active': index == activeIndex}" @click="activeSql(sqlObj, index)" v-else>
              <span>{{transformSql(sqlObj.sql)}}</span>
              <i class="el-icon-ksd-alert" v-if="!sqlObj.capable"></i>
              <div class="group-btn">
                <el-button size="small" type="primary" v-show="!isEditSql||index!==activeIndex" @click.stop="editWhiteSql(sqlObj, index)" text>{{$t('kylinLang.common.edit')}}</el-button>
                <el-button type="primary" size="small" text @click.stop="delWhite(sqlObj.id)">{{$t('kylinLang.common.delete')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="query_panel_box ksd-mt-10">
            <kap-editor ref="whiteInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="whiteSql">
            </kap-editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
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
      <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="whiteSqlList.size"  v-on:handleCurrentChange='whiteSqlListsPageChange' :perPageSize="5" v-if="whiteSqlList.size > 0"></kap-pager>
    </el-dialog>
    <el-dialog
      :visible.sync="frequencyVisible"
      width="440px"
      :title="$t('queryFrequency')"
      class="frequencyDialog">
      <div class="conds">
        <div class="conds-content clearfix">
          <div class="desc">{{$t('frequencyDesc')}}</div>
          <el-slider v-model="oldFrequencyValue" :step="0.1" button-type="sharp" :min="0" :max="1" show-dynamic-values :format-tooltip="formatTooltip"></el-slider>
          <div class="ksd-fright ksd-mt-10 ksd-fs-12">TopX% {{$t('queryFrequency')}}</div>
        </div>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="cancelFrequency" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="saveFrequency" size="medium">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      :visible.sync="submitterVisible"
      width="440px"
      :title="$t('querySubmitter')"
      class="submitterDialog">
      <div class="conds">
        <div class="conds-content">
          <div class="desc">{{$t('submitterDesc')}}</div>
        </div>
        <div class="conds-footer">
          <el-select v-model="selectedUser" v-event-stop :popper-append-to-body="false" filterable size="medium" placeholder="VIP User" class="ksd-mt-10" @change="selectUserChange">
            <el-option-group v-for="group in options" :key="group.label" :label="group.label">
              <el-option v-for="item in group.options" :key="item" :label="item" :value="item"></el-option>
            </el-option-group>
          </el-select>
          <div class="vip-users-block ksd-mb-10">
            <div class="ksd-mt-10"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
            <div class="vip-users">
              <el-tag
                v-for="(user, index) in oldSubmitterUsers"
                :key="index"
                closable
                class="user-label"
                size="small"
                @close="removeUser(index)">
                {{user}}
              </el-tag>
            </div>
          </div>
        </div>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="cancelSubmitter" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="saveSubmitter" size="medium">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      :visible.sync="durationVisible"
      width="440px"
      :title="$t('queryDuration')"
      class="durationDialog">
      <div class="conds">
        <div class="conds-content clearfix">
          <div class="desc">{{$t('durationDesc')}}</div>
          <el-slider v-model="oldDurationValue" :step="1" range button-type="sharp" :min="0" :max="180" show-dynamic-values></el-slider>
          <div class="ksd-fright ksd-mt-10 ksd-fs-12">{{$t('unit')}}</div>
        </div>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="cancelDuration" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="saveDuration" size="medium">{{$t('kylinLang.common.save')}}</el-button>
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
import { loadLiquidFillGauge, liquidFillGaugeDefaultSettings } from '../../util/liquidFillGauge'
import sqlFormatter from 'sql-formatter'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      getFrequency: 'GET_FREQUENCY',
      getSubmitter: 'GET_SUBMITTER',
      getDuration: 'GET_DURATION',
      getRulesImpact: 'GET_RULES_IMPACT',
      getPreferrence: 'GET_PREFERRENCE',
      updateFrequency: 'UPDATE_FREQUENCY',
      updateSubmitter: 'UPDATE_SUBMITTER',
      updateDuration: 'UPDATE_DURATION',
      updatePreferrence: 'UPDATE_PREFERRENCE',
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
  locales: {
    'en': {preferrence: 'Preference', whiteList: 'White List', blackList: 'Black List', favDesc: 'Favorite queries are from both favorite rule filtered query and user defined query.<br/> Favorite query represent your main business analysis scenarios and critical decision point.<br/> System will optimize its to max performance by auto-modeling and pre-calculating.', favoriteRules: 'Favorite Rules', favRulesDesc: 'By filtering SQL\'s frequency, duration and submitter, favorite rule will catch up frequently used and business critical queries.', queryFrequency: 'Query Frequency', querySubmitter: 'Query Submitter', queryDuration: 'Query Duration', frequencyDesc: 'Optimize queries frequently used over last 24 hours', submitterDesc: 'Optimize queries from critical users and groups', durationDesc: 'Optimize queries with long duration', unit: 'Seconds / Job', inputSql: 'Add SQL', delSql: 'Are you sure to delete this sql?', giveUpEdit: 'Are you sure to give up the editor?', acceThreshold: 'Accelerating Threshold', notifyLeftTips: 'Notify me every time when there are ', notifyRightTips: ' favorite queries.', acceResource: 'Accelerating Resource', reasourceDsec: 'The system should ask me for permission for using storage and computing resource to accelerate favorite queries.', ressourceYse: 'Yes', ressourceNo: 'No, I don\'t need to know', whiteListDesc: 'White list helps to manage user manually defined favorite SQLs, especially for SQLs from query history list and imported SQL files.', blackListDesc: 'Black list helps to manage SQLs which are undesired for accelerating, especially for those SQLs will require unreasonable large storage or computing resource to accelerate.', ruleImpact: 'Rules Impact', ruleImpactDesc: 'Percentage of SQL queries selected by the favorite rule.', thereAre: 'There are {modelSpeedEvents} SQLs waiting for acceleration on the threshold of <span class="highlight">{threshold}</span>.', accelerateNow: 'Accelerate now', openTips: 'Expand this block to set the "Acceleration Rule"', messages: 'Error Messages:', suggestion: 'Suggestion:'},
    'zh-cn': {preferrence: '加速偏好', whiteList: '白名单', blackList: '黑名单', favDesc: '经过加速规则筛选或者用户主动选择的SQL查询将成为加速查询。<br/>这类查询可以代表最主要的业务分析和重要的业务决策点。<br/>系统将对其进行自动建模和预计算，确保查询效率得到提升。', favRulesDesc: '加速规则过滤不同SQL查询的频率、时长、用户等特征，筛选出高频使用的、对业务分析重要的SQL查询。', favoriteRules: '加速规则', queryFrequency: '查询频率', querySubmitter: '查询用户', queryDuration: '查询时长', frequencyDesc: '优化过去24小时内查询频率较高的查询', submitterDesc: '优化重要⽤用户或⽤用户组发出的查询', durationDesc: '优化慢查询', unit: '秒 / 任务', inputSql: '新增查询语句', delSql: '确定删除这条查询语句吗？', giveUpEdit: '确定放弃本次编辑吗？', acceThreshold: '加速阈值', notifyLeftTips: '每积累', notifyRightTips: ' 条加速查询时，提醒我。', acceResource: '加速资源', reasourceDsec: '系统需要获取存储资源和计算资源来加速查询时，请征询我的许可。', ressourceYse: '征询许可', ressourceNo: '不需要征询', whiteListDesc: '本列列表管理理⽤用户⼈人为指定加速的SQL查询。⼀一般指⽤用户从查询历史指定或导⼊入的查询⽂文件。', blackListDesc: '本列列表管理理⽤用户不不希望被加速的SQL查询。⼀一般是指加速时对存储空间、计算⼒力力需求过⼤大的查询。', ruleImpact: '加速规则影响⼒', ruleImpactDesc: '被加速规则选出的SQL查询的百分⽐。', thereAre: '已有{modelSpeedEvents}条SQL查询等待加速(阈值为<span class="highlight">{threshold}</span>条SQL)', accelerateNow: '立即加速', openTips: '展开此区块可设定"加速规则"', messages: '错误信息：', suggestion: '修改建议：'}
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  statusFilteArr = [{name: 'el-icon-ksd-acclerate_all', value: 'FULLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_pendding', value: 'WAITING'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ongoing', value: 'ACCELERATING'}]
  checkedStatus = []
  preferrenceVisible = false
  blackListVisible = false
  whiteListVisible = false
  frequencyVisible = false
  submitterVisible = false
  durationVisible = false
  isShowInput = false
  inputHeight = 564
  isEditSql = false
  blackSqlFilter = ''
  whiteSqlFilter = ''
  activeIndex = 0
  whiteSqlList = []
  blackSqlList = []
  blackSql = ''
  whiteSql = ''
  isWhiteErrorMessage = false
  isBlackErrorMessage = false
  whiteMessages = []
  blackMessages = []
  preSettingObj = {
    auto_apply: false,
    batch_enabled: true,
    threshold: 20
  }
  favoriteCurrentPage = 1
  activeNames = ['rules']
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: null
  }
  frequencyObj = {
    enable: true,
    freqValue: 0.2
  }
  submitterObj = {
    enable: true,
    users: ['ADMIN'],
    groups: []
  }
  durationObj = {
    enable: true,
    durationValue: [50, 80]
  }
  selectedUser = ''
  options = [{
    label: this.$t('kylinLang.menu.user'),
    options: ['ADMIN']
  }]
  oldFrequencyValue = 0.2
  oldSubmitterUsers = ['ADMIN']
  oldDurationValue = [50, 80]
  impactRatio = 55

  applySpeed (event) {
    this.applySpeedInfo({size: this.modelSpeedEvents, project: this.currentSelectedProject}).then(() => {
      this.flyEvent(event)
      this.getSpeedInfo(this.currentSelectedProject)
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

  get modelSpeedEvents () {
    return this.$store.state.model.modelSpeedEvents
  }

  editFrequency () {
    this.frequencyVisible = true
    this.oldFrequencyValue = this.frequencyObj.freqValue
  }

  cancelFrequency () {
    this.frequencyVisible = false
  }

  saveFrequency () {
    this.frequencyVisible = false
    this.frequencyObj.freqValue = this.oldFrequencyValue
    this.updateFre()
  }

  updateFre () {
    this.updateFrequency({
      project: this.currentSelectedProject,
      enable: this.frequencyObj.enable,
      freqValue: this.frequencyObj.freqValue
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
      this.getFrequencyObj()
    })
  }

  editSubmitter () {
    this.submitterVisible = true
    this.oldSubmitterUsers = this.submitterObj.users.slice(0)
  }

  cancelSubmitter () {
    this.submitterVisible = false
  }

  saveSubmitter () {
    this.submitterVisible = false
    this.submitterObj.users = this.oldSubmitterUsers.slice(0)
    this.updateSub()
  }

  updateSub () {
    this.updateSubmitter({
      project: this.currentSelectedProject,
      enable: this.submitterObj.enable,
      users: this.submitterObj.users,
      groups: null
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
      this.getSubmitterObj()
    })
  }

  selectUserChange (val) {
    this.oldSubmitterUsers.push(val)
    const index = this.options[0].options.indexOf(val)
    this.selectedUser = ''
    this.options[0].options.splice(index, 1)
  }

  removeUser (index) {
    const user = this.oldSubmitterUsers[index]
    this.oldSubmitterUsers.splice(index, 1)
    this.options[0].options.push(user)
  }

  editDuration () {
    this.durationVisible = true
    this.oldDurationValue = this.durationObj.durationValue
  }

  cancelDuration () {
    this.durationVisible = false
  }

  saveDuration () {
    this.durationVisible = false
    this.durationObj.durationValue = this.oldDurationValue
    this.updateDura()
  }

  updateDura () {
    this.updateDuration({
      project: this.currentSelectedProject,
      enable: this.durationObj.enable,
      durationValue: this.durationObj.durationValue
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
      this.getDurationObj()
    })
  }

  loadRuleImpactRatio () {
    this.getRulesImpact({project: this.currentSelectedProject})
  }

  async loadFavoriteList (pageIndex, pageSize) {
    const res = await this.getFavoriteList({
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      accelerateStatus: this.checkedStatus
    })
    const data = await handleSuccessAsync(res)
    this.favQueList = data
  }

  formatTooltip (val) {
    return val * 100
  }

  filterFav () {
    this.loadFavoriteList()
  }

  openPreferrenceSetting () {
    this.preferrenceVisible = true
  }

  savePreferrence () {
    this.preSettingObj['project'] = this.currentSelectedProject
    this.updatePreferrence(this.preSettingObj).then((res) => {
      this.preferrenceVisible = false
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }, (res) => {
      handleError(res)
    })
  }

  getFormatterSql (sql) {
    return sqlFormatter.format(sql)
  }

  getFrequencyObj () {
    this.getFrequency({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.frequencyObj = data
      })
    }, (res) => {
      handleError(res)
    })
  }

  getSubmitterObj () {
    this.getSubmitter({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.submitterObj = data
        this.oldSubmitterUsers = data.users
      })
    }, (res) => {
      handleError(res)
    })
  }

  getDurationObj () {
    this.getDuration({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.durationObj = data
        this.oldDurationValue = data.durationValue
      })
    }, (res) => {
      handleError(res)
    })
  }

  created () {
    this.loadFavoriteList()
    this.getFrequencyObj()
    this.getSubmitterObj()
    this.getDurationObj()
    this.getPreferrence({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.preSettingObj = data
      })
    }, (res) => {
      handleError(res)
    })
  }

  drawImpactChart () {
    loadLiquidFillGauge('fillgauge', this.impactRatio)
    const config1 = liquidFillGaugeDefaultSettings()
    config1.circleColor = '#FF7777'
    config1.textColor = '#FF4444'
    config1.waveTextColor = '#FFAAAA'
    config1.waveColor = '#FFDDDD'
    config1.circleThickness = 0.2
    config1.textVertPosition = 0.2
    config1.waveAnimateTime = 1000
  }

  mounted () {
    this.$nextTick(() => {
      $('#favo-menu-item').removeClass('rotateY').css('opacity', 0)
      this.drawImpactChart()
      window.onresize = () => {
        $('#fillgauge').empty()
        this.drawImpactChart()
      }
    })
  }

  pageCurrentChange (offset, pageSize) {
    this.favoriteCurrentPage = offset + 1
    this.loadFavoriteList(offset, pageSize)
  }

  openBlackList () {
    this.blackListVisible = true
    this.activeIndex = -1
    this.inputHeight = 564
    this.blackSql = ''
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
  }

  async getBlackList (pageIndex, pageSize) {
    const res = await this.loadBlackList({
      project: this.currentSelectedProject,
      limit: pageSize || 10,
      offset: pageIndex || 0
    })
    const data = await handleSuccessAsync(res)
    this.blackSqlList = data
  }

  openWhiteList () {
    this.whiteListVisible = true
    this.activeIndex = 0
    setTimeout(() => {
      this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
    }, 0)
    this.getWhiteList()
    this.whiteSqlList.size > 0 && this.activeSql(this.whiteSqlList.sqls[0], 0)
  }

  activeSql (sqlObj, index) {
    this.whiteSql = this.formatterSql(sqlObj.sql)
    this.activeIndex = index
    this.isEditSql = false
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 564
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 564 - 150
      this.whiteMessages = sqlObj.sqlAdvices
    }
    this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
  }

  editWhiteSql (sqlObj, index) {
    this.isEditSql = true
    this.inputHeight = 512
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 512
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 512 - 150
    }
    this.whiteSql = this.formatterSql(sqlObj.sql)
    this.activeIndex = index
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
  uploadSuccess (response) {
    this.$message({
      type: 'success',
      message: this.$t('kylinLang.common.actionSuccess')
    })
    this.getWhiteList()
  }
  uploadError (err, file, fileList) {
    handleError({
      data: JSON.parse(err.message),
      status: err.status
    })
  }

  saveWhiteSql () {
    this.saveWhite({sql: this.whiteSql, id: this.whiteSqlList.sqls[this.activeIndex].id, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        if (data.capable) {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.actionSuccess')
          })
          this.getWhiteList()
        } else {
          this.whiteMessages = data.sqlAdvices
          this.inputHeight = 512 - 150
          this.isWhiteErrorMessage = true
        }
      })
    }, (res) => {
      handleError(res)
    })
  }

  toView (sql, index) {
    this.inputHeight = 564
    this.activeIndex = index
    this.isEditSql = false
    this.blackSql = this.formatterSql(sql)
    this.$refs.blackInputBox.$refs.kapEditor.editor.setReadOnly(true)
  }

  viewBlackSql (sql, index) {
    this.isBlackErrorMessage = false
    if (this.blackSql && this.isEditSql) {
      kapConfirm(this.$t('giveUpEdit')).then(() => {
        this.toView(sql, index)
      })
    } else {
      this.toView(sql, index)
    }
  }

  newBlackSql () {
    this.isEditSql = true
    this.inputHeight = 512
    this.isBlackErrorMessage = false
    this.blackSql = ''
    this.activeIndex = -1
    this.$refs.blackInputBox.$refs.kapEditor.editor.setReadOnly(false)
  }

  submitBlackSql () {
    this.addBlack({sql: this.blackSql, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        if (data.capable) {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.actionSuccess')
          })
          this.getBlackList()
        } else {
          this.blackMessages = data.sqlAdvices
          this.isBlackErrorMessage = true
          this.inputHeight = 512 - 150
        }
      })
    }, (res) => {
      handleError(res)
    })
  }

  clearSql () {
    this.blackSql = ''
    this.isBlackErrorMessage = false
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
    this.getBlackList(offset + 1, pageSize)
  }

  whiteSqlListsPageChange (offset, pageSize) {
    this.getWhiteList(offset + 1, pageSize)
  }

  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.statusFilteArr.length; i++) {
      items.push(<el-checkbox label={this.statusFilteArr[i].value} key={this.statusFilteArr[i].value}><i class={this.statusFilteArr[i].name}></i></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.acceleration_th')}</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.checkedStatus} onInput={val => (this.checkedStatus = val)} onChange={this.filterFav}>
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
  #favoriteQuery {
    padding: 0px 20px 50px 20px;
    .favorite-rules {
      border-top: 0;
      .el-collapse-item__arrow {
        border: 1px solid @text-disabled-color;
        line-height: 1.4;
        padding: 0 17px;
        position: relative;
        top: 12px;
        border-radius: 2px;
        font-size: 16px;
        &:hover {
          border: 1px solid @base-color;
          color: @base-color;
        }
        &.is-active {
          transform: none;
          &:before {
            content: "\E603";
          }
        }
      }
      .el-collapse-item__wrap {
        border-bottom: 0;
      }
      .el-collapse-item__header {
        font-size: 16px;
        border-bottom: 0;
        &:hover {
          .el-collapse-item__arrow {
            border: 1px solid @base-color;
            color: @base-color;
          }
        }
      }
      .el-collapse-item__content {
        padding-bottom: 0;
        background-color: @table-stripe-color;
        height: 170px;
        line-height: 1;
        .rules-conds {
          border-radius: 2px;
          display: flex;
          padding: 10px;
          .conds {
            width: 33.34%;
            padding: 0 10px;
            height: 150px;
            .conds-title {
              font-size: 14px;
              font-weight: 500;
              height: 50px;
              line-height: 50px;
              color: @text-title-color;
              border-bottom: 1px solid @line-border-color;
            }
            .conds-content {
              font-size: 14px;
              .desc {
                line-height: 15px;
                color: @text-title-color;
                margin-top: 15px;
              }
              .el-slider {
                width: 95%;
              }
              .show-only {
                width: 100%;
                .el-slider__runway {
                  border-radius: 0;
                  margin: 20px 0;
                }
                .el-slider__runway.disabled .el-slider__bar {
                  background-color: @base-color;
                }
                .el-slider__valueStop,
                .el-slider__button {
                  visibility: hidden;
                }
                .el-slider__values:last-child {
                  margin-left: -20px;
                }
              }
              .users {
                margin: 15px 0;
                .el-icon-ksd-table_admin {
                  font-size: 16px;
                }
                .el-icon-ksd-table_group {
                  font-size: 16px;
                  margin-left: 40px;
                }
              }
            }
            .edit-conds {
              display: none;
              position: relative;
              top: 10px;
            }
            &:hover {
              background-color: @fff;
              box-shadow: 0 0 6px 0 #cfd8dc, 0 2px 4px 0 #cfd8dc;
              .edit-conds {
                display: inline-block;
              }
            }
            &.disabled {
              &:hover {
                .edit-conds {
                  display: none;
                }
              }
              .el-slider__runway.disabled .el-slider__bar {
                background-color: @text-disabled-color !important;
              }
            }
          }
        }
        .fillgauge-block {
          text-align: center;
          padding: 10px;
          position: relative;
          .conds-title {
            position: absolute;
            top: 65px;
            width: 95%;
          }
        }
      }
    }
    .rules_tools {
      background-color: @base-color-10;
      height: 52px;
      padding: 0 10px 0 25px;
      font-size: 0px;
      .highlight {
        font-weight: 500px;
      }
    }
    .open_tips {
      height: 44px;
      line-height: 44px;
      background-color: @table-stripe-color;
      font-size: 12px;
      text-align: center;
    }
    .table-title {
      color: @text-title-color;
      font-size: 16px;
      line-height: 32px;
    }
    .highlight {
      color: @base-color;
    }
    .preferrenceDialog {
      .batch {
        .setting {
          margin-top: 5px;
          font-size: 14px;
          color: @text-title-color;
        }
        .acce-input {
          width: 50px;
          .el-input__inner {
            text-align: right;
          }
        }
      }
      .divider-line {
        border-top: 1px solid @line-border-color;
        margin: 20px -20px;
      }
    }
    .submitterDialog {
      .vip-users {
        .user-label {
          font-size: 14px;
          margin-right: 8px;
          margin-top: 5px;
        }
      }
    }
    .blackListDialog,
    .whiteListDialog {
      .el-dialog {
        min-height: 500px;
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
        .sqlLists {
          .sqlBox {
            border: 1px solid @text-secondary-color;
            border-radius: 2px;
            padding: 10px;
            background-color: @aceditor-bg-color;
            margin-top: 10px;
            height: 75px;
            overflow-y: scroll;
            position: relative;
            .el-icon-ksd-alert {
              position: absolute;
              right: 10px;
              top: 10px;
              color: @warning-color-1;
            }
            .group-btn {
              position: absolute;
              right: 10px;
              bottom: 0px;
              display: none;
            }
            &:hover {
              box-shadow: 0 2px 4px 0 @line-border-color, 0 0 6px 0 @line-border-color;
              .group-btn {
                display: inline-block;
              }
            }
            &.active {
              border-color: @base-color;
              .group-btn {
                display: inline-block;
              }
            }
          }
        }
      }
    }
    .whiteListDialog {
      .el-dialog {
        .sqlLists {
          .sqlBox {
            overflow-y: hidden;
          }
        }
      }
    }
    .favorite-table {
      .status-icon {
        font-size: 20px;
        &.el-icon-ksd-acclerate_pendding,
        &.el-icon-ksd-acclerate_ongoing {
          color: @base-color;
        }
        &.el-icon-ksd-acclerate_portion,
        &.el-icon-ksd-acclerate_all {
          color: @normal-color-1;
        }
      }
      .el-icon-ksd-filter {
        position: relative;
        top: 2px;
      }
    }
    .fav-dropdown {
      .el-icon-ksd-table_setting {
        color: inherit;
      }
    }
  }
</style>
