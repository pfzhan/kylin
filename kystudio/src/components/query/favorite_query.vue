<template>
  <div id="favoriteQuery">
    <div class="clearfix ksd-mb-10 ksd-mt-20 rules_tools">
      <div class="ksd-fleft ksd-mt-10">
        <span><span v-html="$t('thereAre')"></span>
          <el-button type="primary" plain size="medium">{{$t('accelerateNow')}}</el-button>
        </span>
      </div>
      <div class="ksd-fright btn-group">
        <el-button size="medium" icon="el-icon-ksd-acclerate_ready" plain @click="openPreferrenceSetting">
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
          <span v-if="props.row.average_duration < 1000"> < 1s</span>
          <span v-else>{{props.row.average_duration / 1000 | fixed(2)}}s</span>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" align="center" width="120">
        <template slot-scope="props">
          <i class="status-icon" :class="{
            'el-icon-ksd-acclerate': props.row.status === 'FULLY_ACCELERATED',
            'el-icon-ksd-acclerate_portion': props.row.status === 'PARTLY_ACCELERATED',
            'el-icon-ksd-acclerate_ready': props.row.status === 'WAITING',
            'el-icon-ksd-acclerate_ongoing': props.row.status === 'ACCELERATING'
          }"></i>
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
        <el-switch class="ksd-switch" v-model="preSettingObj.auto" active-text="ON" inactive-text="OFF"></el-switch>
        <div class="setting">
          <span>{{$t('notifyLeftTips')}}</span>
          <el-input size="small" class="acce-input" v-model="preSettingObj.accelerateThreshold"></el-input>
          <span>{{$t('notifyRightTips')}}</span>
        </div>
      </div>
      <div class="divider-line"></div>
      <div class="resource">
        <span class="ky-list-title">{{$t('acceResource')}}</span>
        <div class="ksd-mt-10 ksd-mb-10">{{$t('reasourceDsec')}}</div>
        <el-radio-group v-model="preSettingObj.accumulateFavorites">
          <el-radio :label="true">{{$t('ressourceYse')}}</el-radio>
          <el-radio :label="false">{{$t('ressourceNo')}}</el-radio>
        </el-radio-group>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="preferrenceVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" plain @click="preferrenceVisible = false">{{$t('kylinLang.common.save')}}</el-button>
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
            <div class="ksd-null-pic-text" v-if="!sqlLists.length">
              <img  src="../../assets/img/no_data.png" />
              <p>{{$t('kylinLang.common.noData')}}</p>
            </div>
            <div v-for="(sql, index) in sqlLists" :key="index" class="sqlBox" :class="{'active': index == activeIndex}" v-else>
              <span>{{transformSql(sql)}}</span>
              <div class="group-btn">
                <el-button size="small" type="primary" @click.stop="viewBlackSql(sql, index)" text>{{$t('kylinLang.common.view')}}</el-button>
                <el-button type="primary" size="small" text @click="delBlack(sql, index)">{{$t('kylinLang.common.delete')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="query_panel_box ksd-mt-10">
            <kap_editor ref="blackInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="blackSql">
            </kap_editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
                <el-button size="medium" @click="clearSql">{{$t('kylinLang.query.clear')}}</el-button>
                <el-button type="primary" size="medium" plain @click="">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
      </el-row>
      <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="sqlLists.length"  v-on:handleCurrentChange='sqlListsPageChange' :perPageSize="5" v-if="sqlLists.length > 0"></kap-pager>
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
              <el-button type="primary" size="medium" plain @click="" icon="el-icon-ksd-query_import">{{$t('kylinLang.common.import')}}
              </el-button>
            </div>
            <div class="ksd-fright ksd-inline searchInput">
              <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <div class="sqlLists">
            <div class="ksd-null-pic-text" v-if="!sqlLists.length">
              <img  src="../../assets/img/no_data.png" />
              <p>{{$t('kylinLang.common.noData')}}</p>
            </div>
            <div v-for="(sql, index) in sqlLists" :key="index" class="sqlBox" :class="{'active': index == activeIndex}" @click="activeSql(sql, index)" v-else>
              <span>{{transformSql(sql)}}</span>
              <div class="group-btn">
                <el-button size="small" type="primary" @click.stop="editWhiteSql(sql, index)" text>{{$t('kylinLang.common.edit')}}</el-button>
                <el-button type="primary" size="small" text @click.stop="delWhite(sql, index)">{{$t('kylinLang.common.delete')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="query_panel_box ksd-mt-10">
            <kap_editor ref="whiteInputBox" :height="inputHeight" lang="sql" theme="chrome" v-model="whiteSql">
            </kap_editor>
            <div class="operatorBox" v-show="isEditSql">
              <div class="btn-group ksd-fright">
                <el-button type="primary" size="medium" plain @click="">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
            </div>
          </div>
        </el-col>
      </el-row>
      <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="sqlLists.length"  v-on:handleCurrentChange='sqlListsPageChange' :perPageSize="5" v-if="sqlLists.length > 0"></kap-pager>
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
      updatePreferrence: 'UPDATE_PREFERRENCE'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {preferrence: 'Preference', whiteList: 'White List', blackList: 'Black List', favDesc: 'Favorite queries are from both favorite rule filtered query and user defined query.<br/> Favorite query represent your main business analysis scenarios and critical decision point.<br/> System will optimize its to max performance by auto-modeling and pre-calculating.', favoriteRules: 'Favorite Rules', favRulesDesc: 'By filtering SQL\'s frequency, duration and submitter, favorite rule will catch up frequently used and business critical queries.', queryFrequency: 'Query Frequency', querySubmitter: 'Query Submitter', queryDuration: 'Query Duration', frequencyDesc: 'Optimize queries frequently used over last 24 hours', submitterDesc: 'Optimize queries from critical users and groups', durationDesc: 'Optimize queries with long duration', unit: 'Seconds / Job', inputSql: 'Add SQL', delSql: 'Are you sure to delete this sql?', giveUpEdit: 'Are you sure to give up the editor?', acceThreshold: 'Accelerating Threshold', notifyLeftTips: 'Notify me every time when there are ', notifyRightTips: ' favorite queries.', acceResource: 'Accelerating Resource', reasourceDsec: 'The system should ask me for permission for using storage and computing resource to accelerate favorite queries.', ressourceYse: 'Yes', ressourceNo: 'No, I don\'t need to know', whiteListDesc: 'White list helps to manage user manually defined favorite SQLs, especially for SQLs from query history list and imported SQL files.', blackListDesc: 'Black list helps to manage SQLs which are undesired for accelerating, especially for those SQLs will require unreasonable large storage or computing resource to accelerate.', ruleImpact: 'Rules Impact', ruleImpactDesc: 'Percentage of SQL queries selected by the favorite rule.', thereAre: 'There are 13 SQLs waiting for acceleration on the threshold of <span class="highlight">50</span>.', accelerateNow: 'Accelerate now', openTips: '您可以點擊展開設定 Favorite Rules'},
    'zh-cn': {preferrence: '加速偏好', whiteList: '白名单', blackList: '黑名单', favDesc: '经过加速规则筛选或者用户主动选择的SQL查询将成为加速查询。<br/>这类查询可以代表最主要的业务分析和重要的业务决策点。<br/>系统将对其进行自动建模和预计算，确保查询效率得到提升。', favRulesDesc: '加速规则过滤不同SQL查询的频率、时长、用户等特征，筛选出高频使用的、对业务分析重要的SQL查询。', favoriteRules: '加速规则', queryFrequency: '查询频率', querySubmitter: '查询用户', queryDuration: '查询时长', frequencyDesc: '优化过去24小时内查询频率较高的查询', submitterDesc: '优化重要⽤用户或⽤用户组发出的查询', durationDesc: '优化慢查询', unit: '秒 / 任务', inputSql: '新增查询语句', delSql: '确定删除这条查询语句吗？', giveUpEdit: '确定放弃本次编辑吗？', acceThreshold: '加速阈值', notifyLeftTips: '每积累', notifyRightTips: ' 条加速查询时，提醒我。', acceResource: '加速资源', reasourceDsec: '系统需要获取存储资源和计算资源来加速查询时，请征询我的许可。', ressourceYse: '征询许可', ressourceNo: '不需要征询', whiteListDesc: '本列列表管理理⽤用户⼈人为指定加速的SQL查询。⼀一般指⽤用户从查询历史指定或导⼊入的查询⽂文件。', blackListDesc: '本列列表管理理⽤用户不不希望被加速的SQL查询。⼀一般是指加速时对存储空间、计算⼒力力需求过⼤大的查询。', ruleImpact: '加速规则影响⼒', ruleImpactDesc: '被加速规则选出的SQL查询的百分⽐。', thereAre: '已有13条SQL查询等待加速(阈值为<span class="highlight">50</span>条SQL)', accelerateNow: '立即加速', openTips: '您可以點擊展開設定 Favorite Rules'}
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  statusFilteArr = [{name: 'el-icon-ksd-acclerate', value: 'FULLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ready', value: 'WAITING'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ongoing', value: 'ACCELERATING'}]
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
  sqlLists = [`#DROP VIEW IF EXISTS GUOYAO.ZX_RPT_ST_DAY_SUM_NEW;

CREATE VIEW IF NOT EXISTS GUOYAO.ZX_RPT_ST_DAY_SUM_NEW
(
placepointid,
useday,
hsxszje,
wsxszje,
hscbje,
wscbje,
hsmle,
wsmle,
mll,
zxxsje,
nxxsje,
hyxsje,
hywsxsje,
hyxszb,
hyhscbje,
hyhsmle,
hywsmle,
hymll,
lsje,
yhje,
lks,
kdj,
--oemhsxsje,
--oemhscbje,
--oemhsmle,
mxhs,
xszrje,
djqje,
jfhgje,
jfdhje,
hylks,
hyyhje
,HYRS,
kps,
HSYSJE,
GROUPYHJE,
--PROMYHJE,
--MOMPROMYHJE,
SGZKYHJE,
FHYYHJE
)
AS
SELECT a.placepointid,
a.useday,
SUM(nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)),
SUM((nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)) / (1 + nvl(b.taxrate, 0))),
SUM(if(c.duns_loc = 'Y', d.unitprice, b.costingprice * (1 + nvl(b.taxrate, 0))) * b.goodsqty),
SUM(if(c.duns_loc = 'Y', d.notaxsuprice, b.costingprice) * b.goodsqty),
SUM(nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0) - if(c.duns_loc = 'Y', d.unitprice, b.costingprice * (1 + nvl(b.taxrate, 0))) * b.goodsqty),
SUM((nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)) / (1 + nvl(b.taxrate, 0)) - if(c.duns_loc = 'Y', d.notaxsuprice, b.costingprice) * b.goodsqty),
if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty)/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))), --毛利率
sum(if(a.rsatype=1,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --正向销售金额
sum(if(a.rsatype=2,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --逆向销售金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --会员销售金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0))), --会员无税销售金额
if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))), --会员销售占比
sum(if(a.insiderid is null,0,1) * if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty), --会员含税成本金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty)), --会员含税毛利金额
sum(if(a.insiderid is null,0,1) * ((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney )), --会员无税毛利金额
if(sum(if(a.insiderid is null,0,1) * b.realmoney)=0,0,(sum(if(a.insiderid is null,0,1) * (b.realmoney - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty))/sum(if(a.insiderid is null,0,1) * b.realmoney))),  --零售金额
sum(b.resaprice * b.goodsqty),  --零售金额
sum(b.resaprice * b.goodsqty - b.realmoney), --优惠金额
count(distinct(a.rsaid)),  --来客数
sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))/count(distinct(a.rsaid)), --客单价
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --OEM含税销售额
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * b.costingmoney * (1 + nvl(b.taxrate, 0))), --OEM含税成本金额
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))), --OEM含税毛利额
count(1), --明细行数
sum(nvl(b.couponmoney,0)+nvl(b.trade_money,0)),  --折让金额
sum(nvl(b.couponmoney,0)),  --代金券金额
sum(nvl(b.trade_money,0)),   --积分换购金额
sum(nvl(f.exc_money,0))     --积分兑换金额
,(case when count(distinct(a.rsaid)) = 1 and sum(if(a.insiderid is null,0,1)) = 0 then 0 when count(distinct(a.rsaid)) = count(distinct(if(a.insiderid is null,0,a.rsaid))) then count(distinct(if(a.insiderid is null,0,a.rsaid))) else count(distinct(if(a.insiderid is null,0,a.rsaid)))-1 end), --会员来客数
sum(if(a.insiderid is null,0,1) * (b.resaprice * b.goodsqty - b.realmoney)), --会员优惠金额
COUNT(DISTINCT A.INSIDERID), --会员人数
if(COUNT(DISTINCT A.RSAID)=0,0,ROUND(COUNT(DISTINCT CONCAT(A.RSAID,',',B.Goodsid)) / COUNT(DISTINCT A.RSAID), 2)), --客品数
SUM(B.RESAPRICE * B.GOODSQTY), --含税应收金额
SUM(if(e.groupbuyid is null,0,B.RESAPRICE * B.GOODSQTY - B.REALMONEY)), --团购订单优惠金额
sum((b.resaprice-nvl((select rpdtl.promprice from resa_priceprom_doc rpd,resa_priceprom_dtl rpdtl where rpd.promdocid = rpdtl.promdocid and rpd.usestatus = 2 and rpd.placepointid = a.placepointid and rpd.startdate >= a.useday and rpd.enddate < a.useday+1 and rpdtl.goodsid = b.goodsid and rownum = 1),b.resaprice))*b.goodsqty) AS PROMYHJE, --催销价优惠金额
SUM(NVL(A.MANUALMONEY,0)*if(A.REALMONEY=0,0,B.REALMONEY/A.REALMONEY)) + SUM(NVL(B.MANUALMONEY,0)), --手工折扣优惠金额
SUM(if(A.INSIDERID is null, 1, 0) * (B.RESAPRICE * B.GOODSQTY - B.REALMONEY)) --非会员优惠金额
FROM guoyao.gresa_sa_doc_etl a
JOIN guoyao.gresa_sa_dtl_etl b ON a.rsaid = b.rsaid
JOIN guoyao.gpcs_placepoint_etl c ON a.placepointid = c.placepointid
LEFT JOIN bms_batch_def d ON b.batchid = d.batchid
LEFT JOIN zx_group_buy e ON a.rsaid = e.rsaid
LEFT JOIN gresa_sa_integral_etl f ON a.rsaid = f.rsaid
WHERE a.usestatus = 1
GROUP BY a.placepointid, a.useday;`, `#DROP VIEW GUOYAO.ZX_RPT_ST_DAY_GOODS_SUM;

CREATE VIEW IF NOT EXISTS GUOYAO.ZX_RPT_ST_DAY_GOODS_SUM_NEW(
placepointid,
     useday,
     goodsid,
     hsxszje,
     wsxszje,
     hscbje,
     wscbje,
     hsmle,
     wsmle,
     mll,
     zxxsje,
     nxxsje,
     hyxsje,
     hywsxsje,
     hyxszb,
     hyhscbje,
     hyhsmle,
     hywsmle,
     hymll,
     lsje,
     yhje,
     xspc,
     xssl,
     hyxspc,
     hyxssl,
     xszrje,
     receivalmoney,
     kpl,
     CNSALEFLAG
) as
select a.placepointid, --门店id
       a.useday, --逻辑日
       b.goodsid, --货品id
       (sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))) as hsxszje, --含税销售额
       (sum((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)))) as wsxszje, --无税销售额
       (sum(b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hscbje, --含税成本金额
       (sum(b.costingmoney)) as wscbje,  --无税成本金额
       (sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hsmle, --含税毛利额
       (sum((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney)) as wsmle, --无税毛利额
       (if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))))) as mll, --毛利率
       (sum(if(a.rsatype=1,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as zxxsje, --正向销售金额
       (sum(if(a.rsatype=2,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as nxxsje, --逆向销售金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as hyxsje, --会员销售金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)))) as hywsxsje, --会员无税销售金额
       (if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))))) as hyxszb, --会员销售占比
       (sum(if(a.insiderid is null,0,1) * b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hyhscbje, --会员含税成本金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0))))) as hyhsmle, --会员含税毛利金额
       (sum(if(a.insiderid is null,0,1) * ((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney ))) as hywsmle, --会员无税毛利金额
       (if(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))=0,0,(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0))))/sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))))) as hymll,  --会员毛利率
       (sum(b.resaprice * b.goodsqty)) as lsje,  --零售金额
       (sum(b.resaprice * b.goodsqty - b.realmoney)) as yhje, --优惠金额
       (count(distinct(a.rsaid))) as xspc,  --销售频次
       (sum(b.goodsqty)) as xssl,  --销售数量
       ((case when count(distinct(a.rsaid)) = 1 and sum(if(a.insiderid is null,0,1)) = 0 then 0 when count(distinct(a.rsaid)) = count(distinct(if(a.insiderid is null,0,a.rsaid))) then count(distinct(if(a.insiderid is null,0,a.rsaid))) else count(distinct(if(a.insiderid is null,0,a.rsaid)))-1 end)) as hyxspc, --会员销售频次
       (sum(if(a.insiderid is null,0,1) * b.goodsqty)) as hyxssl, --会员销售数量
       (sum(nvl(b.couponmoney,0)+nvl(b.trade_money,0))) as xszrje,  --销售折让金额
       (sum(nvl(b.total_line, 0))) as receivalmoney,
       if(COUNT(DISTINCT A.RSAID) is null, 0, if(COUNT(DISTINCT A.RSAID)=ROUND(SUM(B.GOODSQTY) / COUNT(DISTINCT A.RSAID)), 2, 0)) AS KPL,
       if(NVL(A.CNCARDTYPEID, 0)=0, 0, 1) AS CNSALEFLAG --医保销售
  from guoyao.gresa_sa_doc_etl a, guoyao.gresa_sa_dtl_etl b
 where a.rsaid = b.rsaid
 group by a.placepointid, a.useday, b.goodsid,A.CNCARDTYPEID;`, `#DROP VIEW IF EXISTS GUOYAO.ZX_RPT_ST_DAY_SUM_NEW;

CREATE VIEW IF NOT EXISTS GUOYAO.ZX_RPT_ST_DAY_SUM_NEW
(
placepointid,
useday,
hsxszje,
wsxszje,
hscbje,
wscbje,
hsmle,
wsmle,
mll,
zxxsje,
nxxsje,
hyxsje,
hywsxsje,
hyxszb,
hyhscbje,
hyhsmle,
hywsmle,
hymll,
lsje,
yhje,
lks,
kdj,
--oemhsxsje,
--oemhscbje,
--oemhsmle,
mxhs,
xszrje,
djqje,
jfhgje,
jfdhje,
hylks,
hyyhje
,HYRS,
kps,
HSYSJE,
GROUPYHJE,
--PROMYHJE,
--MOMPROMYHJE,
SGZKYHJE,
FHYYHJE
)
AS
SELECT a.placepointid,
a.useday,
SUM(nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)),
SUM((nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)) / (1 + nvl(b.taxrate, 0))),
SUM(if(c.duns_loc = 'Y', d.unitprice, b.costingprice * (1 + nvl(b.taxrate, 0))) * b.goodsqty),
SUM(if(c.duns_loc = 'Y', d.notaxsuprice, b.costingprice) * b.goodsqty),
SUM(nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0) - if(c.duns_loc = 'Y', d.unitprice, b.costingprice * (1 + nvl(b.taxrate, 0))) * b.goodsqty),
SUM((nvl(b.realmoney, 0) - nvl(b.couponmoney, 0) - nvl(b.trade_money, 0)) / (1 + nvl(b.taxrate, 0)) - if(c.duns_loc = 'Y', d.notaxsuprice, b.costingprice) * b.goodsqty),
if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty)/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))), --毛利率
sum(if(a.rsatype=1,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --正向销售金额
sum(if(a.rsatype=2,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --逆向销售金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --会员销售金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0))), --会员无税销售金额
if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))), --会员销售占比
sum(if(a.insiderid is null,0,1) * if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty), --会员含税成本金额
sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty)), --会员含税毛利金额
sum(if(a.insiderid is null,0,1) * ((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney )), --会员无税毛利金额
if(sum(if(a.insiderid is null,0,1) * b.realmoney)=0,0,(sum(if(a.insiderid is null,0,1) * (b.realmoney - if(c.duns_loc='Y',d.unitprice,b.costingprice*(1+nvl(b.taxrate,0)))*b.goodsqty))/sum(if(a.insiderid is null,0,1) * b.realmoney))),  --零售金额
sum(b.resaprice * b.goodsqty),  --零售金额
sum(b.resaprice * b.goodsqty - b.realmoney), --优惠金额
count(distinct(a.rsaid)),  --来客数
sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))/count(distinct(a.rsaid)), --客单价
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))), --OEM含税销售额
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * b.costingmoney * (1 + nvl(b.taxrate, 0))), --OEM含税成本金额
--sum(zx_get_oem_flag(b.goodsid, a.placepointid) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))), --OEM含税毛利额
count(1), --明细行数
sum(nvl(b.couponmoney,0)+nvl(b.trade_money,0)),  --折让金额
sum(nvl(b.couponmoney,0)),  --代金券金额
sum(nvl(b.trade_money,0)),   --积分换购金额
sum(nvl(f.exc_money,0))     --积分兑换金额
,(case when count(distinct(a.rsaid)) = 1 and sum(if(a.insiderid is null,0,1)) = 0 then 0 when count(distinct(a.rsaid)) = count(distinct(if(a.insiderid is null,0,a.rsaid))) then count(distinct(if(a.insiderid is null,0,a.rsaid))) else count(distinct(if(a.insiderid is null,0,a.rsaid)))-1 end), --会员来客数
sum(if(a.insiderid is null,0,1) * (b.resaprice * b.goodsqty - b.realmoney)), --会员优惠金额
COUNT(DISTINCT A.INSIDERID), --会员人数
if(COUNT(DISTINCT A.RSAID)=0,0,ROUND(COUNT(DISTINCT CONCAT(A.RSAID,',',B.Goodsid)) / COUNT(DISTINCT A.RSAID), 2)), --客品数
SUM(B.RESAPRICE * B.GOODSQTY), --含税应收金额
SUM(if(e.groupbuyid is null,0,B.RESAPRICE * B.GOODSQTY - B.REALMONEY)), --团购订单优惠金额
sum((b.resaprice-nvl((select rpdtl.promprice from resa_priceprom_doc rpd,resa_priceprom_dtl rpdtl where rpd.promdocid = rpdtl.promdocid and rpd.usestatus = 2 and rpd.placepointid = a.placepointid and rpd.startdate >= a.useday and rpd.enddate < a.useday+1 and rpdtl.goodsid = b.goodsid and rownum = 1),b.resaprice))*b.goodsqty) AS PROMYHJE, --催销价优惠金额
SUM(NVL(A.MANUALMONEY,0)*if(A.REALMONEY=0,0,B.REALMONEY/A.REALMONEY)) + SUM(NVL(B.MANUALMONEY,0)), --手工折扣优惠金额
SUM(if(A.INSIDERID is null, 1, 0) * (B.RESAPRICE * B.GOODSQTY - B.REALMONEY)) --非会员优惠金额
FROM guoyao.gresa_sa_doc_etl a
JOIN guoyao.gresa_sa_dtl_etl b ON a.rsaid = b.rsaid
JOIN guoyao.gpcs_placepoint_etl c ON a.placepointid = c.placepointid
LEFT JOIN bms_batch_def d ON b.batchid = d.batchid
LEFT JOIN zx_group_buy e ON a.rsaid = e.rsaid
LEFT JOIN gresa_sa_integral_etl f ON a.rsaid = f.rsaid
WHERE a.usestatus = 1
GROUP BY a.placepointid, a.useday;`, `#DROP VIEW GUOYAO.ZX_RPT_ST_DAY_GOODS_SUM;

CREATE VIEW IF NOT EXISTS GUOYAO.ZX_RPT_ST_DAY_GOODS_SUM_NEW(
placepointid,
     useday,
     goodsid,
     hsxszje,
     wsxszje,
     hscbje,
     wscbje,
     hsmle,
     wsmle,
     mll,
     zxxsje,
     nxxsje,
     hyxsje,
     hywsxsje,
     hyxszb,
     hyhscbje,
     hyhsmle,
     hywsmle,
     hymll,
     lsje,
     yhje,
     xspc,
     xssl,
     hyxspc,
     hyxssl,
     xszrje,
     receivalmoney,
     kpl,
     CNSALEFLAG
) as
select a.placepointid, --门店id
       a.useday, --逻辑日
       b.goodsid, --货品id
       (sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))) as hsxszje, --含税销售额
       (sum((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)))) as wsxszje, --无税销售额
       (sum(b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hscbje, --含税成本金额
       (sum(b.costingmoney)) as wscbje,  --无税成本金额
       (sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hsmle, --含税毛利额
       (sum((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney)) as wsmle, --无税毛利额
       (if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))))) as mll, --毛利率
       (sum(if(a.rsatype=1,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as zxxsje, --正向销售金额
       (sum(if(a.rsatype=2,1,0) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as nxxsje, --逆向销售金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))) as hyxsje, --会员销售金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)))) as hywsxsje, --会员无税销售金额
       (if(sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))=0,0,
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))/sum(nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0))))) as hyxszb, --会员销售占比
       (sum(if(a.insiderid is null,0,1) * b.costingmoney * (1 + nvl(b.taxrate, 0)))) as hyhscbje, --会员含税成本金额
       (sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0))))) as hyhsmle, --会员含税毛利金额
       (sum(if(a.insiderid is null,0,1) * ((nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)) / (1 + nvl(b.taxrate, 0)) - b.costingmoney ))) as hywsmle, --会员无税毛利金额
       (if(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))=0,0,(sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0) - b.costingmoney * (1 + nvl(b.taxrate, 0))))/sum(if(a.insiderid is null,0,1) * (nvl(b.realmoney,0)-nvl(b.couponmoney,0)-nvl(b.trade_money,0)))))) as hymll,  --会员毛利率
       (sum(b.resaprice * b.goodsqty)) as lsje,  --零售金额
       (sum(b.resaprice * b.goodsqty - b.realmoney)) as yhje, --优惠金额
       (count(distinct(a.rsaid))) as xspc,  --销售频次
       (sum(b.goodsqty)) as xssl,  --销售数量
       ((case when count(distinct(a.rsaid)) = 1 and sum(if(a.insiderid is null,0,1)) = 0 then 0 when count(distinct(a.rsaid)) = count(distinct(if(a.insiderid is null,0,a.rsaid))) then count(distinct(if(a.insiderid is null,0,a.rsaid))) else count(distinct(if(a.insiderid is null,0,a.rsaid)))-1 end)) as hyxspc, --会员销售频次
       (sum(if(a.insiderid is null,0,1) * b.goodsqty)) as hyxssl, --会员销售数量
       (sum(nvl(b.couponmoney,0)+nvl(b.trade_money,0))) as xszrje,  --销售折让金额
       (sum(nvl(b.total_line, 0))) as receivalmoney,
       if(COUNT(DISTINCT A.RSAID) is null, 0, if(COUNT(DISTINCT A.RSAID)=ROUND(SUM(B.GOODSQTY) / COUNT(DISTINCT A.RSAID)), 2, 0)) AS KPL,
       if(NVL(A.CNCARDTYPEID, 0)=0, 0, 1) AS CNSALEFLAG --医保销售
  from guoyao.gresa_sa_doc_etl a, guoyao.gresa_sa_dtl_etl b
 where a.rsaid = b.rsaid
 group by a.placepointid, a.useday, b.goodsid,A.CNCARDTYPEID;`, 'Select count (*) from table_1 UPDATE Person SET FirstName = \'Fred\' WHERE LastName = \'Wilson\' UPDATE Person SET Address = \'Zhongshan 23\', City = dsf dlfsjlk']
  blackSql = ''
  whiteSql = this.formatterSql(this.sqlLists[0]) || ''
  preSettingObj = {
    autoApply: true,
    accelerateThreshold: 50,
    accumulateFavorites: true
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
    users: ['Admin'],
    groups: []
  }
  durationObj = {
    enable: true,
    durationValue: [50, 80]
  }
  selectedUser = ''
  options = [{
    label: this.$t('kylinLang.menu.user'),
    options: ['Admin']
  }]
  oldFrequencyValue = 0.2
  oldSubmitterUsers = ['Admin']
  oldDurationValue = [50, 80]
  impactRatio = 55

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
    this.getPreferrence({project: this.currentSelectedProject})
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
  }

  mounted () {
    this.$nextTick(() => {
      $('#favo-menu-item').removeClass('rotateY').css('opacity', 0)
      loadLiquidFillGauge('fillgauge', this.impactRatio)
      const config1 = liquidFillGaugeDefaultSettings()
      config1.circleColor = '#FF7777'
      config1.textColor = '#FF4444'
      config1.waveTextColor = '#FFAAAA'
      config1.waveColor = '#FFDDDD'
      config1.circleThickness = 0.2
      config1.textVertPosition = 0.2
      config1.waveAnimateTime = 1000
    })
  }

  pageCurrentChange (offset, pageSize) {
    this.favoriteCurrentPage = offset + 1
    this.loadFavoriteList(offset, pageSize)
  }

  openBlackList () {
    this.blackListVisible = true
    this.activeIndex = -1
  }

  openWhiteList () {
    this.whiteListVisible = true
    this.activeIndex = 0
    setTimeout(() => {
      this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
    }, 0)
  }

  activeSql (sql, index) {
    this.whiteSql = this.formatterSql(sql)
    this.activeIndex = index
    this.isEditSql = false
    this.inputHeight = 564
    this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(true)
  }

  editWhiteSql (sql, index) {
    this.isEditSql = true
    this.inputHeight = 512
    this.whiteSql = this.formatterSql(sql)
    this.$refs.whiteInputBox.$refs.kapEditor.editor.setReadOnly(false)
  }

  toView (sql, index) {
    this.inputHeight = 564
    this.activeIndex = index
    this.isEditSql = false
    this.blackSql = this.formatterSql(sql)
    this.$refs.blackInputBox.$refs.kapEditor.editor.setReadOnly(true)
  }

  viewBlackSql (sql, index) {
    if (this.blackSql) {
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
    this.blackSql = ''
    this.activeIndex = -1
    this.$refs.blackInputBox.$refs.kapEditor.editor.setReadOnly(false)
  }

  clearSql () {
    this.blackSql = ''
  }

  delBlack (sql, index) {
    kapConfirm(this.$t('delSql'))
  }

  delWhite (sql, index) {
    kapConfirm(this.$t('delSql'))
  }

  formatterSql (sql) {
    return this.getFormatterSql(sql)
  }

  transformSql (sql) {
    return sql.length > 350 ? sql.substr(0, 350) + '...' : sql
  }

  onblackSqlFilterChange () {}

  onWhiteSqlFilterChange () {}

  sqlListsPageChange () {}

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
      }
      .el-collapse-item__content {
        padding-bottom: 0;
        background-color: @table-stripe-color;
        height: 170px;
        line-height: 1;
        .rules-conds {
          border-radius: 2px;
          display: flex;
          padding: 10px 0;
          .conds {
            width: 33.34%;
            padding: 0 20px;
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
            top: 40px;
            width: 95%;
          }
        }
      }
    }
    .rules_tools {
      background-color: @base-color-10;
      height: 52px;
      padding: 0 10px 0 25px;
      .highlight {
        font-weight: 500px;
      }
      .btn-group {
        line-height: 52px;
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
        margin-top: 10px;
        .user-label {
          font-size: 14px;
          margin-right: 8px;
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
        &.el-icon-ksd-acclerate {
          color: @normal-color-1;
        }
        &.el-icon-ksd-acclerate_portion,
        &.el-icon-ksd-acclerate_ongoing {
          color: @base-color;
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
