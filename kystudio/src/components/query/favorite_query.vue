<template>
  <div id="favoriteQuery">
    <el-collapse v-model="activeNames" class="favorite-rules">
      <el-collapse-item name="rules">
        <template slot="title">
          {{$t('favoriteRules')}} <i class="el-icon-ksd-what"></i>
        </template>
        <el-row v-clickoutside="resetEditValue">
          <el-col :span="18" class="rules-conds">
            <div class="conds" :class="{'disabled': !frequencyObj.enable}">
              <div class="conds-title">
                <span>Query Frequency</span>
                <el-switch class="ksd-switch" v-model="frequencyObj.enable" active-text="ON" inactive-text="OFF" @change="updateFre"></el-switch>
                <el-button type="primary" size="small" text class="ksd-fright edit-conds" v-show="!isFrequencyEdit" @click="editFrequency">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content clearfix">
                <div class="desc">Queries usage frequency under 50 times per day are covered, leading to a 37% acceleration.</div>
                <el-slider :class="{'show-only': !isFrequencyEdit}" v-model="frequencyObj.value":step="10" button-type="sharp" :min="0" :max="100" show-dynamic-values :disabled="!isFrequencyEdit" :format-tooltip="formatTooltip"></el-slider>
                <div class="ksd-fright ksd-mt-10">TopX% Frequency</div>
              </div>
              <div class="conds-footer" v-if="isFrequencyEdit">
                <div class="tips ksd-mb-10">Drag (the frequency point) to cover more queries.</div>
                <div class="btn-groups">
                  <el-button @click="cancelFrequency" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" plain @click="saveFrequency" size="medium">{{$t('kylinLang.common.save')}}</el-button>
                </div>
              </div>
            </div>
            <div class="conds" :class="{'disabled': !submitterObj.enable}">
              <div class="conds-title">
                <span>Query Submitter</span>
                <el-switch class="ksd-switch" v-model="submitterObj.enable" active-text="ON" inactive-text="OFF" @change="updateSub"></el-switch>
                <el-button type="primary" size="small" text class="ksd-fright edit-conds" v-show="!isSubmitterEdit" @click="editSubmitter">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content">
                <div class="users">
                  <i class="el-icon-ksd-table_admin"></i> <span>{{submitterObj.value[0]}}</span> Users
                  <i class="el-icon-ksd-table_group"></i> <span>{{submitterObj.value[1]}}</span> Groups
                </div>
                <div class="desc">Queries sent from above submitters will be covered. </div>
              </div>
              <div class="conds-footer" v-if="isSubmitterEdit">
                <el-select v-model="selectedUser" v-event-stop :popper-append-to-body="false" filterable size="medium" placeholder="VIP User" class="ksd-mt-10" @change="selectUserChange">
                  <el-option-group v-for="group in options" :key="group.label" :label="group.label">
                    <el-option v-for="item in group.options" :key="item" :label="item" :value="item"></el-option>
                  </el-option-group>
                </el-select>
                <div class="vip-users-block ksd-mb-10">
                  <div class="ksd-mt-10"><i class="el-icon-ksd-table_admin"></i> VIP User</div>
                  <div class="vip-users">
                    <span v-for="(user, index) in users" :key="user" class="user-label">{{user}}
                      <i class="el-icon-ksd-close" @click="removeUser(index)"></i>
                    </span>
                  </div>
                </div>
                <div class="btn-groups">
                  <el-button @click="cancelSubmitter" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" plain @click="saveSubmitter" size="medium">{{$t('kylinLang.common.save')}}</el-button>
                </div>
              </div>
            </div>
            <div class="conds" :class="{'disabled': !durationObj.enable}">
              <div class="conds-title">
                <span>Query Duration</span>
                <el-switch class="ksd-switch" v-model="durationObj.enable" active-text="ON" inactive-text="OFF" @change="updateDura"></el-switch>
                <el-button type="primary" size="small" text class="ksd-fright edit-conds" v-show="!isDurationEdit" @click="editDuration">{{$t('kylinLang.common.edit')}}</el-button>
              </div>
              <div class="conds-content clearfix">
                <div class="desc">Queries duration between 50s and 75s are covered, leading to a 37% acceleration.</div>
                <el-slider :class="{'show-only': !isDurationEdit}" v-model="durationObj.value" :step="1" range button-type="sharp" :min="0" :max="180" show-dynamic-values :disabled="!isDurationEdit"></el-slider>
                <div class="ksd-fright ksd-mt-10">Seconds / Job</div>
              </div>
              <div class="conds-footer" v-if="isDurationEdit">
                <div class="tips ksd-mb-10">Drag (the starting and ending point) to cover needed queries.</div>
                <div class="btn-groups">
                  <el-button @click="cancelDuration" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" plain @click="saveDuration" size="medium">{{$t('kylinLang.common.save')}}</el-button>
                </div>
              </div>
            </div>
          </el-col>
          <el-col :span="6" class="fillgauge-block">
            <svg id="fillgauge" width="80%" height="165"></svg>
          </el-col>
        </el-row>
      </el-collapse-item>
    </el-collapse>
    <div class="clearfix ksd-mb-10 ksd-mt-20">
      <div class="ksd-fleft ksd-title-label ksd-mt-10">
        <span>{{$t('kylinLang.menu.favorite_query')}}</span>
        <el-tooltip placement="right">
          <div slot="content">{{$t('favDesc')}}</div>
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
        <span>13 more to accelerate <span class="highlight">40</span>, or you can
          <el-button type="primary" text size="medium">accelerate them now !</el-button>
        </span>
      </div>
      <div class="ksd-fright btn-group">
        <el-button size="medium" icon="el-icon-ksd-acclerate_ready" plain @click="openPreferrenceSetting">{{$t('preferrence')}}</el-button>
        <el-button size="medium" icon="el-icon-ksd-table_delete" plain @click="removeFav">{{$t('remove')}}</el-button>
      </div>
    </div>
    <el-table
      :data="favQueList.favorite_queries"
      border
      class="favorite-table"
      @selection-change="handleSelectionChange"
      ref="favoriteTable"
      style="width: 100%">
      <el-table-column type="selection" width="55" align="center"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql" header-align="center" show-overflow-tooltip></el-table-column>
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
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" prop="frequency" sortable align="center" width="120"></el-table-column>
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
      :title="$t('preferrence')"
      :visible.sync="preferrenceVisible"
      width="440px"
      class="preferrenceDialog">
      <div class="batch">
        <span class="ky-list-title">Accelerating Batch</span>
        <el-switch class="ksd-switch" v-model="preSettingObj.auto" active-text="ON" inactive-text="OFF"></el-switch>
        <div class="setting">
          <span>Accelerate</span>
          <el-input size="small" class="acce-input" v-model="preSettingObj.accelerateThreshold"></el-input>
          <span>favorite queries in a batch.</span>
        </div>
      </div>
      <div class="divider-line"></div>
      <div class="resource">
        <span class="ky-list-title">Accelerating Resource</span>
        <div class="ksd-mt-10 ksd-mb-10">System needs your permission to use any resource to refresh model & index ?</div>
        <el-radio-group v-model="preSettingObj.accumulateFavorites">
          <el-radio :label="true">Yes</el-radio>
          <el-radio :label="false">No, I don’t need to know</el-radio>
        </el-radio-group>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="preferrenceVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" plain @click="preferrenceVisible = false">{{$t('kylinLang.common.save')}}</el-button>
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
import { handleSuccess, transToGmtTime } from '../../util/business'
import { loadLiquidFillGauge, liquidFillGaugeDefaultSettings } from '../../util/liquidFillGauge'
import Clickoutside from 'kyligence-ui/src/utils/clickoutside'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      deleteFav: 'DELETE_FAV',
      getRules: 'GET_RULES',
      getRulesImpact: 'GET_RULES_IMPACT',
      getPreferrence: 'GET_PREFERRENCE',
      updateRules: 'UPDATE_RULES',
      updatePreferrence: 'UPDATE_PREFERRENCE'
    })
  },
  directives: { Clickoutside },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {addCandidate: 'Add Candidate Query', preferrence: 'Preferrence', remove: 'Remove', candidateQuery: 'Candidate Query', favDesc: 'Critical SQL statement for business. System will create aggregate index or table index to serve them and do pre-computing to improve query performance.', favoriteRules: 'Favorite Rules'},
    'zh-cn': {addCandidate: '添加查询', preferrence: 'Preferrence', remove: '删除查询', candidateQuery: '待选查询', favDesc: '重要SQL语句的列表。系统针对加速查询中的SQL对应生成聚合索引（agggregate index）或明细表索引（table index），通过预计算索引提升SQL查询响应速度。', favoriteRules: '加速规则'}
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  statusFilteArr = [{name: 'el-icon-ksd-acclerate', value: 'FULLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ready', value: 'WAITING'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ongoing', value: 'ACCELERATING'}]
  checkedStatus = []
  preferrenceVisible = false
  preSettingObj = {
    autoApply: true,
    accelerateThreshold: 20,
    accumulateFavorites: true
  }
  favoriteCurrentPage = 1
  activeNames = ['rules']
  selectToUnFav = {}
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
    value: 20
  }
  submitterObj = {
    enable: true,
    value: [1, 0]
  }
  durationObj = {
    enable: true,
    value: [50, 80]
  }
  users = ['Admin']
  selectedUser = ''
  options = [{
    label: this.$t('kylinLang.menu.user'),
    options: ['Admin']
  }]
  oldFrequencyValue = 20
  oldSubmitterValue = ['Admin']
  oldDurationValue = [58, 80]
  isFrequencyEdit = false
  isSubmitterEdit = false
  isDurationEdit = false
  impactRatio = 55

  resetEditValue () {
    this.isFrequencyEdit = false
    this.isSubmitterEdit = false
    this.isDurationEdit = false
    this.frequencyObj.value = this.oldFrequencyValue
    this.submitterObj.value[0] = this.oldSubmitterValue.length
    this.durationObj.value = this.oldDurationValue
  }

  editFrequency () {
    this.resetEditValue()
    this.isFrequencyEdit = true
  }

  cancelFrequency () {
    this.isFrequencyEdit = false
    this.frequencyObj.value = this.oldFrequencyValue
  }

  saveFrequency () {
    this.isFrequencyEdit = false
    this.oldFrequencyValue = this.frequencyObj.value
    this.updateFre()
  }

  updateFre () {
    this.updateRules({
      project: this.currentSelectedProject,
      ruleName: 'frequency',
      enable: this.frequencyObj.enable,
      rightThreshold: this.frequencyObj.value
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
    })
  }

  editSubmitter () {
    this.resetEditValue()
    this.isSubmitterEdit = true
  }

  cancelSubmitter () {
    this.isSubmitterEdit = false
    this.submitterObj.value[0] = this.oldSubmitterValue.length
  }

  saveSubmitter () {
    this.isSubmitterEdit = false
    this.oldSubmitterValue = this.users
    this.submitterObj.value[0] = this.oldSubmitterValue.length
    this.updateSub()
  }

  updateSub () {
    this.updateRules({
      project: this.currentSelectedProject,
      ruleName: 'submitter',
      enable: this.submitterObj.enable,
      rightThreshold: this.users
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
    })
  }

  selectUserChange (val) {
    this.users.push(val)
    const index = this.options[0].options.indexOf(val)
    this.selectedUser = ''
    this.options[0].options.splice(index, 1)
  }

  removeUser (index) {
    const user = this.users[index]
    this.users.splice(index, 1)
    this.options[0].options.push(user)
  }

  editDuration () {
    this.resetEditValue()
    this.isDurationEdit = true
  }

  cancelDuration () {
    this.isDurationEdit = false
    this.durationObj.value = this.oldDurationValue
  }

  saveDuration () {
    this.isDurationEdit = false
    this.oldDurationValue = this.durationObj.value
    this.updateDura()
  }

  updateDura () {
    this.updateRules({
      project: this.currentSelectedProject,
      ruleName: 'duration',
      enable: this.durationObj.enable,
      leftThreshold: this.durationObj.value[0],
      rightThreshold: this.durationObj.value[1]
    }).then((res) => {
      handleSuccess(res, () => {
        this.loadRuleImpactRatio()
      })
    }, (res) => {
      handleError(res)
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
    if (this.selectToUnFav[this.favoriteCurrentPage]) {
      this.$nextTick(() => {
        this.$refs.favoriteTable.toggleRowSelection(this.selectToUnFav[this.favoriteCurrentPage])
      })
    }
  }

  formatTooltip (val) {
    return val + '%'
  }

  filterFav () {
    this.loadFavoriteList()
  }

  openPreferrenceSetting () {
    this.preferrenceVisible = true
    this.getPreferrence({project: this.currentSelectedProject})
  }

  created () {
    this.loadFavoriteList()
    this.getRules({project: this.currentSelectedProject, ruleName: 'frequency'})
    this.getRules({project: this.currentSelectedProject, ruleName: 'submitter'})
    this.getRules({project: this.currentSelectedProject, ruleName: 'duration'})
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

  handleSelectionChange (rows) {
    this.selectToUnFav[this.favoriteCurrentPage] = rows
  }

  removeFav () {
    let uuidArr = []
    $.each(this.selectToUnFav, (index, item) => {
      const uuids = item.map((t) => {
        return t.uuid
      })
      uuidArr = uuidArr.concat(uuids)
    })
    if (!uuidArr.length) {
      return
    }
    this.deleteFav({project: this.currentSelectedProject, 'uuids': uuidArr}).then((res) => {
      handleSuccess(res, () => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.selectToUnFav = {}
        this.loadFavoriteList()
      })
    }, (res) => {
      handleError(res)
    })
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
        border-bottom: 1px solid @line-border-color;
        font-size: 16px;
      }
      .el-collapse-item__content {
        padding-bottom: 0;
        .rules-conds {
          min-heght: 185px;
          border: 1px solid @line-border-color;
          margin-top: 10px;
          border-radius: 2px;
          display: flex;
          padding: 10px 0;
          .conds {
            width: 33.34%;
            padding: 0 10px;
            border-right: 1px solid @line-border-color;
            &:last-child {
              border-right: 0;
            }
            .conds-title {
              font-size: 12px;
              font-weight: 500;
              padding: 10px 0;
              color: @text-title-color;
              border-bottom: 1px solid @line-border-color;
            }
            .conds-content {
              .desc {
                line-height: 15px;
                color: @text-title-color;
                font-size: 10px;
                margin-top: 10px;
              }
              .el-slider {
                width: 95%;
              }
              .show-only {
                width: 100%;
                .el-slider__runway {
                  border-radius: 0;
                }
                .el-slider__runway.disabled .el-slider__bar {
                  background-color: @base-color;
                }
                .el-slider__valueStop,
                .el-slider__button {
                  display: none;
                }
                .el-slider__values:last-child {
                  margin-left: -20px;
                }
              }
              .users {
                margin: 14px 0;
                .el-icon-ksd-table_admin,
                .el-icon-ksd-table_group {
                  font-size: 16px;
                }
              }
            }
            .vip-users-block {
              // .vip-users {
              //   max-height: 50px;
              //   overflow-y: scroll;
              // }
              .user-label {
                font-size: 14px;
                margin-right: 8px;
                .el-icon-ksd-close {
                  display: none;
                }
                &:hover {
                  background-color: @base-color-10;
                  color: @base-color;
                  .el-icon-ksd-close {
                    display: inline-block;
                  }
                }
              }
            }
            .conds-footer .btn-groups {
              border-top: 1px solid @line-border-color;
              text-align: right;
              padding-top: 10px;
            }
            .edit-conds {
              display: none;
            }
            &:hover {
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
              .conds-content {
                opacity: 0.5;
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
        }
      }
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
          font-color: @text-title-color;
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
