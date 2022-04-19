<template>
  <div class="basic-setting">
    <!-- 项目基本设置 -->
    <EditableBlock
      :header-content="$t('basicInfo')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'basic-info')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('basic-info', scb, ecb)">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectName')}}</div>
        <div class="setting-value fixed">{{project.alias || project.project}}</div>
        <!-- <el-input class="setting-input" size="small" style="width: 250px;" v-model="form.alias"></el-input> -->
      </div>
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectType')}}</div>
        <div class="setting-value fixed"><i :class="projectIcon"></i>{{$t(project.maintain_model_type)}}</div>
      </div>
      <div class="setting-item" v-if="project.maintain_model_type==='MANUAL_MAINTAIN'">
        <span class="setting-label font-medium">
          {{$t('enableSemiAutomatic')}}
          <!-- <span class="beta-label">BETA</span> -->
        </span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.semi_automatic_mode"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('enableSemiAutomaticDesc')}}</div>
      </div>
      <div class="setting-item clearfix">
        <div class="setting-label font-medium">{{$t('description')}}</div>
        <div class="setting-value">{{project.description}}</div>
        <el-input class="setting-input" :rows="3" type="textarea" size="small" v-model="form.description"></el-input>
      </div>
    </EditableBlock>
    <!-- 数据源设置 -->
    <EditableBlock
      :header-content="$t('datasourceSetting')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'datasource-info')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('datasource-info', scb, ecb)">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('JDBCDataSource')}}</div>
        <div class="setting-value fixed">
          <el-tooltip placement="top" :content="$t('forbidenSwitchJdbcTips')" :disabled="!(form.jdbc_datasource_enabled && haveJDBCDatasource)">
            <el-switch
              :disabled="form.jdbc_datasource_enabled ? haveJDBCDatasource : false"
              v-model="form.jdbc_datasource_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')"
            >
            </el-switch>
          </el-tooltip>
        </div>
      </div>
      <template v-if="form.JDBCConnectSetting.length">
        <div v-for="(item, index) in form.JDBCConnectSetting" :key="item.sourceType">
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('jdbcConnectInfo')}}</div>
            <div class="setting-value fixed"><el-button plain icon="el-ksd-icon-edit_22" size="small" @click="modifyDataSourceSetting(index)">{{$t('modify')}}</el-button></div>
          </div>
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('jdbcShowName')}}</div>
            <div class="setting-value fixed">{{item.name}}</div>
          </div>
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('connectString')}}</div>
            <div class="setting-value fixed">{{item.connectionString}}</div>
          </div>
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('username')}}</div>
            <div class="setting-value fixed">{{item.username}}</div>
          </div>
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('password')}}</div>
            <div class="setting-value fixed">••••••</div>
          </div>
          <div class="setting-item">
            <div class="setting-label font-medium">{{$t('driver')}}</div>
            <div class="setting-value fixed">{{item.driver}}</div>
          </div>
        </div>
      </template>
      <template v-else>
        <div class="setting-item">
          <div class="setting-label">{{$t('jdbcConnectInfo')}}</div>
          <div class="setting-value fixed"><el-button :disabled="!form.jdbc_datasource_enabled" plain icon="el-ksd-icon-add_22" size="small" @click="addDataSourceSetting">{{$t('add')}}</el-button></div>
        </div>
      </template>
    </EditableBlock>
    <!-- 项目存储设置 -->
    <EditableBlock
      v-if="$store.state.config.platform !== 'iframe'"
      :header-content="$t('storageSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'storage-quota')"
      @submit="(scb, ecb) => handleSubmit('storage-quota', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('storage-quota', scb, ecb)">
      <el-form ref="setting-storage-quota" :model="form" :rules="storageQuota">
        <div class="setting-item">
          <div class="form-item">
            <span class="setting-label font-medium">{{$t('storageQuota')}}</span>
            <el-form-item class="setting-input" prop="storage_quota_tb_size">
              <el-input size="small" style="width: 100px;" v-number3="form.storage_quota_tb_size" v-model="form.storage_quota_tb_size"></el-input> TB
            </el-form-item>
          </div>
          <!-- <span class="setting-value fixed">{{form.storage_quota_size | dataSize}}</span> -->
          <div class="setting-desc">{{$t('storageQuotaDesc')}}</div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 低效存储设置 -->
    <EditableBlock
      :header-content="$t('indexOptimizationSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'index-optimization')"
      @submit="(scb, ecb) => handleSubmit('index-optimization', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('index-optimization', scb, ecb)">
      <el-form ref="setting-index-optimization" :model="form" :rules="indexOptimization">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
          <div class="setting-desc large">
            <span>{{$t('storageGarbageDesc1')}}</span>
            <el-select
              class="setting-input"
              size="small"
              style="width: 100px;"
              v-model="form.frequency_time_window"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="lowUsageStorageType in lowUsageStorageTypes"
                :key="lowUsageStorageType"
                :label="$t(lowUsageStorageType+'1')"
                :value="lowUsageStorageType">
              </el-option>
            </el-select>
            <span>{{$t('storageGarbageDesc2')}}</span>
            <el-form-item class="setting-input" prop="low_frequency_threshold">
              <el-input-number size="small" style="width: 100px;" :max="9999" input-enabled v-number="form.low_frequency_threshold" v-model="form.low_frequency_threshold" :controls="false"></el-input-number>
            </el-form-item>
            <span>{{$store.state.project.isSemiAutomatic ? $t('storageGarbageDesc3ForSemiAutomatic') : $t('storageGarbageDesc3')}}</span>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 下压查询设置 -->
    <EditableBlock
      :header-content="$t('pushdownSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngine')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.push_down_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-engine', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownEngineDesc')}}</div>
      </div>
      <!-- <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span><span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.push_down_range_limited"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-range', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownRangeDesc')}}</div>
      </div> -->
    </EditableBlock>
    <!-- Segment设置 -->
    <EditableBlock
      :header-content="$t('segmentSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'segment-settings')"
      @submit="(scb, ecb) => handleSubmit('segment-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('segment-settings', scb, ecb)">
      <el-form ref="segment-setting-form" :model="form" :rules="rules">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('segmentMerge')}}</span><span class="setting-value fixed">
            <el-switch
              v-model="form.auto_merge_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
          <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
            <span class="setting-label font-medium">{{$t('autoMerge')}}</span>
            <span class="setting-value">
              {{form.auto_merge_time_ranges.map(autoMergeConfig => $t(autoMergeConfig)).join(', ')}}
            </span>
            <el-checkbox-group class="setting-input" :value="form.auto_merge_time_ranges" @input="handleCheckMergeRanges" :disabled="!form.auto_merge_enabled">
              <el-checkbox
                v-for="autoMergeType in autoMergeTypes"
                :key="autoMergeType"
                :label="autoMergeType">
                {{$t(autoMergeType)}}
              </el-checkbox>
            </el-checkbox-group>
          </div>
          <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
            <span class="setting-label font-medium">{{$t('volatile')}}</span>
            <span class="setting-value">
              {{form.volatile_range.volatile_range_number}} {{$t(form.volatile_range.volatile_range_type.toLowerCase())}}
            </span>
            <el-form-item class="setting-input" prop="volatile_range.volatile_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.volatile_range.volatile_range_number" v-model="form.volatile_range.volatile_range_number" :disabled="!form.auto_merge_enabled"></el-input>
            </el-form-item><el-select
              class="setting-input"
              size="small"
              style="width: 100px;"
              v-model="form.volatile_range.volatile_range_type"
              :disabled="!form.auto_merge_enabled"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="volatileType in volatileTypes"
                :key="volatileType"
                :label="$t(volatileType.toLowerCase())"
                :value="volatileType">
              </el-option>
            </el-select>
            <div class="setting-desc">{{$t('volatileTip')}}</div>
          </div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span><span class="setting-value fixed ksd-fs-12">
            <el-switch
              v-model="form.retention_range.retention_range_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('retentionThresholdDesc')}}</div>
          <div class="field-item" :class="{ disabled: !form.retention_range.retention_range_enabled }">
            <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span>
            <span class="setting-value">
              {{form.retention_range.retention_range_number}} {{$t(form.retention_range.retention_range_type.toLowerCase())}}
            </span>
            <el-form-item class="setting-input" prop="retention_range.retention_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.retention_range.retention_range_number" v-model="form.retention_range.retention_range_number" :disabled="!form.retention_range.retention_range_enabled"></el-input>
            </el-form-item>
            <!-- <span class="setting-input">{{$t(retentionRangeScale)}}</span> -->
            <el-select
              class="setting-input"
              size="small"
              :disabled="!form.retention_range.retention_range_enabled"
              style="width: 100px;"
              v-model="form.retention_range.retention_range_type"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="type in retentionTypes.filter(it => it !== 'WEEK')"
                :key="type"
                :label="$t(type.toLowerCase())"
                :value="type">
              </el-option>
            </el-select>
          </div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('emptySegmentEnable')}} <span class="beta-label">BETA</span></span><span class="setting-value fixed ksd-fs-12">
            <el-switch
              v-model="form.create_empty_segment_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('emptySegmentEnableDesc')}}</div>
        </div>
      </el-form>
    </EditableBlock>
    <EditableBlock
      v-if="$store.state.project.isSemiAutomatic"
      ref="acclerationRuleSettings"
      :header-content="$t('acclerationRuleSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'accleration-rule-settings')"
      @submit="(scb, ecb) => handleSubmit('accleration-rule-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('accleration-rule-settings', scb, ecb)">
      <el-form ref="rulesForm" :rules="rulesSettingRules" :model="rulesObj" size="medium" class="ruleSetting">
        <!-- <div class="conds">
          <div class="conds-title">
            <span>{{$t('queryFrequency')}}</span>
            <el-switch size="small" v-model="rulesObj.count_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-mt-10 ksd-fs-14">
              <el-form-item prop="count_value">
                <span>{{$t('AccQueryStart')}}</span>
                <el-input-number :min="1" :max="1000" v-model.trim="rulesObj.count_value" size="small" class="rule-setting-input count-input" :disabled="!rulesObj.count_enable" :controls="false"></el-input-number>
                <span>{{$t('AccQueryEnd')}}</span>
              </el-form-item>
            </div>
          </div>
        </div> -->

        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('querySubmitter')}}</span><span class="ksd-fs-12">
            <el-switch v-model="rulesObj.submitter_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch></span>
          </div>
          <div class="conds-content">
            <div class="ksd-fs-12 ksd-mt-5">{{$t('querySubmitterTips')}}</div>
            <div class="vip-users-block">
              <el-form-item prop="users">
                <div class="ksd-mt-8 conds-title"><i class="el-icon-ksd-table_admin"></i> User</div>
                <el-select v-model="rulesObj.users" v-event-stop :popper-append-to-body="false" filterable remote :remote-method="remoteMethod" :loading="loading" size="medium" :placeholder="rulesObj.users.length ? '' : $t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                  <span slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!rulesObj.users.length"></span>
                  <el-option v-for="item in filterSubmitterUserOptions" :key="item" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
              <el-form-item prop="user_groups">
              <div class="ksd-mt-8 conds-title"><i class="el-icon-ksd-table_group"></i> User Group</div>
              <el-select v-model="rulesObj.user_groups" v-event-stop :popper-append-to-body="false" filterable size="medium" :placeholder="rulesObj.user_groups.length ? '' : $t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                <span slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!rulesObj.user_groups.length"></span>
                <el-option v-for="item in allSubmittersOptions.group" :key="item" :label="item" :value="item"></el-option>
              </el-select>
              </el-form-item>
            </div>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('queryDuration')}}</span><span class="ksd-fs-12">
            <el-switch @change="changeDurationEnable" v-model="rulesObj.duration_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch></span>
          </div>
          <div class="conds-content clearfix">
            <div class="duration-rule ksd-mt-8 ksd-fs-12">
              {{$t('from')}}
              <el-form-item prop="min_duration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.min_duration" v-number="rulesObj.min_duration" size="small" :class="['rule-setting-input', rulesObj.duration_enable && durationError && 'is-error']" :disabled="!rulesObj.duration_enable" @blur="$refs.rulesForm.validateField('min_duration')"></el-input>
              </el-form-item>
              {{$t('to')}}
              <el-form-item prop="max_duration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.max_duration" v-number="rulesObj.max_duration" size="small" :class="['rule-setting-input', rulesObj.duration_enable && durationError && 'is-error']" :disabled="!rulesObj.duration_enable" @blur="$refs.rulesForm.validateField('max_duration')"></el-input>
              </el-form-item>
              {{$t('secondes')}}
            </div>
            <span class="error-msg" v-if="durationError && durationErrorMsg">{{durationErrorMsg}}</span>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('excludeRule')}}</span><span class="ksd-fs-12">
            <el-switch v-model="rulesObj.excluded_tables_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch></span>
          </div>
          <div class="conds-content clearfix">
              <div class="ksd-mt-8 ksd-fs-14">
                <div class="exclude-rule-msg">
                  <p class="tips">{{$t('excludeRuleTip')}}<span class="review-details" @click="showExcludeRuleDetails = !showExcludeRuleDetails">{{$t('moreDetails')}}<i :class="['arrow', showExcludeRuleDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02']"></i></span></p>
                  <div class="details" v-if="showExcludeRuleDetails">
                    <ol>
                      <li><i class="point">•</i>{{$t('excludeRuleDetailMsg1')}}</li>
                      <li><i class="point">•</i>{{$t('excludeRuleDetailMsg2')}}</li>
                    </ol>
                  </div>
                </div>
                <el-form-item class="exclude_rule-form" prop="excluded_tables" style="display: inline-block;">
                  <el-select
                    class="exclude_rule-select"
                    v-model="rulesObj.excluded_tables"
                    multiple
                    filterable
                    remote
                    :remote-method="filterExcludeTables"
                    :placeholder="rulesObj.excluded_tables.length ? '' : $t('kylinLang.common.pleaseSelectOrSearch')"
                    :loading="loading">
                    <el-option
                      v-for="item in excludeRuleOptions"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                      <span>{{item.label}}<i v-if="item.fact" class="el-icon-ksd-fact_table ksd-ml-5"></i></span>
                    </el-option>
                    <p class="limit-excluded-tables-msg" v-if="showIimitExcludedTableMsg">{{$t('limitExcludedTablesTip')}}</p>
                  </el-select>
                </el-form-item>
              </div>
            </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('hitRules')}}</span>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-fs-12 ksd-mt-8">
              {{$t('ruleTips')}}
              <el-tooltip effect="dark" :content="$t('ruleTooltips')" placement="top"><i class="el-ksd-icon-more_info_16 icon ksd-fs-16"></i></el-tooltip>
            </div>
            <div class="ksd-fs-12 ksd-mt-8">
                {{$t('timeFrame')}}
                <el-form-item prop="effective_days" class="inline-form-item">
                  <el-input-number style="width:100px;" input-enabled v-model="rulesObj.effective_days" size="small" :min="1" :max="30" ></el-input-number>
                </el-form-item>
                {{$t('days')}}
            </div>
            <div class="ksd-fs-12 ksd-mt-8">
                {{$t('hitNums')}}
                <el-form-item prop="min_hit_count" class="inline-form-item">
                  <el-input style="width:100px;" v-model.trim="rulesObj.min_hit_count" v-number4="rulesObj.min_hit_count" size="small"></el-input>
                </el-form-item>
                {{$t('hitNumTips')}}
            </div>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('optimizationSuggestions')}}</span>
          </div>
          <div class="conds-content clearfix">
            <div class="conds-content clearfix">
              <div class="ksd-mt-8 ksd-fs-12">
                {{$t('suggestionTip1')}}
                <el-form-item prop="recommendations_value" style="display: inline-block;">
                  <el-select v-model="rulesObj.recommendations_value" v-event-stop size="mini" :disabled="!rulesObj.recommendation_enable" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" style="width:70px">
                    <el-option v-for="item in [10, 20, 50, 100]" :key="item" :label="item" :value="item"></el-option>
                  </el-select>
                </el-form-item>
                <span v-html="$t('suggestionTip2')"></span>
              </div>
            </div>
            <div class="ksd-fs-12 ksd-mt-12">
                {{$t('recommendationFrequency')}}
                <el-form-item prop="update_frequency" class="inline-form-item">
                  <el-input-number style="width:100px;" input-enabled v-model="rulesObj.update_frequency" size="small" :min="1"></el-input-number>
                </el-form-item>
                <span v-html="$t('recommendationFrequencyTips')"></span>
            </div>
          </div>
        </div>
      </el-form>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { handleError, handleSuccess, handleSuccessAsync, objectClone, ArrayFlat } from '../../../util'
import { projectTypeIcons, lowUsageStorageTypes, autoMergeTypes, volatileTypes, validate, initialFormValue, _getProjectGeneralInfo, _getSegmentSettings, _getPushdownConfig, _getStorageQuota, _getIndexOptimization, _getRetentionRangeScale } from './handler'
import { retentionTypes } from '../handler'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
import SourceAuthorityForm from '../../common/DataSourceModal/SourceJDBC/SourceAuthorityForm/SourceAuthorityForm.vue'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  components: {
    EditableBlock,
    SourceAuthorityForm
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'currentProjectData'
    ]),
    ...mapState({
      currentProject: state => state.project.selected_project,
      dataSource: state => state.datasource.dataSource
    })
  },
  methods: {
    ...mapActions({
      updateProjectGeneralInfo: 'UPDATE_PROJECT_GENERAL_INFO',
      updateSegmentConfig: 'UPDATE_SEGMENT_CONFIG',
      updatePushdownConfig: 'UPDATE_PUSHDOWN_CONFIG',
      updateStorageQuota: 'UPDATE_STORAGE_QUOTA',
      updateIndexOptimization: 'UPDATE_INDEX_OPTIMIZATION',
      resetConfig: 'RESET_PROJECT_CONFIG',
      getFavoriteRules: 'GET_FAVORITE_RULES',
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      updateFavoriteRules: 'UPDATE_FAVORITE_RULES',
      fetchDBandTables: 'FETCH_DB_AND_TABLES',
      checkConnectByGbase: 'CHECK_BASE_CONFIG'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      updateProject: 'UPDATE_PROJECT'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  lowUsageStorageTypes = lowUsageStorageTypes
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes
  retentionTypes = retentionTypes
  form = initialFormValue
  storageQuotaSize = 0
  rulesSettingRules = {
    count_value: [{validator: this.validatePass, trigger: 'blur'}],
    min_duration: [{validator: this.validatePass, trigger: 'blur'}],
    max_duration: [{validator: this.validatePass, trigger: 'blur'}],
    effective_days: [{validator: this.validatePass, trigger: 'blur'}],
    min_hit_count: [{validator: this.validatePass, trigger: 'blur'}],
    update_frequency: [{validator: this.validatePass, trigger: 'blur'}]
  }
  allSubmittersOptions = {
    user: [],
    group: []
  }
  loading = false
  filterUsers = []
  rulesObj = {
    count_enable: true,
    count_value: 0,
    submitter_enable: true,
    users: [],
    user_groups: [],
    duration_enable: false,
    min_duration: 0,
    max_duration: 0,
    recommendation_enable: true,
    recommendations_value: 20,
    excluded_tables_enable: false,
    excluded_tables: [],
    effective_days: 2,
    min_hit_count: 30,
    update_frequency: 2
  }
  rulesAccerationDefault = {}
  durationError = false
  durationErrorMsg = ''
  showExcludeRuleDetails = false
  excludeRuleOptions = []
  dbInfoFilter = {
    project_name: '',
    source_type: 9,
    page_offset: 0,
    page_size: 10,
    table: ''
  }
  filterExcludeTablesTimer = null
  showIimitExcludedTableMsg = false
  JDBCConnectSettingBackup = []
  jdbcDatasourceEnabled = false
  allDatasourceTables = []

  created () {
    this.rulesAccerationDefault = {...this.rulesObj}
  }
  get projectIcon () {
    return projectTypeIcons[this.project.maintain_model_type]
  }
  get retentionRangeScale () {
    return _getRetentionRangeScale(this.form).toLowerCase()
  }
  get rules () {
    return {
      'volatile_range.volatile_range_number': [{ validator: (rule, value, callback) => validate['positiveNumber'].call(this, rule, value, callback), trigger: 'change' }],
      'retention_range.retention_range_number': [{ validator: (rule, value, callback) => validate['positiveNumber'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }
  get storageQuota () {
    return {
      'storage_quota_tb_size': [{ validator: (rule, value, callback) => validate['storageQuotaSize'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }
  get indexOptimization () {
    return {
      'low_frequency_threshold': [{ validator: (rule, value, callback) => validate['storageQuotaNum'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }

  get haveHiveOrKafKa () {
    if (this.dataSource[this.currentProject] && this.dataSource[this.currentProject].length) {
      return this.dataSource[this.currentProject].filter(it => it.source_type === 1 || it.source_type === 9).length
    } else {
      return false
    }
  }

  get haveJDBCDatasource () {
    return this.allDatasourceTables.filter(it => it.source_type !== 1 && it.source_type !== 9).length > 0
  }

  validatePass (rule, value, callback) {
    if (rule.field.indexOf('duration') !== -1 && this.rulesObj.duration_enable) {
      if (rule.field === 'min_duration') {
        if (!value && value !== 0) {
          callback(new Error(this.$t('emptyTips')))
        }
        this.$refs.rulesForm.validateField('max_duration')
        callback()
      } else if (rule.field === 'max_duration') {
        if (!value && value !== 0) {
          callback(new Error(this.$t('emptyTips')))
        } else if (+this.rulesObj.min_duration > +this.rulesObj.max_duration) {
          callback(new Error(this.$t('prevGreaterThanNext')))
        } else if (+this.rulesObj.max_duration > 3600) {
          callback(new Error(this.$t('overTimeLimitTip')))
        } else {
          callback()
        }
      } else {
        callback()
      }
    } else if (rule.field === 'count_value' && this.rulesObj.count_enable) {
      if (!value && value !== 0) {
        callback(new Error(null))
      } else {
        callback()
      }
    } else if (rule.field === 'effective_days') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('effectiveDaysEmptyTips')))
      } else {
        callback()
      }
    } else if (rule.field === 'min_hit_count') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('emptyTips')))
      } else {
        callback()
      }
    } else if (rule.field === 'update_frequency') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('upadateFreEmptyTips')))
      } else {
        callback()
      }
    } else {
      this.durationError = false
      callback()
    }
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  @Watch('rulesObj', { deep: true })
  @Watch('rulesAccerationDefault', { deep: true })
  onFormChange () {
    const basicSetting = this.isFormEdited(this.form, 'basic-info') || this.isFormEdited(this.form, 'datasource-info') || this.isFormEdited(this.form, 'segment-settings') || this.isFormEdited(this.form, 'storage-quota') || this.isFormEdited(null, 'accleration-rule-settings')
    this.$emit('form-changed', { basicSetting })
  }
  initForm () {
    this.handleInit('basic-info')
    this.handleInit('datasource-info')
    this.handleInit('segment-settings')
    this.handleInit('pushdown-settings')
    this.handleInit('storage-quota')
    this.handleInit('index-optimization')
  }
  async mounted () {
    this.initForm()
    this.getAllDatasourceTables()
    if (this.$store.state.project.isSemiAutomatic) {
      this.getAccelerationRules()
      this.getDbAndTablesInfo()
    }
    if ('moveTo' in this.$route.query && this.$route.query.moveTo === 'index-suggest-setting') {
      this.$refs.acclerationRuleSettings && this.$refs.acclerationRuleSettings.$el.scrollIntoView && this.$refs.acclerationRuleSettings.$el.scrollIntoView()
    }
  }
  handleCheckMergeRanges (value) {
    if (value.length > 0) {
      this.form.auto_merge_time_ranges = value
    }
  }
  async handleSwitch (type, value) {
    try {
      switch (type) {
        case 'auto-merge': {
          const submitData = _getSegmentSettings(this.project)
          submitData.auto_merge_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'auto-retention': {
          const submitData = _getSegmentSettings(this.project)
          submitData.retention_range.retention_range_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'pushdown-range': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_range_limited = value
          await this.updatePushdownConfig(submitData); break
        }
        case 'pushdown-engine': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_enabled = value
          await this.updatePushdownConfig(submitData); break
        }
      }
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'basic-info': {
          const submitData = _getProjectGeneralInfo(this.form)
          if (!submitData.semi_automatic_mode) {
            await this.callGlobalDetailDialog({
              msg: this.$t('turnOffTips'),
              title: this.$t('turnOff') + this.$t('enableSemiAutomatic'),
              dialogType: 'warning',
              isBeta: false,
              wid: '600px',
              showDetailBtn: false,
              dangerouslyUseHTMLString: true,
              needConcelReject: true,
              submitText: this.$t('confirmClose')
            })
            await this.updateProjectGeneralInfo(submitData); break
          } else {
            await this.callGlobalDetailDialog({
              msg: this.$t('turnOnTips'),
              title: this.$t('turnOn') + this.$t('enableSemiAutomatic'),
              dialogType: 'warning',
              isBeta: false,
              wid: '400px',
              isCenterBtn: true,
              showDetailBtn: false,
              dangerouslyUseHTMLString: true,
              needConcelReject: true,
              submitText: this.$t('confirmOpen')
            })
            await this.updateProjectGeneralInfo(submitData)
            this.getAccelerationRules()
            this.getDbAndTablesInfo() // 开启智能推荐模式，拉取数据源表
            break
          }
        }
        case 'datasource-info': {
          if (!this.form.JDBCConnectSetting.length) return errorCallback()
          const { connectionString, username, password, name, driver } = this.form.JDBCConnectSetting[0]
          const currentProject = this.currentProjectData
          const data = {
            jdbc_source_connection_url: connectionString,
            jdbc_source_user: username,
            // jdbc_source_pass: password || null,
            jdbc_source_enable: this.form.jdbc_datasource_enabled,
            jdbc_source_name: name,
            jdbc_source_driver: driver
          }
          password && (data['jdbc_source_pass'] = password)
          // this.$refs['gbase-datasource'].$refs['source-auth-form'] && this.$refs['gbase-datasource'].$refs['source-auth-form'].$refs.form.validate()
          if (!connectionString || !username) return errorCallback()
          if (this.JDBCConnectSettingBackup.length && JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(this.form.JDBCConnectSetting)) {
            await this.$msgbox({
              width: '400px',
              type: 'warning',
              centerButton: true,
              showCancelButton: true,
              title: this.$t('saveDatasourceTitle'),
              message: this.$t('saveDatasourceContent'),
              confirmButtonText: this.$t('kylinLang.common.save')
            })
          }
          await this.checkConnectByGbase({...data, project: this.currentSelectedProject})
          if ('override_kylin_properties' in currentProject) {
            currentProject['override_kylin_properties']['kylin.source.jdbc.connection-url'] = connectionString
            currentProject['override_kylin_properties']['kylin.source.jdbc.pass'] = password
            currentProject['override_kylin_properties']['kylin.source.jdbc.source.enable'] = this.form.jdbc_datasource_enabled.toString()
            currentProject['override_kylin_properties']['kylin.source.jdbc.source.name'] = name
            currentProject['override_kylin_properties']['kylin.source.jdbc.user'] = username
          }
          this.JDBCConnectSettingBackup = JSON.parse(JSON.stringify(this.form.JDBCConnectSetting))
          this.jdbcDatasourceEnabled = this.form.jdbc_datasource_enabled
          this.updateProject({project: currentProject})
          break
        }
        case 'segment-settings': {
          if (await this.$refs['segment-setting-form'].validate()) {
            const submitData = _getSegmentSettings(this.form, this.project)
            // const dataMap = { 'HOUR': 1, 'DAY': 2, 'WEEK': 3, 'MONTH': 4, 'QUARTER': 5, 'YEAR': 6 }
            // if (dataMap[submitData.retention_range.retention_range_type] < dataMap[submitData.auto_merge_time_ranges[submitData.auto_merge_time_ranges.length - 1]]) {
            //   this.$message({
            //     type: 'error',
            //     message: this.$t('retentionLessThanAutoMerge')
            //   })
            //   return errorCallback()
            // }
            await this.updateSegmentConfig(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'storage-quota': {
          if (await this.$refs['setting-storage-quota'].validate()) {
            const submitData = _getStorageQuota(this.form, this.project)
            // TB转byte
            this.form.storage_quota_size = submitData.storage_quota_size = +(submitData.storage_quota_tb_size * 1024 * 1024 * 1024 * 1024).toFixed(0)
            await this.updateStorageQuota(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'index-optimization': {
          if (await this.$refs['setting-index-optimization'].validate()) {
            const submitData = _getIndexOptimization(this.form, this.project)
            await this.updateIndexOptimization(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'accleration-rule-settings': {
          if (await this.$refs['rulesForm'].validate() && !this.durationError) {
            await this.saveAcclerationRule()
          } else {
            return errorCallback()
          }
        }
      }
      successCallback()
      await this.$emit('reload-setting')
      if (type === 'storage-quota') {
        setTimeout(() => {
          this.handleInit('storage-quota')
        }, 500)
      }
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  handleInit (type) {
    switch (type) {
      case 'basic-info': {
        this.form = { ...this.form, ..._getProjectGeneralInfo(this.project) }; break
      }
      case 'datasource-info': {
        const { override_kylin_properties: projectConfig } = this.currentProjectData
        this.form.JDBCConnectSetting = []
        if (!projectConfig['kylin.source.jdbc.connection-url']) return
        this.form.JDBCConnectSetting.push({
          connectionString: projectConfig['kylin.source.jdbc.connection-url'] || '',
          username: projectConfig['kylin.source.jdbc.user'] || '',
          password: projectConfig['kylin.source.jdbc.pass'] ? projectConfig['kylin.source.jdbc.pass'] === '*****' ? '' : projectConfig['kylin.source.jdbc.pass'] : '',
          sourceType: +projectConfig['kylin.source.default'] || 9,
          name: projectConfig['kylin.source.jdbc.source.name'] || '',
          driver: projectConfig['kylin.source.jdbc.driver'] || ''
        })
        this.form.jdbc_datasource_enabled = projectConfig['kylin.source.jdbc.source.enable'] ? projectConfig['kylin.source.jdbc.source.enable'] === 'true' : false
        this.JDBCConnectSettingBackup = JSON.parse(JSON.stringify(this.form.JDBCConnectSetting))
        this.jdbcDatasourceEnabled = this.form.jdbc_datasource_enabled
        break
      }
      case 'segment-settings': {
        this.form = { ...this.form, ..._getSegmentSettings(this.project) }
        this.$refs['segment-setting-form'] && this.$refs['segment-setting-form'].clearValidate && this.$refs['segment-setting-form'].clearValidate()
        break
      }
      case 'pushdown-settings': {
        this.form = { ...this.form, ..._getPushdownConfig(this.project) }; break
      }
      case 'storage-quota': {
        this.form = { ...this.form, ..._getStorageQuota(this.project) }; break
      }
      case 'index-optimization': {
        this.form = { ...this.form, ..._getIndexOptimization(this.project) }; break
      }
    }
  }
  async handleResetForm (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'segment-settings': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'segment_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getSegmentSettings(data) }
          this.$refs['segment-setting-form'].clearValidate()
          break
        }
        case 'storage-quota': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'storage_quota_config'})
          let data = await handleSuccessAsync(res)
          const quotaSize = data.storage_quota_size / 1024 / 1024 / 1024 / 1024
          data = {...data, storage_quota_tb_size: quotaSize.toFixed(2)}
          this.form = { ...this.form, ..._getStorageQuota(data) }
          this.$refs['setting-storage-quota'].clearValidate()
          break
        }
        case 'index-optimization': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'garbage_cleanup_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getIndexOptimization(data) }
          this.$refs['setting-index-optimization'].clearValidate()
          break
        }
        case 'accleration-rule-settings': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'favorite_rule_config'})
          const data = await handleSuccessAsync(res)
          const { favorite_rules } = data
          favorite_rules.excluded_tables = favorite_rules.excluded_tables ? favorite_rules.excluded_tables.split(',') : []
          this.rulesObj = {...this.rulesObj, ...favorite_rules}
          this.rulesAccerationDefault = {...this.rulesAccerationDefault, ...favorite_rules}
          this.$refs['rulesForm'].resetFields()
        }
      }
      successCallback()
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.resetSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'basic-info':
        return JSON.stringify(_getProjectGeneralInfo(form)) !== JSON.stringify(_getProjectGeneralInfo(project))
      case 'datasource-info':
        return form.jdbc_datasource_enabled
          ? form.JDBCConnectSetting.length > 0
            ? this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled || JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
            : this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled && JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
          : this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled || JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
      case 'segment-settings':
        return JSON.stringify(_getSegmentSettings(form)) !== JSON.stringify(_getSegmentSettings(project))
      case 'storage-quota':
        // form.storage_quota_size = +(form.storage_quota_tb_size * 1024 * 1024 * 1024 * 1024).toFixed(0)
        return JSON.stringify(_getStorageQuota(form)) !== JSON.stringify(_getStorageQuota(project))
      case 'index-optimization':
        return JSON.stringify(_getIndexOptimization(form)) !== JSON.stringify(_getIndexOptimization(project))
      case 'accleration-rule-settings':
        return JSON.stringify(this.rulesAccerationDefault) !== JSON.stringify(this.rulesObj)
    }
  }
  // 获取优化建议规则
  getAccelerationRules () {
    if (this.currentSelectedProject) {
      this.getFavoriteRules({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.rulesObj = {...data, excluded_tables: data.excluded_tables ? data.excluded_tables.split(',') : []}
          this.rulesAccerationDefault = {...data, excluded_tables: data.excluded_tables ? data.excluded_tables.split(',') : []}
        })
      }).catch((res) => {
        handleError(res)
      })
      this.getUserAndGroups().then((res) => {
        handleSuccess(res, (data) => {
          this.allSubmittersOptions = data
          this.filterUsers = this.allSubmittersOptions.user
        }, (res) => {
          handleError(res)
        })
      })
    }
  }
  remoteMethod (query) {
    if (query !== '') {
      this.loading = true
      setTimeout(() => {
        this.loading = false
        this.filterUsers = this.allSubmittersOptions.user.filter(item => {
          return item.toLowerCase()
            .indexOf(query.toLowerCase()) > -1
        })
      }, 0)
    } else {
      this.filterUsers = this.allSubmittersOptions.user
    }
  }
  get filterSubmitterUserOptions () {
    const filterUsers = objectClone(this.filterUsers)
    return filterUsers.splice(0, 500)
  }
  // 保存优化建议规则
  saveAcclerationRule () {
    return new Promise((resolve, reject) => {
      let submitData = objectClone(this.rulesObj)
      submitData.min_duration = +submitData.min_duration
      submitData.max_duration = +submitData.max_duration
      submitData.excluded_tables = submitData.excluded_tables.join(',')
      // 换成次数字段了，不需要除于 100
      // submitData.freqValue = submitData.freqValue / 100
      this.updateFavoriteRules({ ...submitData, ...{project: this.currentSelectedProject} }).then((res) => {
        handleSuccess(res, (data) => {
          this.rulesAccerationDefault = {...this.rulesAccerationDefault, ...this.rulesObj}
        })
        resolve()
      }, (res) => {
        handleError(res)
        reject()
      })
    })
  }

  changeDurationEnable (val) {
    !val && (this.durationError = val)
  }

  async getAllDatasourceTables () {
    try {
      this.dbInfoFilter.project_name = this.currentSelectedProject
      const response = await this.fetchDBandTables({
        project_name: this.currentSelectedProject,
        source_type: '1,9,8',
        page_offset: 0,
        page_size: 10,
        table: ''
      })
      const results = await handleSuccessAsync(response)
      const { databases } = results
      databases && databases.length > 0 && (this.allDatasourceTables = ArrayFlat(databases.map(it => 'tables' in it ? it.tables : [])))
    } catch (e) {
      handleError(e)
    }
  }

  // 获取 database 和 tables 信息（显示前 50 条记录）
  async getDbAndTablesInfo () {
    try {
      this.dbInfoFilter.project_name = this.currentSelectedProject
      const response = await this.fetchDBandTables(this.dbInfoFilter)
      const results = await handleSuccessAsync(response)
      const { databases } = results
      let dbList = databases ? ArrayFlat(databases.map(item => item.tables)) : []
      if (dbList.length) {
        this.excludeRuleOptions = dbList.map(it => ({label: `${it.database}.${it.name}`, value: `${it.database}.${it.name}`, fact: it.root_fact})).slice(0, 50)
        this.showIimitExcludedTableMsg = dbList.length > 50
      }
    } catch (e) {
      handleError(e)
    }
  }

  // 过滤 db 或 table
  filterExcludeTables (name) {
    clearTimeout(this.filterExcludeTablesTimer)
    this.filterExcludeTablesTimer = setTimeout(() => {
      this.dbInfoFilter.table = name
      this.getDbAndTablesInfo()
    }, 500)
  }

  // 更改数据源设置
  modifyDataSourceSetting (index) {
    const key = Date.now().toString(32)
    const form = objectClone(this.form.JDBCConnectSetting[index])
    this.$msgbox({
      width: '600px',
      title: this.$t('modifyDatasourceConnect'),
      message: <source-authority-form ref='datasourceAuthorityForm' key={key} form={form}/>,
      confirmButtonText: this.$t('kylinLang.common.save'),
      showCancelButton: true,
      closeOnClickModal: false,
      closeOnPressEscape: false,
      callback: (action) => {
        return new Promise((resolve, reject) => {
          if (action === 'confirm') {
            this.$set(this.form.JDBCConnectSetting, index, form)
            resolve()
          } else {
            reject()
          }
        })
      },
      beforeClose: async (action, instance, done) => {
        try {
          if (action === 'confirm') {
            this.$refs.datasourceAuthorityForm && await this.$refs.datasourceAuthorityForm.$refs.form.validate()
            done()
          } else {
            this.$refs.datasourceAuthorityForm && this.$refs.datasourceAuthorityForm.$refs.form.clearValidate()
            done()
          }
        } catch (e) {
          console.error(e)
        }
      }
    })
  }

  // 添加数据源设置
  addDataSourceSetting () {
    const key = Date.now().toString(32)
    const form = {
      connectionString: '',
      username: '',
      password: '',
      name: ''
    }
    this.$msgbox({
      width: '600px',
      title: this.$t('addDatasourceConnect'),
      message: <source-authority-form ref='datasourceAuthorityForm' key={key} form={form}/>,
      confirmButtonText: this.$t('kylinLang.common.save'),
      showCancelButton: true,
      closeOnClickModal: false,
      closeOnPressEscape: false,
      callback: (action) => {
        return new Promise((resolve, reject) => {
          if (action === 'confirm') {
            this.form.JDBCConnectSetting.push(form)
            resolve()
          } else {
            reject()
          }
        })
      },
      beforeClose: async (action, instance, done) => {
        try {
          if (action === 'confirm') {
            this.$refs.datasourceAuthorityForm && await this.$refs.datasourceAuthorityForm.$refs.form.validate()
            done()
          } else {
            if (this.$refs.datasourceAuthorityForm) {
              this.$refs.datasourceAuthorityForm.$refs.form.clearValidate()
            }
            done()
          }
        } catch (e) {
          console.error(e)
        }
      }
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.basic-setting {
  .beta-label {
    display: inline-block;
    height: 18px;
    line-height: 9px;
    background: #EFDBFF;
    color: #531DAB;
    font-size: 11px;
    font-family: Lato-Bold, Lato;
    font-weight: bold;
    border-radius: 2px;
    padding: 5px;
    box-sizing: border-box;
  }
  .clearfix .setting-value,
  .clearfix .setting-input {
    width: calc(~'100% - 114px');
  }
  .ksd-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .el-icon-ksd-expert_mode_small,
  .el-icon-ksd-smart_mode_small {
    margin-right: 5px;
  }
  .form-item {
    display: flex;
    .setting-label {
      padding-top: 6px;
    }
  }
  .el-form-item {
    &.is-error {
      vertical-align: top;
      margin-top: -4px;
    }
    .el-form-item__error {
      width: 100px;
    }
  }
  .duration-rule {
    .el-form-item {
      &.is-error {
        vertical-align: text-top;
        margin-top: -8px;
      }
    }
  }
  .ruleSetting {
    padding: 15px 20px;
    .conds-title {
      font-weight: @font-medium;
    }
    .conds > .conds-title {
      height: 21px;
      line-height: 22px;
      display: flex;
      align-items: center;
    }
    .el-form-item--medium .el-form-item__content, .el-form-item--medium .el-form-item__label {
      line-height: 1;
    }
    .el-form-item {
      margin-bottom: 0;
      &.inline-form-item {
        display: inline-block;
        vertical-align: top;
        margin-top: -4px;
      }
    }
    .conds {
      margin-bottom: 16px;
      // padding-bottom: 15px;
      // border-bottom: 1px solid @line-split-color;
    }
    .exclude-rule-msg {
      font-size: 12px;
      .tips {
        .review-details {
          color: @base-color;
          cursor: pointer;
          position: relative;
          .arrow {
            transform: rotate(90deg);
            font-size: 10px;
            margin-left: 5px;
          }
        }
      }
      .details {
        background: @base-background-color-1;
        padding: 10px 10px;
        margin-top: 5px;
        box-sizing: border-box;
        .point {
          font-style: inherit;
          margin-right: 5px;
        }
      }
    }
    .exclude_rule-form {
      width: 100%;
      .exclude_rule-select {
        width: 100%;
        margin-top: 10px;
      }
    }
  }
  .rule-setting-input {
    display: inline-block;
    width: 100px;
    &.count-input{
      width: 80px;
      &.el-input-number.is-without-controls .el-input__inner{
        text-align: left;
      }
    }
    &.is-error {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
  }
  .error-msg {
    color: #ff0000;
    font-size: 12px;
  }
}
.limit-excluded-tables-msg {
  height: 32px;
  color: @text-normal-color;
  line-height: 32px;
  text-align: center;
  font-size: 12px;
}

</style>
