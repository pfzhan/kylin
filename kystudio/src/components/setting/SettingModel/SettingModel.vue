<template>
  <div class="setting-model">
    <div  class="ksd-mb-10 ksd-fright">
      <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" style="width:200px" size="medium" prefix-icon="el-icon-search" v-model="filter.model_name" v-global-key-event.enter.debounce="searchModels">
      </el-input>
    </div>
    <el-table
      :data="modelList"
      class="model-setting-table"
      border
      :empty-text="emptyText"
      style="width: 100%">
      <el-table-column width="230px" show-overflow-tooltip prop="alias" :label="modelTableTitle"></el-table-column>
      <el-table-column prop="last_modified" show-overflow-tooltip width="218px" :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span v-if="scope.row.config_last_modified>0">{{transToGmtTime(scope.row.config_last_modified)}}</span>
        </template>
      </el-table-column>
      <el-table-column prop="config_last_modifier" show-overflow-tooltip width="105" :label="$t('modifiedUser')"></el-table-column>
      <el-table-column min-width="400px" :label="$t('modelSetting')">
        <template slot-scope="scope">
          <div v-if="scope.row.auto_merge_time_ranges">
            <span class="model-setting-item">
              {{$t('segmentMerge')}}<span v-for="item in scope.row.auto_merge_time_ranges" :key="item">{{$t(item)}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editMergeItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'auto_merge_time_ranges')"></i>
            </common-tip>
          </div>
          <div v-if="scope.row.volatile_range">
            <span class="model-setting-item" @click="editVolatileItem(scope.row)">
              {{$t('volatileRange')}}<span>{{scope.row.volatile_range.volatile_range_number}} {{$t(scope.row.volatile_range.volatile_range_type.toLowerCase())}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editVolatileItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'volatile_range')"></i>
            </common-tip>
          </div>
          <div v-if="scope.row.retention_range">
            <span class="model-setting-item" @click="editRetentionItem(scope.row)">
              {{$t('retention')}}<span>{{scope.row.retention_range.retention_range_number}} {{$t(scope.row.retention_range.retention_range_type.toLowerCase())}}</span>
            </span><common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editRetentionItem(scope.row)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'retention_range')"></i>
            </common-tip>
          </div>
          <div v-if="scope.row.override_props">
            <div v-for="(propValue, key) in scope.row.override_props" :key="key">
              <template v-if="!key.split('.').includes('cube')">
                <span class="model-setting-item" @click="editSparkItem(scope.row, key)">
                  <!-- 去掉前缀kylin.engine.spark-conf. -->
                  {{key.substring(24)}}:<span>{{propValue}}</span>
                </span><common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editSparkItem(scope.row, key)"></i>
                </common-tip><common-tip :content="$t('kylinLang.common.delete')">
                  <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, key)"></i>
                </common-tip>
              </template>
              <template v-else>
                <span class="model-setting-item" @click="editCubeItem(scope.row, key)">
                  {{key.split('.').slice(-1).toString()}}:<span>{{propValue}}</span>
                </span><common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit ksd-mr-5 ksd-ml-10" @click="editCubeItem(scope.row, key)"></i>
                </common-tip><common-tip :content="$t('kylinLang.common.delete')">
                  <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, key)"></i>
                </common-tip>
              </template>
            </div>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="83px"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <common-tip>
              <div slot="content">{{$t('addSettingItem')}}</div>
              <i class="el-icon-ksd-table_add" :class="{'disabled': scope.row.auto_merge_time_ranges&&scope.row.volatile_range&&scope.row.retention_range&&Object.keys(scope.row.override_props).length==4}" @click="addSettingItem(scope.row)"></i>
            </common-tip>
          </template>
      </el-table-column>
    </el-table>
    <kap-pager :totalSize="modelListSize" :curPage="filter.page_offset+1" v-on:handleCurrentChange='currentChange' ref="modleConfigPager" class="ksd-mtb-10 ksd-center" ></kap-pager>
    <el-dialog :title="modelSettingTitle" :visible.sync="editModelSetting" limited-area width="480px" class="model-setting-dialog" @closed="handleClosed" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form ref="form" label-position="top" size="medium" label-width="80px" :model="modelSettingForm" :rules="rules">
        <el-form-item :label="modelTableTitle">
          <el-input v-model.trim="modelSettingForm.name" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('settingItem')" v-if="step=='stepOne'" prop="settingItem">
          <el-select v-model="modelSettingForm.settingItem" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%">
            <el-option
              v-for="item in settingOption"
              :key="item"
              :label="$t(item)"
              :value="item">
            </el-option>
          </el-select>
          <p>{{optionDesc[modelSettingForm.settingItem]}}</p>
        </el-form-item>
        <el-form-item :label="$t('autoMerge')" class="ksd-mb-10" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Auto-merge'">
          <el-checkbox-group v-model="modelSettingForm.autoMerge" class="merge-groups">
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index<3">{{$t(item)}}</el-checkbox></div>
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index>2">{{$t(item)}}</el-checkbox></div>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('volatileRangeItem')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Volatile Range'">
          <el-input v-model="modelSettingForm.volatileRange.volatile_range_number" v-number="modelSettingForm.volatileRange.volatile_range_number" class="retention-input"></el-input>
          <el-select v-model="modelSettingForm.volatileRange.volatile_range_type" class="ksd-ml-8" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option
              v-for="item in units"
              :key="item.label"
              :label="$t(item.label)"
              :value="item.value">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('retentionThreshold')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Retention Threshold'">
          <el-input v-model="modelSettingForm.retentionThreshold.retention_range_number" v-number="modelSettingForm.retentionThreshold.retention_range_number" class="retention-input"></el-input>
          <span class="ksd-ml-10">{{$t(modelSettingForm.retentionThreshold.retention_range_type.toLowerCase())}}</span>
        </el-form-item>
        <el-form-item :label="modelSettingForm.settingItem" v-if="step=='stepTwo'&&modelSettingForm.settingItem.indexOf('spark.')!==-1">
          <el-input v-model="modelSettingForm[modelSettingForm.settingItem]" v-number="modelSettingForm[modelSettingForm.settingItem]" class="retention-input"></el-input><span
          class="ksd-ml-5" v-if="modelSettingForm.settingItem==='spark.executor.memory'">G</span>
        </el-form-item>
        <el-form-item :label="modelSettingForm.settingItem" v-if="step=='stepTwo'&&modelSettingForm.settingItem === 'is-base-cuboid-always-valid'">
          <el-select v-model="modelSettingForm['is-base-cuboid-always-valid']">
            <el-option
              v-for="item in baseCuboidValid"
              :key="item.value"
              :label="item.label"
              :value="item.value"
            >
            </el-option>
          </el-select>
          <!-- <span class="ksd-ml-5" v-if="modelSettingForm.settingItem==='is-base-cuboid-always-valid'">G</span> -->
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="editModelSetting = false" v-if="step=='stepOne' || (step=='stepTwo' && isEdit)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button plain @click="preStep" icon="el-icon-ksd-more_01-copy" size="medium" v-if="step=='stepTwo' && !isEdit">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button @click="nextStep" size="medium" v-if="step=='stepOne'" :disabled="modelSettingForm.settingItem==''">{{$t('kylinLang.common.next')}}<i class="el-icon-ksd-more_02 el-icon--right"></i></el-button>
        <el-button
          @click="submit"
          size="medium"
          v-if="step=='stepTwo'"
          :loading="isLoading"
          :disabled="isSubmit">
            {{$t('kylinLang.common.submit')}}
          </el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'

import locales from './locales'
import { pageCount } from '../../../config'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../../util/business'
import { handleSuccessAsync, handleError, objectClone } from '../../../util/index'

const initialSettingForm = JSON.stringify({
  name: '',
  settingItem: '',
  autoMerge: [],
  volatileRange: {volatile_range_number: 0, volatile_range_type: '', volatile_range_enabled: true},
  retentionThreshold: {retention_range_number: 0, retention_range_type: '', retention_range_enabled: true},
  'spark.executor.cores': null,
  'spark.executor.instances': null,
  'spark.executor.memory': null,
  'spark.sql.shuffle.partitions': null,
  'is-base-cuboid-always-valid': 0
})

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadModelConfigList: 'LOAD_MODEL_CONFIG_LIST',
      updateModelConfig: 'UPDATE_MODEL_CONFIG'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isAutoProject'
    ])
  },
  locales
})
export default class SettingStorage extends Vue {
  modelList = []
  modelListSize = 0
  filter = {
    page_offset: 0,
    page_size: pageCount,
    model_name: ''
  }
  editModelSetting = false
  isLoading = false
  isEdit = false
  step = 'stepOne'
  // settingOption = ['Auto-merge', 'Volatile Range', 'Retention Threshold', 'spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions']
  mergeGroups = ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR']
  units = [{label: 'day', value: 'DAY'}, {label: 'week', value: 'WEEK'}, {label: 'month', value: 'MONTH'}, {label: 'year', value: 'YEAR'}]
  baseCuboidValid = [{label: 'true', value: 0}, {label: 'false', value: 1}]
  modelSettingForm = JSON.parse(initialSettingForm)
  activeRow = null
  get emptyText () {
    return this.filter.model_name ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get modelTableTitle () {
    return this.isAutoProject ? this.$t('kylinLang.model.indexGroupName') : this.$t('kylinLang.model.modelNameGrid')
  }
  get settingOption () {
    if (this.isAutoProject) {
      return ['spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions']
    } else {
      return ['Auto-merge', 'Volatile Range', 'Retention Threshold', 'spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'is-base-cuboid-always-valid']
    }
  }
  get availableRetentionRange () {
    let largestRange = null
    const modelAutoMergeRanges = this.activeRow && this.activeRow.auto_merge_time_ranges || []
    const autoMergeRanges = this.isEdit ? this.modelSettingForm.autoMerge : modelAutoMergeRanges
    this.units.forEach(unit => {
      if (autoMergeRanges.includes(unit.value)) {
        largestRange = unit.value
      }
    })
    return largestRange || ''
  }
  validateSettingItem (rule, value, callback) {
    const autoMergeRanges = this.activeRow && this.activeRow.auto_merge_time_ranges || []
    if (this.step === 'stepOne' && value === 'Retention Threshold' && !autoMergeRanges.length) {
      callback(new Error(null))
    } else {
      callback()
    }
  }
  get optionDesc () {
    return {
      'Auto-merge': this.$t('autoMergeTip'),
      'Volatile Range': this.$t('volatileTip'),
      'Retention Threshold': this.$t('retentionThresholdDesc'),
      'spark.executor.cores': this.$t('sparkCores'),
      'spark.executor.instances': this.$t('sparkInstances'),
      'spark.executor.memory': this.$t('sparkMemory'),
      'spark.sql.shuffle.partitions': this.$t('sparkShuffle'),
      'is-base-cuboid-always-valid': this.$t('baseCuboidConfig')
    }
  }
  get rules () {
    return {
      settingItem: [{ validator: this.validateSettingItem, message: this.$t('pleaseSetAutoMerge') }]
    }
  }
  handleClosed () {
    this.isEdit = false
    this.activeRow = null
    this.$refs['form'].clearValidate()
    this.modelSettingForm = JSON.parse(initialSettingForm)
  }
  addSettingItem (row) {
    if (row.auto_merge_time_ranges && row.volatile_range && row.retention_range && Object.keys(row.override_props).length === 4) {
      return
    }
    this.modelSettingForm.name = row.alias
    this.activeRow = objectClone(row)
    this.step = 'stepOne'
    this.editModelSetting = true
  }
  get modelSettingTitle () {
    return this.isEdit ? this.$t('editSetting') : this.$t('newSetting')
  }
  get isSubmit () {
    if (this.modelSettingForm.settingItem === 'Auto-merge' && !this.modelSettingForm.autoMerge.length) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Volatile Range' && !(this.modelSettingForm.volatileRange.volatile_range_number >= 0 && this.modelSettingForm.volatileRange.volatile_range_number !== '' && this.modelSettingForm.volatileRange.volatile_range_type)) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Retention Threshold' && !(this.modelSettingForm.retentionThreshold.retention_range_number >= 0 && this.modelSettingForm.retentionThreshold.retention_range_number !== '' && this.modelSettingForm.retentionThreshold.retention_range_type)) {
      return true
    } else if (this.modelSettingForm.settingItem.indexOf('spark.') !== -1 && !this.modelSettingForm[this.modelSettingForm.settingItem]) {
      return true
    } else if (this.modelSettingForm.settingItem === 'is-base-cuboid-always-valid' && this.modelSettingForm[this.modelSettingForm.settingItem] === '') {
      return true
    } else {
      return false
    }
  }
  removeAutoMerge (row, type) {
    kapConfirm(this.$t('isDel_' + type)).then(() => {
      const rowCopy = objectClone(row)
      rowCopy[type] = null
      rowCopy['auto_merge_enabled'] = type !== 'auto_merge_time_ranges' ? rowCopy['auto_merge_enabled'] : null
      rowCopy['retention_range'] = type !== 'auto_merge_time_ranges' ? rowCopy['retention_range'] : null
      if (type.indexOf('spark.') !== -1 || type.indexOf('cube.') !== -1) {
        delete rowCopy.override_props[type]
      }
      this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, rowCopy)).then((res) => {
        handleSuccess(res, () => {
          this.getConfigList()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  async nextStep () {
    try {
      await this.$refs['form'].validate()
      this.step = 'stepTwo'
      if (this.modelSettingForm.settingItem === 'Retention Threshold') {
        this.modelSettingForm.retentionThreshold.retention_range_type = this.availableRetentionRange
      }
    } catch (e) {
      handleError(e)
    }
  }
  preStep () {
    this.step = 'stepOne'
  }
  editMergeItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Auto-merge'
    this.modelSettingForm.autoMerge = JSON.parse(JSON.stringify(row.auto_merge_time_ranges))
    this.modelSettingForm.retentionThreshold = JSON.parse(JSON.stringify(row.retention_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editVolatileItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Volatile Range'
    this.modelSettingForm.volatileRange = JSON.parse(JSON.stringify(row.volatile_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editRetentionItem (row) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = 'Retention Threshold'
    this.modelSettingForm.autoMerge = JSON.parse(JSON.stringify(row.auto_merge_time_ranges))
    this.modelSettingForm.retentionThreshold = JSON.parse(JSON.stringify(row.retention_range))
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editSparkItem (row, sparkItemKey) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = sparkItemKey.substring(24)
    this.modelSettingForm[this.modelSettingForm.settingItem] = row.override_props[sparkItemKey]
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  // 编辑base-cubiod相关配置
  editCubeItem (row, key) {
    this.modelSettingForm.name = row.alias
    this.modelSettingForm.settingItem = key.split('.').slice(-1).toString()
    this.modelSettingForm[this.modelSettingForm.settingItem] = row.override_props[key]
    this.activeRow = row
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  submit () {
    if (this.modelSettingForm.settingItem === 'Auto-merge') {
      this.activeRow.auto_merge_time_ranges = this.modelSettingForm.autoMerge
      this.activeRow.auto_merge_enabled = true
      if (this.activeRow.retention_range) {
        this.modelSettingForm.retentionThreshold.retention_range_type = this.availableRetentionRange
        this.activeRow.retention_range = this.modelSettingForm.retentionThreshold
      }
    }
    if (this.modelSettingForm.settingItem === 'Volatile Range') {
      this.activeRow.volatile_range = this.modelSettingForm.volatileRange
    }
    if (this.modelSettingForm.settingItem === 'Retention Threshold') {
      this.activeRow.retention_range = this.modelSettingForm.retentionThreshold
    }
    if (this.modelSettingForm.settingItem.indexOf('spark.') !== -1) {
      this.activeRow.override_props['kylin.engine.spark-conf.' + this.modelSettingForm.settingItem] = this.modelSettingForm[this.modelSettingForm.settingItem]
    }
    if (this.modelSettingForm.settingItem === 'spark.executor.memory') {
      this.activeRow.override_props['kylin.engine.spark-conf.spark.executor.memory'] = this.activeRow.override_props['kylin.engine.spark-conf.spark.executor.memory'] + 'g'
    }
    if (this.modelSettingForm.settingItem === 'is-base-cuboid-always-valid') {
      this.activeRow.override_props['kylin.cube.aggrgroup.is-base-cuboid-always-valid'] = !this.modelSettingForm['is-base-cuboid-always-valid']
    }
    this.isLoading = true
    this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, this.activeRow)).then((res) => {
      handleSuccess(res, () => {
        this.isLoading = false
        this.editModelSetting = false
        this.getConfigList()
      })
    }, (res) => {
      handleError(res)
      this.isLoading = false
    })
  }
  async getConfigList () {
    const res = await this.loadModelConfigList(Object.assign({}, {project: this.currentSelectedProject}, this.filter))
    const resData = await handleSuccessAsync(res)
    this.modelList = resData.value
    this.modelListSize = resData.total_size
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.getConfigList()
  }
  searchModels () {
    this.filter.page_offset = 0
    this.getConfigList()
  }
  created () {
    this.getConfigList()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.setting-model {
  .model-setting-item {
    background-color: @grey-4;
    line-height: 18px;
    > span {
      margin-left: 5px;
    }
  }
  .model-setting-table {
    .el-icon-ksd-table_add.disabled {
      color: @text-disabled-color;
      cursor: not-allowed;
    }
    .el-icon-ksd-table_add,
    .el-icon-ksd-table_edit,
    .el-icon-ksd-symbol_type {
      &:hover {
        color: @base-color;
      }
    }
  }
}
.model-setting-dialog {
  .el-form-item__content p {
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
    margin-top: 5px;
  }
  .merge-groups {
    > div:nth-child(2) {
      margin-top: -15px;
    }
    .el-checkbox {
      &+.el-checkbox {
        margin-left: 30px;
      }
      &:nth-child(4) {
        margin-left: 0;
      }
    }
  }
  .retention-input {
    display: inline-block;
    width: 100px;
  }
}
</style>
