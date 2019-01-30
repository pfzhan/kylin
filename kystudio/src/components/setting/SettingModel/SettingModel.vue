<template>
  <div class="setting-model">
    <el-table
      :data="modelList"
      class="model-setting-table"
      border
      style="width: 100%">
      <el-table-column width="230px" header-align="center" show-overflow-tooltip prop="alias" :label="$t('kylinLang.model.modelNameGrid')"></el-table-column>
      <el-table-column prop="last_modified" header-align="center" show-overflow-tooltip width="207px" :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span v-if="scope.row.config_last_modified>0">{{transToGmtTime(scope.row.config_last_modified)}}</span>
        </template>
      </el-table-column>
      <el-table-column prop="config_last_modifier" header-align="center" show-overflow-tooltip width="100" :label="$t('modifiedUser')"></el-table-column>
      <el-table-column min-width="400px" header-align="center" :label="$t('modelSetting')">
        <template slot-scope="scope">
          <div v-if="scope.row.auto_merge_time_ranges">
            <span class="model-setting-item">
              {{$t('segmentMerge')}}<span v-for="item in scope.row.auto_merge_time_ranges" :key="item">{{$t(item)}}</span>
            </span>
            <i class="el-icon-ksd-table_edit ksd-mr-8 ksd-ml-10" @click="editMergeItem(scope.row)"></i>
            <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'auto_merge_time_ranges')"></i>
          </div>
          <div v-if="scope.row.volatile_range">
            <span class="model-setting-item" @click="editVolatileItem(scope.row)">
              {{$t('volatileRange')}}<span>{{scope.row.volatile_range.volatile_range_number}} {{$t(scope.row.volatile_range.volatile_range_type.toLowerCase())}}</span>
            </span>
            <i class="el-icon-ksd-table_edit ksd-mr-8 ksd-ml-10" @click="editVolatileItem(scope.row)"></i>
            <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'volatile_range')"></i>
          </div>
          <div v-if="scope.row.retention_range">
            <span class="model-setting-item" @click="editRetentionItem(scope.row)">
              {{$t('retention')}}<span>{{scope.row.retention_range.retention_range_number}} {{$t(scope.row.retention_range.retention_range_type.toLowerCase())}}</span>
            </span>
            <i class="el-icon-ksd-table_edit ksd-mr-8 ksd-ml-10" @click="editRetentionItem(scope.row)"></i>
            <i class="el-icon-ksd-symbol_type" @click="removeAutoMerge(scope.row, 'retention_range')"></i>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="83px"
        align="center"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <el-tooltip placement="top">
              <div slot="content">{{$t('addSettingItem')}}</div>
              <i class="el-icon-ksd-table_add" :class="{'disabled': scope.row.auto_merge_time_ranges&&scope.row.volatile_range&&scope.row.retention_range}" @click="addSettingItem(scope.row)"></i>
            </el-tooltip>
          </template>
      </el-table-column>
    </el-table>
    <kap-pager :totalSize="modelListSize"  v-on:handleCurrentChange='currentChange' ref="modleConfigPager" class="ksd-mt-20 ksd-mb-20 ksd-center" ></kap-pager>
    <el-dialog :title="modelSettingTitle" :visible.sync="editModelSetting" width="440px" class="model-setting-dialog" @closed="handleClosed" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form ref="form" label-position="top" size="medium" label-width="80px" :model="modelSettingForm" :rules="rules">
        <el-form-item :label="$t('modelName')">
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
          <p v-if="modelSettingForm.settingItem==='Auto-merge'">{{$t('autoMergeTip')}}</p>
          <p v-if="modelSettingForm.settingItem==='Volatile Range'">{{$t('volatileTip')}}</p>
          <p v-if="modelSettingForm.settingItem==='Retention Threshold'">{{$t('retentionThresholdDesc')}}</p>
        </el-form-item>
        <el-form-item :label="$t('autoMerge')" class="ksd-mb-10" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Auto-merge'">
          <el-checkbox-group v-model="modelSettingForm.autoMerge" class="merge-groups">
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index<3">{{$t(item)}}</el-checkbox></div>
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index>2">{{$t(item)}}</el-checkbox></div>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('volatileRangeItem')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Volatile Range'">
          <el-input v-model.trim="modelSettingForm.volatileRange.volatile_range_number" class="retention-input"></el-input>
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
          <el-input v-model.trim.number="modelSettingForm.retentionThreshold.retention_range_number" class="retention-input"></el-input>
          <span class="ksd-ml-10">{{$t(modelSettingForm.retentionThreshold.retention_range_type.toLowerCase())}}</span>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="editModelSetting = false" v-if="step=='stepOne' || (step=='stepTwo' && isEdit)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button @click="preStep" icon="el-icon-ksd-more_01-copy" size="medium" v-if="step=='stepTwo' && !isEdit">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" plain @click="nextStep" size="medium" v-if="step=='stepOne'" :disabled="modelSettingForm.settingItem==''">{{$t('kylinLang.common.next')}}<i class="el-icon-ksd-more_02 el-icon--right"></i></el-button>
        <el-button
          type="primary"
          plain
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

const initialSettingForm = JSON.stringify({name: '', settingItem: '', autoMerge: [], volatileRange: {volatile_range_number: 0, volatile_range_type: '', volatile_range_enabled: true}, retentionThreshold: {retention_range_number: 0, retention_range_type: '', retention_range_enabled: true}})

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
      'currentSelectedProject'
    ])
  },
  locales
})
export default class SettingStorage extends Vue {
  modelList = []
  modelListSize = 0
  filter = {
    pageOffset: 0,
    pageSize: pageCount
  }
  editModelSetting = false
  isLoading = false
  isEdit = false
  step = 'stepOne'
  settingOption = ['Auto-merge', 'Volatile Range', 'Retention Threshold']
  mergeGroups = ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR']
  units = [{label: 'day', value: 'DAY'}, {label: 'week', value: 'WEEK'}, {label: 'month', value: 'MONTH'}, {label: 'year', value: 'YEAR'}]
  modelSettingForm = JSON.parse(initialSettingForm)
  activeRow = null
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
    if (row.auto_merge_time_ranges && row.volatile_range && row.retention_range) {
      return
    }
    this.modelSettingForm.name = row.alias
    this.activeRow = row
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
    this.updateModelConfig(Object.assign({}, {project: this.currentSelectedProject}, this.activeRow)).then((res) => {
      this.isLoading = true
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
    this.modelList = resData.model_config
    this.modelListSize = resData.size
  }
  currentChange (size, count) {
    this.filter.pageOffset = size
    this.filter.pageSize = count
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
