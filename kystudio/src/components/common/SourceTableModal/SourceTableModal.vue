<template>
  <el-dialog class="source-table-modal" width="440px"
    :title="$t(modalTitle)"
    :visible="isShow"
    @close="isShow && closeHandler(false)">
    <el-form :model="form" :rules="rules" ref="form" v-if="isFormShow" label-position="top">
      <!-- Partition Column Selector -->
      <el-form-item class="margin-bottom-20" prop="partitionColumn" v-if="isFieldShow('partitionColumn')">
        <span class="font-medium" slot="label">
          {{$t('partitionColumn')}}
          <i class="el-icon-ksd-what"></i>
        </span>
        <el-row>
          <el-col :span="13">
            <el-select
              class="margin-top-5"
              filterable
              size="medium"
              :value="form.partitionColumn"
              :disabled="disabled"
              :placeholder="$t('kylinLang.common.pleaseChoose')"
              @input="value => handleInput('partitionColumn', value)">
              <el-option
                v-for="column in partitionColumns"
                :key="column.name"
                :label="column.name"
                :value="column.name">
              </el-option>
            </el-select>
          </el-col>
        </el-row>
      </el-form-item>
      <!-- New/Change Data Range Picker -->
      <el-form-item class="margin-bottom-0" prop="newDataRange" v-if="isFieldShow('newDataRange')">
        <span class="font-medium" slot="label">
          {{$t('loadingRange')}}
        </span>
        <el-row>
          <el-col :span="22">
            <el-date-picker
              class="margin-top-5"
              size="medium"
              type="datetimerange"
              range-separator="-"
              :value="form.newDataRange"
              :disabled="disabled"
              :start-placeholder="$t('kylinLang.common.startTime')"
              :end-placeholder="$t('kylinLang.common.endTime')"
              @input="value => handleInputDate('newDataRange', value)">
            </el-date-picker>
          </el-col>
        </el-row>
      </el-form-item>
      <!-- Refresh Data Range Picker -->
      <el-form-item class="margin-bottom-0" prop="freshDataRange" v-if="isFieldShow('freshDataRange')">
        <span class="font-medium" slot="label">
          {{$t('refreshRange')}}
          <el-tooltip effect="dark" :content="$t('refreshRangeTip')" placement="top" popper-class="source-table-modal-tooltip">
            <i class="el-icon-ksd-what"></i>
          </el-tooltip>
        </span>
        <el-row>
          <el-col :span="22">
            <el-date-picker
              class="margin-top-5"
              size="medium"
              type="datetimerange"
              range-separator="-"
              :value="form.freshDataRange"
              :disabled="disabled"
              :start-placeholder="$t('kylinLang.common.startTime')"
              :end-placeholder="$t('kylinLang.common.endTime')"
              @input="value => handleInputDate('freshDataRange', value)">
            </el-date-picker>
          </el-col>
        </el-row>
      </el-form-item>
      <!-- Data Merge Switcher -->
      <el-form-item class="start-merge" prop="isMergeable" v-if="isFieldShow('isMergeable')">
        <span class="font-medium el-form-item__label no-padding">
          {{$t('isMergeable')}}
        </span>
        <el-switch
          :value="form.isMergeable"
          :active-text="$t('OFF')"
          :inactive-text="$t('ON')"
          @input="value => handleInput('isMergeable', value)">
        </el-switch>
      </el-form-item>
      <!-- Auto Merge Configs -->
      <template v-if="isFieldShow('autoMergeConfigs')">
        <!-- <el-row class="el-form-item__label margin-bottom-5">{{$t('mergePreference')}}</el-row> -->
        <el-form-item class="margin-bottom-5" prop="autoMergeConfigs">
          <el-row class="font-medium el-form-item__label">
            {{$t('autoMerge')}}
            <el-tooltip effect="dark" :content="$t('autoMergeTip')" placement="top" popper-class="source-table-modal-tooltip">
              <i class="el-icon-ksd-what"></i>
            </el-tooltip>
          </el-row>
          <el-row v-for="(autoMergeConfig, index) in form.autoMergeConfigs" :key="index" :gutter="10">
            <el-col :span="17">
              <el-select
                size="medium"
                class="padding-bottom-10"
                :value="autoMergeConfig"
                @input="value => handleInput(`autoMergeConfigs.${index}`, value)">
                <el-option
                  v-for="autoMergeType in autoMergeTypes"
                  :key="autoMergeType"
                  :label="$t(autoMergeType)"
                  :value="autoMergeType"
                  :disabled="form.autoMergeConfigs.includes(autoMergeType)">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="4">
              <el-button v-if="index === 0 && form.autoMergeConfigs.length < autoMergeTypes.length" size="medium" circle @click="handleAddConfig('autoMergeConfigs')">
                <i class="el-icon-ksd-add_2"></i>
              </el-button>
              <el-button v-else-if="index !== 0" size="medium" circle @click="handleRemoveConfig('autoMergeConfigs', index)">
                <i class="el-icon-ksd-table_delete"></i>
              </el-button>
            </el-col>
          </el-row>
        </el-form-item>
      </template>
      <!-- Volatile Config -->
      <template v-if="isFieldShow('volatileConfig')">
        <el-row class="el-form-item__label margin-bottom-5">
          {{$t('volatile')}}
          <el-tooltip effect="dark" :content="$t('volatileTip')" placement="top" popper-class="source-table-modal-tooltip">
            <i class="el-icon-ksd-what"></i>
          </el-tooltip>
        </el-row>
        <el-row class="margin-bottom-10">
          <el-col class="margin-right-5" :span="8">
            <el-form-item class="margin-bottom-0" prop="volatileConfig.value">
              <el-input
                size="medium"
                :value="form.volatileConfig.value"
                :placeholder="$t('kylinLang.common.pleaseInput')"
                @input="value => !isNaN(+value) && handleInput('volatileConfig.value', +value)">
              </el-input>
            </el-form-item>
          </el-col>
          <el-col :span="11">
            <el-form-item class="margin-bottom-0" prop="volatileConfig.type">
              <el-select
                size="medium"
                :value="form.volatileConfig.type"
                :placeholder="$t('kylinLang.common.pleaseChoose')"
                @input="value => handleInput('volatileConfig.type', value)">
                <el-option
                  v-for="volatileType in volatileTypes"
                  :key="volatileType"
                  :label="$t(volatileType)"
                  :value="volatileType">
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
      </template>
      <!-- Pushdown Config -->
      <template v-if="isFieldShow('isAsyncPushDown')">
        <div class="item-desc margin-bottom-10">{{$t('pushdownDesc')}}</div>
        <el-form-item class="margin-bottom-0" prop="isMergeable">
          <el-radio-group :value="form.isPushdownSync" @input="value => handleInput('isPushdownSync', value)">
            <div class="item-desc margin-bottom-10"><el-radio :label="true">{{$t('isPushdown')}}</el-radio></div>
            <div class="item-desc margin-bottom-0"><el-radio :label="false">{{$t('notPushdown')}}</el-radio></div>
          </el-radio-group>
        </el-form-item>
      </template>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="handleSubmit">{{$t('kylinLang.common.save')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { set } from '../../../util/object'
import { handleError, handleSuccessAsync } from '../../../util'
import { fieldVisiableMaps, titleMaps, editTypes, autoMergeTypes, volatileTypes, validate, validateTypes } from './handler'

const {
  INCREMENTAL_SETTING,
  INCREMENTAL_LOADING,
  REFRESH_RANGE,
  DATA_MERGE,
  PUSHDOWN_CONFIG
} = editTypes

const { NEW_DATA_RANGE, PARTITION_COLUMN, VOLATILE_VALUE } = validateTypes

vuex.registerModule(['modals', 'SourceTableModal'], store)

@Component({
  props: {
    projectName: {
      type: String
    }
  },
  computed: {
    ...mapState('SourceTableModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      disabled: state => state.disabled,
      editType: state => state.editType,
      callback: state => state.callback,
      table: state => state.table
    }),
    ...mapGetters('SourceTableModal', [
      'modelName',
      'tableFullName',
      'partitionColumns'
    ])
  },
  methods: {
    ...mapMutations('SourceTableModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapActions({
      saveIncrementalTable: 'SAVE_FACT_TABLE',
      saveDataRange: 'SAVE_DATA_RANGE',
      fetchRangeFreshInfo: 'FETCH_RANGE_FRESH_INFO',
      freshRangeData: 'FRESH_RANGE_DATA',
      updateMergeConfig: 'UPDATE_MERGE_CONFIG',
      updatePushdownConfig: 'UPDATE_PUSHDOWN_CONFIG'
    })
  },
  locales
})
export default class SourceTableModal extends Vue {
  isFormShow = false
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes
  rules = {
    [NEW_DATA_RANGE]: [{ validator: this.validate(NEW_DATA_RANGE), trigger: 'blur' }],
    [PARTITION_COLUMN]: [{ validator: this.validate(PARTITION_COLUMN), trigger: 'blur' }],
    [VOLATILE_VALUE]: [{ validator: this.validate(VOLATILE_VALUE), trigger: 'blur' }]
  }
  get modalTitle () {
    return titleMaps[this.editType]
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    newVal ? (this.isFormShow = true) : setTimeout(() => (this.isFormShow = false), 300)
  }
  @Watch('form.isMergeable')
  onMergeableChange (newValue) {
    this.handleInput('isAutoMerge', newValue)
    this.handleInput('isVolatile', newValue)
  }
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }
  handleInput (path, value) {
    const newForm = set(this.form, path, value)
    this.setModalForm(newForm)
  }
  handleInputDate (path, value) {
    this.handleInput(path, value)
  }
  handleAddConfig (path) {
    const newConfigs = ['', ...this.form[path]]
    this.setModalForm({ [path]: newConfigs })
  }
  handleRemoveConfig (path, index) {
    const newConfigs = [...this.form[path]]
    newConfigs.splice(index, 1)
    this.setModalForm({ [path]: newConfigs })
  }
  closeHandler (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  async handleSubmit () {
    try {
      await this.$refs['form'].validate()

      const data = this.getSubmitData()
      const message = this.$t('kylinLang.common.saveSuccess')
      await this.submit(data)
      this.$message({ type: 'success', message })
      this.closeHandler(true)
    } catch (e) {
      handleError(e)
    }
  }
  async submit (data) {
    switch (this.editType) {
      case INCREMENTAL_SETTING:
        await this.saveIncrementalTable(data.settings)
        return await this.saveDataRange(data.ranges)
      case INCREMENTAL_LOADING:
        return this.saveDataRange(data)
      case REFRESH_RANGE: {
        const response = await this.fetchRangeFreshInfo(data)
        const result = await handleSuccessAsync(response)
        const storageSize = result.byte_size
        const affectedStart = result.affected_start
        const affectedEnd = result.affected_end
        const confirmTitle = this.$t('kylinLang.common.notice')
        const confirmMessage = this.$t('freshStorageCost', { storageSize: Vue.filter('dataSize')(storageSize) })
        await this.$confirm(confirmMessage, confirmTitle, {
          confirmButtonText: this.$t('kylinLang.common.ok'),
          cancelButtonText: this.$t('kylinLang.common.cancel'),
          type: 'warning'
        })
        data.affectedStart = affectedStart
        data.affectedEnd = affectedEnd
        return await this.freshRangeData(data)
      }
      case DATA_MERGE:
        return this.updateMergeConfig(data)
      case PUSHDOWN_CONFIG:
        return this.updatePushdownConfig(data)
    }
  }
  getSubmitData () {
    const { editType, tableFullName, form, projectName, modelName } = this

    switch (editType) {
      case INCREMENTAL_SETTING: {
        const startTime = form.newDataRange[0].getTime()
        const endTime = form.newDataRange[1].getTime()
        const column = form.partitionColumn
        const isIncremental = true
        return {
          settings: { projectName, tableFullName, isIncremental, column },
          ranges: { projectName, tableFullName, startTime, endTime }
        }
      }
      case INCREMENTAL_LOADING: {
        const startTime = form.newDataRange[0].getTime()
        const endTime = form.newDataRange[1].getTime()
        return { projectName, tableFullName, startTime, endTime }
      }
      case REFRESH_RANGE: {
        const startTime = form.freshDataRange[0].getTime()
        const endTime = form.freshDataRange[1].getTime()
        return { projectName, tableFullName, startTime, endTime }
      }
      case DATA_MERGE: {
        const { isAutoMerge, autoMergeConfigs, isVolatile, volatileConfig } = form
        const newAutoMergeConfigs = autoMergeConfigs.filter(autoMergeConfig => autoMergeConfig)
        return { projectName, tableFullName, modelName, isAutoMerge, autoMergeConfigs: newAutoMergeConfigs, isVolatile, volatileConfig }
      }
      case PUSHDOWN_CONFIG: {
        const { isPushdownSync } = form
        return { projectName, tableFullName, isPushdownSync }
      }
      default:
        return null
    }
  }
  validate (type) {
    return validate[type].bind(this)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.source-table-modal {
  .el-dialog__title {
    font-size: 16px;
    font-weight: 500;
    color: #263238;
  }
  .el-form-item__label {
    font-size: 14px;
    font-weight: 500;
    color: #263238;
  }
  .el-icon-ksd-what {
    color: @text-normal-color;
  }
  .el-date-editor--datetimerange.el-input__inner {
    width: 100%;
    .el-range-input {
      width: 47%;
    }
  }
  .item-desc {
    font-size: 14px;
    color: #263238;
    line-height: 21px;
  }
  .el-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .el-select {
    width: 100%;
  }
  .start-merge {
    margin: -10px 0 10px 0;
  }
  .el-form-item__label.no-padding,
  .no-padding {
    padding: 0;
  }
  .margin-top-5 {
    margin-top: 5px;
  }
  .padding-bottom-10 {
    padding-bottom: 10px;
  }
  .margin-bottom-0 {
    margin-bottom: 0;
  }
  .margin-bottom-5 {
    margin-bottom: 5px;
  }
  .margin-bottom-10 {
    margin-bottom: 10px;
  }
  .margin-bottom-20 {
    margin-bottom: 20px;
  }
  .margin-right-5 {
    margin-right: 5px;
  }
}
.source-table-modal-tooltip {
  max-width: 300px;
}
</style>
