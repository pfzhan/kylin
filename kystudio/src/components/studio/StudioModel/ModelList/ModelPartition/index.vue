<template>
  <el-dialog
    :title="$t('modelPartitionSet')"
    width="560px"
    append-to-body
    :visible="isShow"
    class="partition-dialog"
    @close="isShow && handleClose(false)"
    :close-on-press-escape="false"
    :close-on-click-modal="false">
    <!-- <p class="segment-change-tip"><i class="el-icon-ksd-info ksd-mr-5"></i>{{$t('segmentChangedTips')}}</p> -->
    <el-alert
      :title="$t('segmentChangedTips')"
      type="warning"
      :closable="false"
      class="ksd-mb-10"
      v-if="isShowWarning"
      show-icon>
    </el-alert>
    <el-form :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
      <el-form-item  :label="$t('partitionDateTable')" class="clearfix">
        <el-row :gutter="5">
          <el-col :span="12">
            <el-select v-guide.partitionTable v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
              <el-option :label="$t('noPartition')" value=""></el-option>
              <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
            </el-select>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item  :label="$t('partitionDateColumn')" v-if="partitionMeta.table">
        <el-row :gutter="5">
          <el-col :span="11" v-if="partitionMeta.table">
            <el-form-item prop="column">
              <el-select
              v-guide.partitionColumn @change="partitionColumnChange" v-model="partitionMeta.column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable style="width:100%">
              <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.column.length"></i>
                <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                  <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="11">
            <el-select :disabled="isLoadingFormat" v-guide.partitionColumnFormat style="width:100%" v-model="partitionMeta.format" @change="changePartitionSetting" :placeholder="$t('pleaseInputColumn')">
              <el-option :label="f.label" :value="f.value" v-for="f in dateFormatsOptions" :key="f.label"></el-option>
              <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
            </el-select>
          </el-col>
          <el-col :span="1">
            <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
              <div style="display: inline-block;">
                <el-button
                  size="medium"
                  :loading="isLoadingFormat"
                  icon="el-ksd-icon-data_range_search_old"
                  v-guide.getPartitionColumnFormat
                  v-if="partitionMeta.column && $store.state.project.projectPushdownConfig && modelDesc.model_type === 'BATCH'"
                  @click="handleLoadFormat">
                </el-button>
              </div>
            </el-tooltip>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item v-if="((!modelDesc.multi_partition_desc && $store.state.project.multi_partition_enabled) || modelDesc.multi_partition_desc) && partitionMeta.table && !isNotBatchModel">
        <span slot="label">
          <span>{{$t('multilevelPartition')}}</span>
          <el-tooltip effect="dark" :content="$t('multilevelPartitionDesc')" placement="right">
            <i class="el-ksd-icon-more_info_16"></i>
          </el-tooltip>
        </span>
        <el-row>
          <el-col :span="11">
            <el-select
              :disabled="isLoadingNewRange"
              v-model="partitionMeta.multiPartition"
              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
              filterable
              class="partition-multi-partition"
              style="width:100%"
              @change="changePartitionSetting"
            >
                <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!partitionMeta.multiPartition.length"></i>
                <el-option :label="$t('noPartition')" value=""></el-option>
                <el-option :label="t.name" :value="t.name" v-for="t in subPartitionColumnOtions" :key="t.name">
                  <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                </el-option>
              </el-select>
          </el-col>
        </el-row>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" v-if="isShow" :disabled="isLoadingNewRange" :loading="isLoadingSave" v-guide.partitionSaveBtn @click="savePartitionConfirm" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from 'store'
import locales from './locales'
import store, { types } from './store'
import { timeDataType, dateFormats, timestampFormats } from 'config'
import NModel from '../../ModelEdit/model.js'
import { isDatePartitionType, isStreamingPartitionType, isSubPartitionType, kapConfirm } from 'util'
import { handleSuccessAsync, handleError } from 'util/index'
vuex.registerModule(['modals', 'ModelPartition'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('ModelPartition', {
      isShow: state => state.isShow,
      modelDesc: state => state.form.modelDesc,
      modelInstance: state => state.form.modelInstance || state.form.modelDesc && new NModel(state.form.modelDesc) || null,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ModelPartition', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      fetchPartitionFormat: 'FETCH_PARTITION_FORMAT',
      setModelPartition: 'MODEL_PARTITION_SET'
    })
  },
  locales
})
export default class ModelPartition extends Vue {
  partitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  isLoadingNewRange = false
  isLoadingFormat = false
  isLoadingSave = false
  timeDataType = timeDataType
  partitionRules = {
    column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
  }
  dateFormats = dateFormats
  timestampFormats = timestampFormats
  prevPartitionMeta = {
    table: '',
    column: '',
    format: '',
    multiPartition: ''
  }
  isShowWarning = false
  validateBrokenColumn (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenPartitionColumns, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    if (!value && this.partitionMeta.table) {
      return callback(new Error(this.$t('pleaseInputColumn')))
    }
    callback()
  }
  checkIsBroken (brokenKeys, key) {
    if (key) {
      return ~brokenKeys.indexOf(key)
    }
    return false
  }
  get dateFormatsOptions () {
    return this.isNotBatchModel ? timestampFormats : dateFormats
  }
  get isNotBatchModel () {
    const factTable = this.modelInstance.getFactTable()
    return factTable.source_type === 1 || this.modelInstance.model_type !== 'BATCH'
  }
  get brokenPartitionColumns () {
    if (this.partitionMeta.table) {
      let ntable = this.modelInstance.getTableByAlias(this.partitionMeta.table)
      return this.modelInstance.getBrokenModelLinksKeys(ntable.guid, [this.partitionMeta.column])
    }
    return []
  }
  get selectedTable () {
    if (this.partitionMeta.table) {
      for (let i = 0; i < this.partitionTables.length; i++) {
        if (this.partitionTables[i].alias === this.partitionMeta.table) {
          return this.partitionTables[i]
        }
      }
    }
  }
  get columns () {
    if (!this.isShow || this.partitionMeta.table === '') {
      return []
    }
    let result = []
    let factTable = this.modelInstance.getFactTable()
    if (factTable) {
      factTable.columns.forEach((x) => {
        if (this.isNotBatchModel && isStreamingPartitionType(x.datatype)) {
          result.push(x)
        } else if (!this.isNotBatchModel && isDatePartitionType(x.datatype)) {
          result.push(x)
        }
      })
    }
    // let ccColumns = this.modelInstance.getComputedColumns()
    // let cloneCCList = objectClone(ccColumns)
    // cloneCCList.forEach((x) => {
    //   let cc = {
    //     name: x.columnName,
    //     datatype: x.datatype
    //   }
    //   result.push(cc)
    // })
    return result
  }
  get subPartitionColumnOtions () {
    if (!this.isShow || this.partitionMeta.table === '') {
      return []
    }
    let result = []
    let factTable = this.modelInstance.getFactTable()
    if (factTable) {
      factTable.columns.forEach((x) => {
        if (isSubPartitionType(x.datatype)) {
          result.push(x)
        }
      })
    }
    return result
  }
  // 分区设置改变
  changePartitionSetting () {
    if (JSON.stringify(this.prevPartitionMeta) !== JSON.stringify(this.partitionMeta)) {
      if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
        this.isShowWarning = true
      }
    } else {
      this.isShowWarning = false
    }
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
    this.partitionMeta.multiPartition = ''
    this.$refs.partitionForm.validate()
    this.changePartitionSetting()
  }
  partitionColumnChange () {
    this.partitionMeta.format = 'yyyy-MM-dd'
    this.$refs.partitionForm.validate()
    this.changePartitionSetting()
  }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: '',
      multiPartition: ''
    }
    this.prevPartitionMeta = { table: '', column: '', format: '', multiPartition: '' }
    this.filterCondition = ''
    this.isLoadingSave = false
    this.isLoadingFormat = false
    this.isShowWarning = false
  }
  get partitionTables () {
    let result = []
    if (this.isShow && this.modelInstance) {
      Object.values(this.modelInstance.tables).forEach((nTable) => {
        if (nTable.kind === 'FACT') {
          result.push(nTable)
        }
      })
    }
    return result
  }
  showToolTip (value) {
    let len = 0
    value.split('').forEach((v) => {
      if (/[\u4e00-\u9fa5]/.test(v)) {
        len += 2
      } else {
        len += 1
      }
    })
    return len <= 15
  }
  async handleLoadFormat () {
    try {
      this.isLoadingFormat = true
      const response = await this.fetchPartitionFormat({ project: this.currentSelectedProject, table: this.selectedTable.name, partition_column: this.partitionMeta.column })
      this.partitionMeta.format = await handleSuccessAsync(response)
      this.isLoadingFormat = false
    } catch (e) {
      this.isLoadingFormat = false
      handleError(e)
    }
  }
  @Watch('isShow')
  initModeDesc () {
    if (this.isShow) {
      this.$nextTick(() => {
        this.$refs.partitionForm && this.$refs.partitionForm.validate()
      })
      if (this.modelDesc && this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_column) {
        let named = this.modelDesc.partition_desc.partition_date_column.split('.')
        this.partitionMeta.table = this.prevPartitionMeta.table = named[0]
        this.partitionMeta.column = this.prevPartitionMeta.column = named[1]
        this.partitionMeta.format = this.prevPartitionMeta.format = this.modelDesc.partition_desc.partition_date_format
        this.partitionMeta.multiPartition = this.prevPartitionMeta.multiPartition = this.modelDesc.multi_partition_desc && this.modelDesc.multi_partition_desc.columns[0] && this.modelDesc.multi_partition_desc.columns[0].split('.')[1] || ''
      }
    } else {
      this.resetForm()
    }
  }

  get isChangeToFullLoad () {
    return this.prevPartitionMeta.table && !this.partitionMeta.table
  }

  get isChangePartition () {
    return this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format || this.prevPartitionMeta.multiPartition !== this.partitionMeta.multiPartition
  }
  async savePartitionConfirm () {
    await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
    await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
    if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
      // if (this.prevPartitionMeta.table && !this.partitionMeta.table) {
      //   await kapConfirm(this.$t('changeSegmentTip2', {modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
      // }
      // if (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format) {
      //   await kapConfirm(this.$t('changeSegmentTip1', {tableColumn: `${this.partitionMeta.table}.${this.partitionMeta.column}`, dateType: this.partitionMeta.format, modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
      // }
      if (this.isChangeToFullLoad || this.isChangePartition) {
        await kapConfirm(this.$t('changeSegmentTips'), {confirmButtonText: this.$t('kylinLang.common.save'), type: 'warning', dangerouslyUseHTMLString: true}, this.$t('kylinLang.common.tip'))
      }
      this.savePartition()
    } else {
      this.savePartition()
    }
  }
  async savePartition () {
    try {
      let partition_desc = {}
      let hasSetDate = this.partitionMeta.table && this.partitionMeta.column
      partition_desc.partition_date_column = hasSetDate ? this.partitionMeta.table + '.' + this.partitionMeta.column : ''
      partition_desc.partition_date_format = this.partitionMeta.format
      if (!partition_desc.partition_date_column) {
        partition_desc = null
      }
      this.isLoadingSave = true
      const multi_partition_desc = this.partitionMeta.multiPartition ? {columns: [this.partitionMeta.table + '.' + this.partitionMeta.multiPartition]} : null
      await this.setModelPartition({modelId: this.modelDesc.uuid, project: this.currentSelectedProject, partition_desc, multi_partition_desc: multi_partition_desc})
      this.handleClose(true)
      this.isLoadingSave = false
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      handleError(e)
      this.isLoadingSave = false
      this.handleClose(false)
    }
  }
  handleClose (isSubmit) {
    this.isLoadingFormat = false
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit
      })
    }, 300)
  }
}
</script>

<style lang="less" scoped>
@import '../../../../../assets/styles/variables.less';
.partition-dialog {
  .partition-set {
    span {
      color: @error-color-1;
    }
  }
  .segment-change-tip {
    i {
      color: @text-disabled-color;
    }
    font-size: 12px;
    margin-bottom: 10px;
  }
}
</style>
