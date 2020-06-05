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
    <el-form :model="partitionMeta" ref="partitionForm" :rules="partitionRules"  label-width="85px" label-position="top">
      <el-form-item  :label="$t('partitionDateColumn')" class="clearfix">
        <el-row :gutter="5">
          <el-col :span="12">
            <el-select v-guide.partitionTable v-model="partitionMeta.table" @change="partitionTableChange" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" style="width:100%">
              <el-option :label="$t('noPartition')" value=""></el-option>
              <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
            </el-select>
          </el-col>
          <el-col :span="12" v-if="partitionMeta.table">
            <el-form-item prop="column">
              <el-select
              v-guide.partitionColumn @change="partitionColumnChange" v-model="partitionMeta.column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable style="width:100%">
              <i slot="prefix" class="el-input__icon el-icon-search" v-if="!partitionMeta.column.length"></i>
                <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
                  <el-tooltip :content="t.name" effect="dark" placement="top" :disabled="showToolTip(t.name)"><span style="float: left">{{ t.name | omit(15, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ t.datatype.toLocaleLowerCase() }}</span>
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
      </el-form-item>
      <el-form-item  :label="$t('dateFormat')" v-if="partitionMeta.table">
        <el-row :gutter="5">
          <el-col :span="12">
            <el-select :disabled="isLoadingFormat" v-guide.partitionColumnFormat style="width:100%" v-model="partitionMeta.format" :placeholder="$t('pleaseInputColumn')">
              <el-option :label="f.label" :value="f.value" v-for="f in dateFormats" :key="f.label"></el-option>
              <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
            </el-select>
          </el-col>
          <el-col :span="12">
            <el-tooltip effect="dark" :content="$t('detectFormat')" placement="top">
              <div style="display: inline-block;">
                <el-button
                  size="medium"
                  :loading="isLoadingFormat"
                  icon="el-icon-ksd-data_range_search"
                  v-guide.getPartitionColumnFormat
                  v-if="partitionMeta.column&&$store.state.project.projectPushdownConfig"
                  @click="handleLoadFormat">
                </el-button>
              </div>
            </el-tooltip>
          </el-col>
        </el-row>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
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
import { timeDataType, dateFormats } from 'config'
import NModel from '../../ModelEdit/model.js'
import { isDatePartitionType, objectClone, kapConfirm } from 'util'
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
    format: ''
  }
  isLoadingNewRange = false
  isLoadingFormat = false
  isLoadingSave = false
  timeDataType = timeDataType
  partitionRules = {
    column: [{validator: this.validateBrokenColumn, trigger: 'change'}]
  }
  dateFormats = dateFormats
  prevPartitionMeta = {
    table: '',
    column: '',
    format: ''
  }
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
        if (isDatePartitionType(x.datatype)) {
          result.push(x)
        }
      })
    }
    let ccColumns = this.modelInstance.getComputedColumns()
    let cloneCCList = objectClone(ccColumns)
    cloneCCList.forEach((x) => {
      let cc = {
        name: x.columnName,
        datatype: x.datatype
      }
      result.push(cc)
    })
    return result
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
    this.$refs.partitionForm.validate()
  }
  partitionColumnChange () {
    this.partitionMeta.format = 'yyyy-MM-dd'
    this.$refs.partitionForm.validate()
  }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: ''
    }
    this.prevPartitionMeta = { table: '', column: '', format: '' }
    this.filterCondition = ''
    this.isLoadingSave = false
    this.isLoadingFormat = false
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
      }
    } else {
      this.resetForm()
    }
  }
  async savePartitionConfirm () {
    await (this.$refs.rangeForm && this.$refs.rangeForm.validate()) || Promise.resolve()
    await (this.$refs.partitionForm && this.$refs.partitionForm.validate()) || Promise.resolve()
    if (typeof this.modelDesc.available_indexes_count === 'number' && this.modelDesc.available_indexes_count > 0) {
      if (this.prevPartitionMeta.table && !this.partitionMeta.table) {
        await kapConfirm(this.$t('changeSegmentTip2', {modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
      }
      if (this.prevPartitionMeta.table !== this.partitionMeta.table || this.prevPartitionMeta.column !== this.partitionMeta.column || this.prevPartitionMeta.format !== this.partitionMeta.format) {
        await kapConfirm(this.$t('changeSegmentTip1', {tableColumn: `${this.partitionMeta.table}.${this.partitionMeta.column}`, dateType: this.partitionMeta.format, modelName: this.modelDesc.name}), '', this.$t('kylinLang.common.tip'))
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
      await this.setModelPartition({modelId: this.modelDesc.uuid, project: this.currentSelectedProject, partition_desc})
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
}
</style>
