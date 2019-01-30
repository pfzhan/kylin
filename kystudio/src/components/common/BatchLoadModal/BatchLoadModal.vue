<template>
  <el-dialog
    class="batch-load-modal"
    width="780px"
    :visible="isShow"
    :title="$t('batchLoad')"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="() => handleClose()"
    @closed="handleClosed">
    <el-form class="form" v-loading="isDataLoading" ref="form" size="small" :inline="true" :model="tempForm">
      <el-table border :data="form.tables" @selection-change="value => selectedTables = value">
        <el-table-column type="selection" width="38"></el-table-column>
        <el-table-column prop="fullName" :label="$t('table')" header-align="center"></el-table-column>
        <el-table-column prop="refresh" :label="$t('refresh')" width="75" align="center"></el-table-column>
        <el-table-column :label="$t('loadRange')" width="210" header-align="center">
          <template slot-scope="scope">
            <el-popover
              width="338"
              placement="bottom"
              popper-class="batch-load-edit-popper el-form--inline"
              :ref="`popover${scope.$index}`"
              @input="value => !value && handlePopperHide(`popover${scope.$index}`)">
              <el-form-item prop="editDate.0" :rules="rules">
                <el-date-picker
                  type="date"
                  v-model="tempForm.editDate[0]"
                  :clearable="false"
                  :is-auto-complete="true"
                  :disabled="isDisabled"
                  :picker-options="{ disabledDate: time => time.getTime() > tempForm.editDate[1] && tempForm.editDate[1] !== null }"
                  :placeholder="$t('kylinLang.common.startTime')">
                </el-date-picker>
              </el-form-item>
              <span class="split">-</span>
              <el-form-item prop="editDate.1" :rules="rules">
                <el-date-picker
                  type="date"
                  v-model="tempForm.editDate[1]"
                  :clearable="false"
                  :is-auto-complete="true"
                  :disabled="isDisabled"
                  :picker-options="{ disabledDate: time => time.getTime() < tempForm.editDate[0] && tempForm.editDate[0] !== null }"
                  :placeholder="$t('kylinLang.common.endTime')">
                </el-date-picker>
              </el-form-item>
              <div style="text-align: right; margin: 0">
                <el-button size="mini" type="info" text @click="handlePopperHide(`popover${scope.$index}`)">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="mini" @click="handleInputDate(`tables.${scope.$index}.loadRange`, tempForm.editDate, `popover${scope.$index}`)">{{$t('kylinLang.common.ok')}}</el-button>
              </div>
            </el-popover>
            <span>{{getRangeString(scope.row)}}</span><span>
            </span><i class="edit-action el-icon-ksd-table_edit" v-popover="`popover${scope.$index}`" @click="handlePopperShow(scope)"></i>
          </template>
        </el-table-column>
        <el-table-column prop="relatedIndex" :label="$t('relatedIndex')" width="115" header-align="center"></el-table-column>
      </el-table>
      <div class="error-mask" v-if="!isDataLoading && isDataError">
        <div>error! please refresh</div>
      </div>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="() => handleClose()">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="handleSubmit" :loading="isLoading">{{$t('kylinLang.common.save')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { mapState, mapMutations, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import store, { types } from './store'
import vuex from '../../../store'
import { set } from '../../../util/object'
import { handleError, handleSuccessAsync } from '../../../util'
import { getTableLoadRange, getTableBatchLoadSubmitData } from './handler'

vuex.registerModule(['modals', 'BatchLoadModal'], store)

@Component({
  computed: {
    ...mapState('BatchLoadModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('BatchLoadModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      initForm: types.INIT_FORM,
      resetModal: types.RESET_MODAL
    }),
    ...mapActions({
      fetchTables: 'FETCH_TABLES',
      fetchBatchLoadTables: 'FETCH_BATCH_LOAD_TABLES',
      saveTablesBatchLoad: 'SAVE_TABLES_BATCH_LOAD',
      fetchNewestTableRange: 'FETCH_NEWEST_TABLE_RANGE'
    })
  },
  locales
})
export default class BatchLoadModal extends Vue {
  isDataLoading = false
  isDataError = false
  isLoading = false
  isDisabled = false
  isFormShow = false
  batchCount = 5
  selectedTables = []
  tempForm = {
    editDate: []
  }
  get rules () {
    return [
      { required: true, message: '请输入日期', trigger: 'blur' },
      { type: 'date', message: '请输入正确的日期', trigger: 'blur' }
    ]
  }
  getRangeString (table) {
    const startDate = table.loadRange[0] ? dayjs(table.loadRange[0]).format('YYYY-MM-DD') : ''
    const endDate = table.loadRange[1] ? dayjs(table.loadRange[1]).format('YYYY-MM-DD') : ''
    return `${startDate} - ${endDate}`
  }
  handleOpen () {
    this._showForm()
    this.loadSourceTables()
  }
  handleClose (isSubmit = false) {
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }
  handleClosed () {
    this._hideForm()
    this.resetModal()
  }
  handleInput (key, value) {
    this.setModalForm(set(this.form, key, value))
  }
  async handleInputDate (key, value, popperId) {
    const isValid = await this.$refs['form'].validate()
    if (isValid) {
      this.handleInput(key, value)
      this.handlePopperHide(popperId)
    }
  }
  handlePopperShow (scope) {
    this.tempForm.editDate = [...scope.row.loadRange]
  }
  handlePopperHide (popperId) {
    this.$refs[popperId] && this.$refs[popperId].doClose()
    this.$refs['form'] && this.$refs['form'].clearValidate()

    this.tempForm.editDate = []
  }
  async loadTablesRange (tableList) {
    const batchCount = this.batchCount
    const requestBatches = []

    for (let i = 0; i < tableList.length; i++) {
      if (i % batchCount !== 0) {
        const batchIdx = parseInt(i / batchCount)
        requestBatches[batchIdx].push(tableList[i])
      } else {
        requestBatches.push([tableList[i]])
      }
    }
    for (let i = 0; i < requestBatches.length; i++) {
      const requestBatch = requestBatches[i]
      const response = await Promise.all(requestBatch.map(table => this.fetchNewestTableRange({ projectName: this.form.project, tableFullName: table.table })))
      const result = await handleSuccessAsync(response)
      result.forEach((tableRange, resultIdx) => {
        const tableIdx = batchCount * i + resultIdx
        tableList[tableIdx] = { ...tableList[tableIdx], ...tableRange }
      })
    }
    return tableList
  }
  async loadSourceTables () {
    this._showDataLoading()
    try {
      const projectName = this.form.project
      const response = await this.fetchBatchLoadTables({ projectName })
      const tableList = await handleSuccessAsync(response)
      const tableDatas = await this.loadTablesRange(tableList)
      const tables = tableDatas.map(table => getTableLoadRange(table))
      this.setModalForm({ tables })
    } catch (e) {
      handleError(e)
    }
    this._hideDataLoading()
  }

  async handleSubmit () {
    this._showLoading()
    try {
      await this._submit()

      this._notifySuccess()
      this.handleClose(true)
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
  }
  async _submit () {
    const submitData = getTableBatchLoadSubmitData(this.form, this.selectedTables)
    await this.saveTablesBatchLoad(submitData)
  }
  _notifySuccess () {
    this.$message({ type: 'success', message: this.$t('kylinLang.common.saveSuccess') })
  }
  _hideForm () {
    this.isFormShow = false
  }
  _showForm () {
    this.isFormShow = true
  }
  _hideLoading () {
    this.isLoading = false
    this.isDisabled = false
  }
  _showLoading () {
    this.isLoading = true
    this.isDisabled = true
  }
  _hideDataLoading () {
    this.isDataLoading = false
  }
  _showDataLoading () {
    this.isDataLoading = true
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.batch-load-modal {
  .loading {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
  }
  .cell {
    position: relative;
  }
  .edit-action {
    margin-left: 10px;
    cursor: pointer;
  }
  .edit-action:hover {
    color: @base-color;
  }
  .time-text {
    display: inline-block;
    width: 90px;
  }
  .form {
    position: relative;
  }
  .error-mask {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    background: rgba(255, 255, 255, .9);
    z-index: 2000;
  }
}
.batch-load-edit-popper {
  .el-form-item.el-form-item--small {
    margin-bottom: 5px;
    margin-right: 0;
  }
  .el-date-editor.el-input {
    width: 144px;
  }
  .split {
    line-height: 33px;
    margin: 0 7px;
  }
}
</style>
