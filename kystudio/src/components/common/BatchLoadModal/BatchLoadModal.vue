<template>
  <el-dialog
    class="batch-load-modal"
    width="780px"
    :visible="isShow"
    :title="$t('batchLoad')"
    @open="handleOpen"
    @close="() => handleClose()"
    @closed="handleClosed">
    <el-form v-loading="isDataLoading" ref="form" size="small" :inline="true" :model="tempForm">
      <el-table border :data="paginationTables" @selection-change="handleSelectionChange">
        <el-table-column type="selection" width="38"></el-table-column>
        <el-table-column prop="name" :label="$t('table')" header-align="center"></el-table-column>
        <el-table-column prop="refresh" :label="$t('refresh')" width="75" align="center"></el-table-column>
        <el-table-column :label="$t('loadRange')" width="250" header-align="center">
          <template slot-scope="scope">
            {{getRangeString(scope.row)}}
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
                  :picker-options="{ disabledDate: time => time.getTime() > tempForm.editDate[1] }"
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
                  :picker-options="{ disabledDate: time => time.getTime() < tempForm.editDate[0] }"
                  :placeholder="$t('kylinLang.common.endTime')">
                </el-date-picker>
              </el-form-item>
              <div style="text-align: right; margin: 0">
                <el-button size="mini" type="info" text @click="handlePopperHide(`popover${scope.$index}`)">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="mini" @click="handleInputDate(`tables.${scope.$index}.loadRange`, tempForm.editDate, scope.$index)">{{$t('kylinLang.common.ok')}}</el-button>
              </div>
            </el-popover>
            <i class="edit-action el-icon-ksd-table_edit" v-popover="`popover${scope.$index}`" @click="handlePopperShow(scope)"></i>
          </template>
        </el-table-column>
        <el-table-column prop="relatedIndex" :label="$t('relatedIndex')" width="125" header-align="center"></el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mt-20" ref="pager"
        :totalSize="form.tables.length"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
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
import { getFormattedTable } from '../../../util/UtilTable'
import { getTableLoadRange } from './handler'

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
      fetchTables: 'FETCH_TABLES'
    })
  },
  locales
})
export default class BatchLoadModal extends Vue {
  isDataLoading = false
  isLoading = false
  isDisabled = false
  isFormShow = false
  pageOffset = 0
  pageSize = 10
  tempForm = {
    editDate: []
  }
  get rules () {
    return [
      { required: true, message: '请输入日期', trigger: 'blur' },
      { type: 'date', message: '请输入正确的日期', trigger: 'blur' }
    ]
  }
  get paginationTables () {
    const currentShowIdx = this.pageOffset * this.pageSize
    return this.form.tables.slice(currentShowIdx, currentShowIdx + this.pageSize)
  }
  getRangeString (table) {
    const startDate = dayjs(table.loadRange[0]).format('YYYY-MM-DD')
    const endDate = dayjs(table.loadRange[1]).format('YYYY-MM-DD')
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
    const isVaild = await this.$refs['form'].validate()
    if (isVaild) {
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
  handleSelectionChange (value) {
    console.log(value)
  }
  handleCurrentChange (pageOffset, pageSize) {
    this.pageOffset = pageOffset
    this.pageSize = pageSize
  }
  async loadSourceTables () {
    this._showDataLoading()

    const projectName = this.form.project
    const response = await this.fetchTables({ projectName, pageOffset: 0, pageSize: 99999999, isDisableCache: true })
    const result = await handleSuccessAsync(response)
    const tables = result.tables.map(table => getTableLoadRange(getFormattedTable(table)))
    this.setModalForm({ tables })

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
    position: absolute;
    top: 50%;
    right: 15px;
    transform: translateY(-50%);
    cursor: pointer;
  }
  .edit-action:hover {
    color: @base-color;
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
