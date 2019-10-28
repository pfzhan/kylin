<template>
  <el-dialog
    :visible="isShow"
    top="5vh"
    width="960px"
    limited-area
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="handleClose"
    class="importSqlDialog">
    <span slot="title" class="ky-list-title">{{uploadTitle}}</span>
    <div class="upload-block" v-if="uploadFlag==='step1'">
      <img src="../../../assets/img/license.png" alt="" v-show="!uploadItems.length">
      <div class="ksd-mt-10 text" v-show="!uploadItems.length">{{$t('pleImport')}}</div>
      <el-upload
        ref="sqlUpload"
        :headers="uploadHeader"
        action=""
        :on-remove="handleRemove"
        :on-change="fileItemChange"
        :file-list="uploadItems"
        multiple
        :auto-upload="false">
        <el-button type="primary" size="medium">{{$t('sqlFiles')}}
        </el-button>
      </el-upload>
    </div>
    <el-row :gutter="15" v-if="uploadFlag==='step2'">
      <el-col :span="16">
        <div class="clearfix ksd-mb-10">
          <div class="ksd-fleft">
            <div v-if="pagerTableData.length&&whiteSqlData.capable_sql_num" class="ksd-fleft ksd-mr-10">
              <el-button type="primary" size="medium" plain @click="selectAll" v-if="selectSqls.length!==whiteSqlData.capable_sql_num">{{$t('checkAll')}}</el-button><el-button
              type="primary" size="medium" plain @click="cancelSelectAll" v-else>{{$t('cancelAll')}}</el-button>
            </div>

          </div>
          <div class="ksd-fright ksd-inline searchInput" v-if="whiteSqlData.size">
            <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
          </div>
        </div>
        <el-table
          :data="pagerTableData"
          border
          ref="multipleTable"
          @row-click="activeSql"
          @select="handleSelectionChange"
          @select-all="handleSelectAllChange"
          :row-class-name="tableRowClassName"
          class="import-table"
          style="width: 100%">
          <el-table-column type="selection" width="44" align="center" :selectable="selectable"></el-table-column>
          <el-table-column prop="sql" label="SQL" :resizable="false">
            <template slot-scope="props">
              <span class="ksd-nobr-text" style="width: 382px;">{{props.row.sql}}</span>
            </template>
          </el-table-column>
          <el-table-column prop="capable" :label="$t('kylinLang.common.status')" width="80">
            <template slot-scope="props">
              <i :class="{'el-icon-ksd-good_health': props.row.capable, 'el-icon-ksd-error_01': !props.row.capable}"></i>
            </template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.action')" width="80">
            <template slot-scope="props">
              <common-tip :content="$t('kylinLang.common.edit')">
                <i class="el-icon-ksd-table_edit" @click.stop="editWhiteSql(props.row)"></i>
              </common-tip>
              <common-tip :content="$t('kylinLang.common.drop')">
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delWhiteComfirm(props.row.id)"></i>
              </common-tip>
              </template>
          </el-table-column>
        </el-table>
        <kap-pager ref="sqlListsPager" class="ksd-center ksd-mt-10" :totalSize="filteredDataSize" layout="total, prev, pager, next, jumper" v-on:handleCurrentChange='whiteSqlDatasPageChange' :perPageSize="whitePageSize" v-if="filteredDataSize > 0"></kap-pager>
      </el-col>
      <el-col :span="8">
        <div class="ky-list-title ksd-mt-10 ksd-fs-14">{{$t('sqlBox')}}</div>
        <div element-loading-spinner="el-icon-loading">
          <div v-loading="sqlLoading" class="query_panel_box ksd-mt-10">
            <kap-editor ref="whiteInputBox" :height="inputHeight" :dragable="false" :readOnly="this.isReadOnly" lang="sql" theme="chrome" v-model="whiteSql" v-if="isShowEditor">
            </kap-editor>
            <div class="operatorBox" v-if="isEditSql">
              <div class="btn-group ksd-fright ky-no-br-space">
                <el-button size="small" @click="cancelEdit(isWhiteErrorMessage)">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="small" plain :loading="validateLoading" @click="validateWhiteSql()">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
            </div>
          </div>
          <div class="error_messages" v-if="isWhiteErrorMessage">
            <div v-for="(mes, index) in whiteMessages" :key="index">
              <div class="label">{{$t('messages')}}</div>
              <p>{{mes.incapableReason}}</p>
              <div class="label ksd-mt-10">{{$t('suggestion')}}</div>
              <p>{{mes.suggestion}}</p>
            </div>
          </div>
        </div>
      </el-col>
    </el-row>
    <div v-if="uploadFlag==='step3'">
      <el-table
        :data="suggestModels"
        class="model-table"
        border
        ref="modelsTable"
        style="width: 100%"
        @select="handleSelectionModel"
        @selection-change="handleSelectionModelChange"
        @select-all="handleSelectionAllModel"
        max-height="378">
        <el-table-column type="selection" width="44"></el-table-column>
        <el-table-column type="expand" width="44">
          <template slot-scope="scope">
            <el-table :data="sqlsTable(scope.row.sqls)" border :show-header="false" stripe>
              <el-table-column prop="sql" show-overflow-tooltip></el-table-column>
            </el-table>
          </template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.model.modelNameGrid')" prop="alias">
          <template slot-scope="scope">
            <el-input v-model="scope.row.alias" :class="{'name-error': scope.row.isRename}" size="small" @change="handleRename(scope.row)"></el-input>
            <div class="rename-error" v-if="scope.row.isRename">{{$t('renameError')}}</div>
          </template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.fact')" prop="fact_table" show-overflow-tooltip width="120"></el-table-column>
        <el-table-column :label="$t('kylinLang.common.dimension')" prop="dimensions" show-overflow-tooltip width="120" align="right">
          <template slot-scope="scope">{{scope.row.dimensions.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.measure')" prop="all_measures" width="100" align="right">
          <template slot-scope="scope">{{scope.row.all_measures.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.computedColumn')" prop="computed_columns" width="150" align="right">
          <template slot-scope="scope">{{scope.row.computed_columns.length}}</template>
        </el-table-column>
        <el-table-column label="SQL" prop="sqls" width="60" align="right">
          <template slot-scope="scope">{{scope.row.sqls.length}}</template>
        </el-table-column>
      </el-table>
    </div>
    <span slot="footer" class="dialog-footer">
      <div class="ksd-fleft query-count">
        <span v-if="uploadFlag==='step2'">
          <span><i class="el-icon-ksd-good_health"></i>{{whiteSqlData.capable_sql_num}}</span><span class="ksd-ml-10">
          <i class="el-icon-ksd-error_01"></i>{{whiteSqlData.size-whiteSqlData.capable_sql_num}}</span>
        </span>
        <span v-if="uploadFlag==='step1'" class="tips">
          <i class="el-icon-ksd-info ksd-fs-14"></i><span class="ksd-fs-12">{{$t('uploadFileTips')}}</span>
        </span>
      </div>
      <div class="ky-no-br-space">
        <el-button size="medium" @click="handleClose">{{$t('kylinLang.common.close')}}</el-button>
        <el-button type="primary" size="medium" plain v-if="uploadFlag==='step1'" :loading="importLoading" :disabled="!uploadItems.length||fileSizeError"  @click="submitFiles">{{$t('kylinLang.common.next')}}</el-button>
        <el-button type="primary" size="medium" v-if="uploadFlag==='step2'&&!isGenerateModel" :disabled="!finalSelectSqls.length" :loading="submitSqlLoading" @click="submitSqls">{{$t('addTofavorite')}}</el-button>
        <el-button type="primary" size="medium" plain v-if="uploadFlag==='step2'&&isGenerateModel" :loading="generateLoading" :disabled="!finalSelectSqls.length"  @click="submitSqls">{{$t('kylinLang.common.next')}}</el-button>
        <el-button type="primary" size="medium" plain v-if="uploadFlag==='step3'&&isGenerateModel" :loading="submitModelLoading" :disabled="!selectModels.length || isRenameModelExisted" @click="submitModels">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleSuccessAsync, handleError, objectClone } from '../../../util/index'
import { handleSuccess, kapConfirm } from '../../../util/business'

vuex.registerModule(['modals', 'UploadSqlModel'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('UploadSqlModel', {
      isShow: state => state.isShow,
      isGenerateModel: state => state.isGenerateModel,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapActions({
      importSqlFiles: 'IMPORT_SQL_FILES',
      formatSql: 'FORMAT_SQL',
      addTofavoriteList: 'ADD_TO_FAVORITE_LIST',
      validateWhite: 'VALIDATE_WHITE_SQL',
      suggestModel: 'SUGGEST_MODEL',
      saveSuggestModels: 'SAVE_SUGGEST_MODELS',
      validateModelName: 'VALIDATE_MODEL_NAME'
    }),
    // Store方法注入
    ...mapMutations('UploadSqlModel', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class UploadSqlModel extends Vue {
  uploadFlag = 'step1'
  importLoading = false
  messageInstance = null
  uploadItems = []
  pagerTableData = []
  fileSizeError = false
  isWhiteErrorMessage = false
  whiteMessages = []
  validateLoading = false
  isReadOnly = true
  whiteSql = ''
  isEditSql = false
  submitSqlLoading = false
  whiteSqlFilter = ''
  activeSqlObj = null
  whiteSqlData = null
  sqlLoading = false
  inputHeight = 424
  selectSqls = []
  filteredDataSize = 0
  whiteCurrentPage = 0
  timer = null
  whitePageSize = 10
  isShowEditor = true
  sqlFormatterObj = {}
  generateLoading = false
  submitModelLoading = false
  isRenameModelExisted = false
  suggestModels = []
  selectModels = []
  handleClose () {
    this.hideModal()
    this.resetModalForm()
    this.resetImport()
  }
  resetImport () {
    this.uploadFlag = 'step1'
    this.uploadItems = []
    this.whiteSqlData = null
    this.activeSqlObj = null
    this.pagerTableData = []
    this.whiteSqlFilter = ''
    this.importLoading = false
    this.sqlFormatterObj = {}
    this.generateLoading = false
    this.suggestModels = []
    this.selectModels = []
    this.submitModelLoading = false
    this.isRenameModelExisted = false
    this.messageInstance && this.messageInstance.close()
  }
  get uploadTitle () {
    if (this.isGenerateModel) {
      return this.$t('generateModel')
    } else {
      return this.$t('importSql')
    }
  }
  sqlsTable (sqls) {
    return sqls.map((s) => {
      return {sql: s}
    })
  }
  @Watch('inputHeight')
  onHeightChange (val) {
    if (val) {
      this.isShowEditor = false
      this.$nextTick(() => {
        this.isShowEditor = true
      })
    }
  }
  tableRowClassName ({row, rowIndex}) {
    if (this.activeSqlObj && row.id === this.activeSqlObj.id) {
      return 'active-row'
    }
    return ''
  }
  showLoading () {
    this.sqlLoading = true
  }
  hideLoading () {
    this.sqlLoading = false
  }
  onWhiteSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.whiteSqlDatasPageChange(0)
    }, 500)
  }
  delWhiteComfirm (id) {
    kapConfirm(this.$t('delSql'), null, this.$t('delSqlTitle')).then(() => {
      this.delWhite(id)
    })
  }
  handleRename (model) {
    let suggestListRename = false
    model.isRename = false
    if (model.isChecked) {
      for (let m = 0; m < this.suggestModels.length; m++) {
        if (this.suggestModels[m].uuid !== model.uuid && this.suggestModels[m].alias === model.alias.trim()) {
          model.isRename = true
          suggestListRename = true
          this.checkRenameModelExisted()
          break
        }
      }
      if (!suggestListRename) {
        this.validateModelName({alias: model.alias.trim(), uuid: model.uuid, project: this.currentSelectedProject}).then((res) => {
          handleSuccess(res, (data) => {
            if (data) {
              model.isRename = true
            } else {
              model.isRename = false
            }
            this.checkRenameModelExisted()
          })
        }, (res) => {
          handleError(res)
        })
      }
    }
  }
  delWhite (id) {
    for (const key in this.whiteSqlData.data) {
      if (this.whiteSqlData.data[key].id === id) {
        if (this.whiteSqlData.data[key].capable) {
          this.whiteSqlData.capable_sql_num--
        }
        this.whiteSqlData.size--
        this.whiteSqlData.data.splice(key, 1)
        this.$nextTick(() => {
          this.whiteSqlDatasPageChange(this.whiteCurrentPage)
        })
        break
      }
    }
    for (const index in this.selectSqls) {
      if (this.selectSqls[index].id === id) {
        this.selectSqls.splice(index, 1)
        break
      }
    }
  }
  whiteSqlDatasPageChange (currentPage, pageSize) {
    const size = pageSize || 10
    this.whiteCurrentPage = currentPage
    this.whitePageSize = size
    const filteredData = this.whiteFilter(this.whiteSqlData.data)
    this.filteredDataSize = filteredData.length
    this.pagerTableData = filteredData.slice(currentPage * size, (currentPage + 1) * size)
    if (this.filteredDataSize) {
      this.$nextTick(() => {
        this.activeSql(this.pagerTableData[0])
      })
      let targetSelectSqls = []
      this.pagerTableData.forEach((item) => {
        let index = -1
        for (const key in this.selectSqls) {
          if (this.selectSqls[key].id === item.id) {
            index = key
            break
          }
        }
        if (index !== -1) {
          targetSelectSqls.push(item)
        }
      })
      this.$nextTick(() => {
        this.toggleSelection(targetSelectSqls)
      })
    } else {
      this.whiteSql = ''
      this.activeSqlObj = null
      this.isEditSql = false
      this.whiteMessages = []
      this.isWhiteErrorMessage = false
      this.inputHeight = 424
    }
  }
  selectAll () {
    this.selectSqls = this.whiteSqlData.data.filter((item) => {
      return item.capable
    })
    this.selectPagerSqls(true)
  }
  cancelSelectAll () {
    this.selectSqls = []
    this.selectPagerSqls(false)
  }
  handleSelectionChange (val, row) {
    this.mergeSelectSqls(row)
  }
  handleSelectAllChange (val) {
    if (val.length) {
      val.forEach((item) => {
        this.mergeSelectSqls(item, 'batchAdd')
      })
    } else {
      this.pagerTableData.forEach((item) => {
        this.mergeSelectSqls(item, 'batchRemove')
      })
    }
  }
  // 单选一条时：toggle row; batchFlag有值时：批量添加rows或者批量去除rows
  mergeSelectSqls (row, batchFlag) {
    let index = -1
    for (const key in this.selectSqls) {
      if (this.selectSqls[key].id === row.id) {
        index = key
        break
      }
    }
    if (index === -1) {
      if (batchFlag !== 'batchRemove') {
        this.selectSqls.push(row)
      }
    } else {
      if (batchFlag !== 'batchAdd') {
        this.selectSqls.splice(index, 1)
      }
    }
  }
  handleSelectionModel (selection, row) {
    row.isChecked = !row.isChecked
    this.handleRename(row)
  }
  handleSelectionModelChange (selection) {
    this.selectModels = selection
  }
  handleSelectionAllModel (selection) {
    if (selection.length) {
      selection.forEach((m) => {
        m.isChecked = true
        this.handleRename(m)
      })
    } else {
      this.suggestModels.forEach((m) => {
        m.isChecked = false
        this.handleRename(m)
      })
    }
  }
  checkRenameModelExisted () {
    this.isRenameModelExisted = false
    for (let i = 0; i < this.suggestModels.length; i++) {
      if (this.suggestModels[i].isChecked && this.suggestModels[i].isRename) {
        this.isRenameModelExisted = true
        break
      }
    }
  }
  submitModels () {
    this.submitModelLoading = true
    let models = objectClone(this.selectModels)
    models.forEach((m) => {
      delete m.sqls
    })
    this.saveSuggestModels({project: this.currentSelectedProject, models: models}).then((res) => {
      handleSuccess(res, (data) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.submitModelLoading = false
        this.hideModal()
        this.$emit('reloadModelList')
      })
    }, (res) => {
      handleError(res)
      this.submitModelLoading = false
      this.hideModal()
    })
  }
  submitSqls () {
    const unCheckedSQL = this.whiteSqlData.capable_sql_num - this.finalSelectSqls.length
    if (unCheckedSQL) {
      kapConfirm(this.$t('submitConfirm', {unCheckedSQL: unCheckedSQL}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), type: 'warning'}).then(() => {
        this.submit()
      })
    } else {
      this.submit()
    }
  }
  submit () {
    const sqlsData = this.finalSelectSqls
    const sqls = sqlsData.map((item) => {
      return item.sql
    })
    if (!this.isGenerateModel) {
      this.submitSqlLoading = true
      this.addTofavoriteList({project: this.currentSelectedProject, sqls: sqls}).then((res) => {
        handleSuccess(res, (data) => {
          this.submitSqlLoading = false
          const importedMsg = this.$t('addSuccess', {importedNum: data.imported})
          const existedMsg = data.imported < sqls.length ? this.$t('existedMsg', {existedNum: sqls.length - data.imported}) : ''
          this.$alert(importedMsg + existedMsg + this.$t('end'), this.$t('kylinLang.common.notice'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            iconClass: 'el-icon-info primary'
          })
          sqlsData.forEach((item) => {
            this.delWhite(item.id)
          })
          this.$emit('reloadListAndSize')
          this.hideModal()
        })
      }, (res) => {
        handleError(res)
        this.submitSqlLoading = false
        this.hideModal()
      })
    } else {
      this.generateLoading = true
      this.suggestModel({project: this.currentSelectedProject, sqls: sqls}).then((res) => {
        handleSuccess(res, (data) => {
          this.suggestModels = data.map((d) => {
            d.isChecked = true
            d.isRename = false
            return d
          })
          this.generateLoading = false
          this.uploadFlag = 'step3'
          this.$nextTick(() => {
            this.suggestModels.forEach((model) => {
              this.$refs.modelsTable.toggleRowSelection(model)
            })
          })
        })
      }, (res) => {
        handleError(res)
        this.generateLoading = false
      })
    }
  }
  get finalSelectSqls () {
    let finalSqls = []
    finalSqls = this.selectSqls.filter((item) => {
      return item.sql.indexOf(this.whiteSqlFilter) !== -1
    })
    return finalSqls
  }
  selectPagerSqls (isSelectAll) {
    const selectedRows = isSelectAll ? this.pagerTableData.filter((item) => {
      return item.capable
    }) : []
    this.$nextTick(() => {
      this.toggleSelection(selectedRows)
    })
  }
  whiteFilter (data) {
    return data.filter((sqlObj) => {
      return sqlObj.sql.indexOf(this.whiteSqlFilter) !== -1
    })
  }
  toggleSelection (rows) {
    if (rows && rows.length) {
      this.$refs.multipleTable.clearSelection()
      rows.forEach(row => {
        this.$refs.multipleTable.toggleRowSelection(row)
      })
    } else {
      this.$refs.multipleTable.clearSelection()
    }
  }
  get uploadHeader () {
    if (this.$store.state.system.lang === 'en') {
      return {'Accept-Language': 'en'}
    } else {
      return {'Accept-Language': 'cn'}
    }
  }
  async editWhiteSql (sqlObj) {
    this.isEditSql = true
    this.inputHeight = 382
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 382
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 382 - 140
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.$refs.whiteInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({sqls: [sqlObj.sql]})
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
    this.activeSqlObj = sqlObj
    this.isReadOnly = false
  }
  async activeSql (sqlObj) {
    this.activeSqlObj = sqlObj
    this.isEditSql = false
    this.isReadOnly = true
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 424
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 424 - 140
      this.whiteMessages = sqlObj.sqlAdvices
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.$refs.whiteInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({sqls: [sqlObj.sql]})
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
  }
  fileItemChange (file, fileList) {
    let totalSize = 0
    this.uploadItems = fileList.filter((file) => {
      return file.name.toLowerCase().indexOf('.txt') !== -1 || file.name.toLowerCase().indexOf('.sql') !== -1
    }).map((item) => {
      totalSize = totalSize + item.size
      return item.raw ? item.raw : item
    })
    if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
      this.messageInstance = this.$message.warning(this.$t('filesSizeError'))
      this.fileSizeError = true
    } else {
      this.fileSizeError = false
    }
    if (!(file.name.toLowerCase().indexOf('.txt') !== -1 || file.name.toLowerCase().indexOf('.sql') !== -1)) {
      this.$message.error(this.$t('fileTypeError'))
    }
  }
  handleRemove (file, fileList) {
    this.messageInstance && this.messageInstance.close()
    this.uploadItems = fileList
    let totalSize = 0
    this.uploadItems.forEach((item) => {
      totalSize = totalSize + item.size
    })
    if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
      this.messageInstance = this.$message.warning(this.$t('filesSizeError'))
      this.fileSizeError = true
    } else {
      this.fileSizeError = false
    }
  }
  selectable (row) {
    return row.capable ? 1 : 0
  }
  cancelEdit (isErrorMes) {
    this.isEditSql = false
    this.inputHeight = isErrorMes ? 424 - 140 : 424
    this.whiteSql = this.sqlFormatterObj[this.activeSqlObj.id]
    this.activeSqlObj = null
    this.isReadOnly = true
  }
  validateWhiteSql () {
    this.validateLoading = true
    this.validateWhite({sql: this.whiteSql, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.validateLoading = false
        if (data.capable) {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.actionSuccess')
          })
          this.whiteMessages = []
          this.inputHeight = 424
          this.isWhiteErrorMessage = false
          this.isEditSql = false
          this.isReadOnly = true
          for (const key in this.whiteSqlData.data) {
            if (this.whiteSqlData.data[key].id === this.activeSqlObj.id) {
              this.whiteSqlData.data[key].sql = this.whiteSql
              this.sqlFormatterObj[this.activeSqlObj.id] = this.whiteSql
              if (!this.whiteSqlData.data[key].capable) {
                this.whiteSqlData.data[key].capable = true
                this.whiteSqlData.data[key].sqlAdvices = []
                this.whiteSqlData.capable_sql_num++
              }
              break
            }
          }
        } else {
          this.whiteMessages = data.sqlAdvices
          this.inputHeight = 424 - 140
          this.isWhiteErrorMessage = true
        }
      })
    }, (res) => {
      this.validateLoading = false
      handleError(res)
    })
  }
  submitFiles () {
    const formData = new FormData()   // 利用H5 FORMDATA 同时传输多文件和数据
    this.uploadItems.forEach(file => {
      formData.append('files', file)
    })
    this.importLoading = true
    this.importSqlFiles({project: this.currentSelectedProject, formData: formData}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.importLoading = false
        this.uploadFlag = 'step2'
        this.whiteSqlData = data
        this.selectAll()
        this.whiteSqlDatasPageChange(0)
        if (msg) {
          this.$message.warning(msg)
        }
      })
    }, (res) => {
      handleError(res)
      this.importLoading = false
    })
  }
}
</script>
