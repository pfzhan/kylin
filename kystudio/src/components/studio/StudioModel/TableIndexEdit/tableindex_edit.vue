<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="tableIndexModalTitle" append-to-body limited-area top="5vh" class="table-edit-dialog" width="880px" v-if="isShow" :visible="true" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <!-- <el-form :model="tableIndexMeta" :rules="rules" ref="tableIndexForm" label-position="top">
        <el-form-item :label="$t('tableIndexName')" prop="name">
          <el-input v-focus="isShow" v-model="tableIndexMeta.name" auto-complete="off" placeholder="" size="medium" style="width:500px"></el-input>
        </el-form-item>
      </el-form> -->
      <!-- <div class="ky-line ksd-mtb-10"></div> -->
      <div>
        <!-- <el-button type="primary" plain size="medium" @click="selectAll">{{$t('selectAllColumns')}}</el-button><el-button plain size="medium" @click="clearAll">{{$t('clearAll')}}</el-button> -->
        <div class="header">
          <el-alert
            :title="$t('tableIndexShardByTips')"
            type="info"
            :closable="false"
            :show-background="false"
            show-icon>
          </el-alert>
          <el-tooltip :content="$t('excludeTableCheckboxTip')" effect="dark" placement="top"><el-checkbox class="ksd-mr-5" v-if="showExcludedTableCheckBox" v-model="displayExcludedTables">{{$t('excludeTableCheckbox')}}</el-checkbox></el-tooltip>
          <el-input v-model="searchColumn" size="medium" prefix-icon="el-icon-search" style="width:200px" :placeholder="$t('filterByColumns')"></el-input>
        </div>
       <div class="ky-simple-table">
          <el-row class="table-header table-row ksd-mt-10">
            <el-col :span="1"><el-checkbox v-model="isSelectAllTableIndex" :indeterminate="getSelectedColumns.length !== 0 && allColumns.length > getSelectedColumns.length" @change="selectAllTableIndex" size="small" /></el-col>
            <el-col :span="14" class="column-name">{{$t('kylinLang.model.columnName')}}</el-col>
            <el-col :span="3" class="cardinality-item">{{$t('cardinality')}}</el-col>
            <el-col :span="3">ShardBy</el-col>
            <el-col :span="3">{{$t('order')}}</el-col>
          </el-row>
          <div class="table-content table-index-layout" v-scroll.observe.reactive @scroll-bottom="scrollLoad">
            <transition-group name="flip-list" tag="div">
                <el-row v-for="(col, index) in searchAllColumns" :key="col.fullName" class="table-row">
                  <el-col :span="1"><el-checkbox size="small" v-model="col.isUsed" @change="(status) => selectTableIndex(status, col)" /></el-col>
                  <el-col :span="14" class="column-name" :title="col.fullName">{{col.fullName}}<el-tooltip :content="$t('excludedTableIconTip')" effect="dark" placement="top"><i class="excluded_table-icon el-icon-ksd-exclude" v-if="isExistExcludeTable(col.fullName) && displayExcludedTables"></i></el-tooltip></el-col>
                  <el-col :span="3" class="cardinality-item">
                    <template v-if="col.cardinality === null"><i class="no-data_placeholder">NULL</i></template>
                    <template v-else>{{ col.cardinality }}</template>
                  </el-col>
                  <el-col :span="3" @click.native="toggleShard(col)">
                     <i class="el-icon-success" v-if="col.isUsed" :class="{active: col.isShared}"></i>
                  </el-col>
                  <el-col :span="3" class="order-actions">
                    <!-- <div class="action-list" @click="toggleSort(col)" v-if="!(sortCount >= 9 && getRowIndex(col, 'fullName') + 1 > 9)">
                      <span class="ky-dot-tag" v-if="col.isUsed" :class="{'no-sorted': !col.isSorted}">{{col.isSorted ? getRowIndex(col, 'fullName') + 1 : sortCount + 1}}</span>
                      <span class="up-down" :class="{hide: searchColumn}">
                        <i v-visible="col.isUsed && col.isSorted && !checkIsTopSort(col)" @click.stop="upRow(col)" class="el-icon-ksd-arrow_up"></i>
                        <i v-visible="col.isUsed && col.isSorted && !checkIsBottomSort(col)" @click.stop="downRow(col)" class="el-icon-ksd-arrow_down"></i>
                      </span>
                    </div> -->
                    <template  v-if="col.isUsed">
                      <el-tooltip :content="$t('moveTop')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_to_top', {'is-disabled': index === 0 && !searchColumn}]" @click="topRow(col)"></span>
                      </el-tooltip>
                      <el-tooltip :content="$t('moveUp')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_up', {'is-disabled': index === 0}]" @click="upRow(col)"></span>
                      </el-tooltip>
                      <el-tooltip :content="$t('moveDown')" effect="dark" placement="top">
                        <span :class="['icon', 'el-icon-ksd-move_down', {'is-disabled': !searchAllColumns[index + 1] || !searchAllColumns[index + 1].isUsed}]" @click="downRow(col)"></span>
                      </el-tooltip>
                    </template>
                  </el-col>
                </el-row>
              </transition-group>
          </div>
       </div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <!-- <el-checkbox v-model="tableIndexMeta.load_data" :label="true" class="ksd-fleft ksd-mt-8">{{$t('catchup')}}</el-checkbox> -->
        <el-button plain @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :loading="btnLoading" size="medium" @click="submit(false)" :disabled="saveBtnDisable">{{$t('kylinLang.common.save')}}</el-button>
        <el-button type="primary" :loading="btnLoading" size="medium" @click="submit(true)" :disabled="saveBtnDisable">{{$t('saveAndBuild')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../store'
  import { NamedRegex } from 'config'
  import { BuildIndexStatus } from 'config/model'
  import { handleError, kapMessage, handleSuccess, kapConfirm, postCloudUrlMessage } from 'util/business'
  import { objectClone, changeObjectArrProperty, indexOfObjWithSomeKey, filterObjectArray, getQueryString } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'TableIndexEditModal'], store)

  @Component({
    inject: {
      getFavoriteRules: {
        default: () => {}
      }
    },
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('TableIndexEditModal', {
        isShow: state => state.isShow,
        modelInstance: state => state.form.data.modelInstance,
        tableIndexDesc: state => objectClone(state.form.data.tableIndexDesc),
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        editTableIndex: 'EDIT_TABLE_INDEX',
        addTableIndex: 'ADD_TABLE_INDEX'
      }),
      ...mapMutations('TableIndexEditModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class TableIndexEditModal extends Vue {
    btnLoading = false
    openShared = false
    searchColumn = ''
    allColumns = []
    currentPager = 1
    pagerSize = 50
    pager = 0
    tableIndexMetaStr = JSON.stringify({
      id: '',
      col_order: [],
      sort_by_columns: [],
      shard_by_columns: [],
      load_data: false
    })
    tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
    rules = {
      name: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    cloneMeta = ''
    isSelectAllTableIndex = false
    favoriteRules = {}
    displayExcludedTables = false

    @Watch('searchColumn')
    changeSearchColumn (val) {
      const dom = document.querySelector('.table-index-layout .scroll-content')
      dom && (dom.style = 'transform: translate3d(0px, 0px, 0px);')
    }

    get getSelectedColumns () {
      return this.allColumns.filter(it => it.isUsed)
    }

    get showExcludedTableCheckBox () {
      if (this.favoriteRules.excluded_tables) {
        const currentTables = [...new Set(this.allColumns.map(it => it.fullName.split('.')[0]))]
        const excludesTables = this.favoriteRules.excluded_tables.split(',').map(item => item.split('.')[1])
        return currentTables.filter(it => excludesTables.includes(it)).length
      } else {
        return false
      }
    }
    topRow (col) {
      let index = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(0, 0, col)
      this.allColumns.splice(index + 1, 1)
    }
    upRow (col) {
      let i = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(i - 1, 0, col)
      this.allColumns.splice(i + 1, 1)
    }
    downRow (col) {
      let i = this.getRowIndex(col, 'fullName')
      this.allColumns.splice(i + 2, 0, col)
      this.allColumns.splice(i, 1)
    }
    tableRowClassName (row) {
      return row.colorful || row.isSorted ? 'row-colorful' : ''
    }
    getRowIndex (t, key) {
      return indexOfObjWithSomeKey(this.allColumns, key, t[key])
    }
    checkIsTopSort (col) {
      let i = this.getRowIndex(col, 'fullName')
      if (i === 0 && col.isSorted) {
        return true
      }
    }
    checkIsBottomSort (col) {
      let i = this.getRowIndex(col, 'fullName')
      let nextCol = this.allColumns[i + 1]
      if (nextCol && !nextCol.isSorted && col.isSorted) {
        return true
      }
    }
    get sortCount () {
      if (!this.isShow) {
        return
      }
      return filterObjectArray(this.allColumns, 'isSorted', true).length
    }
    toggleDisplay (t) {
      let i = this.getRowIndex(t, 'fullName')
      if (t.isUsed) {
        if (t.isSorted) {
          this.toggleSort(t, i)
        }
        t.isSorted = false
        t.isShared = false
      }
      t.isUsed = !t.isUsed
    }
    // 切换sort状态的列，并带模拟缓动效果
    lockSortAnimate = false
    toggleSort (t, index) {
      if (this.lockSortAnimate) {
        return
      }
      this.lockSortAnimate = true
      let i = index === undefined ? this.getRowIndex(t, 'fullName') : index
      let sortedLen = filterObjectArray(this.allColumns, 'isSorted', true).length
      if (!t.isSorted) {
        if (sortedLen >= 9) {
          kapMessage(this.$t('sortLimitTip'), {type: 'warning'})
          return
        }
        this.allColumns.splice(i, 1)
        this.allColumns.splice(sortedLen, 0, t)
      } else {
        let s = indexOfObjWithSomeKey(this.allColumns, 'isSorted', false)
        if (s === -1) {
          s = this.allColumns.length
        }
        this.allColumns.splice(i, 1)
        this.allColumns.splice(sortedLen - 1, 0, t)
      }
      t.isSorted = !t.isSorted
      setTimeout(() => {
        this.lockSortAnimate = false
      }, 300)
    }
    toggleShard (t) {
      let shardStatus = t.isShared
      changeObjectArrProperty(this.allColumns, '*', 'isShared', false)
      t.isShared = !shardStatus
    }
    scrollLoad () {
      if (this.searchAllColumns && this.searchAllColumns.length !== this.filterResult.length) {
        this.currentPager += 1
      }
    }
    // 是否为屏蔽表的 column
    isExistExcludeTable (col) {
      return this.favoriteRules.excluded_tables && this.favoriteRules.excluded_tables.split(',').map(it => it.split('.')[1] && it.split('.')[1]).includes(col.split('.')[0])
    }
    get filterResult () {
      if (!this.isShow) {
        return []
      }
      return this.allColumns.filter((col) => {
        if (this.displayExcludedTables) {
          return !this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0
        } else {
          return (!this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0) && !this.isExistExcludeTable(col.fullName)
        }
      })
    }
    get searchAllColumns () {
      if (!this.isShow) {
        return []
      }
      return this.filterResult.slice(0, this.pagerSize * this.currentPager)
    }
    getAllColumns () {
      this.allColumns = []
      // let result = []
      let result = this.modelInstance.selected_columns.map((c) => {
        return { fullName: c.column, cardinality: c.cardinality }
      })
      // let modelUsedTables = this.modelInstance && this.modelInstance.getTableColumns() || []
      // modelUsedTables.forEach((col) => {
      //   result.push(col.full_colname)
      // })
      if (this.tableIndexMeta.sort_by_columns) {
        // result = topArrByArr(result, this.tableIndexMeta.sort_by_columns)
        const selected = this.tableIndexMeta.sort_by_columns.map(item => {
          const index = result.findIndex(it => it.fullName === item)
          return {fullName: item, cardinality: result[index].cardinality}
        })
        const unSort = result.filter(item => !this.tableIndexMeta.sort_by_columns.includes(item.fullName))
        result = [...selected, ...unSort]
      }
      // cc列也要放到这里
      // let ccColumns = this.modelInstance && this.modelInstance.computed_columns || []
      // ccColumns.forEach((col) => {
      //   result.push(col.tableAlias + '.' + col.columnName)
      // })
      result.forEach((ctx, index) => {
        let obj = {fullName: ctx.fullName, cardinality: ctx.cardinality, isSorted: false, isUsed: false, isShared: false, colorful: false}
        if (this.tableIndexMeta.sort_by_columns.indexOf(ctx.fullName) >= 0) {
          obj.isSorted = true
        }
        if (this.tableIndexMeta.col_order.indexOf(ctx.fullName) >= 0) {
          obj.isUsed = true
        }
        if (this.tableIndexMeta.shard_by_columns.indexOf(ctx.fullName) >= 0) {
          obj.isShared = true
        }
        this.allColumns.push(obj)
      })
      // 初始判断是否为全选状态
      this.isSelectAllTableIndex = this.allColumns.length && this.allColumns.filter(it => it.isUsed).length === this.allColumns.length
    }
    get saveBtnDisable () {
      return filterObjectArray(this.allColumns, 'isUsed', true).length === 0 || this.cloneMeta === JSON.stringify(this.allColumns)
    }
    @Watch('isShow')
    initTableIndex (val) {
      if (val) {
        if (this.tableIndexDesc) {
          // col_order 数据结构改成object list了，这里重置成原来的column list
          if (this.tableIndexDesc.col_order) {
            this.tableIndexDesc.col_order = this.tableIndexDesc.col_order.map((col) => {
              return col.key
            })
          }
          Object.assign(this.tableIndexMeta, this.tableIndexDesc)
        }
        this.getFavoriteRulesContext()
        this.getAllColumns()
        this.cloneMeta = JSON.stringify(this.allColumns)
      } else {
        this.tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
      }
    }
    pagerChange (pager) {
      this.pager = pager
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    clearAll () {
      this.allColumns.forEach((col) => {
        col.isUsed = false
        col.isShared = false
        col.isSorted = false
      })
    }
    selectAll () {
      this.allColumns.forEach((col) => {
        col.isUsed = true
      })
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.btnLoading = false
      // this.tableIndexMeta.name = ''
      this.searchColumn = ''
      this.isSelectAllTableIndex = false
      // this.$refs.tableIndexForm.resetFields()
      setTimeout(() => {
        this.callback && this.callback({
          isSubmit: isSubmit
        })
        this.resetModalForm()
      }, 200)
    }
    get tableIndexModalTitle () {
      return this.tableIndexMeta.id !== '' ? this.$t('editTableIndexTitle') : this.$t('addTableIndexTitle')
    }
    handleBuildIndexTip (data) {
      let tipMsg = this.$t('kylinLang.model.saveIndexSuccess', {indexType: this.$t('kylinLang.model.tableIndex')})
      if (this.tableIndexMeta.load_data) {
        if (data.type === BuildIndexStatus.NORM_BUILD) {
          tipMsg += ' ' + this.$t('kylinLang.model.buildIndexSuccess1', {indexType: this.$t('kylinLang.model.tableIndex')})
          this.$message({
            type: 'success',
            dangerouslyUseHTMLString: true,
            message: (
              <div>
                <span>{tipMsg}</span>
                <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
              </div>
            )
          })
          return
        }
        if (data.type === BuildIndexStatus.NO_LAYOUT) {
          tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.tableIndex')})
          this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
        } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
          tipMsg += '<br/>' + this.$t('kylinLang.model.buildIndexFail1', {modelName: this.modelInstance.name})
          this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'success', dangerouslyUseHTMLString: true})
        }
      } else {
        this.$message({message: tipMsg, type: 'success'})
      }
    }
    confirmSubmit (isLoadData) {
      this.btnLoading = true
      let successCb = (res) => {
        handleSuccess(res, (data) => {
          this.handleBuildIndexTip(data)
        })
        this.closeModal(true)
        this.btnLoading = false
        // if (!isLoadData && !this.modelInstance.segments.length) {
        //   this.$emit('needShowBuildTips', this.modelInstance.uuid)
        // }
        if (isLoadData && !this.modelInstance.segments.length) {
          this.$emit('openBuildDialog', this.modelInstance, true)
        }
        // 保存并增量构建时，需弹出segment list选择构建区域
        if (isLoadData && this.modelInstance.segments.length > 0 && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column) {
          this.$emit('openComplementAllIndexesDialog', this.modelInstance)
        }
      }
      let errorCb = (res) => {
        this.btnLoading = false
        handleError(res)
      }
      // 按照sort选中列的顺序对col_order进行重新排序
      this.tableIndexMeta.col_order = []
      this.tableIndexMeta.sort_by_columns = []
      this.tableIndexMeta.shard_by_columns = []
      this.allColumns.forEach((col) => {
        if (col.isUsed) {
          this.tableIndexMeta.col_order.push(col.fullName)
        }
        if (col.isShared) {
          this.tableIndexMeta.shard_by_columns.push(col.fullName)
        }
        if (col.isSorted) {
          this.tableIndexMeta.sort_by_columns.push(col.fullName)
        }
      })
      this.tableIndexMeta.project = this.currentSelectedProject
      this.tableIndexMeta.model_id = this.modelInstance.uuid
      'name' in this.tableIndexMeta && delete this.tableIndexMeta.name
      if (this.tableIndexMeta.id) {
        this.editTableIndex(this.tableIndexMeta).then(successCb, errorCb)
      } else {
        this.addTableIndex(this.tableIndexMeta).then(successCb, errorCb)
      }
    }
    async submit (isLoadData) {
      const { status } = this.tableIndexDesc || {}
      // 保存并全量构建时，可以直接提交构建任务，保存并增量构建时，需弹出segment list选择构建区域
      if (isLoadData && this.modelInstance.segments.length > 0 && (!this.modelInstance.partition_desc || this.modelInstance.partition_desc && !this.modelInstance.partition_desc.partition_date_column) || !isLoadData) {
        this.tableIndexMeta.load_data = isLoadData
      }
      if (status && status !== 'EMPTY' && status === 'ONLINE') {
        kapConfirm(this.$t('cofirmEditTableIndex'), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), type: 'warning'}).then(() => {
          this.confirmSubmit(isLoadData)
        })
      } else {
        this.confirmSubmit(isLoadData)
      }
    }
    selectAllTableIndex (v) {
      this.isSelectAllTableIndex = v
      this.allColumns.forEach(item => {
        item.isUsed = v
        item.isSorted = v
      })
    }
    selectTableIndex (status, col) {
      const selectedColumns = this.getSelectedColumns
      const unSelected = this.allColumns.filter(it => !it.isUsed)
      col.isSorted = status
      this.allColumns = [...selectedColumns, ...unSelected]
      selectedColumns.length === this.allColumns.length && (this.isSelectAllTableIndex = true)
      unSelected.length === this.allColumns.length && (this.isSelectAllTableIndex = false)
    }
    // 跳转至job页面
    jumpToJobs () {
      if (getQueryString('from') === 'cloud' || getQueryString('from') === 'iframe') {
        postCloudUrlMessage(this.$route, { name: 'kapJob' })
      } else {
        this.$router.push('/monitor/job')
      }
    }

    // 获取优化建议配置参数
    async getFavoriteRulesContext () {
      this.favoriteRules = await this.getFavoriteRules()
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .table-edit-dialog {
    .el-dialog {
      min-width: 600px;
      .header {
        text-align: right;
        .el-alert--nobg {
          text-align: left;
        }
      }
    }
    .flip-list-move {
      transition: transform .5s;
    }
    .action-list {
      position:relative;
      .up-down {
        position: absolute;
        left: 61%;
        display: none;
        i {
          color:@base-color;
        }
      }
      &:hover {
        .up-down {
          display: inline-block;
        }
      }
    }
    .row-colorful {
      background:@lighter-color-tip!important;
    }
    .el-icon-success {
      cursor:pointer;
      &.active{
        color:@color-success;
      }
      color:@text-placeholder-color;
    }
    .ky-dot-tag {
      cursor:pointer;
    }
    .no-sorted {
      background:@text-placeholder-color;
    }
    .sub-title {
      margin-top:60px;
    }
    .show-pagers{
      width: 42px;
      position: absolute;
      top: 100px;
      right: -16px;
      ul {
        li {
          border:solid 1px #ccc;
          margin-bottom:12px;
          text-align:center;
        }
      }
    }
    .show-more-block {
      width:120px;
      height:20px;
      line-height:20px;
      text-align:center
    }
    .sort-icon {
      .ky-square-box(32px, 32px);
      background: @text-secondary-color;
      display: inline-block;
      color:@fff;
      vertical-align: baseline;
    }
    .table-index-columns {
      li {
        margin-top:20px;
        height:32px;
      }
    }
    .ky-simple-table {
      .el-col {
        position: relative;
        &:first-child {
          text-overflow: initial;
        }
      }
      .order-actions {
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .table-row {
        .column-name {
          text-align: left;
        }
        .cardinality-item {
          text-align: right;
          .no-data_placeholder {
            color: @text-placeholder-color;
            font-size: 12px;
          }
        }
        .icon {
          cursor: pointer;
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
          &.is-disabled {
            pointer-events: none;
            color: @text-disabled-color;
          }
        }
      }
      .excluded_table-icon {
        position: absolute;
        right: 10px;
        line-height: 32px;
      }
    }
  }
</style>
