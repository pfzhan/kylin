<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="$t('editTableIndexTitle')" append-to-body class="table-edit-dialog" width="880px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="tableIndexMeta" :rules="rules" ref="tableIndexForm" label-position="top">
        <el-form-item :label="$t('tableIndexName')" prop="name">
          <el-input v-model="tableIndexMeta.name" auto-complete="off" placeholder="" size="medium" style="width:500px"></el-input>
        </el-form-item>
      </el-form>
      <div class="ky-line"></div>
      <div class="ksd-mt-20">
        <el-button type="primary" plain size="medium" @click="selectAll">{{$t('selectAllColumns')}}</el-button><el-button plain size="medium" @click="clearAll">{{$t('clearAll')}}</el-button>
        <el-input v-model="searchColumn" size="medium" prefix-icon="el-icon-search" class="ksd-fright" style="width:200px" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input>
       <div class="ky-simple-table">
          <el-row class="table-header table-row ksd-mt-10">
            <el-col :span="12">{{$t('kylinLang.model.columnName')}}</el-col>
            <el-col :span="4">{{$t('tableIndex')}}</el-col>
            <el-col :span="4">Sort</el-col>
            <el-col :span="4">Shard</el-col>
          </el-row>
          <div class="table-content" v-scroll>
            <transition-group name="flip-list" tag="div">
                <el-row v-for="col in searchAllColumns" :key="col.fullName" class="table-row" :class="tableRowClassName(col)">
                  <el-col :span="12" :title="col.fullName">{{col.fullName}}</el-col>
                  <el-col :span="4" @click.native="toggleDisplay(col)"><i class="el-icon-success" :class="{active: col.isUsed}" ></i></el-col>
                  <el-col :span="4" >
                    <div class="action-list" @click="toggleSort(col)" v-if="!(sortCount >= 9 && getRowIndex(col, 'fullName') + 1 > 9)">
                      <span class="ky-dot-tag" v-if="col.isUsed" :class="{'no-sorted': !col.isSorted}">{{col.isSorted ? getRowIndex(col, 'fullName') + 1 : sortCount + 1}}</span>
                      <span class="up-down" :class="{hide: searchColumn}">
                        <i v-visible="col.isUsed && col.isSorted && !checkIsTopSort(col)" @click.stop="upRow(col)" class="el-icon-ksd-arrow_up"></i>
                        <i v-visible="col.isUsed && col.isSorted && !checkIsBottomSort(col)" @click.stop="downRow(col)" class="el-icon-ksd-arrow_down"></i>
                      </span>
                    </div>
                  </el-col>
                  <el-col :span="4" @click.native="toggleShard(col)">
                    <i class="el-icon-success" v-if="col.isUsed" :class="{active: col.isShared}"></i>
                  </el-col>
                </el-row>
              </transition-group>
          </div>
       </div>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-checkbox v-model="tableIndexMeta.load_data" :label="true" class="ksd-fleft ksd-mt-8">{{$t('catchup')}}</el-checkbox>
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="submit" :disabled="saveBtnDisable">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../store'
  import { NamedRegex } from 'config'
  import { handleError, kapMessage } from 'util/business'
  import { objectClone, changeObjectArrProperty, indexOfObjWithSomeKey, filterObjectArray, topArrByArr } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'TableIndexEditModal'], store)

  @Component({
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
    pager = 0
    tableIndexMetaStr = JSON.stringify({
      id: '',
      name: '',
      col_order: [],
      sort_by_columns: [],
      shard_by_columns: [],
      load_data: true
    })
    tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
    rules = {
      name: [
        {validator: this.checkName, trigger: 'blur'}
      ]
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
      this.$nextTick(() => {
        this.lockSortAnimate = false
      })
    }
    toggleShard (t) {
      let shardStatus = t.isShared
      changeObjectArrProperty(this.allColumns, '*', 'isShared', false)
      t.isShared = !shardStatus
    }
    get searchAllColumns () {
      if (!this.isShow) {
        return
      }
      return this.allColumns.filter((col) => {
        return !this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0
      })
    }
    allColumns = []
    getAllColumns () {
      this.allColumns = []
      let result = []
      let modelUsedTables = this.modelInstance && this.modelInstance.getTableColumns() || []
      modelUsedTables.forEach((col) => {
        result.push(col.full_colname)
      })
      if (this.tableIndexMeta.sort_by_columns) {
        result = topArrByArr(result, this.tableIndexMeta.sort_by_columns)
      }
      result.forEach((i, index) => {
        let obj = {fullName: i, isSorted: false, isUsed: false, isShared: false, colorful: false}
        if (this.tableIndexMeta.sort_by_columns.indexOf(i) >= 0) {
          obj.isSorted = true
        }
        if (this.tableIndexMeta.col_order.indexOf(i) >= 0) {
          obj.isUsed = true
        }
        if (this.tableIndexMeta.shard_by_columns.indexOf(i) >= 0) {
          obj.isShared = true
        }
        this.allColumns.push(obj)
      })
    }
    get saveBtnDisable () {
      return filterObjectArray(this.allColumns, 'isUsed', true).length === 0
    }
    @Watch('isShow')
    initTableIndex (val) {
      if (val) {
        if (val && this.tableIndexDesc) {
          Object.assign(this.tableIndexMeta, this.tableIndexDesc)
        } else {
          this.tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
        }
        this.getAllColumns()
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
      this.tableIndexMeta.name = ''
      setTimeout(() => {
        this.callback && this.callback({
          isSubmit: isSubmit
        })
        this.resetModalForm()
      }, 200)
    }
    async submit () {
      this.$refs.tableIndexForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        let successCb = () => {
          this.closeModal(true)
          this.btnLoading = false
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
        this.tableIndexMeta.model = this.modelInstance.name
        if (this.tableIndexMeta.id) {
          this.editTableIndex(this.tableIndexMeta).then(successCb, errorCb)
        } else {
          this.addTableIndex(this.tableIndexMeta).then(successCb, errorCb)
        }
      })
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .table-edit-dialog {
    .flip-list-move {
      transition: transform .5s;
    }
    .action-list {
      position:relative;
      .up-down {
        position: absolute;
        right:4px;
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
      background:@normal-color-2!important;
    }
    .el-icon-success {
      cursor:pointer;
      &.active{
        color:@btn-success-normal;
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
  } 
</style>
