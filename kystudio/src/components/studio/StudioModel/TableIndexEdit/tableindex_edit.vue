<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="$t('editTableIndexTitle')" append-to-body class="table-edit-dialog" width="880px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="tableIndexMeta" :rules="rules" ref="tableIndexForm" label-position="top" >
        <el-form-item :label="$t('tableIndexName')" prop="name">
          <el-input v-model="tableIndexMeta.name" auto-complete="off" size="medium" style="width:500px"></el-input>
        </el-form-item>
      </el-form>
      <div class="ky-line"></div>
      <div class="ksd-mt-20">
        <el-button type="primary" plain size="medium" @click="selectAll">{{$t('selectAllColumns')}}</el-button><el-button plain size="medium" @click="clearAll">{{$t('clearAll')}}</el-button>
        <el-input v-model="searchColumn" size="medium" prefix-icon="el-icon-search" class="ksd-fright" style="width:200px" placeholder="请输入内容"></el-input>
         <el-table class="ksd-mt-10"
          :data="searchAllColumns"
          type=index
          border
          height="450"
          :row-class-name="tableRowClassName"
          style="width: 100%">
          <el-table-column
            prop="fullName"
            :label="$t('kylinLang.model.columnName')">
          </el-table-column>
          <el-table-column
            align="center"
            prop="isUsed"
            label="Display"
            width="120">
            <template slot-scope="scope">
              <i class="el-icon-success" :class="{active: scope.row.isUsed}" @click="toggleDisplay(scope.row)"></i>
            </template>
          </el-table-column>
          <el-table-column
            align="center"
            prop="isSorted"
            label="Sort By"
            width="120">
            <template slot-scope="scope">
              <div class="action-list">
                <span class="ky-dot-tag" v-if="scope.row.isUsed" :class="{'no-sorted': !scope.row.isSorted}"  @click="toggleSort(scope.row)">{{scope.row.isSorted ? getRowIndex(scope.row, 'fullName') + 1 : 1}}</span>
                <!-- <span v-if="scope.row.isUsed && scope.row.isSorted">{{scope.$index + 1}}</span> -->
                <span class="up-down" :class="{hide: searchColumn}">
                  <i v-visible="scope.row.isUsed && scope.row.isSorted && scope.$index !== 0" @click="upRow(scope.$index)" class="el-icon-ksd-arrow_up"></i>
                  <i v-visible="scope.row.isUsed && scope.row.isSorted && allColumns[scope.$index+1] && allColumns[scope.$index+1].isSorted" @click="downRow(scope.$index)" class="el-icon-ksd-arrow_down"></i>
                </span>
              </div>
            </template>
          </el-table-column>
          <el-table-column
            align="center"
            prop="isShared"
            label="Shard By"
            width="120">
            <template slot-scope="scope">
              <i class="el-icon-success" v-if="scope.row.isUsed" :class="{active: scope.row.isShared}"  @click="toggleShard(scope.row)"></i>
            </template>
          </el-table-column>
        </el-table>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-checkbox v-model="tableIndexMeta.load_data" :label="true" class="ksd-fleft ksd-mt-8">Load Data into new index after saving</el-checkbox>
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
    upRow (i) {
      let t = this.allColumns[i]
      this.allColumns.splice(i - 1, 0, t)
      this.allColumns.splice(i + 1, 1)
    }
    downRow (i) {
      let t = this.allColumns[i]
      this.allColumns.splice(i + 2, 0, t)
      this.allColumns.splice(i, 1)
    }
    tableRowClassName ({row}) {
      return row.colorful || row.isSorted ? 'row-colorful' : ''
    }
    getRowIndex (t, key) {
      return indexOfObjWithSomeKey(this.allColumns, key, t[key])
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
    toggleSort (t, index) {
      let i = index === undefined ? this.getRowIndex(t, 'fullName') : index
      if (!t.isSorted) {
        let sortedLen = filterObjectArray(this.allColumns, 'isSorted', true).length
        if (sortedLen >= 9) {
          kapMessage('最多只能加9列', {type: 'warning'})
          return
        }
        if (i !== 0) {
          this.allColumns.splice(i, 1, {})
          setTimeout(() => {
            this.allColumns.splice(i, 1)
            this.allColumns.unshift({})
          }, 300)
          setTimeout(() => {
            this.allColumns.shift()
            this.allColumns.unshift(t)
          }, 600)
        }
      } else {
        let s = indexOfObjWithSomeKey(this.allColumns, 'isSorted', false)
        if (s === -1) {
          s = this.allColumns.length
        }
        this.allColumns.splice(i, 1, {})
        setTimeout(() => {
          this.allColumns.splice(i, 1)
        }, 400)
        setTimeout(() => {
          this.allColumns.splice(s - 1, 0, {colorful: true})
        }, 600)
        setTimeout(() => {
          t.colorful = true
          this.allColumns.splice(s - 1, 1, t)
          setTimeout(() => {
            t.colorful = false
          }, 400)
        }, 800)
      }
      t.isSorted = !t.isSorted
    }
    toggleShard (t) {
      changeObjectArrProperty(this.allColumns, '*', 'isShared', false)
      t.isShared = !t.isShared
    }
    get searchAllColumns () {
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
          obj.isUsed = true
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
