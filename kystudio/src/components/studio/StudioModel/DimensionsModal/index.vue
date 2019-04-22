<template>
  <el-dialog class="dimension-modal" width="1000px"
    :title="$t('editDimension') + ' (' + allColumnsLen(true) + '/' + allColumnsLen() + ')'"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    v-event-stop
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
      <div v-scroll v-guide.dimensionScroll style="max-height:60vh; overflow:hidden">
        <div class="add_dimensions" v-guide.batchAddDimensionBox>
          <div v-for="(table, index) in factTable" class="ksd-mb-10" :key="index">
            <div @click="toggleTableShow(table)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <el-checkbox v-model="table.checkedAll" :indeterminate="table.isIndeterminate" @click.native.stop  @change="(isAll) => {selectAllChange(isAll, table.guid)}"></el-checkbox>
              <span class="ksd-ml-2">
                 <i class="el-icon-ksd-fact_table"></i>
              </span>
              <span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}})</span></span>
            </div>
            <el-table
              v-if="table.show"
              border
              :data="table.columns"
              @row-click="(row) => {dimensionRowClick(row, table.guid)}"
              :ref="table.guid"
              :row-class-name="(para) => tableRowClassName(para, table)"
              @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
              @select="(selection, row) => {selectionChange(selection, row, table.guid)}">
              <el-table-column
                type="selection"
                width="40">
              </el-table-column>
              <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <div @click.stop>
                    <el-input size="small" v-model="scope.row.alias"   @change="checkDimensionForm" :disabled="!scope.row.isSelected">
                    </el-input>
                    <div v-if="scope.row.validateNameRule" class="ky-form-error">{{$t('kylinLang.common.nameFormatValidTip')}}</div>
                    <div v-else-if="scope.row.validateSameName" class="ky-form-error">{{$t('kylinLang.common.sameName')}}</div>
                  </div>
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                prop="name"
                :label="$t('column')">
              </el-table-column>
              <el-table-column
                prop="datatype"
                show-overflow-tooltip
                :label="$t('datatype')"
                width="110">
              </el-table-column>
              <el-table-column
                header-align="right"
                align="right"
                show-overflow-tooltip
                prop="cardinality"
                :label="$t('cardinality')"
                width="100">
              </el-table-column>
              <el-table-column
                prop="comment"
                :label="$t('comment')">
              </el-table-column>
            </el-table>
          </div>

          <div v-for="(table, index) in lookupTable" class="ksd-mb-10" :key="index">
            <div @click="toggleTableShow(table)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <el-checkbox v-model="table.checkedAll" :indeterminate="table.isIndeterminate" @click.native.stop  @change="(isAll) => {selectAllChange(isAll, table.guid)}"></el-checkbox>
              <span class="ksd-ml-2">
                <i class="el-icon-ksd-lookup_table"></i>
              </span>
              <span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}})</span></span>
            </div>
            <el-table
              v-if="table.show || isGuideMode"
              border
              :row-class-name="(para) => tableRowClassName(para, table)"
              :data="table.columns" :ref="table.guid"
              @row-click="(row) => {dimensionRowClick(row, table.guid)}"
              @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
              @select="(selection, row) => {selectionChange(selection, row, table.guid)}">
              <el-table-column
                type="selection"
                align="center"
                width="40">
              </el-table-column>
               <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <div @click.stop>
                    <el-input size="small" v-model="scope.row.alias"   @change="checkDimensionForm" :disabled="!scope.row.isSelected">
                    </el-input>
                    <div v-if="scope.row.validateNameRule" class="ky-form-error">{{$t('kylinLang.common.nameFormatValidTip')}}</div>
                    <div v-else-if="scope.row.validateSameName" class="ky-form-error">{{$t('kylinLang.common.sameName')}}</div>
                  </div>
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                prop="name"
                :label="$t('column')">
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                :label="$t('datatype')"
                prop="datatype"
                width="110">
              </el-table-column>
              <el-table-column
                prop="cardinality"
                show-overflow-tooltip
                :label="$t('cardinality')"
                width="100">
              </el-table-column>
               <el-table-column
              :label="$t('comment')">
              </el-table-column>
            </el-table>
          </div>
        </div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <span class="ksd-fleft ksd-mt-10">{{$t('totalSelect')}}{{countAllTableSelectColumns()}}</span>
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" v-guide.saveBatchDimensionBtn :disabled="countAllTableSelectColumns() <= 0" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { NamedRegex } from '../../../../config'
import { objectClone, sampleGuid, filterObjectArray, countObjWithSomeKey } from '../../../../util'
vuex.registerModule(['modals', 'DimensionsModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isGuideMode'
    ]),
    // Store数据注入
    ...mapState('DimensionsModal', {
      isShow: state => state.isShow,
      tables: state => objectClone(state.modelDesc && state.modelDesc.tables),
      modelDesc: state => state.modelDesc,
      usedColumns: state => state.modelDesc.dimensions,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('DimensionsModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA'
    }),
    tableRowClassName ({row, rowIndex}, table) {
      return 'guide-' + table.alias + row.name
    }
  },
  locales
})
export default class DimensionsModal extends Vue {
  isLoading = false
  isFormShow = false
  factTable = []
  lookupTable = []
  // 获取所有的table columns
  getRenderDimensionData () {
    this.factTable = []
    this.lookupTable = []
    Object.values(this.tables).forEach((table) => {
      if (table.kind === 'FACT') {
        this.factTable.push(table)
      } else {
        this.lookupTable.push(table)
      }
      this.$set(table, 'show', false)
      this.$set(table, 'checkedAll', false)
      this.$set(table, 'isIndeterminate', false)
      // 将已经选上的dimension回显到界面上
      table.columns && table.columns.forEach((col) => {
        this.$set(col, 'alias', table.alias + '_' + col.name)
        let len = this.usedColumns.length
        for (let i = 0; i < len; i++) {
          let d = this.usedColumns[i]
          if (table.alias + '.' + col.name === d.column && d.status === 'DIMENSION') {
            col.alias = d.name
            this.$set(col, 'isSelected', true)
            col.guid = d.guid
            break
          } else {
            this.$set(col, 'isSelected', false)
            col.guid = null
          }
        }
      })
      this.renderTableColumnSelected(table)
    })
  }
  checkHasSameNamedColumn () {
    let columns = []
    for (let k = 0; k < this.factTable.length; k++) {
      columns = columns.concat(this.factTable[k].columns)
    }
    let loopupTableLen = this.lookupTable.length
    for (let k = 0; k < loopupTableLen; k++) {
      columns = columns.concat(this.lookupTable[k].columns)
    }
    return () => {
      let hasPassValidate = true
      columns.forEach((col) => {
        this.$set(col, 'validateNameRule', false)
        this.$set(col, 'validateSameName', false)
        if (countObjWithSomeKey(columns, 'alias', col.alias) > 1) {
          hasPassValidate = false
          this.$set(col, 'validateSameName', true)
        } else if (!this.checkDimensionNameRegex(col.alias)) {
          hasPassValidate = false
          this.$set(col, 'validateNameRule', true)
        }
      })
      return hasPassValidate
    }
  }
  checkDimensionNameRegex (alias) {
    if (!NamedRegex.test(alias)) {
      return false
    }
    return true
  }
  columnsCheckFunc = null
  dimensionValidPass = false // 判断表单校验是否通过
  checkDimensionForm () {
    if (!this.columnsCheckFunc) {
      this.columnsCheckFunc = this.checkHasSameNamedColumn()
    }
    this.dimensionValidPass = this.columnsCheckFunc()
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      this.getRenderDimensionData()
      this.columnsCheckFunc = this.checkHasSameNamedColumn()
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }
  mounted () {
  }
  handleClose (isSubmit, data) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: data
      })
    }, 300)
  }
  // 获取header上checkbox的选中状态
  getTableCheckedStatus (guid) {
    let table = this.tables[guid]
    let hasCheckedCount = 0
    table.columns && table.columns.forEach((col) => {
      if (col.isSelected) {
        hasCheckedCount++
      }
    })
    this.$set(table, 'checkedAll', hasCheckedCount === table.columns.length)
    this.$set(table, 'isIndeterminate', hasCheckedCount > 0 && hasCheckedCount < table.columns.length)
  }
  // 点击行触发事件
  selectionChange (selection, row, guid) {
    this.$set(row, 'isSelected', !row.isSelected)
    this.getTableCheckedStatus(guid)
  }
  // 点击header上checkbox触发选择
  selectAllChange (val, guid) {
    let columns = this.tables[guid].columns
    columns.forEach((row) => {
      this.$set(row, 'isSelected', val)
    })
    this.renderTableColumnSelected(this.tables[guid])
  }
  selectionAllChange (selection, guid) {
    let columns = this.tables[guid].columns
    columns.forEach((row) => {
      this.$set(row, 'isSelected', !!selection.length)
    })
    this.getTableCheckedStatus(guid)
  }
  dimensionRowClick (row, guid) {
    this.$refs[guid][0].toggleRowSelection(row)
    this.$set(row, 'isSelected', !row.isSelected)
  }
  toggleTableShow (table) {
    table.show = !table.show
    this.renderTableColumnSelected(table)
  }
  renderTableColumnSelected (table) {
    this.$nextTick(() => {
      if (this.$refs[table.guid] && this.$refs[table.guid][0]) {
        table.columns && table.columns.forEach((col) => {
          this.$refs[table.guid][0].toggleRowSelection(col, !!col.isSelected)
        })
      }
      this.getTableCheckedStatus(table.guid)
    })
  }
  get countTableSelectColumns () {
    return (table) => {
      if (!table) {
        return
      }
      return filterObjectArray(table.columns, 'isSelected', true).length
    }
  }
  get countAllTableSelectColumns () {
    return () => {
      if (!this.tables) {
        return
      }
      let i = 0
      Object.values(this.tables).forEach((table) => {
        table.columns && table.columns.forEach((col) => {
          if (col.isSelected) {
            i++
          }
        })
      })
      return i
    }
  }
  getAllSelectedColumns () {
    let result = []
    Object.values(this.tables).forEach((table) => {
      table.columns && table.columns.forEach((col) => {
        if (col.isSelected) {
          result.push({
            guid: col.guid || sampleGuid(),
            name: col.alias,
            table_guid: table.guid,
            column: table.alias + '.' + col.name,
            status: 'DIMENSION',
            datatype: col.datatype
          })
        }
      })
    })
    return result
  }
  get allColumnsLen () {
    return (isChecked) => {
      let allLen = 0
      this.tables && Object.values(this.tables).forEach((table) => {
        table.columns && table.columns.forEach((col) => {
          if (!isChecked || isChecked && col.isSelected) {
            allLen++
          }
        })
      })
      return allLen
    }
  }
  submit () {
    this.checkDimensionForm()
    if (this.dimensionValidPass) {
      let result = this.getAllSelectedColumns()
      let ccDimensionList = this.usedColumns.filter((x) => {
        return x.isCC
      })
      this.modelDesc.dimensions.splice(0, this.modelDesc.dimensions.length)
      this.modelDesc.dimensions.push(...result, ...ccDimensionList)
      this.handleClose(true)
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.dimension-modal{
  cursor:default;
  .table-title {
    font-size: 14px;
    margin-left: 5px;
    font-weight:@font-medium;
  }
  .table-header {
    padding-left:15px;
    background-color: @grey-4;
    &:hover {
      color: @base-color;
      .right-icon {
        color:@base-color-2!important;
        cursor: pointer;
      }
    }
    // border-bottom:solid 1px @line-border-color;
    height:40px;
    line-height:40px;
    cursor:pointer;
    .right-icon{
      margin-right:20px;
    }
  }
}
</style>
