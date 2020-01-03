<template>
  <el-dialog class="dimension-modal" width="1000px"
    :title="$t('editDimension') + ' (' + allColumnsCount(true) + '/' + allColumnsCount() + ')'"
    :visible="isShow"
    top="5vh"
    limited-area
    append-to-body
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    v-event-stop
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
      <div class="ksd-mb-10 ksd-right">
        <el-input :placeholder="$t('searchColumn')" style="width:230px;" @input="changeSearchVal" v-model="searchChar">
          <i slot="prefix" class="el-input__icon el-icon-search"></i>
        </el-input>
      </div>
      <div v-scroll.reactive v-guide.dimensionScroll style="max-height:60vh; overflow:hidden">
        <div class="add_dimensions" v-guide.batchAddDimensionBox>
          <div v-show="!searchChar">
            <!-- 事实表 -->
            <div v-for="(table, index) in factTable" class="ksd-mb-10" :key="index">
              <div :class="{'error-content-tip': filterErrorContent(table)}">
                <div @click="toggleTableShow(table)" class="table-header">
                  <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
                  <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
                  <el-checkbox v-model="table.checkedAll" :indeterminate="table.isIndeterminate" @click.native.stop  @change="(isAll) => {selectAllChange(isAll, table.guid)}"></el-checkbox>
                  <span class="ksd-ml-2">
                    <i class="el-icon-ksd-fact_table"></i>
                  </span>
                  <span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}}/{{table.columns.length}})</span></span>
                </div>
                <el-table
                  v-if="table.show || isGuideMode"
                  border
                  :data="table.columns"
                  @row-click="(row) => {rowClick(row, table.guid)}"
                  :ref="table.guid"
                  :row-class-name="(para) => tableRowClassName(para, table)"
                  @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
                  @select="(selection, row) => {selectionChange(selection, row, table.guid)}">
                  <el-table-column
                    type="selection"
                    width="44">
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
              <div class="same-name-tip" v-if="filterErrorContent(table)">{{$t('sameNameTip')}}</div>
            </div>
            <!-- 维度表 -->
            <div v-for="(table, index) in lookupTable" class="ksd-mb-10" :key="index">
              <div :class="{'error-content-tip': filterErrorContent(table)}">
                <div @click="toggleTableShow(table)" class="table-header">
                  <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
                  <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
                  <el-checkbox v-model="table.checkedAll" :indeterminate="table.isIndeterminate" @click.native.stop  @change="(isAll) => {selectAllChange(isAll, table.guid)}"></el-checkbox>
                  <span class="ksd-ml-2">
                    <i class="el-icon-ksd-lookup_table"></i>
                  </span>
                  <span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}}/{{table.columns.length}})</span></span>
                </div>
                <el-table
                  v-if="table.show || isGuideMode"
                  border
                  :row-class-name="(para) => tableRowClassName(para, table)"
                  :data="table.columns" :ref="table.guid"
                  @row-click="(row) => {rowClick(row, table.guid)}"
                  @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
                  @select="(selection, row) => {selectionChange(selection, row, table.guid)}">
                  <el-table-column
                    type="selection"
                    align="center"
                    width="44">
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
              <div class="same-name-tip" v-if="filterErrorContent(table)">{{$t('sameNameTip')}}</div>
            </div>
            <!-- 可计算列 -->
            <template v-if="ccTable.columns.length">
              <div class="ksd-mb-10" v-for="ccTable in [ccTable]" :key="ccTable.guid">
                <div :class="{'error-content-tip': filterErrorContent(ccTable)}">
                  <div @click="toggleTableShow(ccTable)" class="table-header">
                    <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!ccTable.show"></i>
                    <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
                    <el-checkbox v-model="ccTable.checkedAll" :indeterminate="ccTable.isIndeterminate" @click.native.stop  @change="(isAll) => {selectAllChange(isAll, ccTable.guid)}"></el-checkbox>
                    <span class="ksd-ml-2">
                      <i class="el-icon-ksd-auto_computed_column"></i>
                    </span>
                    <span class="table-title">{{$t('computedColumns')}} <span>({{countTableSelectColumns(ccTable)}}/{{ccTable.columns.length}})</span></span>
                  </div>
                  <el-table
                    v-if="ccTable.show || isGuideMode"
                    border
                    :row-class-name="(para) => tableRowClassName(para, ccTable)"
                    :data="ccTable.columns" :ref="ccTable.guid"
                    @row-click="(row) => {rowClick(row, ccTable.guid)}"
                    @select-all="(selection) => {selectionAllChange(selection, ccTable.guid)}"
                    @select="(selection, row) => {selectionChange(selection, row, ccTable.guid)}">
                    <el-table-column
                      type="selection"
                      align="center"
                      width="44">
                    </el-table-column>
                    <el-table-column
                      prop="alias"
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
                      prop="column"
                      :label="$t('column')">
                    </el-table-column>
                    <el-table-column
                      show-overflow-tooltip
                      prop="expression"
                      :label="$t('expression')">
                    </el-table-column>
                    <el-table-column
                      show-overflow-tooltip
                      :label="$t('datatype')"
                      prop="datatype"
                      width="110">
                    </el-table-column>
                  </el-table>
                </div>
                <div class="same-name-tip" v-if="filterErrorContent(ccTable)">{{$t('sameNameTip')}}</div>
              </div>
            </template>
          </div>
          <div v-show="searchChar">
            <el-table v-for="searchTable in pagerSearchTable" :key="searchTable.guid"
                  border
                  :empty-text="emptyText"
                  :row-class-name="(para) => tableRowClassName(para, searchTable)"
                  :data="searchTable.columns" :ref="searchTable.guid"
                  @row-click="(row) => {rowClick(row, searchTable.guid)}"
                  @select-all="(selection) => {selectAllCurrentPager(selection, searchTable.guid)}"
                  @select="(selection, row) => {selectionChange(selection, row, searchTable.guid)}">
                  <el-table-column
                    type="selection"
                    align="center"
                    width="44">
                  </el-table-column>
                  <el-table-column
                    prop="alias"
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
                    prop="column"
                    :label="$t('column')">
                    <template slot-scope="scope">{{scope.row.name || scope.row.column}}</template>
                  </el-table-column>
                  <el-table-column
                    show-overflow-tooltip
                    prop="expression"
                    :label="$t('expression')">
                  </el-table-column>
                  <el-table-column
                    show-overflow-tooltip
                    :label="$t('datatype')"
                    prop="datatype"
                    width="110">
                  </el-table-column>
                </el-table>
                <kap-pager class="ksd-center ksd-mtb-10" ref="pager" :curPage="filterArgs.pageOffset+1" :totalSize="searchTotalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
          </div>
        </div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" v-guide.saveBatchDimensionBtn :disabled="allColumnsCount(true) <= 0" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
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
import { NamedRegex, pageCount } from '../../../../config'
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
  searchChar = ''
  ST = null
  ccTable = {columns: []}
  filterArgs = {
    pageOffset: 0,
    pageSize: pageCount
  }
  isClickSubmit = false
  errorGuidList = []

  filterErrorContent (table) {
    return this.isClickSubmit && table.columns.filter(item => item.validateSameName || item.validateNameRule).length
  }

  get emptyText () {
    return this.searchChar ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  changeSearchVal (val) {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.searchChar = val && val.replace(/^\s+|\s+$/, '') || ''
      this.filterArgs.pageOffset = 0
      this.$nextTick(() => {
        if (this.searchChar) {
          this.renderTableColumnSelected(this.pagerSearchTable[0])
        } else {
          [...this.factTable, ...this.lookupTable, this.ccTable].forEach((t) => {
            this.renderTableColumnSelected(t)
          })
        }
      })
    }, 100)
  }
  filterDimensionColumns (table) {
    let columns
    if (this.searchChar) {
      let searchReg = new RegExp(this.searchChar, 'i')
      columns = table.columns && table.columns.filter((col) => {
        return searchReg.test(col.name) || searchReg.test(col.column) // column for cc list search
      })
    } else {
      columns = table.columns
    }
    return columns
  }
  pageCurrentChange (size, count) {
    this.filterArgs.pageOffset = size
    this.filterArgs.pageSize = count
    this.$nextTick(() => {
      this.renderTableColumnSelected(this.pagerSearchTable[0])
    })
  }
  filterTableGuid = sampleGuid()
  // 全量获取搜索columns
  get searchColumns () {
    let columns = []
    this.factTable.forEach((t) => {
      columns.push(...this.filterDimensionColumns(t))
    })
    this.lookupTable.forEach((t) => {
      columns.push(...this.filterDimensionColumns(t))
    })
    columns.push(...this.filterDimensionColumns(this.ccTable))
    return columns
  }
  // 分页获取搜索columns
  get pagerSearchDimensionList () {
    return this.searchColumns.slice(this.filterArgs.pageOffset * this.filterArgs.pageSize, (this.filterArgs.pageOffset + 1) * this.filterArgs.pageSize)
  }
  // 分页搜索table渲染数据
  get pagerSearchTable () {
    return [{
      guid: this.filterTableGuid,
      columns: this.pagerSearchDimensionList,
      show: true
    }]
  }
  // 总搜索条数
  get searchTotalSize () {
    return this.searchColumns.length
  }
  // 渲染之前选过的可计算列dimension
  getRenderCCData () {
    this.ccTable = {}
    this.$set(this.ccTable, 'show', false)
    this.$set(this.ccTable, 'checkedAll', false)
    this.$set(this.ccTable, 'isIndeterminate', false)
    this.ccTable.columns = objectClone(this.modelDesc.computed_columns) || []
    this.$set(this.ccTable, 'guid', sampleGuid())
    this.ccTable.columns.forEach((col) => {
      let len = this.usedColumns.length
      this.$set(col, 'column', col.columnName)
      this.$set(col, 'alias', col.tableAlias + '_' + col.columnName)
      this.$set(col, 'isSelected', false)
      this.$set(col, 'guid', null)
      for (let i = 0; i < len; i++) {
        let d = this.usedColumns[i]
        if (col.tableAlias + '.' + col.columnName === d.column && d.status === 'DIMENSION') {
          col.isSelected = true
          col.alias = d.name
          col.guid = d.guid
          break
        }
      }
    })
    this.renderTableColumnSelected(this.ccTable)
  }
  // 获取所有的table columns，并渲染已经选择过的dimension
  getRenderDimensionData () {
    this.getRenderCCData()
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
        this.$set(col, 'isSelected', false)
        this.$set(col, 'guid', null)
        let len = this.usedColumns.length
        for (let i = 0; i < len; i++) {
          let d = this.usedColumns[i]
          if (table.alias + '.' + col.name === d.column && d.status === 'DIMENSION') {
            col.alias = d.name
            col.isSelected = true
            col.guid = d.guid
            break
          }
        }
      })
      this.renderTableColumnSelected(table)
    })
  }
  // 检测是否有重名
  checkHasSameNamedColumn () {
    let columns = []
    for (let k = 0; k < this.factTable.length; k++) {
      columns = columns.concat(this.factTable[k].columns)
    }
    let loopupTableLen = this.lookupTable.length
    for (let k = 0; k < loopupTableLen; k++) {
      columns = columns.concat(this.lookupTable[k].columns)
    }
    if (this.ccTable.columns) {
      columns = columns.concat(this.ccTable.columns)
    }
    return () => {
      let hasPassValidate = true
      this.errorGuidList = []
      columns.forEach((col) => {
        this.$set(col, 'validateNameRule', false)
        this.$set(col, 'validateSameName', false)
        this.isClickSubmit = false
        if (countObjWithSomeKey(columns, 'alias', col.alias) > 1) {
          hasPassValidate = false
          this.$set(col, 'validateSameName', true)
          this.errorGuidList.push(col.guid || col.table_guid)
        } else if (!this.checkDimensionNameRegex(col.alias)) {
          hasPassValidate = false
          this.$set(col, 'validateNameRule', true)
          this.errorGuidList.push(col.guid || col.table_guid)
        }
      })
      return hasPassValidate
    }
  }
  // 检测name是否符合规范
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
      this.searchChar = ''
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
  getCurrentTable (guid) {
    let table = this.ccTable.guid === guid ? this.ccTable : this.tables[guid]
    return table
  }
  // 获取header上checkbox的选中状态
  getTableCheckedStatus (guid) {
    let table = this.getCurrentTable(guid)
    if (table) {
      let hasCheckedCount = 0
      table.columns && table.columns.forEach((col) => {
        if (col.isSelected) {
          hasCheckedCount++
        }
      })
      this.$set(table, 'checkedAll', hasCheckedCount === table.columns.length)
      this.$set(table, 'isIndeterminate', hasCheckedCount > 0 && hasCheckedCount < table.columns.length)
    }
  }
  // 点击行checkbox
  selectionChange (selection, row, guid) {
    row.isSelected = !row.isSelected
    this.getTableCheckedStatus(guid)
  }
  // 点击行触发事件
  rowClick (row, guid) {
    row.isSelected = !row.isSelected
    this.getTableCheckedStatus(guid)
    this.$refs[guid][0].toggleRowSelection(row)
  }
  // 点击header上checkbox触发选择
  selectAllChange (val, guid) {
    let table = this.getCurrentTable(guid)
    let columns = table.columns
    columns.forEach((row) => {
      this.$set(row, 'isSelected', val)
    })
    this.renderTableColumnSelected(table)
  }
  // 点击table 全选
  selectionAllChange (selection, guid) {
    let table = this.getCurrentTable(guid)
    let columns = table.columns
    columns.forEach((row) => {
      this.$set(row, 'isSelected', !!selection.length)
    })
    this.getTableCheckedStatus(guid)
  }
  // 点击搜索表格全选
  selectAllCurrentPager (selection, guid) {
    let columns = this.pagerSearchDimensionList
    columns.forEach((row) => {
      row.isSelected = !!selection.length
    })
    this.renderTableColumnSelected({
      columns: columns,
      guid: this.filterTableGuid
    })
  }
  toggleTableShow (table) {
    table.show = !table.show
    this.renderTableColumnSelected(table)
  }
  // 单个表渲染已选择的行
  renderTableColumnSelected (table) {
    this.$nextTick(() => {
      if (this.$refs[table.guid] && this.$refs[table.guid][0] && table.show) {
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
  // 获取所有选中的column对象，并拼接成存储格式
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
    this.ccTable.columns.forEach((col) => {
      if (col.isSelected) {
        result.push({
          guid: col.guid || sampleGuid(),
          name: col.alias,
          table_guid: col.guid,
          column: col.tableAlias + '.' + col.columnName,
          status: 'DIMENSION',
          datatype: col.datatype
        })
      }
    })
    return result
  }
  // 统计总列数 或者 选中的总列数  isChecked：是否选中
  get allColumnsCount () {
    return (isChecked) => {
      let allLen = 0
      this.tables && Object.values(this.tables).forEach((table) => {
        table.columns && table.columns.forEach((col) => {
          if (!isChecked || isChecked && col.isSelected) {
            allLen++
          }
        })
      })
      this.ccTable.columns && this.ccTable.columns.forEach((col) => {
        if (!isChecked || isChecked && col.isSelected) {
          allLen++
        }
      })
      return allLen
    }
  }
  submit () {
    this.errorGuidList.length && this.$message.error(this.$t('sameNameTip'))
    this.checkDimensionForm()
    this.isClickSubmit = true
    if (this.dimensionValidPass) {
      let result = this.getAllSelectedColumns()
      this.modelDesc.dimensions = [...result]
      this.handleClose(true)
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.dimension-modal{
  .table-title {
    font-size: 14px;
    margin-left: 5px;
    font-weight: @font-medium;
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
    height:40px;
    line-height:40px;
    cursor:pointer;
    .right-icon{
      margin-right:20px;
    }
  }
  .same-name-tip {
    color: @error-color-1;
    font-size: 12px;
  }
  .error-content-tip {
    border: 1px solid @error-color-1;
  }
}
</style>
