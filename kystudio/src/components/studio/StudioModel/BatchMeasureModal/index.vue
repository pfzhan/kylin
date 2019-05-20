<template>
  <el-dialog class="batch-measure-modal" width="960px"
    :title="$t('batchMeasure')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    :limited-area="true"
    top="5vh"
    v-event-stop
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
      <div class="batch-des">{{$t('batchMeasureDes')}}</div>
      <div class="ksd-mb-10 ksd-right">
        <el-input :placeholder="$t('searchColumn')" style="width:230px;" @input="changeSearchVal" v-model="searchChar"></el-input>
      </div>
      <div v-if="!searchChar">
        <!-- 事实表 -->
        <div v-for="(table, index) in factTable" class="ksd-mb-10" :key="index">
          <div @click="toggleTableShow(table)" class="table-header">
            <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
            <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
            <span class="ksd-ml-2">
              <i class="el-icon-ksd-fact_table"></i>
            </span>
            <span class="table-title">{{table.alias}} <span>({{table.meaColNum}}/{{table.columns.length}})</span></span>
          </div>
          <el-table
            v-if="table.show || isGuideMode"
            border
            :data="table.columns"
            :ref="table.guid">
            <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, table, 'SUM')" :disabled="scope.row.SUM.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, table, 'MIN')" :disabled="scope.row.MIN.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, table, 'MAX')" :disabled="scope.row.MAX.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, table, 'COUNT')" :disabled="scope.row.COUNT.isExist"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <!-- 维度表 -->
        <div v-for="(table, index) in lookupTable" class="ksd-mb-10" :key="index">
          <div @click="toggleTableShow(table)" class="table-header">
            <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
            <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
            <span class="ksd-ml-2">
              <i class="el-icon-ksd-lookup_table"></i>
            </span>
            <span class="table-title">{{table.alias}} <span>({{table.meaColNum}}/{{table.columns.length}})</span></span>
          </div>
          <el-table
            v-if="table.show || isGuideMode"
            border
            :data="table.columns"
            :ref="table.guid">
            <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, table, 'SUM')" :disabled="scope.row.SUM.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, table, 'MIN')" :disabled="scope.row.MIN.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, table, 'MAX')" :disabled="scope.row.MAX.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, table)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, table, 'COUNT')" :disabled="scope.row.COUNT.isExist"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <!-- 可计算列 -->
        <template v-if="ccTable.columns.length">
          <div class="ksd-mb-10" v-for="ccTable in [ccTable]" :key="ccTable.guid">
            <div @click="toggleTableShow(ccTable)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!ccTable.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <span class="ksd-ml-2">
                <i class="el-icon-ksd-auto_computed_column"></i>
              </span>
              <span class="table-title">{{$t('computedColumns')}} <span>({{table.meaColNum}}/{{table.columns.length}})</span></span>
            </div>
            <el-table
              v-if="ccTable.show || isGuideMode"
              border
              :data="ccTable.columns"
              :ref="ccTable.guid">
              <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
              <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, ccTable, 'SUM')" :disabled="scope.row.SUM.isExist"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, ccTable, 'MIN')" :disabled="scope.row.MIN.isExist"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, ccTable, 'MAX')" :disabled="scope.row.MAX.isExist"></el-checkbox>
                </template>
              </el-table-column>
              <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, ccTable)}" align="center">
                <template slot-scope="scope">
                  <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, ccTable, 'COUNT')" :disabled="scope.row.COUNT.isExist"></el-checkbox>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </template>
      </div>
      <div v-else>
        <div v-for="searchTable in pagerSearchTable" :key="searchTable.guid">
          <el-table
            border
            v-if="isShowTest || isGuideMode"
            :data="searchTable.columns">
            <el-table-column show-overflow-tooltip prop="name" :label="$t('column')"></el-table-column>
            <el-table-column prop="SUM" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.SUM.value" @change="handleChange(scope.row, searchTable, 'SUM')" :disabled="scope.row.SUM.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MIN" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MIN.value" @change="handleChange(scope.row, searchTable, 'MIN')" :disabled="scope.row.MIN.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="MAX" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.MAX.value" @change="handleChange(scope.row, searchTable, 'MAX')" :disabled="scope.row.MAX.isExist"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column prop="COUNT" :renderHeader="(h, obj) => {return renderColumn(h, obj, searchTable)}" align="center">
              <template slot-scope="scope">
                <el-checkbox v-model="scope.row.COUNT.value" @change="handleChange(scope.row, searchTable, 'COUNT')" :disabled="scope.row.COUNT.isExist"></el-checkbox>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <kap-pager class="ksd-center ksd-mtb-10" ref="pager"  :totalSize="searchTotalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
      </div>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { objectClone, sampleGuid } from '../../../../util'
import { pageCount } from '../../../../config'
vuex.registerModule(['modals', 'BatchMeasureModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isGuideMode'
    ]),
    // Store数据注入
    ...mapState('BatchMeasureModal', {
      isShow: state => state.isShow,
      modelDesc: state => state.modelDesc,
      tables: state => objectClone(state.modelDesc && state.modelDesc.tables),
      usedColumns: state => state.modelDesc.all_measures,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('BatchMeasureModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class BatchMeasureModal extends Vue {
  isLoading = false
  isFormShow = false
  factTable = []
  lookupTable = []
  searchChar = ''
  expressions = ['SUM', 'MIN', 'MAX', 'COUNT']
  ST = null
  ccTable = {columns: []}
  filterArgs = {
    pageOffset: 0,
    pageSize: pageCount
  }
  isShowTest = false
  submit () {
    let allMeasureArr = []
    this.searchColumns.forEach((column) => {
      if (column.isMeasureCol) {
        if (column.SUM.value && !column.SUM.isExist) {
          const measure = {
            name: column.table_alias + '_' + column.name + '_SUM',
            expression: 'SUM',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + column.name}],
            table_guid: column.table_guid
          }
          allMeasureArr.push(measure)
        }
        if (column.MIN.value && !column.MIN.isExist) {
          const measure = {
            name: column.table_alias + '_' + column.name + '_MIN',
            expression: 'MIN',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + column.name}],
            table_guid: column.table_guid
          }
          allMeasureArr.push(measure)
        }
        if (column.MAX.value && !column.MAX.isExist) {
          const measure = {
            name: column.table_alias + '_' + column.name + '_MAX',
            expression: 'MAX',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + column.name}],
            table_guid: column.table_guid
          }
          allMeasureArr.push(measure)
        }
        if (column.COUNT.value && !column.COUNT.isExist) {
          const measure = {
            name: column.table_alias + '_' + column.name + '_COUNT',
            expression: 'COUNT',
            parameter_value: [{type: 'column', value: column.table_alias + '.' + column.name}],
            table_guid: column.table_guid
          }
          allMeasureArr.push(measure)
        }
      }
    })
    this.modelDesc.all_measures = this.modelDesc.all_measures.concat(allMeasureArr)
    this.handleClose(true)
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
  changeSearchVal (val) {
    this.isShowTest = false
    this.$nextTick(() => {
      this.isShowTest = true
      this.searchChar = val && val.replace(/^\s+|\s+$/, '') || ''
    })
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
      this.getRenderMeasureData()
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }
  // 分页搜索table渲染数据
  get pagerSearchTable () {
    return [{
      guid: this.filterTableGuid,
      columns: this.pagerSearchMeasureList,
      show: true,
      nums: {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
    }]
  }
  filterMeasureColumns (table) {
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
  // 全量获取搜索columns
  get searchColumns () {
    let columns = []
    this.factTable.forEach((t) => {
      columns.push(...this.filterMeasureColumns(t))
    })
    this.lookupTable.forEach((t) => {
      columns.push(...this.filterMeasureColumns(t))
    })
    columns.push(...this.filterMeasureColumns(this.ccTable))
    return columns
  }
  // 分页获取搜索columns
  get pagerSearchMeasureList () {
    return this.searchColumns.slice(this.filterArgs.pageOffset * this.filterArgs.pageSize, (this.filterArgs.pageOffset + 1) * this.filterArgs.pageSize)
  }
  pageCurrentChange (size, count) {
    this.filterArgs.pageOffset = size
    this.filterArgs.pageSize = count
  }
  // 总搜索条数
  get searchTotalSize () {
    return this.searchColumns.length
  }
  // 渲染之前选过的可计算列measure
  getRenderCCData () {
    this.ccTable = {}
    this.$set(this.ccTable, 'show', false)
    this.$set(this.ccTable, 'guid', sampleGuid())
    this.ccTable.columns = objectClone(this.modelDesc.computed_columns) || []
    let meaColNum = 0
    const nums = {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
    this.ccTable.columns.forEach((col) => {
      this.$set(col, 'SUM', {isExist: false, value: false})
      this.$set(col, 'MIN', {isExist: false, value: false})
      this.$set(col, 'MAX', {isExist: false, value: false})
      this.$set(col, 'COUNT', {isExist: false, value: false})
      this.$set(col, 'isMeasureCol', false)
      this.$set(col, 'table_guid', this.factTable[0].guid)
      this.$set(col, 'table_alias', this.factTable[0].alias)
      const len = this.usedColumns.length
      for (let i = 0; i < len; i++) {
        let d = this.usedColumns[i]
        if (this.expressions.indexOf(d.expression) !== -1 && d.parameter_value[0].value === col.name) {
          col[d.expression].value = true
          col[d.expression].isExist = true
          this.$set(col, 'isMeasureCol', true)
          nums[d.expression.toLowerCase() + 'Num']++
        }
        if (col.isMeasureCol) {
          meaColNum++
        }
      }
    })
    this.$set(this.ccTable, 'nums', nums)
    this.$set(this.ccTable, 'meaColNum', meaColNum)
  }
  // 获取所有的table columns，并渲染已经选择过的measure
  getRenderMeasureData () {
    this.factTable = []
    this.lookupTable = []
    Object.values(this.tables).forEach((table) => {
      if (table.kind === 'FACT') {
        this.factTable.push(table)
      } else {
        this.lookupTable.push(table)
      }
      this.$set(table, 'show', false)
      let meaColNum = 0
      const nums = {sumNum: 0, minNum: 0, maxNum: 0, countNum: 0}
      // 将已经选上的measure回显到界面上
      table.columns && table.columns.forEach((col) => {
        this.$set(col, 'SUM', {isExist: false, value: false})
        this.$set(col, 'MIN', {isExist: false, value: false})
        this.$set(col, 'MAX', {isExist: false, value: false})
        this.$set(col, 'COUNT', {isExist: false, value: false})
        this.$set(col, 'isMeasureCol', false)
        this.$set(col, 'table_guid', table.guid)
        this.$set(col, 'table_alias', table.alias)
        const len = this.usedColumns.length
        for (let i = 0; i < len; i++) {
          let d = this.usedColumns[i]
          if (this.expressions.indexOf(d.expression) !== -1 && d.parameter_value[0].value === table.alias + '.' + col.name) {
            col[d.expression].value = true
            col[d.expression].isExist = true
            this.$set(col, 'isMeasureCol', true)
            nums[d.expression.toLowerCase() + 'Num']++
          }
        }
        if (col.isMeasureCol) {
          meaColNum++
        }
      })
      this.$set(table, 'nums', nums)
      this.$set(table, 'meaColNum', meaColNum)
    })
    this.getRenderCCData()
  }
  toggleTableShow (table) {
    table.show = !table.show
  }
  handleChange (row, table, property) {
    if (!(row.SUM.isExist && row.MIN.isExist && row.MAX.isExist && row.COUNT.isExist)) {
      if (row[property].value) {
        const cloneRow = objectClone(row)
        cloneRow[property].value = !cloneRow[property].value
        // debugger
        if (!(cloneRow.SUM.value || cloneRow.MIN.value || cloneRow.MAX.value || cloneRow.COUNT.value)) {
          table.meaColNum++
          row.isMeasureCol = true
        }
      } else {
        if (!(row.SUM.value || row.MIN.value || row.MAX.value || row.COUNT.value)) {
          table.meaColNum--
          row.isMeasureCol = false
        }
      }
    }
  }
  renderColumn (h, { column, store }, table) {
    table = table || {}
    let totalNums = 0
    const len = store.states.data.length
    for (let i = 0; i < len; i++) {
      let d = store.states.data[i]
      if (d[column.property].value) {
        totalNums++
      }
    }
    this.$set(column, 'totalNums', totalNums)
    const toggleAllMeasures = (val) => {
      table.columns.forEach((d) => {
        if (val) {
          d[column.property] = {value: val, isExist: d[column.property].isExist}
          !d[column.property].isExist && table.nums[column.property.toLowerCase() + 'Num']++
          d.isMeasureCol = true
        } else {
          if (!d[column.property].isExist) {
            d[column.property] = {value: val, isExist: d[column.property].isExist}
            table.nums[column.property.toLowerCase() + 'Num']--
          }
          if (!(d.SUM.value || d.MIN.value || d.MAX.value || d.COUNT.value)) {
            d.isMeasureCol = false
          }
        }
      })
      this.$set(column, 'isAllSelected', !column.isAllSelected)
      const numArr = table.nums && Object.values(table.nums)
      this.$set(table, 'meaColNum', Math.max.apply(null, numArr))
    }
    return (<span>
      <el-checkbox
        disabled={ store.states.data && store.states.data.length === 0 }
        indeterminate={ totalNums > 0 && len > totalNums}
        onChange={ toggleAllMeasures }
        value={ column.isAllSelected }></el-checkbox> <span>{ column.property }({column.totalNums}/{len})</span>
    </span>)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
  .batch-measure-modal {
    cursor:default;
    .batch-des {
      color: @text-title-color;
      font-size: 14px;
    }
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
      height:40px;
      line-height:40px;
      cursor:pointer;
      .right-icon{
        margin-right:20px;
      }
    }
  }
</style>
