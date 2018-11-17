<template>
  <el-dialog class="dimension-modal" width="1000px"
    :title="$t('editDimension')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    v-event-stop
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
        <div class="add_dimensions">
          <div v-for="(table, index) in factTable" :key="index">
            <div @click="toggleTableShow(table)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <span class="ksd-ml-2"><i class="el-icon-ksd-fact_table"></i></span><span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}})</span> </span>
            </div>
            <el-table
              v-if="table.show"
              class="ksd-mt-10"
              border
              :data="table.columns"
              @row-click="(row) => {dimensionRowClick(row, table.guid)}"
              :ref="table.guid"
              @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
              @select="selectionChange">
              <el-table-column
                type="selection"
                width="55">
              </el-table-column>
              <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <el-input size="small" @click.native.stop v-model="scope.row.alias" :disabled="!scope.row.isSelected">
                  </el-input>
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
                show-overflow-tooltip
                prop="cardinality"
                :label="$t('cardinality')"
                width="100">
                <template slot-scope="scope">
                </template>
              </el-table-column>
              <el-table-column
              prop="comment"
              :label="$t('Comment')">
              </el-table-column>
            </el-table>
          </div>

          <div v-for="(table, index) in lookupTable" :key="index">
            <div @click="toggleTableShow(table)" class="table-header">
              <i class="el-icon-arrow-right ksd-fright ksd-mt-14 right-icon" v-if="!table.show"></i>
              <i class="el-icon-arrow-down  ksd-fright ksd-mt-14 right-icon" v-else></i>
              <span class="ksd-ml-2"><i class="el-icon-ksd-lookup_table"></i></span><span class="table-title">{{table.alias}} <span>({{countTableSelectColumns(table)}})</span></span>
            </div>
            <el-table
              v-if="table.show"
              class="ksd-mt-10"
              border
              :data="table.columns" :ref="table.guid"
              @row-click="(row) => {dimensionRowClick(row, table.guid)}"
              @select-all="(selection) => {selectionAllChange(selection, table.guid)}"
              @select="selectionChange">
              <el-table-column
                type="selection"
                width="55">
              </el-table-column>
               <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <el-input size="small" v-model="scope.row.alias" @click.native.stop :disabled="!scope.row.isSelected" :placeholder="scope.row.name"></el-input>
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
                show-overflow-tooltip
                :label="$t('cardinality')"
                width="100">
                <template slot-scope="scope">
                </template>
              </el-table-column>
               <el-table-column
              :label="$t('Comment')">
              </el-table-column>
            </el-table>
          </div>
        </div>
    </template>
    <div slot="footer" class="dialog-footer">
      <span class="ksd-fleft ksd-mt-10">{{$t('totalSelect')}}{{countAllTableSelectColumns()}}</span>
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit"  :disabled="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
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
// import { sourceTypes } from '../../../../config'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
// import { objectClone } from 'util'
import { objectClone, sampleGuid, filterObjectArray } from '../../../../util'
vuex.registerModule(['modals', 'DimensionsModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('DimensionsModal', {
      isShow: state => state.isShow,
      tables: state => objectClone(state.modelDesc.tables),
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
    })
  },
  locales
})
export default class DimensionsModal extends Vue {
  isLoading = false
  isFormShow = false
  factTable = []
  lookupTable = []
  // 获取所有的table columns
  getTableColumns () {
    this.factTable = []
    this.lookupTable = []
    Object.values(this.tables).forEach((table) => {
      if (table.kind === 'FACT') {
        this.$set(table, 'show', true)
        this.factTable.push(table)
      } else {
        this.$set(table, 'show', false)
        this.lookupTable.push(table)
      }
      // 将已经选上的dimension回显到界面上
      table.columns && table.columns.forEach((col) => {
        col.alias = col.alias || col.name
        let len = this.usedColumns.length
        for (let i = 0; i < len; i++) {
          let d = this.usedColumns[i]
          if (table.alias + '.' + col.name === d.column) {
            if (d.status === 'DIMENSION') {
              col.alias = d.name
              this.$set(col, 'isSelected', true)
              col.guid = d.guid
            }
            break
          } else {
            col.alias = col.name
            this.$set(col, 'isSelected', false)
            col.guid = null
          }
        }
      })
      this.$nextTick(() => {
        this.renerTableColumnSelected(table)
      })
    })
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      this.getTableColumns()
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
  selectionChange (selection, row) {
    this.$set(row, 'isSelected', !row.isSelected)
  }
  selectionAllChange (selection, guid) {
    if (!selection.length) {
      let columns = this.tables[guid].columns
      columns.forEach((row) => {
        this.$set(row, 'isSelected', false)
      })
    } else {
      selection.forEach((row) => {
        this.$set(row, 'isSelected', true)
      })
    }
  }
  dimensionRowClick (row, guid) {
    this.$refs[guid][0].toggleRowSelection(row)
    this.$set(row, 'isSelected', !row.isSelected)
  }
  toggleTableShow (table) {
    table.show = !table.show
    this.renerTableColumnSelected(table)
  }
  renerTableColumnSelected (table) {
    if (table.show) {
      this.$nextTick(() => {
        table.columns && table.columns.forEach((col) => {
          if (col.isSelected) {
            this.$refs[table.guid][0].toggleRowSelection(col)
          }
        })
      })
    }
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
  submit () {
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
    let ccDimensionList = this.usedColumns.filter((x) => {
      return x.isCC
    })
    this.modelDesc.dimensions.splice(0, this.modelDesc.dimensions.length)
    this.modelDesc.dimensions.push(...result, ...ccDimensionList)
    this.handleClose(true)
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
    &:hover {
      .right-icon {
        color:@base-color-2!important;
        cursor: pointer;
      }
    }
    border-bottom:solid 1px @line-border-color;
    height:40px;
    line-height:40px;
    cursor:pointer;
    .right-icon{
      margin-right:20px;
    }
  }
}
</style>
