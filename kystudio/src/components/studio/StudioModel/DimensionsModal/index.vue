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
          <div v-for="(table, index) in factTableColumns" class="ksd-mb-20">
            <span><i class="el-icon-ksd-fact_table"></i></span><span class="table-title">{{table.tableName}} </span>
            <el-table
              class="ksd-mt-10"
              border
              :data="table.columns"
              @row-click="dimensionRowClick"
              :ref="table.tableName"
              @select-all="selectionAllChange(table.tableName)"
              @select="selectionChange">
              <el-table-column
                type="selection"
                width="55">
              </el-table-column>
              <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <el-input size="small" @click.native.stop v-model="scope.row.name" :disabled="!scope.row.isSelected" @change="(value) => { changeName(scope.row, table, value) }">
                  </el-input>
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                property="column"
                :label="$t('column')">
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                :label="$t('datatype')"
                width="110">
                <template slot-scope="scope">
                  {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                :label="$t('cardinality')"
                width="100">
                <template slot-scope="scope">
                  {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
                </template>
              </el-table-column>
               <el-table-column
                :label="$t('type')"
                width="100">
                <template slot-scope="scope">
                  <el-radio-group size="mini" @click.native.stop v-model="scope.row.derived" :disabled="!scope.row.isSelected" @change="changeType(scope.row)">
                    <el-radio-button plain type="primary" size="mini" label="false">Normal</el-radio-button><!--
                    注释是为了取消button之间的间距，不要删--><el-radio-button plain type="warning" label="true" size="mini">Derived</el-radio-button>
                  </el-radio-group>
                </template>
              </el-table-column>
              <el-table-column
              :label="$t('Comment')">
              </el-table-column>
            </el-table>
          </div>

          <div v-for="(table, index) in lookupTableColumns" class="ksd-mb-20">
            <span><i class="el-icon-ksd-lookup_table"></i></span><span class="table-title">{{table.tableName}} </span>
            <el-table
              class="ksd-mt-10"
              border
              :data="table.columns" :ref="table.tableName"
              @row-click="dimensionRowClick"
              @select-all="selectionAllChange(table.tableName)"
              @select="selectionChange">
              <el-table-column
                type="selection"
                width="55">
              </el-table-column>
               <el-table-column
                :label="$t('name')">
                <template slot-scope="scope">
                  <el-input size="small" v-model="scope.row.name" @click.native.stop :disabled="!scope.row.isSelected" :placeholder="scope.row.name"></el-input>
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                property="column"
                :label="$t('column')">
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                :label="$t('datatype')"
                width="110">
                <template slot-scope="scope">
                  {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
                </template>
              </el-table-column>
              <el-table-column
                show-overflow-tooltip
                :label="$t('cardinality')"
                width="100">
                <template slot-scope="scope">
                  {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
                </template>
              </el-table-column>
              <el-table-column
                :label="$t('type')"
                width="100">
                <template slot-scope="scope">
                  <el-radio-group size="mini" @click.native.stop v-model="scope.row.derived" :disabled="!scope.row.isSelected" @change="changeType(scope.row)">
                    <el-radio-button plain type="primary" size="mini" label="false">Normal</el-radio-button><!--
                    注释是为了取消button之间的间距，不要删--><el-radio-button plain type="warning" label="true" size="mini">Derived</el-radio-button>
                  </el-radio-group>
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
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="handleClick"  :disabled="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
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
import { sourceTypes } from '../../../../config'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
// import { handleSuccessAsync, handleError } from '../../../util'
vuex.registerModule(['modals', 'DimensionsModal'], store)
@Component({
  // props: {
  //   modelTables: {
  //     type: Array,
  //     default: null
  //   }
  // },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('DimensionsModal', {
      isShow: state => state.isShow,
      modelTables: state => state.modelTables
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
  factTableColumns = [{tableName: 'DEFAULT.KYLIN_SALES', column: 'PRICE'}]
  lookupTableColumns = [{tableName: 'DEFAULT.KYLIN_CAL_DT', column: 'CAL_DT'}]

  getTableColumns () {
    this.modelTables.forEach((NTable) => {
      if (NTable.kind === 'FACT') {
        NTable.columns.forEach((col) => {
          this.factTableColumns.push({tableName: NTable.name, column: col.name, name: col.name, isSelected: false})
        })
      } else {
        NTable.columns.forEach((col) => {
          this.lookupTableColumns.push({tableName: NTable.name, column: col.name, name: col.name, isSelected: false})
        })
      }
    })
  }

  // get modalTitle () {
  //   return titleMaps[this.sourceType]
  // }
  // get cancelText () {
  //   return this.$t(cancelMaps[this.sourceType])
  // }
  // get confirmText () {
  //   return this.$t(confirmMaps[this.sourceType])
  // }
  get isNewSource () {
    return this.sourceType === sourceTypes.NEW
  }
  get isTableTree () {
    return this.sourceType === sourceTypes.HIVE ||
      this.sourceType === sourceTypes.RDBMS ||
      this.sourceType === sourceTypes.RDBMS2
  }
  get isSourceSetting () {
    return this.sourceType === sourceTypes.SETTING
  }
  get isKafka () {
    return this.sourceType === sourceTypes.KAFKA
  }
  isSourceShow (sourceType) {
    return this.sourceType === sourceType
  }

  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true

      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }

  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 300)
  }
  handleClick () {
  }
  destroyed () {
    if (!module.hot) {
      vuex.unregisterModule(['modals', 'DimensionsModal'])
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.dimension-modal{
  .table-title {
    font-size: 16px;
    margin-left: 5px;
  }
}
</style>
