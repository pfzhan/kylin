<template>
<div class="add_dimensions">
  <el-button type="blue" class="dimensions-button" @click="suggestionDimensions"  :loading="suggestLoading" :disabled="!getSqlResult">{{$t('sqlOutput')}}
    <common-tip :content="$t('outputTipOne') + $t(getStrategy) + $t('outputTipTwo')" ><icon name="question-circle" style="color:gray"></icon></common-tip></el-button>
  <el-button type="blue" class="dimensions-button" @click="reset">{{$t('reset')}}</el-button>
  <div class="line" style="margin-bottom: -15px;margin-right: -30px;margin-left: -30px;"></div>
  <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
  <div v-for="(table, index) in factTableColumns">
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag v-if="index === 0">[ Fact Table ]</el-tag>
    <el-tag v-else>[ Lookup Table (limited) ]</el-tag>
    <el-table
      border
      :data="table.columns"
      @row-click="dimensionRowClick"
      style="width: 100%" :ref="table.tableName"
      @select-all="selectionAllChange(table.tableName)"
      @select="selectionChange">
      <el-table-column
        type="selection"
        width="55">
      </el-table-column>
      <el-table-column
        :label="$t('name')">
        <template scope="scope">
          <el-input @click.native.stop v-model="scope.row.name" :disabled="!scope.row.isSelected">
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
        <template scope="scope">
          {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
        </template>
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        :label="$t('cardinality')"
        width="100">
        <template scope="scope">
          {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
        </template>
      </el-table-column>
      <el-table-column
      width="185">
      </el-table-column>
    </el-table>
  </div>

  <div v-for="(table, index) in lookupTableColumns" style="margin-top: 20px;">
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag>[ Lookup Table ]</el-tag>
    <el-table
      border
      :data="table.columns" :ref="table.tableName"
      style="width: 100%"
      @row-click="dimensionRowClick"
      @select-all="selectionAllChange(table.tableName)"
      @select="selectionChange">
      <el-table-column
        type="selection"
        width="55">
      </el-table-column>
       <el-table-column
        :label="$t('name')">
        <template scope="scope">
          <el-input v-model="scope.row.name" @click.native.stop :disabled="!scope.row.isSelected" :placeholder="scope.row.name"></el-input>
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
        <template scope="scope">
          {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
        </template>
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        :label="$t('cardinality')"
        width="100">
        <template scope="scope">
          {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('type')"
        width="185">
        <template scope="scope">
          <el-radio-group @click.native.stop v-model="scope.row.derived" :disabled="!scope.row.isSelected" @change="changeType(scope.row)">
            <el-radio-button label="false">Normal</el-radio-button><!--
            注释是为了取消button之间的间距，不要删--><el-radio-button label="true">Derived</el-radio-button>
          </el-radio-group>
        </template>
      </el-table-column>
    </el-table>
  </div>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { removeNameSpace, getNameSpaceTopName } from '../../../util/index'
import { handleSuccess, handleError, kapConfirm } from '../../../util/business'
export default {
  name: 'adddimensions',
  props: ['modelDesc', 'cubeDesc', 'sampleSql', 'oldData'],
  data () {
    return {
      factTableColumns: [],
      lookupTableColumns: [],
      multipleSelection: {},
      suggestLoading: false,
      sqlDimensions: [],
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    ...mapActions({
      getSqlDimensions: 'GET_SQL_DIMENSIONS'
    }),
    dimensionRowClick: function (row, event, column) {
      this.$set(row, 'isSelected', !row.isSelected)
      this.$refs[row.table][0].toggleRowSelection(row)
      this.multipleSelection[row.table] = this.multipleSelection[row.table] || []
      if (row.isSelected) {
        var hasColumn = this.multipleSelection[row.table].filter((co) => {
          return co.column === row.column
        })
        if (!(hasColumn && hasColumn.length)) {
          this.multipleSelection[row.table].push(row)
        }
      } else {
        this.multipleSelection[row.table] = this.multipleSelection[row.table].filter((co) => {
          return !(co.column === row.column)
        })
      }
    },
    getTableColumns: function () {
      this.factTableColumns = []
      this.lookupTableColumns = []
      this.modelDesc.dimensions.forEach((dimension) => {
        this.multipleSelection[dimension.table] = []
        if (this.modelDesc.factTables.indexOf(dimension.table) !== -1) {
          let colArr = []
          let tableObj = {tableName: dimension.table, columns: colArr}
          dimension.columns.forEach((col) => {
            colArr.push({table: dimension.table, column: col, name: col, isSelected: false})
          })
          if (dimension.table === removeNameSpace(this.modelDesc.fact_table)) {
            this.factTableColumns.unshift(tableObj)
          } else {
            this.factTableColumns.push(tableObj)
          }
        } else {
          let colArr = []
          let tableObj = {tableName: dimension.table, columns: colArr}
          dimension.columns.forEach((col) => {
            var suggestDerivedInfo = this.suggestDerived(dimension.table, col) === null ? 'false' : 'true'
            colArr.push({table: dimension.table, column: col, name: col, derived: suggestDerivedInfo, isSelected: false})
          })
          this.lookupTableColumns.push(tableObj)
        }
      })
    },
    suggestDerived: function (table, column) {
      var derivedList = this.modelDesc.suggestionDerived
      for (var s = 0; s < (derivedList && derivedList.length || 0); s++) {
        if (table === derivedList[s].table && derivedList[s].derived) {
          if (derivedList[s].derived.indexOf(column) >= 0) {
            return true
          }
        }
      }
      return null
    },
    getCubeColumnInTable: function (dimensions) {
      dimensions.forEach((dimension) => {
        if (this.modelDesc.factTables.indexOf(dimension.table) !== -1) {
          this.multipleSelection[dimension.table].push({table: dimension.table, column: dimension.column, name: dimension.name, isSelected: true})
          this.factTableColumns.forEach((table) => {
            if (table.tableName === dimension.table) {
              table.columns.forEach((column) => {
                if (column.column === dimension.column) {
                  this.$nextTick(() => {
                    this.$refs[dimension.table][0].toggleRowSelection(column, true)
                  })
                  this.$set(column, 'isSelected', true)
                  this.$set(column, 'name', dimension.name)
                }
              })
            }
          })
        } else {
          let type = 'true'
          if (dimension.column) {
            type = 'false'
          }
          this.lookupTableColumns.forEach((table) => {
            if (table.tableName === dimension.table) {
              table.columns.forEach((column) => {
                if ((type === 'false' && column.column === dimension.column) || (type === 'true' && column.column === dimension.derived[0])) {
                  this.multipleSelection[dimension.table].push({table: dimension.table, column: column.column, name: dimension.name, isSelected: true, derived: type})
                  this.$nextTick(() => {
                    this.$refs[dimension.table][0].toggleRowSelection(column, true)
                  })
                  this.$set(column, 'isSelected', true)
                  this.$set(column, 'name', dimension.name)
                  this.$set(column, 'derived', type)
                }
              })
            }
          })
        }
      })
    },
    changeType: function (column) {
      this.multipleSelection[column.table].forEach((col) => {
        if (col.column === column.column) {
          this.$set(col, 'derived', column.derived)
        }
      })
    },
    selectionChange: function (val, row) {
      this.multipleSelection[row.table] = val
      this.$set(row, 'isSelected', !row.isSelected)
      if (!row.isSelected && row.derived) {
        this.$set(row, 'derived', row.derived)
      }
    },
    selectionAllChange: function (tableName) {
      if (this.$refs[tableName][0].store.states.selection.length > 0) {
        this.$refs[tableName][0].data.forEach((selection) => {
          this.$set(selection, 'isSelected', true)
        })
        this.multipleSelection[tableName] = this.$refs[tableName][0].data
      } else {
        this.$refs[tableName][0].data.forEach((selection) => {
          this.$set(selection, 'isSelected', false)
          if (selection.derived) {
            this.$set(selection, 'derived', selection.derived)
          }
        })
        this.multipleSelection[tableName] = []
      }
    },
    suggestionDimensions: function () {
      this.suggestLoading = true
      let sqlSuggestdimensions = []
      this.sqlDimensions.forEach((col) => {
        let table = getNameSpaceTopName(col)
        let colName = removeNameSpace(col)
        let suggestDerivedInfo = this.suggestDerived(table, colName) === null ? 'false' : 'true'
        let dimensionObj = {table: table, column: null, name: colName, derived: null}
        if (!suggestDerivedInfo) {
          dimensionObj.derived = [colName]
        } else {
          dimensionObj.column = colName
        }
        sqlSuggestdimensions.push(dimensionObj)
      })
      this.getTableColumns()
      this.getCubeColumnInTable(sqlSuggestdimensions)
      this.suggestLoading = false
    },
    reset: function () {
      kapConfirm(this.$t('resetTip'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        this.getTableColumns()
        this.getCubeColumnInTable(this.oldData.oldDimensions || [])
      })
    }
  },
  computed: {
    getStrategy: function () {
      if (this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'default') {
        return 'dataOriented'
      } else if (this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'mixed') {
        return 'mix'
      } else {
        return 'businessOriented'
      }
    },
    getSqlResult: function () {
      let sqlResult = false
      this.sampleSql.result.forEach((row) => {
        if (row.status !== 'FAILED') {
          sqlResult = true
        }
      })
      return sqlResult
    }
  },
  created () {
    this.getTableColumns()
    this.getCubeColumnInTable(this.cubeDesc.dimensions)
    if (this.getSqlResult) {
      this.getSqlDimensions(this.cubeDesc).then((res) => {
        handleSuccess(res, (data) => {
          this.sqlDimensions = data
        })
      }, (res) => {
        handleError(res)
      })
    }
    this.$on('addDimensionsFormValid', (t) => {
      let coincide = false
      for (let table in this.multipleSelection) {
        if (this.multipleSelection[table] && this.multipleSelection[table].length > 0) {
          this.multipleSelection[table].forEach((column) => {
            if (this.sqlDimensions.indexOf(table + '.' + column.name) >= 0) {
              coincide = true
            }
          })
        }
      }
      if (!coincide && this.getSqlResult && this.getSqlResult === 'businessOriented') {
        kapConfirm(this.$t('noCoincide'), {
          confirmButtonText: this.$t('kylinLang.common.continue')
        }).then(() => {
          this.$emit('validSuccess', this.multipleSelection)
        })
      } else {
        this.$emit('validSuccess', this.multipleSelection)
      }
    })
  },
  locales: {
    'en': {name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', reset: 'Reset', sqlOutput: 'Dimensions Suggestion', outputTipOne: 'Dimensions suggested based on inputed SQL patterns.', resetTip: 'Reset will call last saving back and overwrite existing dimensions. Please confirm to continue?', dataOriented: 'Data Oriented', mix: 'Mix', businessOriented: 'Business Oriented', noCoincide: 'On the business oriented preference, you are suggested to use most dimensions from SQL patterns. Otherwise, optimizer can barely offer useful suggestion.'},
    'zh-cn': {name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', reset: '重置', sqlOutput: '推荐维度', outputTipOne: '系统将根据您输入的SQL语句推荐对应维度。', resetTip: '重置操作会返回上一次保存过的维度列表，并覆盖现有的纬度，请确认是否继续此操作？', dataOriented: '模型优先', mix: '综合', businessOriented: '业务优先', noCoincide: '在业务优先的优化偏好下，您未选择输入的SQL中出现的维度。优化器将难以提供合适的优化建议。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .add_dimensions{
    max-height: 500px;
    overflow-x: hidden;
    overflow-y: scroll;
    .el-table td, .el-table th {
      height: auto;
    }
    .el-table__row{
      cursor: pointer;
      .el-input{
        padding: 10px 0 10px 0;
      }
    }
    .el-tag{
      background: transparent;
      border: none;
    }
    .el-table{
      margin-top: 10px;
    }
    .dimensions-button {
      height: 40px;
    }
    .dimensions-button:hover {
      border-color: #218fea !important;
      background: transparent!important;
    }
  }
</style>
