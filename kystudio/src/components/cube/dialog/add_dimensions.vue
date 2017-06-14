<template>
<div>  
  <div v-for="(table, index) in factTableColumns"  >
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag>FactTable </el-tag>    
    <el-table  
      :data="table.columns"
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
          <el-input v-model="scope.row.name" :disabled="!scope.row.isSelected">
          </el-input>
        </template>
      </el-table-column>
      <el-table-column
        property="column"
        :label="$t('column')">
      </el-table-column>
      <el-table-column
        :label="$t('datatype')">
        <template scope="scope">
          {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('cardinality')">
        <template scope="scope">
          {{modelDesc.columnsDetail&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
        </template>
      </el-table-column>      
      <el-table-column>
      </el-table-column>        
    </el-table>
  </div> 

  <div v-for="(table, index) in lookupTableColumns">
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag>LookupTable </el-tag>       
    <el-table  
      :data="table.columns" :ref="table.tableName"
      style="width: 100%"
      @select-all="selectionAllChange(table.tableName)"      
      @select="selectionChange">
      <el-table-column
        type="selection"
        width="55">
      </el-table-column> 
       <el-table-column
        :label="$t('name')">
        <template scope="scope">
          <el-input v-model="scope.row.name" :disabled="!scope.row.isSelected" :placeholder="scope.row.name"></el-input>
        </template>
      </el-table-column>
      <el-table-column
        property="column"
        :label="$t('column')">
      </el-table-column>    
      <el-table-column
        :label="$t('datatype')">
        <template scope="scope">
          {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].datatype}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('cardinality')">
        <template scope="scope">
          {{modelDesc.columnsDetail[table.tableName + '.' + scope.row.column]&&modelDesc.columnsDetail[table.tableName + '.' + scope.row.column].cardinality}}
        </template>
      </el-table-column>       
      <el-table-column
        :label="$t('type')">
        <template scope="scope">
          <el-radio-group v-model="scope.row.derived" :disabled="!scope.row.isSelected" @change="changeType(scope.row)">
            <el-radio-button label="false">Normal</el-radio-button>
            <el-radio-button label="true">Derived</el-radio-button>
          </el-radio-group>
        </template>
      </el-table-column>          
    </el-table>
  </div> 
</div>  
</template>
<script>
export default {
  name: 'dimensions',
  props: ['modelDesc', 'cubeDimensions'],
  data () {
    return {
      rootFactTable: null,
      factTableColumns: [],
      lookupTableColumns: [],
      multipleSelection: {},
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    getTableColumns: function () {
      let _this = this
      // console.log(_this.modelDesc.dimensions, 'lll')
      _this.modelDesc.dimensions.forEach(function (dimension) {
        _this.multipleSelection[dimension.table] = []
        if (_this.modelDesc.factTables.indexOf(dimension.table) !== -1) {
          let colArr = []
          let tableObj = {tableName: dimension.table, columns: colArr}
          dimension.columns.forEach(function (col) {
            colArr.push({table: dimension.table, column: col, name: col, isSelected: false})
          })
          _this.factTableColumns.push(tableObj)
        } else {
          let colArr = []
          let tableObj = {tableName: dimension.table, columns: colArr}
          dimension.columns.forEach(function (col) {
            var suggestDerivedInfo = suggestDerived(dimension.table, col) === null ? 'false' : 'true'
            colArr.push({table: dimension.table, column: col, name: col, derived: suggestDerivedInfo, isSelected: false})
          })
          _this.lookupTableColumns.push(tableObj)
        }
      })
      function suggestDerived (table, column) {
        var derivedList = _this.modelDesc.suggestionDerived
        for (var s = 0; s < derivedList.length; s++) {
          if (table === derivedList[s].table && column === derivedList[s].column) {
            return derivedList[s].derived
          }
        }
      }
    },
    getCubeColumnInTable: function () {
      let _this = this
      _this.cubeDimensions.forEach(function (dimension) {
        if (_this.modelDesc.factTables.indexOf(dimension.table) !== -1) {
          _this.multipleSelection[dimension.table].push({table: dimension.table, column: dimension.column, name: dimension.name, isSelected: true})
          _this.factTableColumns.forEach(function (table) {
            if (table.tableName === dimension.table) {
              table.columns.forEach(function (column) {
                if (column.column === dimension.column) {
                  _this.$nextTick(function () {
                    _this.$refs[dimension.table][0].toggleRowSelection(column, true)
                  })
                  _this.$set(column, 'isSelected', true)
                  _this.$set(column, 'name', dimension.name)
                }
              })
            }
          })
        } else {
          let type = 'true'
          if (dimension.column) {
            type = 'false'
          }
          _this.lookupTableColumns.forEach(function (table) {
            if (table.tableName === dimension.table) {
              table.columns.forEach(function (column) {
                if ((type === 'false' && column.column === dimension.column) || (type === 'true' && column.column === dimension.derived[0])) {
                  _this.multipleSelection[dimension.table].push({table: dimension.table, column: column.column, name: dimension.name, isSelected: true, derived: type})
                  _this.$nextTick(function () {
                    _this.$refs[dimension.table][0].toggleRowSelection(column, true)
                  })
                  _this.$set(column, 'isSelected', true)
                  _this.$set(column, 'name', dimension.name)
                  _this.$set(column, 'derived', type)
                }
              })
            }
          })
        }
      })
    },
    changeType: function (column) {
      let _this = this
      _this.multipleSelection[column.table].forEach(function (col) {
        if (col.column === column.column) {
          _this.$set(col, 'derived', column.derived)
        }
      })
    },
    selectionChange: function (val, row) {
      this.multipleSelection[row.table] = val
      this.$set(row, 'isSelected', !row.isSelected)
      if (!row.isSelected && row.derived) {
        this.$set(row, 'derived', 'true')
      }
    },
    selectionAllChange: function (tableName) {
      let _this = this
      if (_this.$refs[tableName][0].store.states.selection.length > 0) {
        _this.$refs[tableName][0].data.forEach(function (selection) {
          _this.$set(selection, 'isSelected', true)
        })
        _this.multipleSelection[tableName] = _this.$refs[tableName][0].data
      } else {
        _this.$refs[tableName][0].data.forEach(function (selection) {
          _this.$set(selection, 'isSelected', false)
          if (selection.derived) {
            _this.$set(selection, 'derived', 'true')
          }
        })
        _this.multipleSelection[tableName] = []
      }
    }
  },
  created () {
    this.getTableColumns()
    this.getCubeColumnInTable()
    this.$on('addDimensionsFormValid', (t) => {
      this.$emit('validSuccess', this.multipleSelection)
    })
  },
  locales: {
    'en': {name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment'},
    'zh-cn': {name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
</style>
