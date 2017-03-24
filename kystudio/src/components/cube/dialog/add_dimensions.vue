<template>
<div >  
  <div v-for="table in factTableColumns"  >
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
          <el-input v-model="scope.row.name" :disabled="!scope.row.isSelected" :placeholder="scope.row.name"></el-input>
        </template>
      </el-table-column>
      <el-table-column
        property="column"
        :label="$t('column')">
      </el-table-column>
      <el-table-column
        :label="$t('datatype')">
      </el-table-column>
      <el-table-column
        :label="$t('cardinality')">
      </el-table-column>      
      <el-table-column>
      </el-table-column>        
    </el-table>
  </div> 

  <div v-for="table in lookupTableColumns">
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
      </el-table-column>
      <el-table-column
        :label="$t('cardinality')">
      </el-table-column>       
      <el-table-column
        :label="$t('type')">
        <template scope="scope">
          <el-radio-group v-model="scope.row.derived" :disabled="!scope.row.isSelected">
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
import { removeNameSpace } from '../../../util/index'
export default {
  name: 'dimensions',
  props: ['modelDesc', 'cubeDimensions'],
  data () {
    return {
      rootFactTable: null,
      factTable: [],
      factTableColumns: [],
      lookupTable: [],
      lookupTableColumns: [],
      multipleSelection: {},
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    getRootFactTable: function () {
      this.rootFactTable = removeNameSpace(this.modelDesc.fact_table)
    },
    getTable: function () {
      let _this = this
      _this.factTable.push(this.rootFactTable)
      _this.multipleSelection[this.rootFactTable] = []
      _this.modelDesc.lookups.forEach(function (lookup) {
        if (lookup.kind === 'FACT') {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          _this.factTable.push(lookup.alias)
          _this.multipleSelection[lookup.alias] = []
        } else {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          _this.lookupTable.push(lookup.alias)
          _this.multipleSelection[lookup.alias] = []
        }
      })
    },
    getTableColumns: function () {
      let _this = this
      _this.modelDesc.dimensions.forEach(function (dimension) {
        if (_this.factTable.indexOf(dimension.table) !== -1) {
          let colArr = []
          let tableObj = {tableName: dimension.table, columns: colArr}
          dimension.columns.forEach(function (col) {
            colArr.push({table: dimension.table, column: col, name: col, isSelected: false})
          })
          _this.factTableColumns.push(tableObj)
        } else {
          let colArr = []
          let tableObj = {tableName: dimension.table, column: colArr}
          dimension.columns.forEach(function (col) {
            colArr.push({table: dimension.table, column: col, name: col, derived: 'true', isSelected: false})
          })
          _this.lookupTableColumns.push(tableObj)
        }
      })
    },
    selectionChange: function (val, row) {
      this.multipleSelection[row.table] = val
      this.$set(row, 'isSelected', !row.isSelected)
      if (!row.isSelected && row.derived) {
        this.$set(row, 'derived', 'true')
      }
      this.$refs[row.table][0].toggleRowSelection({column: 'TRANS_ID', isSelected: false, name: 'TRANS_ID', table: 'TEST_KYLIN_FACT'}, true)
      console.log(this.$refs[row.table][0].data[0])
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
      console.log(_this.multipleSelection[tableName])
    }
  },
  computed: {
    getLookupTableColumns: function () {
      console.log('33f')
    }
  },
  created () {
    this.getRootFactTable()
    this.getTable()
    this.getTableColumns()
  },
  locales: {
    'en': {name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment'},
    'zh-cn': {name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释'}
  }
}
</script>
<style scoped="">

</style>
