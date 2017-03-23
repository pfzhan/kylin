<template>
<div >  
  <div v-for="table in factTableColumn"  >
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag>FactTable </el-tag>    
    <el-table  
      :data="table.column"
      style="width: 100%"
      @select-all="selectionAllChange"
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

  <div v-for="table in lookupTableColumn">
    <el-tag>{{table.tableName}} </el-tag>
    <el-tag>LookupTable </el-tag>       
    <el-table  
      :data="table.column"
      style="width: 100%"
      @select-all="selectionAllChange"      
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
      factTableColumn: [],
      lookupTable: [],
      lookupTableColumn: [],
      multipleSelection: {},
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    getRootFactTable: function () {
      this.rootFactTable = removeNameSpace(this.modelDesc.fact_table)
    },
    getFactTable: function () {
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
    getFactTableColumn: function () {
      let _this = this
      _this.modelDesc.dimensions.forEach(function (dimension) {
        if (_this.factTable.indexOf(dimension.table) !== -1) {
          let colArr = []
          let tableObj = {tableName: dimension.table, column: colArr}
          dimension.columns.forEach(function (col) {
            colArr.push({table: dimension.table, column: col, name: col, isSelected: false})
          })
          _this.factTableColumn.push(tableObj)
        } else {
          let colArr = []
          let tableObj = {tableName: dimension.table, column: colArr}
          dimension.columns.forEach(function (col) {
            colArr.push({table: dimension.table, column: col, name: col, derived: 'true', isSelected: false})
          })
          _this.lookupTableColumn.push(tableObj)
        }
      })
    },
    selectionChange: function (val, row) {
      this.multipleSelection[row.table] = val
      this.$set(row, 'isSelected', !row.isSelected)
    },
    selectionAllChange: function (val) {
      let _this = this
      let tableName = ''
      if (val.length > 0) {
        tableName = val[0].table
        val.forEach(function (selection) {
          if (!selection.isSelected) {
            _this.$set(selection, 'isSelected', !selection.isSelected)
          }
        })
        _this.multipleSelection[tableName] = val
      } else {
        val.forEach(function (selection) {
          if (selection.isSelected) {
            _this.$set(selection, 'isSelected', 'false')
          }
        })
      }
    }
  },
  computed: {
    getLookupTableColumn: function () {
      console.log('33f')
    }
  },
  created () {
    this.getRootFactTable()
    this.getFactTable()
    this.getFactTableColumn()
  },
  locales: {
    'en': {name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment'},
    'zh-cn': {name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释'}
  }
}
</script>
<style scoped="">

</style>
