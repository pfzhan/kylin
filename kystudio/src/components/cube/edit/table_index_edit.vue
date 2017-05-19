<template>
 <div class="table-index">
  <p v-if="!rawTableUsable()">{{$t('noSupportRawTable')}}</p>
  <div v-else>
    <el-checkbox v-model="usedRawTable" @change="changeUsed()">{{$t('ConfigRawTable')}}</el-checkbox>
    <el-table  v-if="usedRawTable"
    :data="convertedRawTable"
    :row-class-name="tableRowClassName"
    border class="table_margin"
    style="width: 100%">
     <el-table-column
      :label="$t('ID')"
      header-align="center"
      align="center"
      width="80">
      <template scope="scope">
        <el-tag>{{scope.$index + 1 + 15*(currentPage-1)}}</el-tag>
      </template>
     </el-table-column>
     <el-table-column
      :label="$t('column')"
      header-align="center"
      prop="column"
      align="center">
     </el-table-column>
     <el-table-column
        :label="$t('dataType')"
        header-align="center"
        align="center">
        <template scope="scope">
          {{modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column].datatype}}
        </template>   
     </el-table-column>  
     <el-table-column
        :label="$t('tableAlias')"
        prop="table"
        header-align="center"
        align="center">   
      </el-table-column>
      <el-table-column
        :label="$t('Encoding')"
        header-align="center"
        align="center">   
            <template scope="scope">
              <el-select v-model="scope.row.encoding" @change="changeRawTable(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in initEncodingType(scope.row)"
                   :label="item.name"
                   :value="item.name + ':' + item.version">
                   <el-tooltip effect="light" :content="$t('$store.state.config.encodingTip[item.name]')" placement="right">
                     <span style="float: left;;width: 90%">{{ item.name }}</span>
                     <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-if="item.version>1">{{ item.version }}</span>
                  </el-tooltip>
                </el-option>              
              </el-select>
            </template>
      </el-table-column>  
      <el-table-column
        :label="$t('Length')"
        header-align="center"
        align="center">   
        <template scope="scope">
          <el-input v-model="scope.row.valueLength"      :disabled="scope.row.encoding.indexOf('dict')>=0||scope.row.encoding.indexOf('date')>=0||scope.row.encoding.indexOf('time')>=0||scope.row.encoding.indexOf('var')>=0||scope.row.encoding.indexOf('orderedbytes')>=0"  @change="changeRawTable(scope.row, scope.$index)"></el-input>  
        </template>  
    </el-table-column>       
    <el-table-column
        :label="$t('Index')"
        header-align="center"
        align="center">
            <template scope="scope">
              <el-select v-model="scope.row.index" @change="changeRawTable(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in rawTableIndexOptions"
                   :label="item"
                   :value="item">
                </el-option>              
              </el-select>
            </template>
      </el-table-column>       
    </el-table> 
    </div>
    <pager ref="pager" :perPageSize="15" :totalSize="totalRawTable"  v-on:handleCurrentChange='currentChange' ></pager>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, loadBaseEncodings } from '../../../util/business'
import { removeNameSpace, getNameSpace } from '../../../util/index'
export default {
  name: 'tableIndex',
  props: ['cubeDesc', 'modelDesc', 'isEdit', 'rawTable'],
  data () {
    return {
      usedRawTable: false,
      totalRawTable: 0,
      currentPage: 1,
      convertedRawTable: [],
      rawTableDetail: [],
      rawTableIndexOptions: ['discrete', 'fuzzy', 'sorted']
    }
  },
  methods: {
    ...mapActions({
      loadRawTable: 'GET_RAW_TABLE'
    }),
    changeUsed: function () {
      if (this.usedRawTable === false) {
        this.rawTable.needDelete = true
        this.rawTable.tableDetail.columns.splice(0, this.rawTable.tableDetail.columns.length)
      } else {
        this.getBaseColumnsData()
      }
    },
    getEncoding: function (encode) {
      let code = encode.split(':')
      return code[0]
    },
    getLength: function (encode) {
      let code = encode.split(':')
      return code[1]
    },
    getVersion: function (encode) {
      let code = encode.split(':')
      return code[1]
    },
    initEncodingType: function (column) {
      let _this = this
      let datatype = this.modelDesc.columnsDetail[column.table + '.' + column.column].datatype
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      let filterEncodings = baseEncodings.filterByColumnType(datatype)
      baseEncodings.addEncoding('orderedbytes', 1)
      if (this.isEdit) {
        let _encoding = _this.getEncoding(column.encoding)
        let _version = parseInt(_this.getVersion(column.encoding))
        let addEncodings = baseEncodings.addEncoding(_encoding, _version)
        return addEncodings
      } else {
        return filterEncodings
      }
    },
    tableRowClassName: function (row, index) {
      if (row.index === 'sorted') {
        return 'info-row'
      }
      return ''
    },
    changeRawTable: function (column, index) {
      let _this = this
      _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'index', column.index)
      _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'encoding_version', _this.getVersion(column.encoding))
      if (column.valueLength) {
        _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'encoding', _this.getEncoding(column.encoding) + ':' + column.valueLength)
      } else {
        _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'encoding', _this.getEncoding(column.encoding))
      }
      if (column.encoding.indexOf('dict') >= 0 || column.encoding.indexOf('date') >= 0 || column.encoding.indexOf('time') >= 0 || column.encoding.indexOf('var') >= 0 || column.encoding.indexOf('orderedbytes') >= 0) {
        _this.$set(column, 'valueLength', null)
      }
    },
    initConvertedRawTable: function () {
      let _this = this
      _this.totalRawTable = _this.rawTable.tableDetail.columns.length
      let rawTableDetail = _this.rawTable.tableDetail.columns.slice(15 * (_this.currentPage - 1), 15 * (_this.currentPage))
      _this.convertedRawTable.splice(0, _this.convertedRawTable.length)
      rawTableDetail.forEach(function (rawTable) {
        let version = rawTable.encoding_version || 1
        _this.convertedRawTable.push({column: rawTable.column, table: rawTable.table, encoding: _this.getEncoding(rawTable.encoding) + ':' + version, valueLength: _this.getLength(rawTable.encoding), index: rawTable.index})
      })
    },
    getBaseColumnsData: function () {
      let _this = this
      _this.modelDesc.dimensions.forEach(function (dimension) {
        dimension.columns.forEach(function (column) {
          let index = 'discrete'
          if (_this.modelDesc.partition_desc && dimension.table + '.' + column === _this.modelDesc.partition_desc.partition_date_column) {
            index = 'sorted'
          }
          _this.rawTable.tableDetail.columns.push({
            index: index,
            encoding: 'orderedbytes',
            table: dimension.table,
            column: column
          })
        })
      })
      this.modelDesc.metrics.forEach(function (measure) {
        let index = 'discrete'
        if (_this.modelDesc.partition_desc && measure === _this.modelDesc.partition_desc.partition_date_column) {
          index = 'sorted'
        }
        _this.rawTable.tableDetail.columns.push({
          index: index,
          encoding: 'orderedbytes',
          table: getNameSpace(measure),
          column: removeNameSpace(measure)
        })
      })
      this.initConvertedRawTable()
    },
    rawTableUsable: function () {
      if (this.cubeDesc && this.cubeDesc.engine_type && (this.cubeDesc.engine_type === 100 || this.cubeDesc.engine_type === 99)) {
        return true
      } else {
        return false
      }
    },
    currentChange: function (value) {
      this.currentPage = value
      this.initConvertedRawTable()
    }
  },
  created () {
    let _this = this
    if (_this.rawTableUsable()) {
      if (_this.rawTable.tableDetail.columns.length > 0) {
        _this.usedRawTable = true
        _this.initConvertedRawTable()
      } else {
        if (_this.isEdit) {
          this.loadRawTable(this.cubeDesc.name).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              if (data) {
                _this.usedRawTable = true
                _this.$set(_this.rawTable, 'tableDetail', data)
                _this.initConvertedRawTable()
              }
            })
          }).catch((res) => {
            handleError(res, (data, code, status, msg) => {
            })
          })
        }
      }
    }
  },
  locales: {
    'en': {noSupportRawTable: 'Only KAP PLUS Provides Raw Table', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index', ConfigRawTable: 'Config Raw Table'},
    'zh-cn': {noSupportRawTable: '只有KAP PLUS 提供Raw Table功能', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引', ConfigRawTable: '配置Raw Table'}
  }
}
</script>
<style>
.table-index .row_padding {
  padding-top: 5px;
  padding-bottom: 5px;
 }
 .table-index .table_margin {
   margin-top: 20px;
   margin-bottom: 20px;
 }
.table-index .tag_margin {
  margin-left: 4px;
  margin-bottom: 2px;
  margin-top: 2px;
  margin-right: 4px;
 }
.table-index .dropdown ul {
  height: 150px;
  overflow: scroll;
 }
.table-index .el-table .info-row {
  background-color: #c9e5f5;
}
</style>
