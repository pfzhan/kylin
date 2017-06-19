<template>
 <div class="table-index" id="table-index">
  <p v-if="!rawTableUsable">{{$t('noSupportRawTable')}}</p>
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
        <el-tag style="background: transparent;">{{scope.$index + 1 + 15*(currentPage-1)}}</el-tag>
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
          {{modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column]&&modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column].datatype}}
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
              <el-select v-model="scope.row.encoding" @change=" changeEncoding(scope.row, scope.$index);changeRawTable(scope.row, scope.$index);">
                <el-option
                    v-for="(item, index) in initEncodingType(scope.row)" :key="index"
                   :label="item.name"
                   :value="item.name + ':' + item.version">
                   <el-tooltip effect="dark" :content="$t('kylinLang.cube.'+$store.state.config.encodingTip[item.name])" placement="top">
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
          <el-input v-model="scope.row.valueLength" :disabled="scope.row.encoding.indexOf('dict')>=0||scope.row.encoding.indexOf('date')>=0||scope.row.encoding.indexOf('time')>=0||scope.row.encoding.indexOf('var')>=0||scope.row.encoding.indexOf('orderedbytes')>=0"  @change="changeRawTable(scope.row, scope.$index)"></el-input>  
        </template>  
    </el-table-column>

     <el-table-column
        :label="$t('sortBy')"
        header-align="center"
        align="center">   
            <template scope="scope">
              <el-select v-model="scope.row.is_sortby" @change="changeRawTable(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in booleanSelect" :key="index"
                   :label="item.name"
                   :value="item.value">
                </el-option>              
              </el-select>
            </template>
      </el-table-column> 

       <el-table-column
        :label="$t('shardBy')"
        header-align="center"
        align="center">   
            <template scope="scope">
              <el-select v-model="scope.row.is_shardby" @change="changeRawTable(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in booleanSelect" :key="index"
                   :label="item.name"
                   :value="item.value">
                </el-option>              
              </el-select>
            </template>
      </el-table-column> 

    <el-table-column
        :label="$t('Index')"
        header-align="center"
        align="center">
            <template scope="scope">
              <el-select v-model="scope.row.index" @change="changeRawTable(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in rawTableIndexOptions" :key="index"
                   :label="item"
                   :value="item">
                </el-option>             
              </el-select>
            </template>
      </el-table-column>       
    </el-table> 
    </div>
    <pager v-if="usedRawTable" ref="pager" :perPageSize="15" :totalSize="totalRawTable"  v-on:handleCurrentChange='currentChange' ></pager>


     
    
     <div class="ksd-common-table ksd-mt-20" v-if="usedRawTable && rawTable.tableDetail.columns.length">
     <!-- <p class="ksd-left" style="margin-bottom: 15px;">{{$t('dragSorted')}}</p> -->
       <el-row class="tableheader">
         <el-col :span="1">{{$t('ID')}}</el-col>
         <el-col :span="6">{{$t('column')}}</el-col>
         <el-col :span="4">{{$t('dataType')}}</el-col>
         <el-col :span="5">{{$t('tableAlias')}}</el-col>
         <el-col :span="2">{{$t('Encoding')}}</el-col>
         <el-col :span="4">{{$t('Length')}}</el-col>
         <el-col :span="2">{{$t('Index')}}</el-col>
       </el-row>
        <el-row style="cursor:move" class="tablebody" v-if="row.is_sortby===true" v-for="(row, index) in rawTable.tableDetail.columns" :key="row.column" v-dragging="{ item: row, list: rawTable.tableDetail.columns, group: 'row' }">
          <el-col :span="1" >{{index+1}}</el-col>
          <el-col :span="6">{{row.column}}</el-col>
          <el-col :span="4">
              {{modelDesc.columnsDetail[row.table+'.'+row.column]&&modelDesc.columnsDetail[row.table+'.'+row.column].datatype}}
          </el-col>
          <el-col :span="5"> 
            {{row.table}}
          </el-col>
          <el-col :span="2">
            {{row.encoding}} 
          </el-col>
          <el-col :span="4">
            {{row.valueLength}}
          </el-col>
          <el-col :span="2">
            {{row.index}}
          </el-col>
        </el-row>
        </div>
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
      rawTableIndexOptions: ['discrete', 'fuzzy'],
      booleanSelect: [{name: 'true', value: true}, {name: 'false', value: false}]
    }
  },
  methods: {
    ...mapActions({
      loadRawTable: 'GET_RAW_TABLE'
    }),
    changeUsed: function () {
      if (this.usedRawTable === false) {
        this.$store.state.cube.cubeRowTableIsSetting = false
        this.rawTable.tableDetail.columns.splice(0, this.rawTable.tableDetail.columns.length)
      } else {
        this.$store.state.cube.cubeRowTableIsSetting = true
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
      let datatype = this.modelDesc.columnsDetail[column.table + '.' + column.column] && this.modelDesc.columnsDetail[column.table + '.' + column.column].datatype || ''
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
      if (row.is_sortby === true) {
        return 'info-row'
      }
      return ''
    },
    changeEncoding (column, index) {
      var encoding = this.getEncoding(column.encoding)
      if (encoding === 'integer') {
        column.valueLength = 4
      } else {
        column.valueLength = ''
      }
    },
    changeRawTable: function (column, index) {
      let _this = this
      _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'index', column.index)
      _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'is_sortby', column.is_sortby)
      _this.$set(_this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index], 'is_shardby', column.is_shardby)
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
        _this.convertedRawTable.push({column: rawTable.column, table: rawTable.table, encoding: _this.getEncoding(rawTable.encoding) + ':' + version, valueLength: _this.getLength(rawTable.encoding), index: rawTable.index, is_sortby: rawTable.is_sortby, is_shardby: rawTable.is_shardby})
      })
    },
    getBaseColumnsData: function () {
      console.log(this.rawTable.tableDetail, 998)
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      let _this = this
      _this.modelDesc.dimensions.forEach(function (dimension) {
        dimension.columns.forEach(function (column) {
          let index = 'discrete'
          let sorted = false
          if (_this.modelDesc.partition_desc && dimension.table + '.' + column === _this.modelDesc.partition_desc.partition_date_column) {
            sorted = true
          }
          var columType = _this.modelDesc.columnsDetail[dimension.table + '.' + column] && _this.modelDesc.columnsDetail[dimension.table + '.' + column].datatype
          let encodingVersion = 1
          if (['time', 'date', 'integer'].indexOf(columType) < 0) {
            columType = ''
          }
          if (columType === 'integer') {
            columType = columType + ':4'
            encodingVersion = baseEncodings.getEncodingMaxVersion(columType)
          }
          _this.rawTable.tableDetail.columns.push({
            index: index,
            encoding: columType || 'orderedbytes',
            table: dimension.table,
            column: column,
            encoding_version: encodingVersion,
            is_sortby: sorted,
            is_shardby: false
          })
        })
      })
      this.modelDesc.metrics.forEach(function (measure) {
        let index = 'discrete'
        let sorted = false
        if (_this.modelDesc.partition_desc && measure === _this.modelDesc.partition_desc.partition_date_column) {
          sorted = true
        }
        var columType = _this.modelDesc.columnsDetail[getNameSpace(measure) + '.' + removeNameSpace(measure)] && _this.modelDesc.columnsDetail[getNameSpace(measure) + '.' + removeNameSpace(measure)].datatype
        let encodingVersion = 1
        if (['time', 'date', 'integer'].indexOf(columType) < 0) {
          columType = ''
        }
        if (columType === 'integer') {
          columType = columType + ':4'
          encodingVersion = baseEncodings.getEncodingMaxVersion(columType)
        }
        _this.rawTable.tableDetail.columns.push({
          index: index,
          encoding: columType || 'orderedbytes',

          table: getNameSpace(measure),
          column: removeNameSpace(measure),
          encoding_version: encodingVersion,
          is_sortby: sorted,
          is_shardby: false
        })
      })
      this.initConvertedRawTable()
    },
    currentChange: function (value) {
      this.currentPage = value
      this.initConvertedRawTable()
    }
  },
  computed: {
    rawTableUsable () {
      if (this.cubeDesc && this.cubeDesc.engine_type && (+this.cubeDesc.engine_type === 100 || +this.cubeDesc.engine_type === 99)) {
        return true
      } else {
        return false
      }
    }
  },

  mounted () {
    // this.$dragging.$on('dragend', ({ value }) => {
    //   console.log(value.list, 112233)
    //   Object.assign(this.rawTable.tableDetail.columns, value.list)
    // })
    this.$dragging.$on('dragged', ({ value }) => {
      this.initConvertedRawTable()
    })
    let _this = this
    if (_this.rawTableUsable) {
      if (_this.rawTable.tableDetail && _this.rawTable.tableDetail.columns.length > 0 && this.$store.state.cube.cubeRowTableIsSetting) {
        _this.usedRawTable = true
        _this.initConvertedRawTable()
      } else {
        if (_this.isEdit) {
          // var rawtbaleName = this.cubeDesc.name + (this.cubeDesc.status === 'DRAFT' ? '_draft' : '')
          _this.initConvertedRawTable()
          this.loadRawTable(this.cubeDesc.name).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              if (this.$store.state.cube.cubeRowTableIsSetting) {
                _this.usedRawTable = true
                var rawtbale = this.cubeDesc.is_draft ? data.draft : data.rawTable
                if (rawtbale) {
                  _this.$set(_this.rawTable, 'tableDetail', rawtbale)
                  _this.initConvertedRawTable()
                }
              }
            })
          }).catch((res) => {
            handleError(res, () => {
            })
          })
        }
      }
    }
  },
  locales: {
    'en': {noSupportRawTable: 'Only KAP PLUS Provides Raw Table', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index', ConfigRawTable: 'Config Table Index', 'sortBy': 'SortBy', 'shardBy': 'ShardBy', dragSorted: 'Drag the rows below to sort'},
    'zh-cn': {noSupportRawTable: '只有KAP PLUS 提供Raw Table功能', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引', ConfigRawTable: '配置Table Index', 'sortBy': 'SortBy', 'shardBy': 'ShardBy', dragSorted: '拖动下面的表行进行排序'}
  }
}
</script>
<style lang="less">
@import '../../../less/config.less';
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
  background: transparent;
}
.ksd-common-table .tablebody{
  background: @tableBC;
}
.ksd-common-table .tableheader{
  border-color: @grey-color;
}
.ksd-common-table .tablebody{
  border-color: @grey-color;
}
#table-index{
  .el-input__inner{
    height: 30px;
  }
}
</style>
