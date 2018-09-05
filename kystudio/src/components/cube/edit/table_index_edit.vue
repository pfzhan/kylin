<template>
 <div class="table-index-edit ksd-mrl-20">
  <p v-if="!rawTableUsable">{{$t('noSupportRawTable')}}</p>
  <div v-else>
    <span class="table-index-title ksd-mr-10">{{$t('ConfigRawTable')}}</span>
    <el-switch
      v-model="usedRawTable" :disabled="lockRawTable" @change="changeUsed()"
      active-text="OFF"
      inactive-text="ON">
    </el-switch>
    <el-table v-if="usedRawTable"
    :data="convertedRawTable"
    :row-class-name="tableRowClassName"
    border class="ksd-mt-14">
     <el-table-column
      :label="$t('ID')"
      header-align="center"
      align="center"
      width="80">
      <template slot-scope="scope">
        <span>{{scope.$index + 1 + 15*(currentPage-1)}}</span>
      </template>
     </el-table-column>
     <el-table-column
      show-overflow-tooltip
      :label="$t('column')"
      header-align="center"
      prop="column"
      align="center">
     </el-table-column>
     <el-table-column
        show-overflow-tooltip
        :label="$t('dataType')"
        prop="dataType"
        header-align="center"
        align="center"> 
     </el-table-column>  
     <el-table-column
      show-overflow-tooltip
        :label="$t('tableAlias')"
        prop="table"
        header-align="center"
        align="center">   
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        :label="$t('Encoding')"
        header-align="center"
        align="center">   
            <template slot-scope="scope">
              <el-select size="small" v-model="scope.row.encoding" :disabled="lockRawTable" @change=" changeEncoding(scope.row, scope.$index);changeRawTable(scope.row, scope.$index);">
                <el-option
                    v-for="(item, index) in scope.row.selectEncoding" :key="index"
                   :label="item.name"
                   :value="item.name + ':' + item.version">
                   <el-tooltip effect="dark" :content="$t('kylinLang.cube.'+$store.state.config.encodingTip[item.name])" placement="top" :enterable="false" popper-class="no-event">
                     <span style="float: left;;width: 90%">{{ item.name }}</span>
                     <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-if="item.version>1">{{ item.version }}</span>
                  </el-tooltip>
                </el-option>              
              </el-select>
            </template>
      </el-table-column>  
      <el-table-column
        show-overflow-tooltip
        :label="$t('Length')"
        header-align="center"
        align="center">   
        <template slot-scope="scope">
          <el-input size="small" v-model="scope.row.valueLength" :disabled="lockRawTable || scope.row.encoding.indexOf('dict')>=0||scope.row.encoding.indexOf('date')>=0||scope.row.encoding.indexOf('time')>=0||scope.row.encoding.indexOf('var')>=0||scope.row.encoding.indexOf('orderedbytes')>=0||scope.row.encoding.indexOf('boolean')>=0"  @change="changeRawTable(scope.row, scope.$index)"></el-input>  
        </template>  
    </el-table-column>

     <el-table-column
        :label="$t('sortBy')"
        header-align="center"
        align="center">   
            <template slot-scope="scope">
              <el-select size="small" v-model="scope.row.is_sortby" :disabled="lockRawTable" @change="changeRawTable(scope.row, scope.$index)">
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
            <template slot-scope="scope">
              <el-select size="small" v-model="scope.row.is_shardby" :disabled="lockRawTable" @change="changeRawTable(scope.row, scope.$index)">
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
            <template slot-scope="scope">
              <el-select size="small" v-model="scope.row.index" :disabled="lockRawTable" @change="changeRawTable(scope.row, scope.$index)">
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


     
    
     <div class="sortby-table ksd-mt-20" v-if="usedRawTable && rawTable.tableDetail && rawTable.tableDetail.columns.length">
       <el-row class="sortby-header">
         <el-col :span="1">{{$t('ID')}}</el-col>
         <el-col :span="6">{{$t('column')}}</el-col>
         <el-col :span="4">{{$t('dataType')}}</el-col>
         <el-col :span="5">{{$t('tableAlias')}}</el-col>
         <el-col :span="2">{{$t('Encoding')}}</el-col>
         <el-col :span="4">{{$t('Length')}}</el-col>
         <el-col :span="2">{{$t('Index')}}</el-col>
       </el-row>
        <el-row class="sortby-body" v-if="row.is_sortby===true" v-for="(row, index) in rawTable.tableDetail.columns" :key="row.column" v-dragging="{ item: row, list: rawTable.tableDetail.columns, group: 'row' }">
          <el-col :span="1" >{{index+1}}</el-col>
          <el-col :span="6">{{row.column}}</el-col>
          <el-col :span="4">
              {{modelDesc.columnsDetail[row.table+'.'+row.column]&&modelDesc.columnsDetail[row.table+'.'+row.column].datatype}}
          </el-col>
          <el-col :span="5"> 
            {{row.table}}
          </el-col>
          <el-col :span="2">
            {{row.encoding.split(':')[0]}} 
          </el-col>
          <el-col :span="4">
            {{row.encoding.split(':')[1]}}
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
import { loadBaseEncodings } from '../../../util/business'
import { objectClone } from '../../../util/index'
export default {
  name: 'tableIndex',
  props: ['cubeDesc', 'modelDesc', 'isEdit', 'rawTable', 'cubeInstance'],
  data () {
    return {
      usedRawTable: false,
      totalRawTable: 0,
      currentPage: 1,
      convertedRawTable: [],
      ST: null,
      rawTableDetail: [],
      rawTableIndexOptions: ['discrete', 'fuzzy'],
      selectEncodingCache: {},
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
        this.$nextTick(() => {
          this.rawTable.tableDetail.columns.splice(0, this.rawTable.tableDetail.columns.length)
        })
      } else {
        this.$store.state.cube.cubeRowTableIsSetting = true
        this.$nextTick(() => {
          this.getBaseColumnsData()
          setTimeout(() => {
            this.initConvertedRawTable()
          }, 500)
        })
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
    initEncodingType: function (columnEncoding, columnType) {
      let _this = this
      let datatype = columnType || ''
      if (this.selectEncodingCache[datatype]) {
        return this.selectEncodingCache[datatype]
      }
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      let filterEncodings = baseEncodings.filterByColumnType(datatype)
      filterEncodings = baseEncodings.addEncoding('orderedbytes', 1)
      filterEncodings = baseEncodings.removeEncoding('dict')
      if (this.isEdit) {
        let _encoding = _this.getEncoding(columnEncoding)
        let _version = parseInt(_this.getVersion(columnEncoding))
        let addEncodings = baseEncodings.addEncoding(_encoding, _version)
        addEncodings = baseEncodings.removeEncoding('dict')
        this.selectEncodingCache[datatype] = addEncodings
        return addEncodings
      } else {
        this.selectEncodingCache[datatype] = filterEncodings
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
      var curRowTable = this.rawTable.tableDetail.columns[15 * (this.currentPage - 1) + index]
      this.$set(curRowTable, 'index', column.index)
      this.$set(curRowTable, 'is_sortby', column.is_sortby)
      this.$set(curRowTable, 'is_shardby', column.is_shardby)
      this.$set(curRowTable, 'encoding_version', this.getVersion(column.encoding))
      var columnEncoding = this.getEncoding(column.encoding)
      if (column.valueLength) {
        this.$set(curRowTable, 'encoding', columnEncoding + ':' + column.valueLength)
      } else {
        this.$set(curRowTable, 'encoding', columnEncoding)
      }
      if (column.encoding.indexOf('dict') >= 0 || column.encoding.indexOf('date') >= 0 || column.encoding.indexOf('time') >= 0 || column.encoding.indexOf('var') >= 0 || column.encoding.indexOf('orderedbytes') >= 0) {
        this.$set(column, 'valueLength', null)
      }
    },
    initConvertedRawTable: function () {
      this.totalRawTable = this.rawTable.tableDetail.columns.length
      let rawTableDetail = this.rawTable.tableDetail.columns.slice(15 * (this.currentPage - 1), 15 * (this.currentPage))
      this.convertedRawTable.splice(0, this.convertedRawTable.length)
      rawTableDetail.forEach((rawTable) => {
        let version = rawTable.encoding_version || 1
        var rawObj = {column: rawTable.column, table: rawTable.table, encoding: this.getEncoding(rawTable.encoding) + ':' + version, valueLength: this.getLength(rawTable.encoding), index: rawTable.index, is_sortby: rawTable.is_sortby, is_shardby: rawTable.is_shardby}
        rawObj.dataType = this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[rawObj.table + '.' + rawObj.column] && this.modelDesc.columnsDetail[rawObj.table + '.' + rawObj.column].datatype
        rawObj.selectEncoding = this.initEncodingType(rawObj.encoding, rawObj.dataType)
        this.convertedRawTable.push(rawObj)
      })
    },
    getBaseColumnsData: function () {
      this.rawTable.tableDetail.columns = objectClone(this.$store.state.cube.cubeRawTableBaseData[this.modelDesc.project + '' + this.modelDesc.name])
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
    },
    lockRawTable () {
      return this.cubeInstance && this.cubeInstance.segments && this.cubeInstance.segments.length > 0
    }
  },

  mounted () {
    this.$dragging.$on('dragged', ({ value }) => {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.initConvertedRawTable()
      }, 1000)
    })
    if (this.rawTableUsable) {
      if (this.rawTable.tableDetail && this.rawTable.tableDetail.columns.length > 0 && this.$store.state.cube.cubeRowTableIsSetting) {
        this.usedRawTable = true
        this.initConvertedRawTable()
      } else {
        if (this.isEdit) {
          this.initConvertedRawTable()
        }
      }
    }
  },
  locales: {
    'en': {noSupportRawTable: 'Only KAP PLUS Provides Raw Table', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index', ConfigRawTable: 'Config Table Index', 'sortBy': 'Sort By', 'shardBy': 'Shard By', dragSorted: 'Drag the rows below to sort'},
    'zh-cn': {noSupportRawTable: '只有KAP PLUS 提供Raw Table功能', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引', ConfigRawTable: '配置Table Index', 'sortBy': 'Sort By', 'shardBy': 'Shard By', dragSorted: '拖动下面的表行进行排序'}
  }
}
</script>
<style lang="less">
@import '../../../assets/styles/variables.less';
  .table-index-edit {
    margin-bottom: 50px;
    .table-index-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .sortby-table {
      border-collapse:collapse;
      color: @text-title-color;
      border-top:solid 1px @line-border-color;
      border-left:solid 1px @line-border-color;
      .sortby-header {
        background: @modeledit-bg-color;
        height: 47px;
        text-align: left;
        .el-col {
          line-height: 28px;
          border-right:solid 1px @line-border-color;
          border-bottom:solid 1px @line-border-color;
          padding: 9px 10px;
          border-right:solid 1px @line-border-color;
        }
      }
      .sortby-body {
        cursor: move;
        height: 47px;
        .el-col {
          height: 47px;
          line-height: 28px;
          padding: 9px 10px;
          border-right:solid 1px @line-border-color;
          border-bottom:solid 1px @line-border-color;
          word-wrap: break-word;
          white-space:nowrap;
          text-overflow: ellipsis;
          overflow: hidden;
        }
      }
    }
  }
</style>
