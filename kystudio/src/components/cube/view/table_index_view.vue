<template>
 <div>
  <p v-if="!cubeDesc.rawTable" class="noRawTable">{{$t('noRawTable')}}</p>
  <div v-else>
    <el-table  
      :data="currentTableIndex"
      border stripe
      style="width: 100%">
      <el-table-column
        :label="$t('ID')"
        header-align="center"
        align="center"
        width="80">
        <template scope="scope">
          <el-tag class="index-tag">{{scope.$index + 1 + 15*(currentPage-1)}}</el-tag>
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
          {{cubeDesc.modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column]&&cubeDesc.modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column].datatype}}
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
            {{getEncoding(scope.row.encoding)}}
          </template>
      </el-table-column>  
      <el-table-column
          :label="$t('Length')"
          header-align="center"
          align="center">   
          <template scope="scope">
            {{getLength(scope.row.encoding)}}          
          </template> 
      </el-table-column>
      <el-table-column
          label="Sorted By"
          header-align="center"
          align="center">   
              <template scope="scope">
              {{scope.row.is_sortby}}
              </template>
        </el-table-column>   
      <el-table-column
          label="Shard By"
          header-align="center"
          align="center">   
          <template scope="scope">
            {{scope.row.is_shardby}}          
          </template> 
      </el-table-column>     
      <el-table-column
          :label="$t('Index')"
          prop="index"
          header-align="center"
          align="center">   
      </el-table-column>       
    </el-table> 
    <pager ref="pager" :perPageSize="15" :totalSize="cubeDesc.rawTable.columns.length"  v-on:handleCurrentChange='currentChange' ></pager>
   </div>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'tableIndex',
  props: ['cubeDesc'],
  data () {
    return {
      currentTableIndex: [],
      currentPage: 1
    }
  },
  methods: {
    ...mapActions({
      getRawTableDesc: 'GET_RAW_TABLE_DESC'
    }),
    currentChange: function (value) {
      this.currentPage = value
      this.currentTableIndex = this.cubeDesc.rawTable.columns.slice(15 * (value - 1), 15 * value)
    },
    getEncoding: function (encode) {
      let code = encode.split(':')
      return code[0]
    },
    getLength: function (encode) {
      let code = encode.split(':')
      return code[1]
    }
  },
  created () {
    if (!this.cubeDesc.rawTable) {
      this.getRawTableDesc({cubeName: this.cubeDesc.name, project: this.cubeDesc.project, version: {version: this.cubeDesc.cubeVersion || null}}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          var rawData = data.rawTable || data.draft
          this.$set(this.cubeDesc, 'rawTable', rawData)
          this.currentChange(1)
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
  },
  locales: {
    'en': {noRawTable: 'No Raw Table Configuration Information', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index'},
    'zh-cn': {noRawTable: '无Raw Table配置信息', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
 .row_padding {
  padding-top: 5px;
  padding-bottom: 5px;
 }
 .tag_margin {
  margin-left: 4px;
  margin-bottom: 2px;
  margin-top: 2px;
  margin-right: 4px;
 }
 .dropdown ul {
  height: 150px;
  overflow: scroll;
 }
 .noRawTable {
  height: 60px;
  line-height: 60px;
  border:1px solid #393e53;
  border-radius: 2px;
  text-align: center;
 }
 .index-tag{
    background: transparent;
 }
</style>
