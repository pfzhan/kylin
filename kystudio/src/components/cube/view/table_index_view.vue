<template>
 <div>
  <p v-if="!cubeDesc.rawTable" class="noRawTable">{{$t('noRawTable')}}</p>
  <el-table  v-else
    :data="cubeDesc.rawTable.columns"
    border stripe
    style="width: 100%">
    <el-table-column
      :label="$t('ID')"
      header-align="center"
      align="center"
      width="80">
      <template scope="scope">
        <el-tag class="index-tag">{{scope.$index}}</el-tag>
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
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'tableIndex',
  props: ['cubeDesc'],
  methods: {
    ...mapActions({
      loadRawTable: 'GET_RAW_TABLE',
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
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
    let _this = this
    if (!_this.cubeDesc.rawTable) {
      _this.loadRawTable({cubeName: this.cubeDesc.name, project: this.cubeDesc.project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          var rawData = data.rawTable || data.draft
          _this.$set(_this.cubeDesc, 'rawTable', rawData)
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
    this.loadDataSourceByProject({project: this.cubeDesc.project, isExt: true})
  },
  locales: {
    'en': {noRawTable: 'No Table Index Configuration Information.', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index'},
    'zh-cn': {noRawTable: '无Table Index配置信息。', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引'}
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
