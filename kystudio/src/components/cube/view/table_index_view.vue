<template>
 <div>
  <p v-if="!cubeDesc.rawTable">{{$t('noRawTable')}}</p>
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
        <el-tag>{{scope.$index}}</el-tag>
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
        {{cubeDesc.modelDesc.columnsDetail[scope.row.table+'.'+scope.row.column].datatype}}
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
      loadRawTable: 'GET_RAW_TABLE'
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
      _this.loadRawTable(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cubeDesc, 'rawTable', data)
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            duration: 3000
          })
          if (status === 404) {
            // _this.$router.replace('access/login')
          }
        })
      })
    }
  },
  locales: {
    'en': {noRawTable: 'No Raw Table Configuration Information', tableIndex: 'Table Index', ID: 'ID', column: 'Column', dataType: 'Data Type', tableAlias: 'Table Alias', Encoding: 'Encoding', Length: 'Length', Index: 'Index'},
    'zh-cn': {noRawTable: '无Raw Table配置信息', tableIndex: '表索引', ID: 'ID', column: '列', dataType: '数据类型', tableAlias: '表别名', Encoding: '编码', Length: '长度', Index: '索引'}
  }
}
</script>
<style scoped="">
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
</style>
