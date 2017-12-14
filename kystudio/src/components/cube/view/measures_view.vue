<template>
<div> 
  <div class="title" style="padding: 5px 0 1px 0;">{{$t('measures')}}</div>
  <el-table
    :data="cubeDesc.desc.measures"
    border stripe 
    style="width: 100%">
    <el-table-column
      show-overflow-tooltip
      property="name"
      :label="$t('name')"
      header-align="center"
      align="center">
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      property="function.expression"
      :label="$t('expression')"
      align="center"
      header-align="center"
      width="180">
    </el-table-column>    
    <el-table-column
      show-overflow-tooltip
      :label="$t('parameters')"
      header-align="center"
      align="center">
      <template slot-scope="scope">
        <parameter_tree :measure="scope.row">
        </parameter_tree>  
      </template>
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      :label="$t('datatype')"
      header-align="center"
      align="center"
      width="110"> 
      <template slot-scope="scope">
        <span v-if="cubeDesc.modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{cubeDesc.modelDesc.columnsDetail[scope.row.function.parameter.value].datatype}}
        </span>
      </template>  
    </el-table-column>  
    <el-table-column
      show-overflow-tooltip
      :label="$t('comment')"
      header-align="center"
      align="center"
      width="110">   
      <template slot-scope="scope">
        <span v-if="cubeDesc.modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{cubeDesc.modelDesc.columnsDetail[scope.row.function.parameter.value].comment}}
        </span>
      </template>  
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      property="function.returntype"    
      :label="$t('returnType')"
      header-align="center"
      align="center"
      width="120">
    </el-table-column>                  
  </el-table>
  <el-row v-if="!isPlusVersion">
    <el-col :span="24" style="padding: 15px 0 1px 0;">{{$t('advancedColumnFamily')}}</el-col>
  </el-row> 
  <el-table v-if="!isPlusVersion"
    :data="cubeDesc.desc.hbase_mapping.column_family"
    style="width: 100%">
    <el-table-column
        show-overflow-tooltip
        property="name"
        :label="$t('columnFamily')"
        width="150">
    </el-table-column>       
    <el-table-column
        show-overflow-tooltip
        :label="$t('measures')">
        <template slot-scope="scope">
          <el-col :span="24">
            <el-tag class="tag_margin" type="primary" v-for="(mr, index) in scope.row.columns[0].measure_refs" :key="index">{{mr}}</el-tag>
          </el-col>
        </template>
    </el-table-column>                                             
  </el-table>  
</div>
</template>
<script>
import parameterTree from '../../common/parameter_tree'
export default {
  name: 'measures',
  props: ['cubeDesc'],
  components: {
    'parameter_tree': parameterTree
  },
  computed: {
    isPlusVersion () {
      var kapVersionInfo = this.$store.state.system.serverAboutKap
      return kapVersionInfo && kapVersionInfo['kap.version'] && kapVersionInfo['kap.version'].indexOf('Plus') !== -1
    }
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', parameters: 'Parameters', datatype: 'Datatype', comment: 'Comment', returnType: 'Return Type', advancedDictionaries: 'Advanced Dictionaries', builderClass: 'Builder Class', reuse: 'Reuse', advancedColumnFamily: 'Advanced ColumnFamily', columnFamily: 'ColumnFamily', measures: 'Measures'},
    'zh-cn': {name: '名称', expression: '表达式', parameters: '参数', datatype: '数据类型', comment: '注释', returnType: '返回类型', advancedDictionaries: '高级字典', builderClass: '构造类', reuse: '复用', advancedColumnFamily: '高级列簇', columnFamily: '列簇', measures: '度量'}
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
