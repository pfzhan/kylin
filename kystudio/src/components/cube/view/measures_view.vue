<template>
<div class="measures-view ksd-mt-20 "> 
  <p class="measure-title ksd-lineheight-14">{{$t('measures')}}</p>
  <el-table
    class="ksd-mt-10 measure-table"
    :data="cubeDesc.desc.measures"
    border>
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
      <template slot-scope="scope">
        <span v-if="scope.row.function.expression === 'CORR'">CORR (Beta)</span>
        <span v-else>{{scope.row.function.expression}}</span>
      </template>
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
  <p v-if="!isPlusVersion" class="measure-title ksd-lineheight-14 ksd-mt-20">
    {{$t('advancedColumnFamily')}}
  </p>
  <el-table v-if="!isPlusVersion"
    class="ksd-mt-8 cf-table" border
    :data="cubeDesc.desc.hbase_mapping.column_family">
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
            <div class="measure-select">
              <el-tag size="small" type="primary" v-for="(mr, index) in scope.row.columns[0].measure_refs" :key="index">{{mr}}</el-tag>
            </div>
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
<style lang="less">
@import '../../../assets/styles/variables.less';
  .measures-view {
    .measure-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .measure-table tr {
      th:first-child,td:first-child {
        border-right: 1px solid @line-border-color;
      }
    }
    .cf-table tr {
      th:first-child,td:first-child {
        border-right: 1px solid @line-border-color;
      }
    }
    .measure-select {
      margin-right: 10px;
      padding: 6px 8px;
      border: 1px solid @line-border-color1;
      background-color: @fff;
    }
  }
</style>
