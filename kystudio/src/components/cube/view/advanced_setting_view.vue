<template>
<div>  
  <el-row>
    <el-col :span="24">{{$t('aggregationGroups')}}</el-col>
  </el-row>
  <el-table
    :show-header=false
    :data="desc.aggregation_groups"
    style="width: 100%">
    <el-table-column
      type="index"
      width="50">
    </el-table-column>    
    <el-table-column>
      <template scope="scope">
        <el-row>
          <el-col :span="8">{{$t('Includes')}}</el-col>
          <el-col :span="16"><el-tag type="primary" v-for="include in scope.row.includes">{{include}}</el-tag></el-col>
        </el-row>
        <el-row>
          <el-col :span="8">{{$t('mandatoryDimensions')}}</el-col>
          <el-col :span="16"><el-tag type="primary" v-for="dim in scope.row.select_rule.mandatory_dims">{{dim}}</el-tag></el-col>
        </el-row>
        <el-row>
          <el-col :span="8">{{$t('hierarchyDimensions')}}</el-col>
          <el-col :span="16">
            <el-col :span="24" v-for="hierarchy_dims in scope.row.select_rule.hierarchy_dims">
              <el-tag type="primary" v-for="dim in hierarchy_dims">{{dim}}</el-tag>
            </el-col>
          </el-col>
        </el-row>
        </el-row>      
        <el-row>
          <el-col :span="8">{{$t('jointDimensions')}}</el-col>
          <el-col :span="16">
            <el-col :span="24" v-for="joint_dims in scope.row.select_rule.joint_dims">
              <el-tag type="primary" v-for="dim in joint_dims">{{dim}}</el-tag>
            </el-col>
          </el-col>                    
        </el-row>          
      </template>
    </el-table-column>                 
  </el-table>

  <el-row>
    <el-col :span="24">Rowkeys</el-col>
  </el-row>
  <el-table
    :data="desc.rowkey.rowkey_columns"
    style="width: 100%">
    <el-table-column
      type="index"
      :label="$t('ID')">
    </el-table-column>
    <el-table-column
        property="column"
        :label="$t('column')">
    </el-table-column>       
    <el-table-column
        :label="$t('encoding')">
        <template scope="scope">
          {{getEncoding(scope.row.encoding)}}
        </template>
    </el-table-column>    
    <el-table-column
        :label="$t('length')">
        <template scope="scope">
          {{getLength(scope.row.encoding)}}          
        </template>        
    </el-table-column>    
    <el-table-column
        :label="$t('shardBy')">
        <template scope="scope">
          {{scope.row.isShardBy}}
        </template>
    </el-table-column>    
    <el-table-column
        :label="$t('dataType')">
    </el-table-column>    
    <el-table-column
        :label="$t('cardinality')">
    </el-table-column>                                        
  </el-table>

  <el-row>
    <el-col :span="24">{{$t('advancedDictionaries')}}</el-col>
  </el-row>
  <el-table
    :data="desc.dictionaries"
    style="width: 100%">
    <el-table-column
        property="column"
        :label="$t('column')">
    </el-table-column>       
    <el-table-column
        property="builder"
        :label="$t('builderClass')">
    </el-table-column>    
    <el-table-column
        property="reuse"
        :label="$t('reuse')">        
    </el-table-column>                                           
  </el-table>




  <el-row>
    <el-col :span="24">{{$t('advancedColumnFamily')}}</el-col>
  </el-row> 
  <el-table
    :data="desc.hbase_mapping.column_family"
    style="width: 100%">
    <el-table-column
        property="name"
        :label="$t('columnFamily')">
    </el-table-column>       
    <el-table-column
        :label="$t('measures')">
        <template scope="scope">
          <el-col :span="24">
            <el-tag type="primary" v-for="mr in scope.row.columns[0].measure_refs">{{mr}}</el-tag>
          </el-col>
        </template>
    </el-table-column>                                             
  </el-table>   
</div>  
</template>
<script>
export default {
  name: 'AdvancedSetting',
  props: ['desc'],
  data () {
    return {
      rowkeyColumns: []
    }
  },
  methods: {
    getEncoding: function (encode) {
      let code = encode.split(':')
      return code[0]
    },
    getLength: function (encode) {
      let code = encode.split(':')
      return code[1]
    }
  },
  locales: {
    'en': {aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', ID: 'ID', column: 'Column', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', cardinality: 'Cardinality', advancedDictionaries: 'Advanced Dictionaries', builderClass: 'Builder Class', reuse: 'Reuse', advancedColumnFamily: 'Advanced ColumnFamily', columnFamily: 'ColumnFamily', measures: 'Measures'},
    'zh-cn': {aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', ID: 'ID', column: '列', encoding: '编码', length: '长度h', shardBy: 'Shard By', dataType: '数据类型', cardinality: '基数', advancedDictionaries: '高级字典', builderClass: '构造类', reuse: '复用', advancedColumnFamily: '高级列族', columnFamily: '列族', measures: '度量'}
  }
}
</script>
<style scoped="">

</style>
