<template>
  <div id="dimensions-view">  
    <div class="title">{{$t('dimensions')}}</div>
    <el-card style="padding: 10px;">
      <el-tag class="tag_margin"
        v-for="(dimension, dim_index) in cubeDesc.desc.dimensions"
        :key="dim_index"
        :type="dimension.derived?'gray':'primary'">
        {{dimension.table+'.'+ (dimension.derived ? dimension.derived&&dimension.derived[0]:dimension.column)}}
      </el-tag>
    </el-card>
    <div class="ksd-mt-20">
      {{$t('kylinLang.cube.optimizeStrategy')}}: 
      <el-tag>{{$t(strategy)}}</el-tag>
    </div>
    <el-tabs v-model="activeName" class="el-tabs--default ksd-mt-40">
      <el-tab-pane name="first">
        <span slot="label">{{$t('aggregationGroups')}}</span>
        {{$t('kylinLang.cube.maxGroupColumn')}} {{dim_cap}}
        <el-table
          :show-header=false
          :data="cubeDesc.desc.aggregation_groups"
          style="width: 100%">
          <el-table-column
            type="index"
            width="50">
          </el-table-column>    
          <el-table-column>
            <template slot-scope="scope">
              <el-row class="row_padding">
                <el-col :span="4">{{$t('Includes')}}</el-col>
                <el-col :span="20">
                  <el-card>
                    <el-tag class="tag_margin" type="primary" v-for="(include, include_index) in scope.row.includes" :key="include_index">{{include}}</el-tag>
                  </el-card>
                </el-col>
              </el-row>
              <el-row class="row_padding">
                <el-col :span="4">{{$t('mandatoryDimensions')}}</el-col>
                <el-col :span="20">
                  <el-card v-if="scope.row.select_rule.mandatory_dims.length>0">
                    <el-tag class="tag_margin" type="primary" v-for="(dim, dim_index) in scope.row.select_rule.mandatory_dims" :key="dim_index">{{dim}}</el-tag>
                  </el-card>
                  </el-col>
              </el-row>
              <el-row class="row_padding">
                <el-col :span="4">{{$t('hierarchyDimensions')}}</el-col>
                <el-col :span="20">
                  <el-col class="row_padding" :span="24" v-for="(hierarchy_dims, hierarchy_index) in scope.row.select_rule.hierarchy_dims" :key="hierarchy_index">
                    <el-card>
                      <el-tag class="tag_margin" type="primary" v-for="(dim, hierarchy_dims_index) in hierarchy_dims" :key="hierarchy_dims_index">{{dim}}</el-tag>
                    </el-card>
                  </el-col>
                </el-col>
              </el-row>
              </el-row>      
              <el-row class="row_padding">
                <el-col :span="4">{{$t('jointDimensions')}}</el-col>
                <el-col :span="20">
                  <el-col class="row_padding" :span="24" v-for="(joint_dims, joint_index) in scope.row.select_rule.joint_dims" :key="joint_index">
                    <el-card>
                      <el-tag class="tag_margin" type="primary" v-for="(dim, joint_dims_index) in joint_dims" :key="joint_dims_index">{{dim}}</el-tag>
                    </el-card>
                  </el-col>
                </el-col>                    
              </el-row>          
            </template>
          </el-table-column>                 
        </el-table>
      </el-tab-pane>
      <el-tab-pane name="second">
        <span slot="label">Rowkeys</span>
        <el-table
          :data="cubeDesc.desc.rowkey.rowkey_columns"
          border
          style="width: 100%">
          <el-table-column
            type="index"
            :label="$t('ID')"
              header-align="center"
              align="center"
              width="55">
          </el-table-column>
          <el-table-column
              show-overflow-tooltip
              property="column"
              :label="$t('column')">
          </el-table-column>       
          <el-table-column
              show-overflow-tooltip
              :label="$t('encoding')"
              header-align="center"
              align="center"
              width="130">
              <template slot-scope="scope">
                {{getEncoding(scope.row.encoding)}}
              </template>
          </el-table-column>    
          <el-table-column
              show-overflow-tooltip
              :label="$t('length')"
              header-align="center"
              align="center"
              width="110">
              <template slot-scope="scope">
                {{getLength(scope.row.encoding)}}          
              </template>        
          </el-table-column>    
          <el-table-column
              show-overflow-tooltip
              :label="$t('shardBy')"
              header-align="center"
              align="center"
              width="110">
              <template slot-scope="scope">
                {{scope.row.isShardBy}}
              </template>
          </el-table-column>    
          <el-table-column
              show-overflow-tooltip
              :label="$t('dataType')"
              header-align="center"
              align="center"
              width="110">
              <template slot-scope="scope">
                {{cubeDesc.modelDesc.columnsDetail[scope.row.column] && cubeDesc.modelDesc.columnsDetail[scope.row.column].datatype}}
              </template>
          </el-table-column>    
          <el-table-column
              show-overflow-tooltip
              :label="$t('cardinality')"
              header-align="center"
              align="center"
              width="110">
              <template slot-scope="scope">
                {{cubeDesc.modelDesc.columnsDetail[scope.row.column] && cubeDesc.modelDesc.columnsDetail[scope.row.column].cardinality}}
              </template>
          </el-table-column>                                           
        </el-table>
      </el-tab-pane>
    </el-tabs> 
  </div>  
</template>
<script>
export default {
  name: 'dimensions',
  props: ['cubeDesc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project'),
      activeName: 'first'
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
  computed: {
    dim_cap () {
      return this.cubeDesc.desc.aggregation_groups && this.cubeDesc.desc.aggregation_groups[0].select_rule.dim_cap || 0
    },
    strategy () {
      if (this.cubeDesc.desc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'auto' || this.cubeDesc.desc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'mixed') {
        return 'defaultOriented'
      } else if (this.cubeDesc.desc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'default') {
        return 'dataOriented'
      } else if (this.cubeDesc.desc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'whitelist') {
        return 'businessOriented'
      }
      return ''
    }
  },
  locales: {
    'en': {name: 'Name', type: 'Type', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', tableAlias: 'Table Alias', aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', ID: 'ID', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', dimensions: 'Dimensions', dimensionOptimizations: 'Dimension Optimizations', businessOriented: 'Business Oriented', defaultOriented: 'Default', dataOriented: 'Data Oriented'},
    'zh-cn': {name: '名称', type: '类型', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', tableAlias: '表别名', aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', ID: 'ID', encoding: '编码', length: '长度', shardBy: 'Shard By', dataType: '数据类型', dimensions: '维度', dimensionOptimizations: '维度优化', businessOriented: '业务优先', defaultOriented: '默认', dataOriented: '模型优先'}
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
  #dimensions-view{
    .el-card{
      border-color: @grey-color;
    }
    .title{
      text-transform: capitalize;
      font-weight: bold;
      padding-top: 10px;
    }
  }
</style>
