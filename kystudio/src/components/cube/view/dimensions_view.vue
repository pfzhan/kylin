<template>
  <div class="dimensions-view ksd-mt-20 ">  
    <p class="dimension-title ksd-lineheight-14">{{$t('dimensions')}}</p>
    <div class="ksd-mt-10 dimension-select">
      <el-tag size="small" class="ksd-mrl-2"
        v-for="(dimension, dim_index) in cubeDesc.desc.dimensions"
        :key="dim_index"
        :type="dimension.derived?'warning':''">
        {{dimension.table+'.'+ (dimension.derived ? dimension.derived&&dimension.derived[0]:dimension.column)}}
      </el-tag>
    </div>
    <div class="ksd-mtb-30">
      <span class="dimension-title ksd-mr-10">{{$t('kylinLang.cube.optimizeStrategy')}}: </span>
      <el-select size="medium" v-model="cubeDesc.desc.override_kylin_properties['kap.smart.conf.aggGroup.strategy']" :disabled="true">
        <el-option
          v-for="item in strategyOptions"
          :key="item.value"
          :label="$t(item.name)"
          :value="item.value">
        </el-option>
      </el-select>
    </div>
    <div class="ky-dashed ksd-mt-20 ksd-mb-20"></div>
    <el-tabs v-model="activeName" class="ksd-mt-20">
      <el-tab-pane name="first">
        <span slot="label">{{$t('aggregationGroups')}}</span>
        <p class="ksd-lineheight-14 ksd-mb-20">{{$t('kylinLang.cube.maxGroupColumn')}}: {{dim_cap}}</p>

        <el-row class="ksd-mb-10" v-for="(group, group_index) in cubeDesc.desc.aggregation_groups" :key="group_index" style="border-bottom: 0;">
          <el-col :span="24">
            <el-card class="agg-card">
              <p class="dimension-title ksd-lineheight-18" slot="header">{{$t('aggregationGroups')}}—{{group_index+1}}</p>
              <el-row class="ksd-mtb-10" :gutter="14">
                <el-col :span="24">
                  <p class="ksd-lineheight-14">{{$t('Includes')}}(<span>{{group.includes.length}}</span>)</p>
                </el-col>
              </el-row>
              <el-row class="ksd-mb-10" :gutter="14">
                <el-col :span="24" class="includes_tag">
                  <div class="dimension-select" ref="includesSelect">
                    <el-tag class="ksd-mrl-2" :class="{useDimension: updateIncludesDimUsed(group_index, include)}" size="small" type="primary" v-for="(include, include_index) in group.includes" :key="include_index">{{include}}</el-tag>
                  </div>
                </el-col>
              </el-row>
              <el-row class="ksd-mb-10 ksd-mt-20" :gutter="14">
                <el-col :span="24">
                  <p class="ksd-lineheight-14">{{$t('mandatoryDimensions')}}</p>
                </el-col>
              </el-row>
              <el-row :gutter="14">
                <el-col :span="24" >
                  <div class="dimension-select">
                    <el-tag class="ksd-mrl-2" size="small" type="primary" v-for="(dim, dim_index) in group.select_rule.mandatory_dims" :key="dim_index">{{dim}}</el-tag>
                  </div> 
                </el-col>
              </el-row>

              <el-row class="ksd-mb-10 ksd-mt-20">
                <el-col :span="24">
                  <p class="ksd-lineheight-14">{{$t('hierarchyDimensions')}}</p>
                </el-col>
              </el-row>
              <el-row class="ksd-mb-10" :gutter="14" v-if="group.select_rule.hierarchy_dims.length <= 0">
                <el-col :span="24" >
                  <div class="dimension-select">
                  </div> 
                </el-col>
              </el-row>               
              <el-row class="ksd-mb-10" :gutter="14" v-else v-for="(hierarchy_dims, hierarchy_index) in group.select_rule.hierarchy_dims" :key="hierarchy_index">
                <el-col :span="24" >
                  <div class="dimension-select">
                    <el-tag class="ksd-mrl-2" size="small" type="primary" v-for="(dim, dim_index) in hierarchy_dims" :key="dim_index">{{dim}}</el-tag>
                  </div> 
                </el-col>
              </el-row> 

              <el-row class="ksd-mt-20 ksd-mb-10" :gutter="14">
                <el-col :span="24">{{$t('jointDimensions')}}</el-col>
              </el-row>

              <el-row class="ksd-mb-10" :gutter="14" v-if="group.select_rule.joint_dims.length <= 0">
                <el-col :span="24" >
                  <div class="dimension-select">
                  </div> 
                </el-col>
              </el-row> 
              <el-row class="ksd-mb-10" :gutter="14" v-else v-for="(joint_dims, joint_index) in group.select_rule.joint_dims" :key="joint_index">
                <el-col :span="24" >
                  <div class="dimension-select">
                    <el-tag class="ksd-mrl-2" size="small" type="primary" v-for="(dim, dim_index) in joint_dims" :key="dim_index">{{dim}}</el-tag>
                  </div> 
                </el-col>
              </el-row>            
            </el-card>
          </el-col>
        </el-row>


      </el-tab-pane>
      <el-tab-pane name="second">
        <span slot="label">Rowkeys</span>
        <el-table
          class="rowkeys-table"
          :data="cubeDesc.desc.rowkey.rowkey_columns"
          border>
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
      activeName: 'first',
      strategyOptions: [
        {value: 'auto', name: 'defaultOriented'},
        {value: 'default', name: 'dataOriented'},
        {value: 'whitelist', name: 'businessOriented'}
      ]
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
    },
    updateIncludesDimUsed (index, dimension) {
      let isUsed = false
      if (this.cubeDesc.desc.aggregation_groups[index].select_rule.mandatory_dims.indexOf(dimension) >= 0) {
        isUsed = true
      }
      this.cubeDesc.desc.aggregation_groups[index].select_rule.hierarchy_dims.forEach((dim) => {
        if (dim.indexOf(dimension) >= 0) {
          isUsed = true
        }
      })
      this.cubeDesc.desc.aggregation_groups[index].select_rule.joint_dims.forEach((dim) => {
        if (dim.indexOf(dimension) >= 0) {
          isUsed = true
        }
      })
      return isUsed
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
@import '../../../assets/styles/variables.less';
  .dimensions-view{
    .dimension-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .dimension-select {
      padding: 10px;
      border: 1px solid @line-border-color1;
      background-color: @fff;
    }
    .agg-card {
      border: 1px solid @text-placeholder-color;
      box-shadow: 0px 2px 4px 0px @text-placeholder-color,
                  0px 0px 6px 0px @text-placeholder-color;
      .dimension-select {
        padding: 6px 8px;
        min-height: 36px
      }
      .el-card__header {
        padding: 10px 20px;
        background-color: @grey-4;
      }
      .el-tag {
        background: @base-color-11;
        color: @fff;
        .el-icon-close {
          color: @fff;
        }
      }
      .includes_tag {
        .el-tag {
          background: @base-color-10;
          border:1px solid @base-color;
          color: @base-color;
          .el-icon-close {
            color: @base-color;
          }
          .el-icon-close:hover {
            color: @fff;
          }
        }
        [data-tag=useDimension], .useDimension {
          background: @base-color-11;
          color: @fff;
        }
      }
    }
    .rowkeys-table tr {
      th:first-child,td:first-child {
        border-right: 1px solid @line-border-color1;
      }
    }
  }
</style>
