<template>
<div class="dimensionBox">
  <el-row>
    <el-col :span="18" style="padding-right: 30px;border-right: 1px solid #393e53;padding-bottom: 30px;margin-top: -15px;padding-top: 15px;">  
    <!-- 以前的输入sql -->
      <!-- <el-row>
        <el-col :span="24">
          <el-button type="primary"  @click.native="collectSql" :disabled="isReadyCube" >{{$t('collectsqlPatterns')}}</el-button>
        </el-col>
      </el-row> -->
      <!-- <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div> -->
      <el-row class="row_padding border_bottom" style="border: none;">
        <el-row class="row_padding">
          <el-col :span="24" style="font-size: 14px;">
            <span>{{$t('dimensions')}}</span>
            <div style="float: right;width: 220px;">
              <ul class="dimension-type">
                <li><div class="normal"></div></li>
                <li>Normal</li>
              </ul>
              <ul class="dimension-type" style="float: right;">
                <li><div class="direved"></div></li>
                <li>Direved</li>
              </ul>
            </div>
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="24">
            <el-button type="blue" @click.native="addDimensions" :disabled="isReadyCube" >
              +&nbsp;{{$t('dimensions')}}
            </el-button>
          </el-col>
        </el-row>
        <el-row class="row_padding" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
        <el-col :span="24" style="padding: 15px; background: #2f3243;">
          <el-card class="ksd_noshadow" style="border: none;background: #292b38;padding: 10px;">
            <el-tag class="tag_margin" style="cursor:pointer;"
              @click.native="showDetail(dimension.table+'.'+dimension.name)"
              v-for="(dimension, index) in cubeDesc.dimensions"
              :key="index" 
              :class="{ active: (dimension.table+'.'+dimension.name)===isActiveItem }"
              :type="dimension.derived?'gray':'primary'">
              {{dimension.table+'.'+dimension.name}}
            </el-tag>
          </el-card>
        </el-col>
        </el-row>
        <div class="line" style="margin-bottom: -15px;margin-right: -30px;margin-left: -30px;"></div>
        <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
        <div style="font-size: 14px;">
          {{$t('dimensionOptimizations')}}
          <kap-common-popover class='d-tip'>
            <div slot="content">
              <ul>
                <li>{{$t('dO')}}</li>
              </ul>
            </div>
            <icon name="question-circle-o"></icon>
          </kap-common-popover>
        </div>
        <div style="margin-top: 20px;">
          <el-button type="blue" icon="menu" @click.native="cubeSuggestions" :disabled="isReadyCube">{{$t('cubeSuggestion')}}</el-button>
          <el-button type="default" icon="setting" @click.native="resetDimensions" :disabled="isReadyCube">{{$t('resetDimensions')}}</el-button>
        </div>
      </el-row>
      <!-- 旧版按钮 -->
      <!-- <el-row class="row_padding border_bottom borderLeft">
        <el-col :span="5">
          <el-button type="primary" style="margin-left: 20px;" icon="menu" @click.native="cubeSuggestions" :disabled="isReadyCube" >{{$t('cubeSuggestion')}}</el-button>
        </el-col>
        <el-col :span="5">
          <el-button type="primary" icon="setting" @click.native="resetDimensions" :disabled="isReadyCube" >{{$t('resetDimensions')}}</el-button>
        </el-col>
      </el-row> -->

      <el-row class="row_padding border_bottom" style="line-height:36px;border: none;">
        <el-col :span="24">
          {{$t('aggregationGroups')}}
          <kap-common-popover class='d-tip'>
            <div slot="content">
              <ul>
                <li>{{$t('AGG')}}</li>
              </ul>
            </div>
            <icon name="question-circle-o"></icon>
          </kap-common-popover>
        </el-col>
      </el-row>
      <el-row class="row_padding border_bottom borderLeft" style="line-height:36px;border:none;padding-left:0;color:rgba(255,255,255,0.5);margin-top: -8px;padding-top: 0;">
        <el-col :span="5">Total cuboid number: {{totalCuboid}}</el-col>
        <el-col :span="12" >
        <kap-common-popover class='d-tip'>
          <div slot="content">
            <ul>
              <li>{{$t('maxGroup')}}</li>
            </ul>
          </div>
          <icon name="question-circle-o"></icon>
        </kap-common-popover>
        Max group by column: <el-input id="apply-l" v-model="dim_cap" :disabled="isReadyCube"  style="width:100px;"></el-input><el-button id="apply-r" type="grey" style="height: 32px;margin-left: 5px;" @click.native="changeDimCap();cubeSuggestions()">Apply</el-button> </el-col>
      </el-row>
      <div class="line"></div>
      <el-row class="row_padding border_bottom" v-for="(group, group_index) in cubeDesc.aggregation_groups" :key="group_index" style="border-bottom: 0;">
        <div style="height: 30px;line-height: 30px;margin-top: -15px;">
          <span style="float: right;color: rgba(255,255,255,0.5);">Cuboid Number: {{cuboidList[group_index]}} {{groupErrorList[group_index]}}</span>
        </div>
        <el-col :span="24">
          <el-card class="ksd_noshadow" style="border: none;">
            <!-- <el-row>
              <el-col :span="6">
                Cuboid Number: {{cuboidList[group_index]}} {{groupErrorList[group_index]}}
              </el-col> 
            </el-row> -->
            <el-row class="row_padding" style="background: #2f3243;padding-left: 30px;" id="dimensions-item">
              <!-- <el-col :span="1">#{{group_index+1}}</el-col> -->
              <el-col :span="22">
                <el-row class="row_padding">
                  <el-col :span="5" class="dimensions-title">{{$t('Includes')}}</el-col>
                </el-row> 
                <el-row> 
                  <el-col :span="24">
                    <area_label :disabled="isReadyCube" :labels="convertedRowkeys" :datamap="{label:'column', value:'column'}" :refreshInfo="{index: group_index, key: 'includes'}" @refreshData="refreshIncludeData"   :selectedlabels="group.includes" @change="refreshAggragation(group_index)" @checklabel="showDetail"> 
                    </area_label>
                  </el-col>
                </el-row>
                <el-row class="row_padding">
                  <el-col :span="5" class="dimensions-title">{{$t('mandatoryDimensions')}}</el-col>
                </el-row>  
                <el-row>
                  <el-col :span="24" >
                    <area_label :placeholder="$t('kylinLang.common.pleaseSelect')" :labels="group.includes" :disabled="isReadyCube" :refreshInfo="{index: group_index, key: 'mandatory_dims'}" @refreshData="refreshMandatoryData"  :selectedlabels="group.select_rule.mandatory_dims" @change="refreshAggragation(group_index)"   @checklabel="showDetail"> 
                    </area_label>
                  </el-col>
                </el-row>
                <el-row class="row_padding">
                  <el-col :span="5" class="dimensions-title">{{$t('hierarchyDimensions')}}</el-col>
                </el-row>
                <el-row>
                  <el-col :span="24">
                    <el-row class="row_padding" :gutter="10" v-for="(hierarchy_dims, hierarchy_index) in group.select_rule.hierarchy_dims" :key="hierarchy_index">
                       <el-col :span="23" >
                        <area_label :labels="group.includes"  :disabled="isReadyCube" :refreshInfo="{gindex: group_index, hindex: hierarchy_index, key: 'hierarchy_dims'}" @refreshData="refreshHierarchyData"  :selectedlabels="hierarchy_dims" @change="refreshAggragation(group_index)" @checklabel="showDetail"> 
                        </area_label>
                      </el-col>  
                      <el-col :span="1" style="margin-top: 5px;">
                        <el-button type="danger" icon="minus" size="mini" @click="removeHierarchyDims(hierarchy_index, group.select_rule.  hierarchy_dims)">
                      </el-button>
                    </el-col>
                  </el-row>
                </el-col>
              </el-row>
              <el-row>
                <el-col :span="5">
                  <el-button type="default" icon="plus" @click="addHierarchyDims( group.select_rule.hierarchy_dims)" style="margin-top: 8px;">
                  {{$t('newHierarchy')}}
                  </el-button>                
                </el-col>
              </el-row>      
              <el-row class="row_padding">
                <el-col :span="5" class="dimensions-title">{{$t('jointDimensions')}}</el-col>
              </el-row>  
              <el-row>
                <el-col :span="24">
                  <el-row class="row_padding" :gutter="10" v-for="(joint_dims, joint_index) in group.select_rule.joint_dims" :key="joint_index">
                    <el-col :span="23" >
                      <area_label :labels="group.includes" :disabled="isReadyCube"  :refreshInfo="{gindex: group_index, jindex: joint_index, key: 'joint_dims'}" @refreshData="refreshJointData"  :selectedlabels="joint_dims" @change="refreshAggragation(group_index)" @checklabel="showDetail"> 
                      </area_label>
                    </el-col>
                    <el-col :span="1" style="margin-top:5px;">                
                      <el-button type="danger" icon="minus" size="mini" @click="removeJointDims(joint_index, group.select_rule.joint_dims)">
                      </el-button>
                    </el-col>  
                 </el-row>
                </el-col>
               </el-row> 
               <el-row style="padding-bottom: 20px;">
                 <el-col :span="5">
                  <el-button type="default" icon="plus" :disabled="isReadyCube"  @click="addJointDims( group.select_rule.joint_dims)">
                  {{$t('newJoint')}}
                  </el-button>                 
                </el-col>                    
              </el-row>
            </el-col>
            <el-col :span="1" class="close-dimentions">            
              <el-button type="danger" :disabled="isReadyCube"  icon="minus" size="mini" @click="removeAggGroup(group_index)">
              </el-button>
            </el-col>
          </el-row>
        </el-card>  
      </el-col>
    </el-row>

    <el-row class="row_padding">
      <el-col :span="24">
        <el-button type="default" icon="plus" @click="addAggGroup" :disabled="isReadyCube" style="margin-top: 10px;" class="table_margin">
        {{$t('addAggregationGroups')}}
        </el-button>
      </el-col>
    </el-row>
    <div class="line" style="margin-bottom: 5px;margin-right: -30px;margin-left: -30px;"></div>
    <div class="line-primary" style="margin-left: -30px; margin-right: -30px;margin-top: -5px;"></div>
      <el-row class="row_padding" style="margin-bottom: 5px;">
        <el-col :span="24">
          Rowkeys
        </el-col>
      </el-row>
      <div class="ksd-common-table rowkeys">
       <el-row class="tableheader">
         <el-col :span="1">{{$t('ID')}}</el-col>
         <el-col :span="9">{{$t('column')}}</el-col>
         <el-col :span="4">{{$t('encoding')}}</el-col>
         <el-col :span="2">{{$t('length')}}</el-col>
         <el-col :span="2">{{$t('shardBy')}}</el-col>
         <el-col :span="4">{{$t('dataType')}}</el-col>
         <el-col :span="2">{{$t('cardinality')}}</el-col>
       </el-row>
        <el-row id="dimension-row" v-if="convertedRowkeys.length" class="tablebody" v-for="(row, index) in convertedRowkeys"  v-dragging="{ item: row, list: convertedRowkeys, group: 'row' }" :key="row.column">
          <el-col :span="1">{{index+1}}</el-col>
          <el-col :span="9">{{row.column}}</el-col>
          <el-col :span="4">
            <el-select v-model="row.encoding" @change="changeEncoding(row, index);changeRowkey(row, index);">
              <el-option
                  v-for="(item, encodingindex) in initEncodingType(row)"
                  :key="encodingindex"
                 :label="item.name"
                 :value="item.name + ':' + item.version">
                 <el-tooltip effect="dark" :content="$t('kylinLang.cube.'+$store.state.config.encodingTip[item.name])" placement="top">
                   <span style="float: left;width: 90%">{{ item.name }}</span>
                   <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-show="item.version>1">{{ item.version }}</span>
                </el-tooltip>
              </el-option>              
            </el-select>
          </el-col>
          <el-col :span="2"> 
            <el-input v-model="row.valueLength"  :disabled="row.encoding.indexOf('dict')>=0||row.encoding.indexOf('date')>=0||row.encoding.indexOf('time')>=0" @change="changeRowkey(row, index)"></el-input> 
          </el-col>
          <el-col :span="2">
              <el-select v-model="row.isShardBy" @change="changeRowkey(row, index)">
                <el-option
                v-for="item in shardByType"
                :key="item.name"
                :label="item.name"
                :value="item.value">
                </el-option>
              </el-select>
          </el-col>
          <el-col :span="4"> {{modelDesc.columnsDetail&&modelDesc.columnsDetail[row.column]&&modelDesc.columnsDetail[row.column].datatype}}</el-col>
          <el-col :span="2">{{modelDesc.columnsDetail&&modelDesc.columnsDetail[row.column]&&modelDesc.columnsDetail[row.column].cardinality}}</el-col>
        </el-row>
        </div>
    </el-col>
    <el-col :span="6" class="dimension-right">
       <el-table
          :data="featureData"
          border
          style="width: 100%; margin-bottom: 10px;">
          <el-table-column
            :label="$t('kylinLang.dataSource.statistics')"
            width="110">
            <template scope="scope">
              {{$t('kylinLang.dataSource.'+scope.row.name)}}
            </template>
          </el-table-column>
          <el-table-column
            prop="content"
            label=""
            >
          </el-table-column>
        </el-table>
          <el-table
          :data="modelStatics"
          border
          style="width: 100%">
          <el-table-column
            prop="name"
            width="90"
            :label="$t('kylinLang.dataSource.sampleData')">
          </el-table-column>
          <el-table-column
            prop="content"
            label=""
            >
          </el-table-column>
        </el-table>
    </el-col>
  </el-row> 
  <div class="line" style="margin: 0px -30px 0 -30px;"></div>
    <el-dialog :title="$t('addDimensions')" v-model="addDimensionsFormVisible" top="5%" size="large" v-if="addDimensionsFormVisible">
      <add_dimensions  ref="addDimensionsForm" v-on:validSuccess="addDimensionsValidSuccess" :modelDesc="modelDesc" :cubeDimensions="cubeDesc.dimensions"></add_dimensions>
      <span slot="footer" class="dialog-footer">
        <el-button @click="addDimensionsFormVisible = false">{{$t('cancel')}}</el-button>
        <el-button type="primary" @click="checkAddDimensions">{{$t('yes')}}</el-button>
      </span>     
    </el-dialog>  
  </div>
</template>
<script>
import { handleSuccess, handleError, loadBaseEncodings, getTableNameInfoByAlias } from '../../../util/business'
import { changeDataAxis, indexOfObjWithSomeKey, ObjectArraySortByArray } from '../../../util/index'
import { mapActions } from 'vuex'
import areaLabel from '../../common/area_label'
import addDimensions from '../dialog/add_dimensions'
export default {
  name: 'dimensions',
  props: ['cubeDesc', 'modelDesc', 'isEdit'],
  data () {
    return {
      dim_cap: 0,
      totalCuboid: 0,
      sqlBtnLoading: false,
      addDimensionsFormVisible: false,
      addSQLFormVisible: false,
      selected_dimension: {},
      sqlString: '',
      isActiveItem: '',
      selected_project: this.modelDesc.project,
      pfkMap: {},
      cuboidList: [],
      groupErrorList: [],
      shardByType: [{name: 'true', value: true}, {name: 'false', value: false}],
      rowkeyColumns: [],
      oldRowkey: [],
      currentRowkey: [],
      convertedRowkeys: [],
      featureData: [],
      modelStatics: [],
      testSort: [{name: 1}, {name: 2}, {name: 3}, {name: 4}]
    }
  },
  components: {
    'add_dimensions': addDimensions,
    'area_label': areaLabel
  },
  created () {
    this.initCalCuboid()
    this.initConvertedRowkeys()
  },
  methods: {
    ...mapActions({
      calCuboid: 'CAL_CUBOID',
      getCubeSuggestions: 'GET_CUBE_SUGGESTIONS',
      loadTableExt: 'LOAD_DATASOURCE_EXT',
      saveSampleSql: 'SAVE_SAMPLE_SQL',
      getSmartDimensions: 'GET_CUBE_DIMENSIONS'
    }),
    collectSql () {
      this.addSQLFormVisible = true
    },
    refreshIncludeData (data, refreshInfo) {
      var index = refreshInfo.index
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[index], key, data)
      // this.cubeDesc.aggregation_groups[index].select_rule[key] = data
    },
    refreshMandatoryData (data, refreshInfo) {
      var index = refreshInfo.index
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[index].select_rule, key, data)
      // this.cubeDesc.aggregation_groups[index].select_rule[key] = data
    },
    refreshJointData (data, refreshInfo) {
      var gindex = refreshInfo.gindex
      var jindex = refreshInfo.jindex
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[gindex].select_rule[key], jindex, data)
      // this.cubeDesc.aggregation_groups[gindex].select_rule[key][jindex] = data
    },
    refreshHierarchyData (data, refreshInfo) {
      var gindex = refreshInfo.gindex
      var jindex = refreshInfo.hindex
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[gindex].select_rule[key], jindex, data)
      // this.cubeDesc.aggregation_groups[gindex].select_rule[key][jindex] = data
    },
    resetDimensions: function () {
      // this.cubeDesc.dimensions.splice(0, this.cubeDesc.dimensions.length)
      this.dim_cap = 0
      this.totalCuboid = 0
      this.cubeDesc.aggregation_groups.splice(0, this.cubeDesc.aggregation_groups.length)
      // this.cubeDesc.rowkey.rowkey_columns.splice(0, this.cubeDesc.rowkey.rowkey_columns.length)
      // this.initConvertedRowkeys()
    },
    showDetail: function (text, target) {
      this.isActiveItem = text
      var columnNameInfo = text && text.split('.') || []
      if (columnNameInfo.length) {
        var alias = columnNameInfo[0]
        var column = columnNameInfo[1]
        var tableInfo = getTableNameInfoByAlias(this.modelDesc, alias)
        if (tableInfo) {
          var database = tableInfo.database
          var tableName = tableInfo.tableName
          this.loadTableExt(database + '.' + tableName).then((res) => {
            handleSuccess(res, (data) => {
              // var columnFeatureData = filterObjectArray(data.columns_stats, 'column_name', column)
              var objIndex = indexOfObjWithSomeKey(data.columns_stats, 'column_name', column)
              var columnFeatureData = data.columns_stats[objIndex]
              this.featureData = []
              if (columnFeatureData) {
                // this.featureData.push({name: 'statistics', content: ''})
                this.featureData.push({name: 'columns', content: columnFeatureData.column_name})
                this.featureData.push({name: 'cardinality', content: columnFeatureData.cardinality})
                this.featureData.push({name: 'maxLengthVal', content: columnFeatureData.max_length_value})
                this.featureData.push({name: 'maximum', content: columnFeatureData.max_value})
                this.featureData.push({name: 'minLengthVal', content: columnFeatureData.min_length_value})
                this.featureData.push({name: 'minimal', content: columnFeatureData.min_value})
                this.featureData.push({name: 'nullCount', content: columnFeatureData.null_count})
              }
              var sampleData = data.sample_rows[objIndex] || null
              this.modelStatics = [{name: 'ID', content: column}]
              for (var i = 0, len = sampleData && sampleData.length || 0; i < len; i++) {
                this.modelStatics.push({ name: i + 1, content: sampleData[i] })
              }
            })
          })
        }
      }
    },
    // collectSqlToServer () {
    //   if (this.sqlString !== '') {
    //     this.sqlBtnLoading = true
    //     this.saveSampleSql({modelName: this.modelDesc.name, cubeName: this.cubeDesc.name, sqls: this.sqlString.split(/;/)}).then((res) => {
    //       this.sqlBtnLoading = false
    //       handleSuccess(res, (data, code, status, msg) => {
    //         this.$set(this.modelDesc, 'suggestionDerived', data.dimensions)
    //         // this.$set(this.cubeDesc, 'aggregation_groups', data.aggregation_groups)
    //         this.$set(this.cubeDesc, 'override_kylin_properties', data.override_kylin_properties)
    //         this.dim_cap = data.aggregation_groups[0].select_rule.dim_cap || 0
    //         this.addSQLFormVisible = false
    //       })
    //     }, (res) => {
    //       this.sqlBtnLoading = false
    //       handleError(res)
    //     })
    //   }
    // },
    cubeSuggestions: function () {
      this.getCubeSuggestions({cubeDescData: JSON.stringify(this.cubeDesc)}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cubeDesc, 'dimensions', data.dimensions)
          this.$set(this.cubeDesc, 'aggregation_groups', data.aggregation_groups)
          this.$set(this.cubeDesc, 'override_kylin_properties', data.override_kylin_properties)
          this.dim_cap = data.aggregation_groups && data.aggregation_groups[0] && data.aggregation_groups[0].select_rule.dim_cap || 0
          this.$set(this.cubeDesc.rowkey, 'rowkey_columns', data.rowkey.rowkey_columns)
          this.initConvertedRowkeys()
          this.initCalCuboid()
        })
      }, (res) => {
        handleError(res)
      })
    },
    addDimensions: function () {
      this.addDimensionsFormVisible = true
    },
    loadSpecial (database, tableName) {
      this.loadTableExt(database + '.' + tableName).then((res) => {
        handleSuccess(res, (data) => {
          this.statistics = data.columns_stats
          var sampleData = changeDataAxis(data.sample_rows)
          var basicColumn = [[]]
          for (var i = 0, len = sampleData && sampleData.length || 0; i < len; i++) {
            for (var m = 0; m < sampleData[i].length; m++) {
              basicColumn[0].push(this.tableData.columns[m].name)
            }
            break
          }
          this.modelStatics = basicColumn.concat(sampleData)
        })
      })
    },
    checkAddDimensions: function () {
      this.$refs['addDimensionsForm'].$emit('addDimensionsFormValid')
    },
    addDimensionsValidSuccess: function (data) {
      this.cubeDesc.dimensions.splice(0, this.cubeDesc.dimensions.length)
      for (let table in data) {
        if (data[table] && data[table].length > 0) {
          data[table].forEach((column) => {
            let colObj = {name: column.name, table: table, column: null, derived: null}
            if (column.derived === 'true') {
              this.$set(colObj, 'derived', [column.column])
            } else {
              this.$set(colObj, 'column', column.column)
            }
            this.cubeDesc.dimensions.push(colObj)
          })
        }
      }
      this.initRowkeyColumns()
      this.initAggregationGroup()
      this.addDimensionsFormVisible = false
    },
    editDimension: function (dimension) {
      this.selected_dimension = dimension
      this.editDimensionFormVisible = true
    },
    checkEditDimension: function () {
      this.$refs['editDimensionForm'].$emit('editDimensionFormValid')
    },
    editDimensionValidSuccess: function (data) {
      let index = this.cubeDesc.dimensions.indexOf(this.selected_dimension)
      this.$set(this.cubeDesc.dimensions[index], 'name', data.name)
      if (this.cubeDesc.dimensions[index].column && data.type === 'derived') {
        this.$set(this.cubeDesc.dimensions[index], 'derived', [this.cubeDesc.dimensions[index].column])
        this.$set(this.cubeDesc.dimensions[index], 'column', null)
      }
      if (this.cubeDesc.dimensions[index].derived && data.type === 'normal') {
        this.$set(this.cubeDesc.dimensions[index], 'column', this.cubeDesc.dimensions[index].derived[0])
        this.$set(this.cubeDesc.dimensions[index], 'derived', null)
      }
      this.editDimensionFormVisible = false
    },
    initRowkeyColumns: function () {
      this.currentRowkey = []
      this.oldRowkey = []
      this.modelDesc.lookups.forEach((lookup) => {
        let table = lookup.alias
        this.pfkMap[table] = {}
        lookup.join.primary_key.forEach((pk, index) => {
          this.pfkMap[table][pk] = lookup.join.foreign_key[index]
        })
      })
      this.cubeDesc.dimensions.forEach((dimension, index) => {
        if (dimension.derived && dimension.derived.length) {
          let lookup = []
          this.modelDesc.lookups.forEach(function (lookupTable) {
            if (lookupTable.alias === dimension.table) {
              lookup = lookupTable
            }
          })
          lookup.join.foreign_key.forEach((fk, index) => {
            if (this.currentRowkey.indexOf(fk) === -1) {
              this.currentRowkey.push(fk)
            }
          })
        } else if (dimension.column && !dimension.derived) {
          let tableName = dimension.table
          let columnName = dimension.column
          let rowkeyColumn = dimension.table + '.' + dimension.column
          if (this.pfkMap[tableName] && this.pfkMap[tableName][columnName]) {
            rowkeyColumn = this.pfkMap[tableName][columnName]
          }
          if (this.currentRowkey.indexOf(rowkeyColumn) === -1) {
            this.currentRowkey.push(rowkeyColumn)
          }
        }
      })
      this.cubeDesc.rowkey.rowkey_columns.forEach((rowkeyColumn) => {
        this.oldRowkey.push(rowkeyColumn.column)
      })
      this.currentRowkey.forEach((rowkey) => {
        if (this.oldRowkey.indexOf(rowkey) === -1) {
          let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
          this.cubeDesc.rowkey.rowkey_columns.push({
            column: rowkey,
            encoding: 'dict',
            encoding_version: baseEncodings.getEncodingMaxVersion('dict'),
            isShardBy: false
          })
        }
      })
      this.oldRowkey.forEach((rowkey) => {
        if (this.currentRowkey.indexOf(rowkey) === -1) {
          for (let i = 0; i < this.cubeDesc.rowkey.rowkey_columns.length; i++) {
            if (this.cubeDesc.rowkey.rowkey_columns[i].column === rowkey) {
              this.cubeDesc.rowkey.rowkey_columns.splice(i, 1)
            }
          }
        }
      })
      this.initConvertedRowkeys()
    },
    initConvertedRowkeys: function () {
      this.convertedRowkeys.splice(0)
      this.$nextTick(() => {
        this.cubeDesc.rowkey.rowkey_columns.forEach((rowkey) => {
          let version = rowkey.encoding_version || 1
          this.convertedRowkeys.push({column: rowkey.column, encoding: this.getEncoding(rowkey.encoding) + ':' + version, valueLength: this.getLength(rowkey.encoding), isShardBy: rowkey.isShardBy})
        })
      })
    },
    rowKeyToDesc () {
    },
    initEncodingType: function (rowkey) {
      if (!this.modelDesc.columnsDetail[rowkey.column]) {
        return
      }
      let datatype = this.modelDesc.columnsDetail[rowkey.column].datatype
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      let filterEncodings = baseEncodings.filterByColumnType(datatype)
      if (this.isEdit) {
        let _encoding = this.getEncoding(rowkey.encoding)
        let _version = parseInt(this.getVersion(rowkey.encoding))
        let addEncodings = baseEncodings.addEncoding(_encoding, _version)
        return addEncodings
      } else {
        return filterEncodings
      }
    },
    changeEncoding (column, index) {
      var encoding = this.getEncoding(column.encoding)
      if (encoding === 'integer') {
        column.valueLength = 4
      } else {
        column.valueLength = ''
      }
    },
    changeRowkey: function (rowkey, index) {
      this.$set(this.cubeDesc.rowkey.rowkey_columns[index], 'isShardBy', rowkey.isShardBy)
      this.$set(this.cubeDesc.rowkey.rowkey_columns[index], 'encoding_version', this.getVersion(rowkey.encoding))
      if (rowkey.valueLength) {
        this.$set(this.cubeDesc.rowkey.rowkey_columns[index], 'encoding', this.getEncoding(rowkey.encoding) + ':' + rowkey.valueLength)
      } else {
        this.$set(this.cubeDesc.rowkey.rowkey_columns[index], 'encoding', this.getEncoding(rowkey.encoding))
      }
      if (rowkey.encoding.indexOf('dict') >= 0 || rowkey.encoding.indexOf('date') >= 0 || rowkey.encoding.indexOf('time') >= 0) {
        this.$set(rowkey, 'valueLength', null)
      }
    },
    changeDimCap: function () {
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup) => {
        this.$set(aggregationGroup.select_rule, 'dim_cap', +this.dim_cap)
        this.refreshAggragation()
      })
    },
    initAggregationGroup: function () {
      if (!this.isEdit && this.currentRowkey.length > 0 && this.cubeDesc.aggregation_groups.length <= 0) {
        let newGroup = {includes: this.currentRowkey, select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}}
        this.cubeDesc.aggregation_groups.push(newGroup)
        this.cuboidList.push(0)
      }
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
        for (let i = 0; i < aggregationGroup.includes.length; i++) {
          if (this.currentRowkey.indexOf(aggregationGroup.includes[i]) === -1) {
            let removeColumn = aggregationGroup.includes[i]
            aggregationGroup.includes.splice(i, 1)
            i--
            let mandatory = aggregationGroup.select_rule.mandatory_dims
            if (mandatory && mandatory.length) {
              let columnIndex = mandatory.indexOf(removeColumn)
              if (columnIndex >= 0) {
                aggregationGroup.select_rule.mandatory_dims.splice(columnIndex, 1)
              }
            }
            let hierarchys = aggregationGroup.select_rule.hierarchy_dims
            if (hierarchys && hierarchys.length) {
              for (let i = 0; i < hierarchys.length; i++) {
                let hierarchysIndex = hierarchys[i].indexOf(removeColumn)
                if (hierarchysIndex >= 0) {
                  aggregationGroup.select_rule.hierarchy_dims[i].splice(hierarchysIndex, 1)
                }
                if (hierarchys[i].length === 0) {
                  aggregationGroup.select_rule.hierarchy_dims.splice(i, 1)
                  i--
                }
              }
            }
            let joints = aggregationGroup.select_rule.joint_dims
            if (joints && joints.length) {
              for (let i = 0; i < joints.length; i++) {
                let jointIndex = joints[i].indexOf(removeColumn)
                if (jointIndex >= 0) {
                  aggregationGroup.select_rule.joint_dims[i].splice(jointIndex, 1)
                }
                if (joints[i].length === 0) {
                  aggregationGroup.select_rule.joint_dims.splice(i, 1)
                  i--
                }
              }
            }
          }
        }
        this.refreshAggragation(groupIndex)
      })
    },
    refreshAggragation: function (groupindex) {
      groupindex = groupindex || 0
      this.calCuboid({cubeDescData: JSON.stringify(this.cubeDesc), aggIndex: groupindex}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cuboidList, groupindex, data)
          this.$set(this.groupErrorList, groupindex, '')
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$set(this.groupErrorList, groupindex, msg)
        })
      })
    },
    initCalCuboid: function () {
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
        this.refreshAggragation(groupIndex)
      })
      this.calcAllCuboid()
    },
    calcAllCuboid: function () {
      this.calCuboid({cubeDescData: JSON.stringify(this.cubeDesc), aggIndex: -1}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.totalCuboid = data
        })
      }).catch((res) => {
        handleError(res)
      })
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
    removeAggGroup: function (index) {
      this.cubeDesc.aggregation_groups.splice(index, 1)
      this.cuboidList.splice(index, 1)
    },
    addAggGroup: function () {
      this.cubeDesc.aggregation_groups.push({includes: this.currentRowkey, select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}})
      this.cuboidList.push(0)
    },
    removeHierarchyDims: function (index, hierarchyDims) {
      hierarchyDims.splice(index, 1)
    },
    addHierarchyDims: function (hierarchyDims) {
      hierarchyDims.push([])
    },
    removeJointDims: function (index, jointDims) {
      jointDims.splice(index, 1)
    },
    addJointDims: function (jointDims) {
      jointDims.push([])
    }
  },
  mounted () {
    this.getSmartDimensions({model: this.cubeDesc.model_name, cube: this.cubeDesc.name}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.$set(this.modelDesc, 'suggestionDerived', data.dimensions)
        // this.$set(this.cubeDesc, 'aggregation_groups', data.aggregation_groups)
        // this.$set(this.cubeDesc, 'override_kylin_properties', data.override_kylin_properties)
        this.dim_cap = data.aggregation_groups[0].select_rule.dim_cap || 0
        // this.$set(this.cubeDesc.rowkey, 'rowkey_columns', data.rowkey.rowkey_columns)
        // this.initConvertedRowkeys()
        // this.initCalCuboid()
      })
    })
    this.$dragging.$on('dragged', ({ value }) => {
      this.cubeDesc.rowkey.rowkey_columns = ObjectArraySortByArray(this.convertedRowkeys, this.cubeDesc.rowkey.rowkey_columns, 'column', 'column')
    })
  },
  computed: {
    isReadyCube () {
      return this.cubeDesc.status === 'READY'
    }
  },
  locales: {
    'en': {dimensions: 'Dimensions', name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', action: 'Action', addDimensions: 'Add Dimensions', editDimension: 'Edit Dimensions', filter: 'Filter...', cancel: 'Cancel', yes: 'Yes', aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', addAggregationGroups: 'Aggregation Groups', newHierarchy: 'New Hierarchy', newJoint: 'New Joint', ID: 'ID', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', resetDimensions: 'Reset', cubeSuggestion: 'Optimize', collectsqlPatterns: 'Collect SQL Patterns', dimensionOptimizations: 'Dimension optimizations', dO: 'Clicking on the optimize will output the suggested dimension type (normal / derived), aggregate group settings, and Rowkey order.Reset will drop all existing the aggregate group settings and Rowkey order.', AGG: 'Aggregation group is group of cuboids that are constrained by common rules. Users can apply different settings on cuboids in all aggregation groups to meet the query requirements, and saving storage space.', maxGroup: 'Dimension limitations mean max dimensions may be contained within a group of SQL queries. In a set of queries, if each query required the number of dimensions is not more than five, you can set 5 here.'},
    'zh-cn': {dimensions: '维度', name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', action: '操作', addDimensions: '添加维度', editDimension: 'Edit Dimension', filter: '过滤器', cancel: '取消', yes: '确定', aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', addAggregationGroups: '添加聚合组', newHierarchy: '新的层数', newJoint: '新的组合', ID: 'ID', encoding: '编码', length: '长度', shardBy: 'Shard By', dataType: '数据类型', resetDimensions: '重置', cubeSuggestion: 'Optimize', collectsqlPatterns: '输入sql', dimensionOptimizations: '维度优化', dO: '点击优化维度将输出优化器推荐的维度类型（正常／衍生）、聚合组设置与Rowkey顺序。重置则会清空已有的聚合组设置与当前Rowkey顺序。', AGG: '聚合组是指受到共同规则约束的维度组合。 使用者可以对所有聚合组里的维度组合进行不同设置以满足查询需求，并最大化节省存储空间。', maxGroup: '查询最大维度数是指一组查询语句中所含维度的最大值。在查询中，每条查询所需的维度数基本都不超过五，则可以在这里设置5。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .dimensionBox{
    .active{
      background: #0aaacc!important;
    }
  }
  .table_margin {
    margin-top: 20px;
    margin-bottom: 20px;
  }
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
  .border_bottom {
    border-bottom: 1px solid #ddd;
  }
  .border_right {
    border-right: 1px solid #ddd;
  }
  .dimensionBox{
    *{
      border-color: @grey-color!important;
    }
    .el-tag{
      cursor: pointer;
    }
    .tablebody {
      background-color: @grey-color;
    }
    .ksd-common-table *{
      background: @tableBC;
    }
    .row_padding{
      // border-left: 1px solid @tableBC;
    }
    .el-card{
      background: transparent;
    }
  }
  .borderLeft{
    border-left: 1px solid @grey-color;
    padding-left: 20px;
  }
  .add-d:hover{
    color: rgba(33, 143, 234, 1);
  }
  #dimension-row{
    .el-icon-caret-top{
      height: 28px;
      margin-right: 1px;
    }
  }
  .dimensions-title{
    height: 30px;
    line-height: 30px;
  }
  #dimensions-item{
    .el-input__inner{
      border: none;
    }
  }
  .dimension-right{
    padding-left: 25px;
    margin-top: -15px;
    padding-top: 15px;
    .cell{
      white-space: nowrap;
    }
  }
  .close-dimentions{
    position: absolute;
    right: 0px;
    top: 50px;
  }
  .select-d .el-input__inner{
    height: 30px;
  }
  #apply-l{
    background: transparent;
  }
  #apply-l,#apply-r{
    height: 30px;
    .el-input__inner{
      height: 100%;
    }
  }
  #apply-r{
    border-radius: 3px;
    position: relative;
  }
  .rowkeys{
    .el-input__inner{
      height: 30px;
    }
  }
  .dimension-type{
    .normal, .direved{
      width: 40px;
      height: 20px;
      border-radius: 3px;
    }
    .normal{
      background: #0b89bb;
    }
    .direved{
      background: #515465;
    }
    li{
      float: left;
      line-height: 20px;
    }
    li:nth-child(2){
      margin-left: 10px;
    }
  }
  .d-tip{
    position: relative;
    top: 3px;
  }
</style>
