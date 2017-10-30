<template>
<div class="dimensionBox">
  <el-row>
    <el-col :span="18" style="padding-right: 30px;border-right: 1px solid #393e53;padding-bottom: 30px;margin-top: -15px;padding-top: 15px;">
      <el-row class="row_padding border_bottom" style="border: none;">
        <el-row class="row_padding">
          <el-col :span="24" style="font-size: 14px;">
            <span>{{$t('dimensions')}}</span>
            <div style="float: right;width: 260px;">
              <ul class="dimension-type">
                <li><el-tag type="primary" class="normal">Dimension</el-tag></li>
                <li>Normal</li>
              </ul>
              <ul class="dimension-type" style="float: right;">
                <li><el-tag type="gray" class="direved">Dimension</el-tag></li>
                <li>Derived</li>
              </ul>
            </div>
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="24">
            <el-button type="blue" icon="plus" @click.native="addDimensions" :disabled="isReadyCube" >
              {{$t('addDimensions')}}
            </el-button>
          </el-col>
        </el-row>
        <el-row class="row_padding" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
        <el-col :span="24" style="padding: 15px; background: #2f3243;">
          <el-card class="ksd_noshadow dimensions_tag" style="border: none;background: #292b38;padding: 10px;">
            <el-tag class="tag_margin" style="cursor:pointer;" :hit="true"
              @click.native="showDetail(dimension.table+'.'+dimension.name)"
              v-for="(dimension, index) in cubeDesc.dimensions"
              :key="index"
              :type="dimension.derived?'gray':'primary'">
              {{dimension.table+'.'+ (dimension.derived ? dimension.derived&&dimension.derived[0]:dimension.column)}}
            </el-tag>
          </el-card>
        </el-col>
        </el-row>
        <div class="line" style="margin-bottom: -15px;margin-right: -30px;margin-left: -30px;"></div>
        <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
        <div style="font-size: 14px;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
          {{$t('dimensionOptimizations')}}
            <common-tip :content="$t('dO')" >
              <icon name="question-circle" class="ksd-question-circle"></icon>
          </common-tip>
        </div>
        <div style="margin-top: 20px;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
          <el-button type="blue" icon="menu" :loading="suggestLoading" @click.native="cubeSuggestions" :disabled="isReadyCube">{{$t('cubeSuggestion')}}</el-button>
          <el-button type="default" icon="setting" @click.native="resetDimensions" :disabled="isReadyCube">{{$t('resetDimensions')}}</el-button>
        </div>
      </el-row>

<div class="line" style="margin-bottom: -15px;margin-right: -30px;margin-left: -30px;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length"></div>


  <el-tabs v-model="activeName" class="el-tabs--default ksd-mt-40 fixTagsTransform" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
    <el-tab-pane name="first">
    <span slot="label">{{$t('aggregationGroups')}} <common-tip :content="$t('AGG')" >
             <icon name="question-circle" class="ksd-question-circle"></icon>
          </common-tip></span>
       <!-- 维度优化 -->
     <!--   <el-row class="row_padding border_bottom" style="line-height:36px;border: none;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
        <el-col :span="24">
          {{$t('aggregationGroups')}}
          <common-tip :content="$t('AGG')" >
             <icon name="question-circle-o"></icon>
          </common-tip>
        </el-col>
      </el-row> -->
      <el-row class="row_padding border_bottom borderLeft "  style="line-height:36px;border:none;padding-left:0;color:rgba(255,255,255,0.5);margin-top: 4px;padding-top: 0;">
        <el-col :span="5">Total cuboid number: <i class="cuboid_number">{{totalCuboid}}</i></el-col>
        <el-col :span="12" >
         <common-tip :content="$t('maxGroup')" >
           <icon name="question-circle" class="ksd-question-circle"></icon>
          </common-tip>
        {{$t('kylinLang.cube.maxGroupColumn')}}
         <el-input id="apply-l" v-model="dim_cap" :disabled="isReadyCube"  style="width:100px;"></el-input><el-button :loading="applyLoading" id="apply-r" type="grey" style="height: 32px;margin-left: 5px;" :disabled="isReadyCube"  @click.native="changeDimCap();">Apply</el-button> </el-col>
      </el-row>
      <div class="line" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length"></div>
       <el-row class="row_padding border_bottom agg_tag" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length" v-for="(group, group_index) in cubeDesc.aggregation_groups" :key="group_index" style="border-bottom: 0;">
        <div style="height: 30px;line-height: 30px;margin-top: -15px;">
          <span style="float: right;color: rgba(255,255,255,0.5);">Cuboid Number: <i class="cuboid_number">{{cuboidList[group_index]}} <span style="color:#ff4949">{{groupErrorList[group_index]}}</span></i></span>
        </div>
        <el-col :span="24">
          <el-card class="ksd_noshadow" style="border: none;">
            <el-row class="row_padding" style="background: #2f3243;padding-left: 30px;" id="dimensions-item">
              <el-col :span="22">
                <el-row class="row_padding">
                  <el-col :span="5" class="dimensions-title">{{$t('Includes')}} (<span style="font-size:14px;color:#218fea;">{{group.includes.length}}</span>)</el-col>
                </el-row>
                <el-row>
                  <el-col :span="24" class="includes_tag">
                    <area_label ref="includesSelect" :disabled="isReadyCube" :labels="rowkeyColumns.convertedRowkeys" :datamap="{label:'column', value:'column'}" :refreshInfo="{index: group_index, key: 'includes'}" @refreshData="refreshIncludeData"   :selectedlabels="group.includes" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail" @removeTag="removeIncludes">
                    </area_label>
                  </el-col>
                </el-row>
                <el-row class="row_padding">
                  <el-col :span="5" class="dimensions-title">{{$t('mandatoryDimensions')}}</el-col>
                </el-row>
                <el-row>
                  <el-col :span="24" >
                    <area_label :placeholder="$t('kylinLang.common.pleaseSelect')" :labels="group.select_range && group.select_range.mandatoryDims" :disabled="isReadyCube" :refreshInfo="{index: group_index, key: 'mandatory_dims'}" @refreshData="refreshMandatoryData"  :selectedlabels="group.select_rule.mandatory_dims" @change="dimensionsChangeCalc(group_index)"   @checklabel="showDetail">
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
                        <area_label :labels="group.select_range && group.select_range.hierarchyDims"  :disabled="isReadyCube" :refreshInfo="{gindex: group_index, hindex: hierarchy_index, key: 'hierarchy_dims'}" @refreshData="refreshHierarchyData"  :selectedlabels="hierarchy_dims" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail">
                        </area_label>
                      </el-col>
                      <el-col :span="1" style="margin-top: 5px;">
                        <el-button type="danger" icon="minus" size="mini" :disabled="isReadyCube"  @click="removeHierarchyDims(group_index, hierarchy_index, group.select_rule.hierarchy_dims)">
                      </el-button>
                    </el-col>
                  </el-row>
                </el-col>
              </el-row>
              <el-row>
                <el-col :span="5">
                  <el-button type="default" icon="plus" :disabled="isReadyCube"  @click="addHierarchyDims(group_index, group.select_rule.hierarchy_dims)" style="margin-top: 8px;">
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
                      <area_label :labels="group.select_range && group.select_range.jointDims[joint_index]" :disabled="isReadyCube"  :refreshInfo="{gindex: group_index, jindex: joint_index, key: 'joint_dims'}" @refreshData="refreshJointData"  :selectedlabels="joint_dims" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail">
                      </area_label>
                    </el-col>
                    <el-col :span="1" style="margin-top:5px;">
                      <el-button type="danger" icon="minus" :disabled="isReadyCube"  size="mini" @click="removeJointDims(group_index, joint_index, group.select_rule.joint_dims)">
                      </el-button>
                    </el-col>
                 </el-row>
                </el-col>
               </el-row>
               <el-row style="padding-bottom: 20px;">
                 <el-col :span="5">
                  <el-button type="default" icon="plus" :disabled="isReadyCube"  @click="addJointDims(group_index, group.select_rule.joint_dims)">
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

    <el-row class="row_padding" >
      <el-col :span="24">
        <el-button type="default" icon="plus" @click="addAggGroup" :disabled="isReadyCube" style="margin-top: 10px;" class="table_margin">
        {{$t('addAggregationGroups')}}
        </el-button>
      </el-col>
    </el-row>
    <div class="line" style="margin-bottom: 5px;margin-right: -30px;margin-left: -30px;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length"></div>



    </el-tab-pane>
    <el-tab-pane label="Rowkeys" name="second">
      <span slot="label"> Rowkeys
          <common-tip :content="$t('kylinLang.cube.rowkeyTip')" >
             <icon name="question-circle" class="ksd-question-circle"></icon>
          </common-tip></span>

       <div class="line-primary" style="margin-left: -30px; margin-right: -30px;margin-top: -5px;" ></div>
 <!--      <el-row class="row_padding" style="margin-bottom: 5px;" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
        <el-col :span="24">
          Rowkeys
          <common-tip :content="$t('kylinLang.cube.rowkeyTip')" >
             <icon name="question-circle-o"></icon>
          </common-tip>
        </el-col>
      </el-row> -->
      <div class="ksd-mb-20" style="color:grey">{{$t('dragtips')}}</div>
      <div class="ksd-common-table rowkeys" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length && rowkeyColumns.convertedRowkeys.length">
       <el-row class="tableheader" >
         <el-col :span="1">{{$t('ID')}}</el-col>
         <el-col :span="9">{{$t('column')}}</el-col>
         <el-col :span="4">{{$t('encoding')}}</el-col>
         <el-col :span="2">{{$t('length')}}</el-col>
         <el-col :span="2">{{$t('shardBy')}}</el-col>
         <el-col :span="4">{{$t('dataType')}}</el-col>
         <el-col :span="2">{{$t('cardinality')}}</el-col>
       </el-row>
        <el-row  :disabled="isReadyCube"  v-show="rowkeyColumns.convertedRowkeys.length" class="tablebody dimension-row" v-for="(row, index) in rowkeyColumns.convertedRowkeys"  v-dragging="{ item: row, list: rowkeyColumns.convertedRowkeys, group: 'row' }" :key="row.column">
          <el-col :span="1">{{index+1}}</el-col>
          <el-col :span="9" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53;">
           <common-tip placement="right" :tips="row.column" class="drag_bar">{{row.column}}</common-tip></el-col>
          <el-col :span="4" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53">
            <select class="rowkeySelect" v-model="row.encoding" @change="changeEncoding(row, index);changeRowkey(row, index);">
              <option v-for="(item, encodingindex) in row.selectEncodings" :key="encodingindex" :value="item.name + ':' + item.version">{{item.name}}</option>
            </select>
          </el-col>
          <el-col :span="2" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53">
            <el-input v-model="row.valueLength"  :disabled="row.encoding.indexOf('dict')>=0||row.encoding.indexOf('date')>=0||row.encoding.indexOf('time')>=0||row.encoding.indexOf('boolean')>=0" @change="changeRowkey(row, index)"></el-input>
          </el-col>
          <el-col :span="2" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53">
            <select class="rowkeySelect" v-model="row.isShardBy" @change="changeRowkey(row, index)">
              <option v-for="item in shardByType" :key="item.name" :value="item.value">{{item.name}}</option>
            </select>
          </el-col>
          <el-col :span="4" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53"> {{row.datatype}}</el-col>
          <el-col :span="2" style="word-wrap: break-word; white-space:nowrap;text-overflow: ellipsis;overflow: hidden;border-left:solid 1px #393e53">{{row.cardinality}}</el-col>
        </el-row>
        </div>

    </el-tab-pane>
  </el-tabs>
<!-- 维度优化 -->







    <!-- rowkeys -->

    </el-col>
    <el-col :span="6" class="dimension-right">
       <el-table
          :data="featureData"
          tooltip-effect="dark"
          border
          style="width: 100%; margin-bottom: 10px;">
          <el-table-column
            :label="$t('kylinLang.dataSource.statistics')"
            >
            <template scope="scope">
              {{$t('kylinLang.dataSource.'+scope.row.name)}}
            </template>
          </el-table-column>
          <el-table-column
          show-overflow-tooltip
            prop="content"
            label=""
            >
          </el-table-column>
        </el-table>
          <el-table
          tooltip-effect="dark"
          :data="modelStatics"
          border
          style="width: 100%">
          <el-table-column
            prop="name"
            show-overflow-tooltip
            width="86"
            :label="$t('sampleData')">
          </el-table-column>
          <el-table-column
          show-overflow-tooltip
            prop="content"
            label=""
            >
          </el-table-column>
        </el-table>
    </el-col>
  </el-row>
  <div class="line" style="margin: 0px -30px 0 -30px;"></div>
    <el-dialog :title="$t('addDimensions')" v-model="addDimensionsFormVisible" top="5%" size="large" v-if="addDimensionsFormVisible" :before-close="dimensionsClose" :close-on-press-escape="false" :close-on-click-modal="false">
      <span slot="title">{{$t('addDimensions')}}
        <common-tip :content="$t('kylinLang.cube.dimensionTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
      </span>
      <add_dimensions  ref="addDimensionsForm" v-on:validSuccess="addDimensionsValidSuccess" :modelDesc="modelDesc" :cubeDesc="cubeDesc" :sampleSql="sampleSql" :oldData="oldData"></add_dimensions>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dimensionsClose()">{{$t('cancel')}}</el-button>
        <el-button type="primary" @click="checkAddDimensions">{{$t('yes')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>
<script>
import { handleSuccess, handleError, loadBaseEncodings, getTableNameInfoByAlias, kapConfirm } from '../../../util/business'
import { changeDataAxis, indexOfObjWithSomeKey, ObjectArraySortByArray, objectClone } from '../../../util/index'
import { mapActions } from 'vuex'
import areaLabel from '../../common/area_label'
import addDimensions from '../dialog/add_dimensions'
export default {
  name: 'dimensions',
  props: ['cubeDesc', 'modelDesc', 'isEdit', 'cubeInstance', 'sampleSql', 'oldData'],
  data () {
    return {
      activeName: 'first',
      dim_cap: 0,
      totalCuboid: 0,
      applyLoading: false,
      addDimensionsFormVisible: false,
      addSQLFormVisible: false,
      selected_project: this.modelDesc.project,
      pfkMap: {},
      cuboidList: [],
      groupErrorList: [],
      shardByType: [{name: 'true', value: true}, {name: 'false', value: false}],
      rowkeyColumns: {
        oldRowkeys: [],
        currentRowkeys: [],
        convertedRowkeys: [],
        removeRowkeys: [],
        additionRowkeys: []
      },
      featureData: [],
      modelStatics: [],
      tableStaticsCache: {},
      suggestLoading: false,
      cuboidCache: {},
      ST: null,
      scrollST: null,
      beforeScrollPos: 260,
      selectEncodingCache: {},
      dimensionRightDom: null
    }
  },
  components: {
    'add_dimensions': addDimensions,
    'area_label': areaLabel
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
    resetDimensions: function () {
      this.dim_cap = 0
      this.cubeDesc.aggregation_groups.splice(0, this.cubeDesc.aggregation_groups.length)
      this.initRowkeyColumns()
      this.initAggregationGroup(true)
      this.calcAllCuboid()
    },
    showDetail: function (text, target) {
      this.dimensionRightDom.style.paddingTop = (document.getElementById('scrollBox').scrollTop - 180) + 'px'
      var columnNameInfo = text && text.split('.') || []
      if (columnNameInfo.length) {
        var alias = columnNameInfo[0]
        var column = columnNameInfo[1]
        var tableInfo = getTableNameInfoByAlias(this.modelDesc, alias)
        if (tableInfo) {
          var database = tableInfo.database
          var tableName = tableInfo.tableName
          if (this.tableStaticsCache[database + '.' + tableName]) {
            this.transTableDetailData(this.tableStaticsCache[database + '.' + tableName], column)
            return
          }
          this.loadTableExt({tableName: database + '.' + tableName, project: this.selected_project}).then((res) => {
            handleSuccess(res, (data) => {
              this.transTableDetailData(data, column)
              if (data.sample_rows && data.sample_rows.length) {
                this.tableStaticsCache[database + '.' + tableName] = data
              }
            })
          })
        }
      }
    },
    transTableDetailData (data, column) {
      if (!data) {
        return
      }
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
    },
    cubeSuggestions: function () {
      this.suggestLoading = true
      this.getCubeSuggestions({cubeDescData: JSON.stringify(this.cubeDesc)}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cubeDesc, 'dimensions', data.dimensions)
          this.$set(this.cubeDesc, 'aggregation_groups', data.aggregation_groups)
          // this.$set(this.cubeDesc, 'override_kylin_properties', data.override_kylin_properties)
          this.dim_cap = data.aggregation_groups && data.aggregation_groups[0] && data.aggregation_groups[0].select_rule.dim_cap || 0
          this.$set(this.cubeDesc.rowkey, 'rowkey_columns', data.rowkey.rowkey_columns)
          this.initConvertedRowkeys()
          this.initCalCuboid()
          this.suggestLoading = false
          this.initAllAggSelectRange()
        })
      }, (res) => {
        this.suggestLoading = false
        handleError(res)
      })
    },
    addDimensions: function () {
      this.addDimensionsFormVisible = true
    },
    dimensionsClose () {
      kapConfirm(this.$t('kylinLang.common.willClose'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.addDimensionsFormVisible = false
      })
    },
    loadSpecial (database, tableName) {
      this.loadTableExt({tableName: database + '.' + tableName, project: this.selected_project}).then((res) => {
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
      if (this.cubeDesc.dimensions.length === 0) {
        this.cubeDesc.aggregation_groups.splice(0, this.cubeDesc.aggregation_groups.length)
      }
      this.addDimensionsFormVisible = false
      this.$nextTick(() => {
        setTimeout(() => {
          this.initRowkeyColumns((rowkeys) => {
            if (rowkeys && rowkeys.length > 25) {
              kapConfirm(this.$t('moreRowkeyTip'))
            }
          })
          setTimeout(() => {
            this.initAggregationGroup()
            this.calcAllCuboid()
          }, 1000)
        }, 1000)
      })
    },
    initModelLine: function () {
      this.modelDesc.lookups.forEach((lookup) => {
        let table = lookup.alias
        this.pfkMap[table] = {}
        lookup.join.primary_key.forEach((pk, index) => {
          this.pfkMap[table][pk] = lookup.join.foreign_key[index]
        })
      })
    },
    initRowkeyColumns: function (moreRowKeyCallback) {
      this.rowkeyColumns.currentRowkeys.splice(0, this.rowkeyColumns.currentRowkeys.length)
      this.rowkeyColumns.oldRowkeys = []
      this.rowkeyColumns.removeRowkeys = []
      this.rowkeyColumns.additionRowkeys = []
      this.cubeDesc.dimensions.forEach((dimension, index) => {
        if (dimension.derived && dimension.derived.length) {
          for (let pk in this.pfkMap[dimension.table]) {
            let fk = this.pfkMap[dimension.table][pk]
            if (this.rowkeyColumns.currentRowkeys.indexOf(fk) === -1) {
              this.rowkeyColumns.currentRowkeys.push(fk)
            }
          }
        } else if (dimension.column && !dimension.derived) {
          let tableName = dimension.table
          let columnName = dimension.column
          let rowkeyColumn = dimension.table + '.' + dimension.column
          if (this.pfkMap[tableName] && this.pfkMap[tableName][columnName]) {
            rowkeyColumn = this.pfkMap[tableName][columnName]
          }
          if (this.rowkeyColumns.currentRowkeys.indexOf(rowkeyColumn) === -1) {
            this.rowkeyColumns.currentRowkeys.push(rowkeyColumn)
          }
        }
      })
      this.cubeDesc.rowkey.rowkey_columns.forEach((rowkeyColumn) => {
        this.rowkeyColumns.oldRowkeys.push(rowkeyColumn.column)
      })
      this.rowkeyColumns.currentRowkeys.forEach((rowkey) => {
        if (this.rowkeyColumns.oldRowkeys.indexOf(rowkey) === -1) {
          this.rowkeyColumns.additionRowkeys.push(rowkey)
          let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
          this.cubeDesc.rowkey.rowkey_columns.push({
            column: rowkey,
            encoding: 'dict',
            encoding_version: baseEncodings.getEncodingMaxVersion('dict'),
            isShardBy: false
          })
        }
      })
      this.rowkeyColumns.oldRowkeys.forEach((rowkey) => {
        if (this.rowkeyColumns.currentRowkeys.indexOf(rowkey) === -1) {
          this.rowkeyColumns.removeRowkeys.push(rowkey)
          let len = this.cubeDesc.rowkey.rowkey_columns.length
          for (let i = 0; i < len; i++) {
            if (this.cubeDesc.rowkey.rowkey_columns[i].column === rowkey) {
              this.cubeDesc.rowkey.rowkey_columns.splice(i, 1)
              break
            }
          }
        }
      })
      this.initConvertedRowkeys(moreRowKeyCallback)
    },
    initConvertedRowkeys: function (moreRowKeyCallback) {
      this.rowkeyColumns.convertedRowkeys.splice(0, this.rowkeyColumns.convertedRowkeys.length)
      this.$nextTick(() => {
        this.cubeDesc.rowkey.rowkey_columns.forEach((rowkey) => {
          let version = rowkey.encoding_version || 1
          var rowKeyObj = {column: rowkey.column, encoding: this.getEncoding(rowkey.encoding) + ':' + version, valueLength: this.getLength(rowkey.encoding), isShardBy: rowkey.isShardBy}
          rowKeyObj.selectEncodings = this.initEncodingType(rowKeyObj)
          var rowkeyColumnInfo = this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[rowKeyObj.column] || null
          if (rowkeyColumnInfo) {
            rowKeyObj.datatype = rowkeyColumnInfo.datatype
            rowKeyObj.cardinality = rowkeyColumnInfo.cardinality
          }
          this.rowkeyColumns.convertedRowkeys.push(rowKeyObj)
        })
        if (typeof moreRowKeyCallback === 'function') {
          moreRowKeyCallback(this.rowkeyColumns.convertedRowkeys)
        }
      })
    },
    initEncodingType: function (rowkey) {
      if (!this.modelDesc.columnsDetail[rowkey.column]) {
        return
      }
      let datatype = this.modelDesc.columnsDetail[rowkey.column].datatype
      if (this.selectEncodingCache[datatype]) {
        return this.selectEncodingCache[datatype]
      }
      let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
      let filterEncodings = baseEncodings.filterByColumnType(datatype)
      if (this.isEdit) {
        let _encoding = this.getEncoding(rowkey.encoding)
        let _version = parseInt(this.getVersion(rowkey.encoding))
        let addEncodings = baseEncodings.addEncoding(_encoding, _version)
        this.selectEncodingCache[datatype] = addEncodings
        return addEncodings
      } else {
        this.selectEncodingCache[datatype] = filterEncodings
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
      var curRowkeyColumn = this.cubeDesc.rowkey.rowkey_columns[index]
      var version = this.getVersion(rowkey.encoding)
      var encoding = this.getEncoding(rowkey.encoding)
      this.$set(curRowkeyColumn, 'isShardBy', rowkey.isShardBy)
      this.$set(curRowkeyColumn, 'encoding_version', version)
      if (rowkey.valueLength) {
        this.$set(curRowkeyColumn, 'encoding', encoding + ':' + rowkey.valueLength)
      } else {
        this.$set(curRowkeyColumn, 'encoding', encoding)
      }
      if (rowkey.encoding.indexOf('dict') >= 0 || rowkey.encoding.indexOf('date') >= 0 || rowkey.encoding.indexOf('time') >= 0) {
        this.$set(rowkey, 'valueLength', null)
      }
    },
    changeDimCap: function () {
      this.applyLoading = true
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup) => {
        this.$set(aggregationGroup.select_rule, 'dim_cap', +this.dim_cap)
      })
      this.initCalCuboid()
    },
    initAggregationGroup: function (isReset) {
      if ((!this.isEdit || this.isEdit && isReset) && this.cubeDesc.aggregation_groups.length <= 0) {
        let newGroup = {includes: objectClone(this.currentRowkey), select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}}
        this.cubeDesc.aggregation_groups.push(newGroup)
        this.cuboidList.push(0)
      }
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
        this.rowkeyColumns.removeRowkeys.forEach((removeColumn) => {
          let removeIndex = aggregationGroup.includes.indexOf(removeColumn)
          if (aggregationGroup.includes.indexOf(removeColumn) >= 0) {
            aggregationGroup.includes.splice(removeIndex, 1)
            this.removeIncludes(removeColumn, {index: groupIndex})
          }
        })
        this.refreshAggragation(groupIndex)
        this.initAggSelectRange(this.cubeDesc.aggregation_groups[groupIndex], groupIndex)
      })
      if (this.cubeDesc.aggregation_groups.length > 0) {
        this.cubeDesc.aggregation_groups[0].includes = this.cubeDesc.aggregation_groups[0].includes.concat(this.rowkeyColumns.additionRowkeys)
        this.refreshAggragation(0)
        this.initAggSelectRange(this.cubeDesc.aggregation_groups[0], 0)
      }
    },
    dimensionsChangeCalc: function (groupindex) {
      this.refreshAggragation(groupindex)
      this.$nextTick(() => {
        this.calcAllCuboid()
      })
    },
    refreshAggragation: function (groupindex) {
      this.$nextTick(() => {
        this.updateIncludesDimUsed(groupindex)
      })
    },
    initCalCuboid: function () {
      this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
        this.refreshAggragation(groupIndex)
      })
      this.calcAllCuboid()
    },
    calcAllCuboid: function () {
      this.groupErrorList = []
      if (this.cuboidCache[JSON.stringify(this.cubeDesc)]) {
        let cacheData = this.cuboidCache[JSON.stringify(this.cubeDesc)]
        var data = cacheData.slice(0)
        this.totalCuboid = data && data[0]
        data = data.splice(1, data.length)
        data.forEach((cuboid, index) => {
          this.$set(this.cuboidList, index, cuboid)
        })
        this.applyLoading = false
        return
      }
      this.calCuboid({cubeDescData: JSON.stringify(this.cubeDesc)}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          if (data) {
            this.cuboidCache[JSON.stringify(this.cubeDesc)] = data.slice(0)
            this.totalCuboid = data && data[0]
            data = data.splice(1, data.length)
            data.forEach((cuboid, index) => {
              this.$set(this.cuboidList, index, cuboid)
            })
          }
        })
        this.applyLoading = false
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.groupErrorList = []
          var nullAggIndex = -1
          this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
            if (aggregationGroup.includes.length < 1) {
              nullAggIndex = groupIndex
            }
          })
          // 解析出是哪个组的错误
          var groupIndex = msg && msg.replace(/^Aggregation\s+group\s+(\d+).*?$/, '$1')
          if (groupIndex === msg && nullAggIndex !== -1) {
            // 如果有空agg在空agg处报错
            groupIndex = nullAggIndex
          }
          // 否则全局弹出提示
          if (!/^\d+$/.test(groupIndex)) {
            this.$message(msg)
          } else {
            this.$set(this.groupErrorList, groupIndex, msg)
          }
          this.applyLoading = false
          this.totalCuboid = 'N/A'
          this.cubeDesc.aggregation_groups.forEach((aggregationGroup, groupIndex) => {
            this.$set(this.cuboidList, groupIndex, 'N/A')
          })
        })
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
    updateIncludesDimUsed (index) {
      this.cubeDesc.aggregation_groups[index].includes.forEach((dimension, i) => {
        let isUsed = false
        if (this.cubeDesc.aggregation_groups[index].select_rule.mandatory_dims.indexOf(dimension) >= 0) {
          isUsed = true
        }
        this.cubeDesc.aggregation_groups[index].select_rule.hierarchy_dims.forEach((dim) => {
          if (dim.indexOf(dimension) >= 0) {
            isUsed = true
          }
        })
        this.cubeDesc.aggregation_groups[index].select_rule.joint_dims.forEach((dim) => {
          if (dim.indexOf(dimension) >= 0) {
            isUsed = true
          }
        })
        let cloumnIndex = indexOfObjWithSomeKey(this.$refs.includesSelect[index].tags, 'textContent', dimension)
        var tag = this.$refs.includesSelect[index].tags[cloumnIndex]
        if (tag) {
          if (!isUsed) {
            tag.setAttribute('data-tag', '')
          } else {
            tag.setAttribute('data-tag', 'useDimension')
          }
        }
      })
    },
    removeIncludes (removeColumn, refreshInfo) {
      var groupIndex = refreshInfo.index
      let mandatory = this.cubeDesc.aggregation_groups[groupIndex].select_rule.mandatory_dims
      if (mandatory && mandatory.length) {
        let columnIndex = mandatory.indexOf(removeColumn)
        if (columnIndex >= 0) {
          mandatory.splice(columnIndex, 1)
        }
      }
      let hierarchys = this.cubeDesc.aggregation_groups[groupIndex].select_rule.hierarchy_dims
      var hierarchysLen = hierarchys && hierarchys.length || 0
      for (let h = 0; h < hierarchysLen; h++) {
        let hierarchysIndex = hierarchys[h].indexOf(removeColumn)
        if (hierarchysIndex >= 0) {
          hierarchys[h].splice(hierarchysIndex, 1)
        }
        if (hierarchys[h].length === 0) {
          hierarchys.splice(h, 1)
          h--
          hierarchysLen--
        }
      }
      let joints = this.cubeDesc.aggregation_groups[groupIndex].select_rule.joint_dims
      var jointsLen = joints && joints.length || 0
      for (let j = 0; j < jointsLen; j++) {
        let jointIndex = joints[j].indexOf(removeColumn)
        if (jointIndex >= 0) {
          joints[j].splice(jointIndex, 1)
        }
        if (joints[j].length === 0) {
          joints.splice(j, 1)
          j--
          jointsLen--
        }
      }
    },
    refreshIncludeData (data, refreshInfo) {
      var index = refreshInfo.index
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[index], key, data)
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[index], index)
      // this.cubeDesc.aggregation_groups[index].select_rule[key] = data
    },
    refreshMandatoryData (data, refreshInfo) {
      var index = refreshInfo.index
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[index].select_rule, key, data)
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[index], index)
      // this.cubeDesc.aggregation_groups[index].select_rule[key] = data
    },
    refreshJointData (data, refreshInfo) {
      var gindex = refreshInfo.gindex
      var jindex = refreshInfo.jindex
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[gindex].select_rule[key], jindex, data)
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
      // this.cubeDesc.aggregation_groups[gindex].select_rule[key][jindex] = data
    },
    refreshHierarchyData (data, refreshInfo) {
      var gindex = refreshInfo.gindex
      var jindex = refreshInfo.hindex
      var key = refreshInfo.key
      this.$set(this.cubeDesc.aggregation_groups[gindex].select_rule[key], jindex, data)
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
      // this.cubeDesc.aggregation_groups[gindex].select_rule[key][jindex] = data
    },
    initAllAggSelectRange: function () {
      if (this.cubeDesc.aggregation_groups.length > 0) {
        this.cubeDesc.aggregation_groups.forEach((group, gindex) => {
          this.initAggSelectRange(group, gindex)
        })
      }
    },
    initAggSelectRange: function (group, gindex) {
      this.$set(group, 'select_range', {
        mandatoryDims: [],
        hierarchyDims: [],
        jointDims: []
      })
      let usedInMandatory = []
      let usedInHierarchy = []
      let usedInJoint = {allDims: [], eachOtherDims: []}
      usedInMandatory = group.select_rule.mandatory_dims
      group.select_rule.hierarchy_dims.forEach((dim) => {
        usedInHierarchy = usedInHierarchy.concat(dim)
      })
      group.select_rule.joint_dims.forEach((dim, index) => {
        usedInJoint.allDims = usedInJoint.allDims.concat(dim)
        this.$set(usedInJoint.eachOtherDims, index, [])
        this.$set(group.select_range.jointDims, index, [])
      })
      group.select_rule.joint_dims.forEach((dim, index) => {
        usedInJoint.eachOtherDims.forEach((eoDim, eoIndex) => {
          if (index !== eoIndex) {
            usedInJoint.eachOtherDims[eoIndex] = eoDim.concat(dim)
          }
        })
      })
      group.includes.forEach((column) => {
        if (usedInHierarchy.indexOf(column) === -1 && usedInJoint.allDims.indexOf(column) === -1) {
          group.select_range.mandatoryDims.push(column)
        }
        if (usedInMandatory.indexOf(column) === -1 && usedInJoint.allDims.indexOf(column) === -1) {
          group.select_range.hierarchyDims.push(column)
        }
        if (usedInMandatory.indexOf(column) === -1 && usedInHierarchy.indexOf(column) === -1) {
          group.select_rule.joint_dims.forEach((dim, index) => {
            if (usedInJoint.eachOtherDims[index].indexOf(column) === -1) {
              group.select_range.jointDims[index].push(column)
            }
          })
        }
      })
    },
    removeAggGroup: function (index) {
      this.cubeDesc.aggregation_groups.splice(index, 1)
      this.cuboidList.splice(index, 1)
      this.initCalCuboid()
    },
    addAggGroup: function () {
      this.cubeDesc.aggregation_groups.push({
        includes: [],
        select_rule: {
          mandatory_dims: [],
          hierarchy_dims: [],
          joint_dims: []
        },
        select_range: {
          mandatoryDims: [],
          hierarchyDims: [],
          jointDims: []
        }
      })
      this.cuboidList.push(0)
    },
    removeHierarchyDims: function (gindex, index, hierarchyDims) {
      hierarchyDims.splice(index, 1)
      this.initCalCuboid()
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    addHierarchyDims: function (gindex, hierarchyDims) {
      hierarchyDims.push([])
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    removeJointDims: function (gindex, index, jointDims) {
      jointDims.splice(index, 1)
      this.initCalCuboid()
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    addJointDims: function (gindex, jointDims) {
      jointDims.push([])
      this.cubeDesc.aggregation_groups[gindex].select_range.jointDims.push([])
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    scrollRightBar () {
      clearTimeout(this.scrollST)
      this.scrollST = setTimeout(() => {
        var sTop = document.getElementById('scrollBox').scrollTop
        if (sTop < this.beforeScrollPos) {
          this.dimensionRightDom.style.paddingTop = '20px'
          // this.beforeScrollPos = document.getElementById('scrollBox').scrollTop
        }
      }, 50)
    }
  },
  created () {
    this.initModelLine()
    this.initAllAggSelectRange()
    this.initCalCuboid()
    setTimeout(() => {
      this.initConvertedRowkeys()
    }, 1000)
  },
  mounted () {
    this.dimensionRightDom = this.$el.querySelectorAll('.dimension-right')[0]
    document.getElementById('scrollBox').addEventListener('scroll', this.scrollRightBar, false)
    this.getSmartDimensions({model: this.cubeDesc.model_name, cube: this.cubeDesc.name}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.$set(this.modelDesc, 'suggestionDerived', data.dimensions)
      })
    })
    this.dim_cap = this.cubeDesc.aggregation_groups && this.cubeDesc.aggregation_groups[0] && this.cubeDesc.aggregation_groups[0].select_rule.dim_cap || 0
    this.$dragging.$on('dragged', ({ value }) => {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.cubeDesc.rowkey.rowkey_columns = ObjectArraySortByArray(this.rowkeyColumns.convertedRowkeys, this.cubeDesc.rowkey.rowkey_columns, 'column', 'column')
      }, 2000)
    })
  },
  computed: {
    isReadyCube () {
      return this.cubeInstance && this.cubeInstance.segments && this.cubeInstance.segments.length > 0
      // return this.cubeDesc.status === 'READY'
    }
  },
  beforeDestroy () {
    document.getElementById('scrollBox').removeEventListener('scroll', this.scrollRightBar, false)
  },
  locales: {
    'en': {dimensions: 'Dimensions', name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', action: 'Action', addDimensions: 'Add Dimensions', editDimension: 'Edit Dimensions', filter: 'Filter...', cancel: 'Cancel', yes: 'Yes', aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', addAggregationGroups: 'Aggregation Groups', newHierarchy: 'New Hierarchy', newJoint: 'New Joint', ID: 'ID', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', resetDimensions: 'Reset', cubeSuggestion: 'Optimize', collectsqlPatterns: 'Collect SQL Patterns', dimensionOptimizations: 'Dimension Optimizations', dO: 'Clicking on the optimize will output the suggested dimension type (normal / derived), aggregate group settings, and Rowkey order.<br/>Reset will drop all existing the aggregate group settings and Rowkey order.', AGG: 'Aggregation group is group of cuboids that are constrained by common rules. <br/>Users can apply different settings on cuboids in all aggregation groups to meet the query requirements, and saving storage space.', maxGroup: 'Dimension limitations mean max dimensions may be contained within a group of SQL queries. In a set of queries, if each query required the number of dimensions is not more than five, you can set 5 here.', moreRowkeyTip: 'Current selected normal dimensions are exploding, "Optimize" may suggest unreasonable less cuboid.', dimensionUsed: 'This dimension is used as a mandatory、 a hierarchy and a joint dimension.', sampleData: 'Sample', dragtips: 'you can drag and drop to reorder'},
    'zh-cn': {dimensions: '维度', name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', action: '操作', addDimensions: '添加维度', editDimension: 'Edit Dimension', filter: '过滤器', cancel: '取消', yes: '确定', aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', addAggregationGroups: '添加聚合组', newHierarchy: '新的层级维度', newJoint: '新的联合维度', ID: 'ID', encoding: '编码', length: '长度', shardBy: 'Shard By', dataType: '数据类型', resetDimensions: '重置', cubeSuggestion: '维度优化', collectsqlPatterns: '输入sql', dimensionOptimizations: '维度优化', dO: '点击优化维度将输出优化器推荐的维度类型（正常／衍生）、聚合组设置与Rowkey顺序。<br/>重置则会清空已有的聚合组设置与当前Rowkey顺序。', AGG: '聚合组是指受到共同规则约束的维度组合。 <br/>使用者可以对所有聚合组里的维度组合进行不同设置以满足查询需求，并最大化节省存储空间。', maxGroup: '查询最大维度数是指一组查询语句中所含维度的最大值。在查询中，每条查询所需的维度数基本都不超过5，则可以在这里设置5。', moreRowkeyTip: '当前选择的普通维度太多，一键优化可能给出过度剪枝的设置。', dimensionUsed: '该维度被用作必须 、层级以及联合维度。', sampleData: '采样数据', dragtips: '您可以通过拖拽列表对rowkey进行排序'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .dimensionBox{
    .rowkeySelect::-ms-expand { display: none; }
    .rowkeySelect {
      height: 30px;
      width: 100%;
      color:rgba(255, 255, 255, 0.6);
      border: solid 1px #393e53;
      background: url('../../../assets/img/arrow.png') no-repeat scroll right center transparent!important;
      /*很关键：将默认的select选择框样式清除*/
      appearance:none;
      -moz-appearance:none;
      -webkit-appearance:none;
      padding-left:10px;
      -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
      outline: none;
      background-color: transparent;
      option{
        padding:4px;
        border:none;

      }
    }
    .el-tag--primary {
      background: rgba(33,143,234,0.1);
      color: rgb(33,143,234);
      border-color: rgba(33,143,234,0.2)!important;
    }
    .el-tag--gray {
      background: rgba(101,105,128,0.4);
      color: rgb(255,255,255);
    }
    .cuboid_number{
      color:#218fea;
    }
    .dimensions_tag {
      .el-tag--primary:hover {
        background: rgb(11,137,187)!important;
        color: rgb(255,255,255)!important;
      }
      .el-tag--gray:hover {
        background: rgba(101,105,128,1)!important;
        color: rgb(255,255,255)!important;
      }
    }
    .agg_tag {
        .el-tag--primary {
          background-color: rgb(34,122,198);
          color: rgb(255,255,255);
        }
      .includes_tag {
        .el-tag--primary {
          background-color: rgba(33,143,234,0.1);
          color: rgb(33,143,234)!important;
          border-color: rgba(33,143,234,0.2);
        }
        [data-tag=useDimension], .useDimension {
          background-color: rgb(34,122,198)!important;
          color: rgb(255,255,255)!important;
        }
      }
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
  .dimension-row{
    cursor: move;
    .el-col{
      padding-left:10px;
      padding-right:10px;
    }
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
    border:solid 1px #48576a;
    border-radius: 3px;
    position: relative;
  }
  .rowkeys{
    .el-input__inner{
      height: 30px;
    }
  }
  .dimension-type{
    .el-tag--primary:hover {
      background: rgba(33,143,234,0.1);
      color: rgb(33,143,234);
      border-color: rgba(33,143,234,0.2);
    }
    .normal, .direved{
      height: 25px;
      border-radius: 3px;
    }
    .direved {
      border-color:#48576a;
    }
    li{
      float: left;
      line-height: 24px;
    }
    li:nth-child(2){
      margin-left: 10px;
    }
  }
  .d-tip{
    position: relative;
    top: 3px;
  }
  .fixTagsTransform{
    .el-select__tags{
      top:0;
      transform: translateY(0);
    }
  }
</style>
