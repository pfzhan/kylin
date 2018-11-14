<template>
  <div class="dimension-edit ksd-mrl-20">
    <el-row>
      <el-col :span="18" class="dimension-left">
        <div class="add-dimensions">
          <el-row>
            <el-col :span="24">
              <div style="width: 150px;" class="ksd-fright dimension-type">
                <ul class="ksd-fright ksd-ml-20">
                  <li>
                    <div class="dimension-block"></div>
                  </li>
                  <li class="ksd-mt-4 ksd-fs-12">Derived</li>
                </ul>
                <ul class="ksd-fright">
                  <li>
                    <div class="dimension-block"></div>
                  </li>
                  <li class="ksd-mt-4 ksd-fs-12">Normal</li>
                </ul>
              </div>
            </el-col>
          </el-row>
          <el-row>
            <el-col :span="24">
              <span class="dimension-title ksd-fz-16">{{$t('dimensions')}}</span>
              <el-button type="primary" class="ksd-ml-10" icon="el-icon-edit" @click.native="addDimensions" :disabled="isReadyCube" >
                {{$t('dimensions')}}
              </el-button>
            </el-col>
          </el-row>  
          <el-row>
            <el-col :span="24" class="ksd-mt-10">
              <el-card class="dimension-label" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
                <el-tag tabindex="0" class="ksd-ml-6 ksd-mt-4" size="small" :hit="true"
                  @click.native="showDetail(dimension.table+'.'+ (dimension.derived ? dimension.derived&&dimension.derived[0]:dimension.column))"
                  v-for="(dimension, index) in cubeDesc.dimensions"
                  :key="index+'dimension'"
                  :type="dimension.derived?'warning':''">
                  {{dimension.table+'.'+ (dimension.derived ? dimension.derived&&dimension.derived[0]:dimension.column)}}
                </el-tag>
              </el-card>
              <div v-else class="no-dims">{{$t('kylinLang.cube.noDims')}}</div>
            </el-col>
          </el-row>
        </div>
        <div class="ky-line ksd-mtb-30"></div>
        <div class="optimize-strategy">
          <div class="dimension-title ksd-fs-16">
            {{$t('dimensionOptimizations')}}
          </div>
          <div class="ksd-mt-20 optimize-select">
            <span>{{$t('kylinLang.cube.optimizeStrategy')}}</span>
            <common-tip :content="$t('dO')" >
              <i class="el-icon-question ksd-fs-10"></i>
            </common-tip>
            <el-select size="medium" class="ksd-ml-10" v-model="cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy']" @change="changeStrategy">
              <el-option
                v-for="item in strategyOptions"
                :key="item.value"
                :label="$t(item.name)"
                :value="item.value">
              </el-option>
            </el-select>
            <common-tip :content="$t('cubeSuggestTip')" class="ksd-ml-20">
              <el-button size="medium" plain type="primary" :loading="suggestLoading" @click="cubeSuggestions" :disabled="isReadyCube">{{$t('cubeSuggestion')}}</el-button>
            </common-tip>
            <common-tip :content="$t('resetTip')" class="ksd-ml-10">
              <el-button plain  size="medium" @click="resetDimensions" :disabled="isReadyCube">{{$t('resetDimensions')}}</el-button>
            </common-tip>
          </div>
        </div>

        <div class="ky-dashed ksd-mt-20 ksd-mb-20" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length"></div>


        <el-tabs v-model="activeName" class="el-tabs--default ksd-mt-20 fixTagsTransform" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length">
          <el-tab-pane name="first">
            <span slot="label">{{$t('aggregationGroups')}} 
              <common-tip :content="$t('AGG')" >
                <i class="el-icon-question ksd-fs-10"></i>
              </common-tip>
            </span>
            <el-row :gutter="20" class="ksd-lineheight-32">
              <el-col :span="8">
                Total cuboid number: <span>{{totalCuboid}}</span>
              </el-col>
              <el-col :span="16">
                <el-button size="medium" class="ksd-ml-20 ksd-fright" :loading="applyLoading" :disabled="isReadyCube"  @click.native="changeDimCap();">Apply</el-button>
                <el-input style="width: 180px;" class="ksd-ml-10 ksd-fright" size="medium" v-model="dim_cap" :disabled="isReadyCube"></el-input>
                <common-tip :content="$t('maxGroup')" class="ksd-fright ksd-ml-2">
                  <i class="el-icon-question ksd-fs-10 "></i>
                </common-tip>
                <span class="ksd-fright">{{$t('kylinLang.cube.maxGroupColumn')}}</span>
              </el-col>
            </el-row>
            <el-row class="ksd-mt-30 ksd-mb-18">
              <el-col :span="24">
                <el-button size="medium" type="primary" plain icon="el-icon-plus" @click="addAggGroup" :disabled="isReadyCube" >
                {{$t('addAggregationGroups')}}
                </el-button>
              </el-col>
            </el-row>

            <el-row class="ksd-mb-20" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length" v-for="(group, group_index) in cubeDesc.aggregation_groups" :key="group_index" style="border-bottom: 0;">
              <el-col :span="24">
                <el-card class="agg-card">
                  <div slot="header">
                    <span class="dimension-title">{{$t('aggregationGroups')}}—{{group_index+1}}</span>
                    <span class="ksd-fs-12"> (Cuboid Number: {{cuboidList[group_index]}} {{groupErrorList[group_index]}}
                      </span>)
                    <i class="ksd-fright el-icon-close ksd-fs-10 ksd-mt-4" v-if="!isReadyCube" @click="removeAggGroup(group_index)"></i>
                  </div>
                  <el-row class="ksd-mtb-10" :gutter="14">
                    <el-col :span="24">
                      {{$t('Includes')}} (<span>{{group.includes.length}}</span>)
                    </el-col>
                  </el-row>
                  <el-row class="ksd-mb-10" :gutter="14">
                    <el-col :span="24" class="includes_tag">
                      <area_label ref="includesSelect" placeholder="" :disabled="isReadyCube" :labels="rowkeyColumns.convertedRowkeys" :datamap="{label:'column', value:'column'}" :refreshInfo="{index: group_index, key: 'includes'}" @refreshData="refreshIncludeData"   :selectedlabels="group.includes" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail" @removeTag="removeIncludes">
                        </area_label>
                    </el-col>
                  </el-row>
                  <el-row class="ksd-mb-10 ksd-mt-20" :gutter="14">
                    <el-col :span="24">
                      {{$t('mandatoryDimensions')}}
                    </el-col>
                  </el-row>
                  <el-row :gutter="14">
                    <el-col :span="24" >
                      <area_label placeholder="" :labels="group.select_range && group.select_range.mandatoryDims" :disabled="isReadyCube" :refreshInfo="{index: group_index, key: 'mandatory_dims'}" @refreshData="refreshMandatoryData"  :selectedlabels="group.select_rule.mandatory_dims" @change="dimensionsChangeCalc(group_index)"   @checklabel="showDetail" @loadComplete="dimensionsChangeCalc(group_index)">
                      </area_label>
                    </el-col>
                  </el-row>
                  <el-row class="ksd-mb-10 ksd-mt-20">
                    <el-col :span="24">{{$t('hierarchyDimensions')}}</el-col>
                  </el-row>
                  <el-row class="ksd-mtb-10">
                    <el-col :span="24">
                      <el-button size="medium" plain  type="primary" icon="el-icon-plus" :disabled="isReadyCube"  @click="addHierarchyDims(group_index, group.select_rule.hierarchy_dims)">
                      {{$t('newHierarchy')}}
                      </el-button>
                    </el-col>
                  </el-row>
                  <el-row class="ksd-mb-10" v-for="(hierarchy_dims, hierarchy_index) in group.select_rule.hierarchy_dims" :key="hierarchy_index+'hierarchy'">
                    <el-col :span="22" >
                      <area_label placeholder="" :labels="group.select_range && group.select_range.hierarchyDims[hierarchy_index]"  :disabled="isReadyCube" :refreshInfo="{gindex: group_index, hindex: hierarchy_index, key: 'hierarchy_dims'}" @refreshData="refreshHierarchyData"  :selectedlabels="hierarchy_dims" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail" @loadComplete="dimensionsChangeCalc(group_index)">
                      </area_label>
                    </el-col>
                    <el-col :span="2">
                      <el-button class="ksd-fright deleteBtn" icon="el-icon-delete" :disabled="isReadyCube"  @click="removeHierarchyDims(group_index, hierarchy_index, group.select_rule.hierarchy_dims)">
                      </el-button>
                    </el-col>
                  </el-row>

                  <el-row class="ksd-mt-20 ksd-mb-10" :gutter="14">
                    <el-col :span="24">{{$t('jointDimensions')}}</el-col>
                  </el-row>
                  <el-row class="ksd-mb-10 ksd-mt-10" :gutter="14">
                    <el-col :span="24">
                      <el-button size="medium" plain type="primary" icon="el-icon-plus" :disabled="isReadyCube"  @click="addJointDims(group_index, group.select_rule.joint_dims)">
                      {{$t('newJoint')}}
                      </el-button>
                    </el-col>
                  </el-row>
                  <el-row class="ksd-mb-10" v-for="(joint_dims, joint_index) in group.select_rule.joint_dims" :key="joint_index+'joint'">
                    <el-col :span="22" >
                      <area_label placeholder="" :labels="group.select_range && group.select_range.jointDims[joint_index]" :disabled="isReadyCube"  :refreshInfo="{gindex: group_index, jindex: joint_index, key: 'joint_dims'}" @refreshData="refreshJointData"  :selectedlabels="joint_dims" @change="dimensionsChangeCalc(group_index)" @checklabel="showDetail" @loadComplete="dimensionsChangeCalc(group_index)">
                      </area_label>
                    </el-col>
                    <el-col :span="2">
                      <el-button class="ksd-fright deleteBtn" icon="el-icon-delete" :disabled="isReadyCube" @click="removeJointDims(group_index, joint_index, group.select_rule.joint_dims)">
                      </el-button>
                    </el-col>
                  </el-row>
                </el-card>
              </el-col>
            </el-row>

          </el-tab-pane>

          <el-tab-pane label="Rowkeys" name="second">
            <span slot="label"> Rowkeys
              <common-tip :content="$t('kylinLang.cube.rowkeyTip')" >
                 <i class="el-icon-question ksd-fs-10"></i>
              </common-tip>
            </span>

            <div class="ksd-mb-20">{{$t('dragtips')}}</div>
            <div class="rowkeys-table" v-if="cubeDesc.dimensions && cubeDesc.dimensions.length && rowkeyColumns.convertedRowkeys.length">
              <el-row class="rowkeys-header" >
                <el-col :span="1">{{$t('ID')}}</el-col>
                <el-col :span="9">{{$t('column')}}</el-col>
                <el-col :span="4">{{$t('encoding')}}</el-col>
                <el-col :span="2">{{$t('length')}}</el-col>
                <el-col :span="2">{{$t('shardBy')}}</el-col>
                <el-col :span="4">{{$t('dataType')}}</el-col>
                <el-col :span="2">{{$t('cardinality')}}</el-col>
              </el-row>
              <el-row  :disabled="isReadyCube"  v-show="rowkeyColumns.convertedRowkeys.length" class="rowkeys-body" v-for="(row, index) in rowkeyColumns.convertedRowkeys"  v-dragging="{ item: row, list: rowkeyColumns.convertedRowkeys, group: 'row' }" :key="row.column">
                <el-col :span="1">{{index+1}}</el-col>
                <el-col :span="9">
                 <common-tip placement="right" :tips="row.column" class="drag_bar">{{row.column}}</common-tip></el-col>
                <el-col :span="4">
                  <select class="rowkeySelect el-input__inner" v-model="row.encoding" @change="changeEncoding(row, index);changeRowkey(row, index);">
                    <option class="el-select-dropdown__item hover" v-for="(item, encodingindex) in row.selectEncodings" :key="encodingindex" :value="item.name + ':' + item.version">{{item.name}}</option>
                  </select>
                </el-col>
                <el-col :span="2">
                  <el-input size="small" v-model="row.valueLength"  :disabled="row.encoding.indexOf('dict')>=0||row.encoding.indexOf('date')>=0||row.encoding.indexOf('time')>=0||row.encoding.indexOf('boolean')>=0" @change="changeRowkey(row, index)"></el-input>
                </el-col>
                <el-col :span="2">
                  <select class="rowkeySelect el-input__inner" v-model="row.isShardBy" @change="changeRowkey(row, index)">
                    <option v-for="item in shardByType" :key="item.name" :value="item.value">{{item.name}}</option>
                  </select>
                </el-col>
                <el-col :span="4"> {{row.datatype}}</el-col>
                <el-col :span="2">{{row.cardinality}}</el-col>
              </el-row>
            </div>

          </el-tab-pane>
        </el-tabs>

      </el-col>
      <el-col :span="6" class="dimension-right">
        <p class="dimension-title ksd-mb-10">
          {{$t('kylinLang.dataSource.statistics')}}
        </p>
        <el-table
          :data="featureData"
          tooltip-effect="dark"
          border
          :show-header="false"
          style="width: 100%; margin-bottom: 10px;">
          <el-table-column
            label="">
            <template slot-scope="scope">
              {{$t('kylinLang.dataSource.'+scope.row.name)}}
            </template>
          </el-table-column>
          <el-table-column
          show-overflow-tooltip
            prop="content"
            label="">
          </el-table-column>
        </el-table>

        <p class="dimension-title ksd-mb-10">{{$t('sampleData')}}</p>
        <el-table
          :show-header="false"
          tooltip-effect="dark"
          :data="modelStatics"
          border
          style="width: 100%">
          <el-table-column
            prop="name"
            show-overflow-tooltip
            width="86"
            label="">
          </el-table-column>
          <el-table-column
          show-overflow-tooltip
            prop="content"
            label="">
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>


    <el-dialog :title="$t('editDimensions')" :visible.sync="addDimensionsFormVisible" top="5%" :before-close="dimensionsClose" :close-on-press-escape="false" :close-on-click-modal="false" width="1000px">
      <span slot="title" class="el-dialog__title">{{$t('editDimensions')}}
        <common-tip :content="$t('kylinLang.cube.dimensionTip')" >
          <i class="el-icon-question ksd-fs-10"></i>
        </common-tip>
      </span>
      <add_dimensions  ref="addDimensionsForm" v-on:validSuccess="addDimensionsValidSuccess" :modelDesc="modelDesc" :cubeDesc="cubeDesc" :sampleSql="sampleSql" :oldData="oldData" :addDimensionsFormVisible="addDimensionsFormVisible"></add_dimensions>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="dimensionsClose()">{{$t('cancel')}}</el-button>
        <el-button type="primary" plain size="medium" @click="checkAddDimensions">{{$t('kylinLang.common.submit')}}</el-button>
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
  props: ['cubeDesc', 'modelDesc', 'isEdit', 'cubeInstance', 'sampleSql', 'oldData', 'healthStatus'],
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
      dimensionRightDom: null,
      oldStrategy: 'auto',
      strategyOptions: [
        {value: 'auto', name: 'defaultOriented'},
        {value: 'default', name: 'dataOriented'},
        {value: 'whitelist', name: 'businessOriented'}
      ]
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
      kapConfirm(this.$t('resetDimensionsTip'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        this.dim_cap = 0
        this.cubeDesc.aggregation_groups.splice(0, this.cubeDesc.aggregation_groups.length)
        this.initRowkeyColumns()
        this.initAggregationGroup(true)
        this.calcAllCuboid()
      })
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
    checkAutoDimensions: function () {
      this.$refs['autoDimensionsForm'].$emit('autoDimsFormValid')
    },
    changeStrategy: function (val, strategy) {
      if (val === 'whitelist' && this.sampleSql.sqlCount <= 0) {
        this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] = strategy || this.oldStrategy
        if (!strategy) {
          kapConfirm(this.$t('kylinLang.cube.businessOrientedTip'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            showCancelButton: false,
            type: 'warning'
          })
        }
        return
      }
      if (val === 'default' && this.healthStatus.status !== 'GOOD') {
        this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] = strategy || this.oldStrategy
        if (!strategy) {
          kapConfirm(this.$t('kylinLang.cube.dataOrientedTip'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            showCancelButton: false,
            type: 'warning'
          })
        }
        return
      }
      this.oldStrategy = this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy']
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
        let newGroup = {includes: objectClone(this.currentRowkey) || [], select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}}
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
      if (group && group.includes) {
        this.$set(group, 'select_range', {
          mandatoryDims: [],
          hierarchyDims: [],
          jointDims: []
        })
        let usedInMandatory = []
        let usedInHierarchy = {allDims: [], eachOtherDims: []}
        let usedInJoint = {allDims: [], eachOtherDims: []}
        usedInMandatory = group.select_rule.mandatory_dims
        group.select_rule.hierarchy_dims.forEach((dim, index) => {
          usedInHierarchy.allDims = usedInHierarchy.allDims.concat(dim)
          this.$set(usedInHierarchy.eachOtherDims, index, [])
          this.$set(group.select_range.hierarchyDims, index, [])
        })
        group.select_rule.hierarchy_dims.forEach((dim, index) => {
          usedInHierarchy.eachOtherDims.forEach((eoDim, eoIndex) => {
            if (index !== eoIndex) {
              usedInHierarchy.eachOtherDims[eoIndex] = eoDim.concat(dim)
            }
          })
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
          if (usedInHierarchy.allDims.indexOf(column) === -1 && usedInJoint.allDims.indexOf(column) === -1) {
            group.select_range.mandatoryDims.push(column)
          }
          if (usedInMandatory.indexOf(column) === -1 && usedInJoint.allDims.indexOf(column) === -1) {
            group.select_rule.hierarchy_dims.forEach((dim, index) => {
              if (usedInHierarchy.eachOtherDims[index].indexOf(column) === -1) {
                group.select_range.hierarchyDims[index].push(column)
              }
            })
          }
          if (usedInMandatory.indexOf(column) === -1 && usedInHierarchy.allDims.indexOf(column) === -1) {
            group.select_rule.joint_dims.forEach((dim, index) => {
              if (usedInJoint.eachOtherDims[index].indexOf(column) === -1) {
                group.select_range.jointDims[index].push(column)
              }
            })
          }
        })
      }
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
      hierarchyDims.unshift([])
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    removeJointDims: function (gindex, index, jointDims) {
      jointDims.splice(index, 1)
      this.initCalCuboid()
      this.initAggSelectRange(this.cubeDesc.aggregation_groups[gindex], gindex)
    },
    addJointDims: function (gindex, jointDims) {
      jointDims.unshift([])
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
      }, 400)
    }
  },
  created () {
    this.changeStrategy(this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'], 'auto')
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
    this.oldStrategy = this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy']
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
    'en': {dimensions: 'Dimensions', name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', action: 'Action', editDimensions: 'Edit Dimensions', editDimension: 'Edit Dimensions', filter: 'Filter...', cancel: 'Cancel', yes: 'Ok', aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', addAggregationGroups: 'Aggregation Groups', newHierarchy: 'New Hierarchy', newJoint: 'New Joint', ID: 'ID', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', resetDimensions: 'Reset', cubeSuggestion: 'Optimize', collectsqlPatterns: 'Collect SQL Patterns', dimensionOptimizations: 'Dimension Optimizations', dO: 'Optimize strategy contains:<br/>1.  Default: automatically suggest aggregate group settings and Rowkey settings.<br/>2. Data-oriented: digest source data feature to suggest one aggregate group, which optimizes all dimensions for flexible analysis scenarios.<br/> 3. Business-oriented: only digest SQL patterns to suggest multiple aggregate groups. It\'s used to answer some known queries.', cubeSuggestTip: 'Click the optimize to generate following output: <br/>aggregate group settings and Rowkey settings.', resetTip: 'Reset will drop all existing the aggregate group settings and Rowkey order.', AGG: 'Aggregation group is group of cuboids that are constrained by common rules. <br/>Users can apply different settings on cuboids in all aggregation groups to meet the query requirements, and saving storage space.', maxGroup: 'Dimension limitations mean max dimensions may be contained within a group of SQL queries. In a set of queries, if each query required the number of dimensions is not more than five, you can set 5 here.', moreRowkeyTip: 'Current selected normal dimensions are exploding, "Optimize" may suggest unreasonable less cuboid.', dimensionUsed: 'This dimension is used as a mandatory、 a hierarchy and a joint dimension.', sampleData: 'Sample', dragtips: 'you can drag and drop to reorder', dataOriented: 'Data Oriented', businessOriented: 'Business Oriented', defaultOriented: 'Default', resetDimensionsTip: 'Reset will call last saving back and overwrite existing Aggregation groups and Rowkeys. Please confirm to continue?'},
    'zh-cn': {dimensions: '维度', name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', action: '操作', editDimensions: '编辑维度', editDimension: 'Edit Dimension', filter: '过滤器', cancel: '取消', yes: '确定', aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', addAggregationGroups: '添加聚合组', newHierarchy: '新的层级维度', newJoint: '新的联合维度', ID: 'ID', encoding: '编码', length: '长度', shardBy: 'Shard By', dataType: '数据类型', resetDimensions: '重置', cubeSuggestion: '维度优化', collectsqlPatterns: '输入sql', dimensionOptimizations: '维度优化', dO: '优化策略包括：<br/>1. 默认：自动的根据优化器输入，推荐出聚合组优化方式与Rowkey顺序。 <br/>2. 模型优先：参考数据的特征，推荐一个聚合组优化了所有的维度，更适用于灵活度高的查询。<br/>3. 业务优先：参考输入的SQL语句，推荐N个完全由必要维度组成的聚合组，旨在定向回答这些SQL语句。', cubeSuggestTip: '点击维度优化将输出：优化器推荐的聚合组设置与Rowkey顺序。', resetTip: '重置则会清空已有的聚合组设置与当前Rowkey顺序。', AGG: '聚合组是指受到共同规则约束的维度组合。 <br/>使用者可以对所有聚合组里的维度组合进行不同设置以满足查询需求，并最大化节省存储空间。', maxGroup: '查询最大维度数是指一组查询语句中所含维度的最大值。在查询中，每条查询所需的维度数基本都不超过5，则可以在这里设置5。', moreRowkeyTip: '当前选择的普通维度太多，一键优化可能给出过度剪枝的设置。', dimensionUsed: '该维度被用作必须 、层级以及联合维度。', sampleData: '采样数据', dragtips: '您可以通过拖拽列表对rowkey进行排序', dataOriented: '模型优先', businessOriented: '业务优先', defaultOriented: '默认', resetDimensionsTip: '重置操作会返回上一次保存过的聚合组和Rowkeys列表，请确认是否继续此操作？'}
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .dimension-edit{
    .rowkeySelect::-ms-expand { display: none; }
    .rowkeySelect {
      line-height: 28px;
      height: 28px;
      width: 100%;
      background: url('../../../assets/img/arrow.png') no-repeat scroll right center transparent!important;
      /*很关键：将默认的select选择框样式清除*/
      appearance:none;
      -moz-appearance:none;
      -webkit-appearance:none;
      padding-left:10px;
      -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
      outline: none;
      option{
        padding:4px;
        border:none;

      }
    }
    .optimize-strategy {
      .el-select {
        width: 220px;
      }
      .optimize-select >span {
        color: @text-title-color;
      }
    }
    .dimension-right {
      padding-left: 25px;
      margin-top: -15px;
      padding-top: 15px;
      .cell{
        white-space: nowrap;
      }
    }
    .dimension-left {
      padding: 15px 30px 50px 0px;
      border-right: 1px solid @text-placeholder-color;
      margin: -10px 0px;
      min-height: 215px;
    }
    .rowkeys-table {
      border-collapse:collapse;
      color: @text-title-color;
      border-top:solid 1px @line-border-color;
      border-left:solid 1px @line-border-color;
      .rowkeys-header {
        background: @modeledit-bg-color;
        height: 47px;
        text-align: left;
        .el-col {
          line-height: 28px;
          border-right:solid 1px @line-border-color;
          border-bottom:solid 1px @line-border-color;
          padding: 9px 10px;
          word-wrap: break-word;
          white-space:nowrap;
          text-overflow: ellipsis;
          overflow: hidden;
        }
      }
      .rowkeys-body {
        cursor: move;
        height: 47px;
        .el-col {
          height: 47px;
          line-height: 28px;
          padding: 9px 10px;
          border-right:solid 1px @line-border-color;
          border-bottom:solid 1px @line-border-color;
          word-wrap: break-word;
          white-space:nowrap;
          text-overflow: ellipsis;
          overflow: hidden;
        }
      }
    }
    .dimension-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .add-dimensions {
      .dimension-type {
        .dimension-block {
          width: 8px;
          height: 8px;
          margin: 11px 6px 0px;
        }
        >:nth-child(1) .dimension-block{
          background: @warning-color-1;
        }
        >:nth-child(2) .dimension-block{
          background: @base-color-1;
        }
        li{
          float: left;
          line-height: 24px;
        }
      }  
      .dimension-label  {
        .el-tag {
          cursor: pointer;
          outline: 0;
        }
        .el-tag:focus {
          box-shadow: 1px 0 2px 0 @text-secondary-color,
                      0 1px 2px 0 @text-normal-color;
        }
      }
      .no-dims {
        border: 1px solid @text-placeholder-color;
        border-radius: 2px;
        line-height: 145px;
        height: 145px;
        text-align: center;
      }
    }
    .agg-card {
      border: 1px solid @text-placeholder-color;
      box-shadow: 0px 2px 4px 0px @text-placeholder-color,
                  0px 0px 6px 0px @text-placeholder-color;
      .deleteBtn {
        padding: 10px;
      }
      .el-card__header {
        padding: 10px 20px;
        background-color: @grey-4;
        i {
          cursor: pointer;
        }
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
          .el-icon-close {
            color: @fff;
          }
        }
      }
    }
  }
  
</style>
