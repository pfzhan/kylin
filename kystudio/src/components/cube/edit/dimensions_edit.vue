<template>
<div>
  <el-row>
    <el-col :span="18" class="border_right">  
      <el-row class="row_padding border_bottom">
        <el-col :span="5">
          <el-button type="primary" icon="setting" @click.native="resetDimensions">{{$t('resetDimensions')}}</el-button>
        </el-col>
        <el-col :span="5">
          <el-button type="primary" icon="menu" @click.native="cubeSuggestions">{{$t('cubeSuggestion')}}</el-button>
        </el-col>
      </el-row>


      <el-row class="row_padding border_bottom">
        <el-row class="row_padding">
          <el-col :span="24">{{$t('dimensions')}}</el-col>
        </el-row>
        <el-row class="row_padding">
        <el-col :span="24">         
         <el-button  icon="plus" @click.native="addDimensions">{{$t('addDimensions')}}</el-button></el-col>
       </el-row>
       <el-row class="row_padding">
        <el-col  :span="24">
          <el-card >
            <el-tag class="tag_margin"
            v-for="(dimension, index) in cubeDesc.dimensions"
            :key="index"
            :type="dimension.derived?'gray':'primary'">
            {{dimension.table+'.'+dimension.name}}
            </el-tag>
          </el-card>
        </el-col>
        </el-row>
      </el-row>

      <el-row class="row_padding border_bottom">
        <el-col :span="6">{{$t('aggregationGroups')}}</el-col>
        <el-col :span="6">Max group by column:</el-col>
        <el-col :span="3"><el-input v-model="dim_cap" @change="changeDimCap"></el-input></el-col>
      </el-row>
      <el-row class="row_padding border_bottom" v-for="(group, group_index) in cubeDesc.aggregation_groups" :key="group_index">
        <el-col :span="24">
          <el-card >
            <el-row>
              <el-col :span="6">
                Cuboid Number: {{cuboidList[group_index]}}
              </el-col> 
            </el-row>
            <el-row class="row_padding" >
              <el-col :span="1">#{{group_index+1}}</el-col>
              <el-col :span="22">
                <el-row class="row_padding">
                  <el-col :span="5">{{$t('Includes')}}</el-col>
                </el-row> 
                <el-row> 
                  <el-col :span="24">
                    <area_label :labels="currentRowkey"  :selectedlabels="group.includes" @change="refreshAggragation(group, group_index)" @checklabel="selectTagshowDetail($event, group.includes)"> 
                    </area_label>
                  </el-col>
                </el-row>
                <el-row class="row_padding">
                  <el-col :span="5">{{$t('mandatoryDimensions')}}</el-col>
                </el-row>  
                <el-row>
                  <el-col :span="24" >
                    <area_label :labels="group.includes"  :selectedlabels="group.select_rule.mandatory_dims" @change="refreshAggragation(group, group_index)" @checklabel="selectTagshowDetail($event, group.select_rule.mandatory_dims)"> 
                    </area_label>
                  </el-col>
                </el-row>
                <el-row class="row_padding">
                  <el-col :span="5">{{$t('hierarchyDimensions')}}</el-col>
                </el-row>
                <el-row>
                  <el-col :span="24">
                    <el-row class="row_padding" :gutter="10" v-for="(hierarchy_dims, hierarchy_index) in group.select_rule.hierarchy_dims" :key="hierarchy_index">
                       <el-col :span="23" >
                        <area_label :labels="group.includes"  :selectedlabels="hierarchy_dims" @change="refreshAggragation(group, group_index)" @checklabel="selectTagshowDetail($event, hierarchy_dims)"> 
                        </area_label>
                      </el-col>  
                      <el-col :span="1">
                        <el-button type="danger" icon="minus" size="mini" @click="removeHierarchyDims(hierarchy_index, group.select_rule.  hierarchy_dims)">
                      </el-button>
                    </el-col>
                  </el-row>
                </el-col>
              </el-row>
              <el-row>
                <el-col :span="5">
                  <el-button icon="plus" @click="addHierarchyDims( group.select_rule.hierarchy_dims)">
                  {{$t('newHierarchy')}}
                  </el-button>                
                </el-col>
              </el-row>      
              <el-row class="row_padding">
                <el-col :span="5">{{$t('jointDimensions')}}</el-col>
              </el-row>  
              <el-row>
                <el-col :span="24">
                  <el-row class="row_padding" :gutter="10" v-for="(joint_dims, joint_index) in group.select_rule.joint_dims" :key="joint_index">
                    <el-col :span="23" >
                      <area_label :labels="group.includes"  :selectedlabels="joint_dims" @change="refreshAggragation(group, group_index)" @checklabel="selectTagshowDetail($event, joint_dims)"> 
                      </area_label>
                    </el-col>
                    <el-col :span="1" >                
                      <el-button type="danger" icon="minus" size="mini" @click="removeJointDims(joint_index, group.select_rule.joint_dims)">
                      </el-button>
                    </el-col>  
                 </el-row>
                </el-col>
               </el-row> 
               <el-row>
                 <el-col :span="5">
                  <el-button icon="plus" @click="addJointDims( group.select_rule.joint_dims)">
                  {{$t('newJoint')}}
                  </el-button>                 
                </el-col>                    
              </el-row>
            </el-col>
            <el-col :span="1">            
              <el-button type="danger" icon="minus" size="mini" @click="removeAggGroup(group_index)">
              </el-button>
            </el-col>
          </el-row>
        </el-card>  
      </el-col>
    </el-row>
    <el-row class="row_padding border_bottom">
      <el-col :span="24">
        <el-button icon="plus" @click="addAggGroup" class="table_margin">
        {{$t('addAggregationGroups')}}
        </el-button>
      </el-col>
    </el-row>
      <el-row class="row_padding">
        <el-col :span="24">Rowkeys</el-col>
      </el-row>
     <!--  <table class="ksd-common-table">
        <tr> 
          <th>{{$t('ID')}}</th>
          <th>{{$t('column')}}</th>
          <th>{{$t('encoding')}}</th>
          <th>{{$t('length')}}</th>
          <th>{{$t('shardBy')}}</th>
          <th>{{$t('dataType')}}</th>
          <th>{{$t('cardinality')}}</th>
        </tr>
        <tr v-for="(row, index) in convertedRowkeys" :key="row.column" v-dragging="{ item: row, list: convertedRowkeys, group: 'row' }">
          <td>{{index+1}}</td>
          <td><common-tip :tips="row.column" class="drag_bar">{{(row.column)|omit(16,'...')}}</common-tip></td>
          <td>
              <el-select v-model="row.encoding" @change="changeRowkey(row, index)">
                <el-option
                    v-for="(item, encodingindex) in initEncodingType(row)"
                    :key="encodingindex"
                   :label="item.name"
                   :value="item.name + ':' + item.version">
                   <el-tooltip effect="light" :content="$t('kylinLang.cube[$store.state.config.encodingTip[item.name]]')" placement="right">
                     <span style="float: left;width: 90%">{{ item.name }}</span>
                     <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-show="item.version>1">{{ item.version }}</span>
                  </el-tooltip>
                </el-option>              
              </el-select>
          </td>
          <td> 
            <el-input v-model="row.valueLength"  :disabled="row.encoding.indexOf('dict')>=0||row.encoding.indexOf('date')>=0||row.encoding.indexOf('time')>=0" @change="changeRowkey(row, index)"></el-input> 
          </td>
          <td>
            <el-select v-model="row.isShardBy" @change="changeRowkey(row, index)">
              <el-option
              v-for="item in shardByType"
              :key="item.name"
              :label="item.name"
              :value="item.value">
              </el-option>
            </el-select>
          </td>
          <td>{{modelDesc.columnsDetail[row.column].datatype}}</td>
          <td>{{modelDesc.columnsDetail[row.column].cardinality}}</td>
        </tr>
      </table> -->
      <div style="position:relative">
      <ul class="dragBar">
         <li v-for="(rowkey, index) in convertedRowkeys" v-dragging="{ item: rowkey, list: convertedRowkeys, group: 'rowkey' }"></li>
      </ul>
      <el-table class="table_margin"
        :data="convertedRowkeys"
        style="width: 100%">

        <el-table-column
          :label="$t('ID')"
          width="55"
          :allData="convertedRowkeys"
          header-align="center"
          align="center">

          <template scope="scope" >
            <el-tag >{{scope.$index+1}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column
            property="column"
            :label="$t('column')"
            header-align="center"
           align="center">
           <!-- <template scope="scope" ><common-tip :tips="scope.column">{{(scope.column)|omit(16,'...')}}</common-tip></template> -->
        </el-table-column>       
        <el-table-column
            :label="$t('encoding')"
            header-align="center"
           align="center"
           width="150">
            <template scope="scope">
              <el-select v-model="scope.row.encoding" @change="changeRowkey(scope.row, scope.$index)">
                <el-option
                    v-for="(item, index) in initEncodingType(scope.row)"
                    :key="index"
                   :label="item.name"
                   :value="item.name + ':' + item.version">
                   <el-tooltip effect="light" :content="$t('kylinLang.cube[$store.state.config.encodingTip[item.name]]')" placement="right">
                     <span style="float: left;width: 90%">{{ item.name }}</span>
                     <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-show="item.version>1">{{ item.version }}</span>
                  </el-tooltip>
                </el-option>              
              </el-select>
            </template>
        </el-table-column>    
        <el-table-column
            :label="$t('length')"
            header-align="center"
           align="center"
           width="110">
            <template scope="scope">
              <el-input v-model="scope.row.valueLength"  :disabled="scope.row.encoding.indexOf('dict')>=0||scope.row.encoding.indexOf('date')>=0||scope.row.encoding.indexOf('time')>=0" @change="changeRowkey(scope.row, scope.$index)"></el-input>     
            </template>        
        </el-table-column>    
        <el-table-column
         :label="$t('shardBy')"
         header-align="center"
         align="center"
         width="110">
          <template scope="scope">
            <el-select v-model="scope.row.isShardBy" @change="changeRowkey(scope.row, scope.$index)">
              <el-option
              v-for="item in shardByType"
              :key="item.name"
              :label="item.name"
              :value="item.value">
              </el-option>
            </el-select>              
          </template>
        </el-table-column>    
        <el-table-column
            :label="$t('dataType')"
            header-align="center"
           align="center"
           width="140">
           <template scope="scope">
             {{modelDesc.columnsDetail[scope.row.column].datatype}}
           </template>
        </el-table-column>    
        <el-table-column
            :label="$t('cardinality')"
            header-align="center"
           align="center"
           width="110">
           <template scope="scope">
             {{modelDesc.columnsDetail[scope.row.column].cardinality}}
           </template>
        </el-table-column>                                        
      </el-table>  
      </div>
    </el-col>
    <el-col :span="6">
    </el-col>
  </el-row> 
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
import { handleSuccess, handleError, loadBaseEncodings } from '../../../util/business'
import { mapActions } from 'vuex'
import areaLabel from '../../common/area_label'
import addDimensions from '../dialog/add_dimensions'
export default {
  name: 'dimensions',
  props: ['cubeDesc', 'modelDesc', 'isEdit'],
  data () {
    return {
      dim_cap: 0,
      addDimensionsFormVisible: false,
      selected_dimension: {},
      selected_project: localStorage.getItem('selected_project'),
      pfkMap: {},
      cuboidList: [],
      shardByType: [{name: 'true', value: true}, {name: 'false', value: false}],
      rowkeyColumns: [],
      oldRowkey: [],
      currentRowkey: [],
      convertedRowkeys: []
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
      getCubeSuggestions: 'GET_CUBE_SUGGESTIONS'
    }),
    resetDimensions: function () {
      let _this = this
      _this.cubeDesc.dimensions.splice(0, _this.cubeDesc.dimensions.length)
      _this.dim_cap = 0
      _this.cubeDesc.aggregation_groups.splice(0, _this.cubeDesc.aggregation_groups.length)
      _this.cubeDesc.rowkey.rowkey_columns.splice(0, _this.cubeDesc.rowkey.rowkey_columns.length)
      this.initConvertedRowkeys()
    },
    cubeSuggestions: function () {
      this.getCubeSuggestions({cubeDescData: JSON.stringify(this.cubeDesc)}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cubeDesc, 'dimensions', data.dimensions)
          this.$set(this.cubeDesc, 'aggregation_groups', data.aggregation_groups)
          this.$set(this.cubeDesc, 'override_kylin_properties', data.override_kylin_properties)
          this.dim_cap = data.aggregation_groups[0].dim_cap || 0
          this.$set(this.cubeDesc.rowkey, 'rowkey_columns', data.rowkey.rowkey_columns)
          this.initConvertedRowkeys()
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          console.log(status, 30000)
          // if (status === 404) {
          //   _this.$router.replace('access/login')
          // }
        })
      })
    },
    addDimensions: function () {
      this.addDimensionsFormVisible = true
    },
    checkAddDimensions: function () {
      this.$refs['addDimensionsForm'].$emit('addDimensionsFormValid')
    },
    addDimensionsValidSuccess: function (data) {
      let _this = this
      _this.cubeDesc.dimensions.splice(0, _this.cubeDesc.dimensions.length)
      for (let table in data) {
        if (data[table] && data[table].length > 0) {
          data[table].forEach(function (column) {
            let colObj = {name: column.name, table: table, column: null, derived: null}
            if (column.derived === 'true') {
              _this.$set(colObj, 'derived', [column.column])
            } else {
              _this.$set(colObj, 'column', column.column)
            }
            _this.cubeDesc.dimensions.push(colObj)
          })
        }
      }
      _this.initRowkeyColumns()
      _this.initAggregationGroup()
      _this.addDimensionsFormVisible = false
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
      let _this = this
      _this.currentRowkey = []
      _this.oldRowkey = []
      this.modelDesc.lookups.forEach(function (lookup) {
        let table = lookup.alias
        _this.pfkMap[table] = {}
        lookup.join.primary_key.forEach(function (pk, index) {
          _this.pfkMap[table][pk] = lookup.join.foreign_key[index]
        })
      })
      this.cubeDesc.dimensions.forEach(function (dimension, index) {
        if (dimension.derived && dimension.derived.length) {
          let lookup = []
          _this.modelDesc.lookups.forEach(function (lookupTable) {
            if (lookupTable.alias === dimension.table) {
              lookup = lookupTable
            }
          })
          lookup.join.foreign_key.forEach(function (fk, index) {
            if (_this.currentRowkey.indexOf(fk) === -1) {
              _this.currentRowkey.push(fk)
            }
          })
        } else if (dimension.column && !dimension.derived) {
          let tableName = dimension.table
          let columnName = dimension.column
          let rowkeyColumn = dimension.table + '.' + dimension.column
          if (_this.pfkMap[tableName] && _this.pfkMap[tableName][columnName]) {
            rowkeyColumn = _this.pfkMap[tableName][columnName]
          }
          if (_this.currentRowkey.indexOf(rowkeyColumn) === -1) {
            _this.currentRowkey.push(rowkeyColumn)
          }
        }
      })
      _this.cubeDesc.rowkey.rowkey_columns.forEach(function (rowkeyColumn) {
        _this.oldRowkey.push(rowkeyColumn.column)
      })
      _this.currentRowkey.forEach(function (rowkey) {
        if (_this.oldRowkey.indexOf(rowkey) === -1) {
          let baseEncodings = loadBaseEncodings(_this.$store.state.datasource)
          _this.cubeDesc.rowkey.rowkey_columns.push({
            column: rowkey,
            encoding: 'dict',
            encoding_version: baseEncodings.getEncodingMaxVersion('dict'),
            isShardBy: false
          })
        }
      })
      _this.oldRowkey.forEach(function (rowkey) {
        if (_this.currentRowkey.indexOf(rowkey) === -1) {
          for (let i = 0; i < _this.cubeDesc.rowkey.rowkey_columns.length; i++) {
            if (_this.cubeDesc.rowkey.rowkey_columns[i].column === rowkey) {
              _this.cubeDesc.rowkey.rowkey_columns.splice(i, 1)
            }
          }
        }
      })
      _this.initConvertedRowkeys()
    },
    initConvertedRowkeys: function () {
      let _this = this
      _this.convertedRowkeys = []
      _this.cubeDesc.rowkey.rowkey_columns.forEach(function (rowkey) {
        let version = rowkey.encoding_version || 1
        _this.convertedRowkeys.push({column: rowkey.column, encoding: _this.getEncoding(rowkey.encoding) + ':' + version, valueLength: _this.getLength(rowkey.encoding), isShardBy: rowkey.isShardBy})
      })
    },
    initEncodingType: function (rowkey) {
      let _this = this
      let datatype = this.modelDesc.columnsDetail[rowkey.column].datatype
      let baseEncodings = loadBaseEncodings(_this.$store.state.datasource)
      let filterEncodings = baseEncodings.filterByColumnType(datatype)
      if (this.isEdit) {
        let _encoding = _this.getEncoding(rowkey.encoding)
        let _version = parseInt(_this.getVersion(rowkey.encoding))
        // console.log(_encoding, _version, 456235587)
        let addEncodings = baseEncodings.addEncoding(_encoding, _version)
        return addEncodings
      } else {
        return filterEncodings
      }
    },
    changeRowkey: function (rowkey, index) {
      let _this = this
      _this.$set(_this.cubeDesc.rowkey.rowkey_columns[index], 'isShardBy', rowkey.isShardBy)
      _this.$set(_this.cubeDesc.rowkey.rowkey_columns[index], 'encoding_version', _this.getVersion(rowkey.encoding))
      if (rowkey.valueLength) {
        _this.$set(_this.cubeDesc.rowkey.rowkey_columns[index], 'encoding', _this.getEncoding(rowkey.encoding) + ':' + rowkey.valueLength)
      } else {
        _this.$set(this.cubeDesc.rowkey.rowkey_columns[index], 'encoding', _this.getEncoding(rowkey.encoding))
      }
      if (rowkey.encoding.indexOf('dict') >= 0 || rowkey.encoding.indexOf('date') >= 0 || rowkey.encoding.indexOf('time') >= 0) {
        _this.$set(rowkey, 'valueLength', null)
      }
    },
    changeDimCap: function () {
      let _this = this
      this.cubeDesc.aggregation_groups.forEach(function (aggregationGroup) {
        _this.$set(aggregationGroup, 'dim_cap', _this.dim_cap)
        _this.refreshAggragation()
      })
    },
    initAggregationGroup: function () {
      let _this = this
      if (!_this.isEdit && _this.currentRowkey.length > 0 && _this.cubeDesc.aggregation_groups.length <= 0) {
        let newGroup = {includes: _this.currentRowkey, select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}}
        _this.cubeDesc.aggregation_groups.push(newGroup)
        _this.cuboidList.push(0)
      }
      _this.cubeDesc.aggregation_groups.forEach(function (aggregationGroup, groupIndex) {
        for (let i = 0; i < aggregationGroup.includes.length; i++) {
          if (_this.currentRowkey.indexOf(aggregationGroup.includes[i]) === -1) {
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
        _this.refreshAggragation(groupIndex)
      })
    },
    refreshAggragation: function (index) {
      let _this = this
      _this.calCuboid({cubeDescData: JSON.stringify(_this.cubeDesc), aggIndex: index}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cuboidList, index, data)
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          console.log(status, 30000)
          // if (status === 404) {
          //   _this.$router.replace('access/login')
          // }
        })
      })
    },
    initCalCuboid: function () {
      let _this = this
      _this.cubeDesc.aggregation_groups.forEach(function (aggregationGroup, groupIndex) {
        _this.refreshAggragation(groupIndex)
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
      this.cubeDesc.aggregation_groups.push({includes: [], select_rule: {mandatory_dims: [], hierarchy_dims: [], joint_dims: []}})
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
    this.$dragging.$on('dragend', ({ value }) => {
      console.log(value)
    })
  },
  locales: {
    'en': {dimensions: 'Dimensions', name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', action: 'Action', addDimensions: 'Add Dimensions', editDimension: 'Edit Dimensions', filter: 'Filter...', cancel: 'Cancel', yes: 'Yes', aggregationGroups: 'Aggregation Groups', Includes: 'Includes', mandatoryDimensions: 'Mandatory Dimensions', hierarchyDimensions: 'Hierarchy Dimensions', jointDimensions: 'Joint Dimensions', addAggregationGroups: 'Aggregation Groups', newHierarchy: 'New Hierarchy', newJoint: 'New Joint', ID: 'ID', encoding: 'Encoding', length: 'Length', shardBy: 'Shard By', dataType: 'Data Type', resetDimensions: 'Reset', cubeSuggestion: 'Cube Suggestion'},
    'zh-cn': {dimensions: '维度', name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', action: '操作', addDimensions: '添加维度', editDimension: 'Edit Dimension', filter: '过滤器', cancel: '取消', yes: '确定', aggregationGroups: '聚合组', Includes: '包含的维度', mandatoryDimensions: '必需维度', hierarchyDimensions: '层级维度', jointDimensions: '联合维度', addAggregationGroups: '添加聚合组', newHierarchy: '新的层数', newJoint: '新的组合', ID: 'ID', encoding: '编码', length: '长度', shardBy: 'Shard By', dataType: '数据类型', resetDimensions: '重置', cubeSuggestion: 'Cube 建议'}
  }
}
</script>
<style lang="less">
 .dragBar {
   position: absolute;
   z-index: 9999;
   top: 40px;
   li{
     height: 40px;
     line-height: 40px;
     width: 100px;
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
</style>
