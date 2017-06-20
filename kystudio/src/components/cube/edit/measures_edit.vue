<template>
<div id="measures">
  <div style="margin-top: 20px;">
    <el-button type="blue" icon="menu" @click.native="cubeSuggestions" :disabled="isReadyCube">{{$t('measuresSuggestion')}}</el-button>
    <el-button type="default" icon="setting" @click.native="resetMeasures" :disabled="isReadyCube">{{$t('resetMeasures')}}</el-button>
     <common-tip :content="$t('measuresSuggestTip')" >
             <icon name="question-circle-o"></icon>
    </common-tip>
  </div>
  <el-table class="table_margin"
    :data="cubeDesc.measures"
    border stripe 
    style="width: 100%">
    <el-table-column
      property="name"
      :label="$t('name')"
      header-align="center"
      align="center">
    </el-table-column>
    <el-table-column
      property="function.expression"
      header-align="center"
      align="center"
      :label="$t('expression')"
      width="180">
    </el-table-column>    
    <el-table-column
      :label="$t('parameters')"
      header-align="center"
      align="center">
      <template scope="scope">
        <parameter_tree :measure="scope.row">
        </parameter_tree>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('datatype')"
      header-align="center"
      align="center"
      width="110">
      <template scope="scope">
        <span v-if="modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{modelDesc.columnsDetail[scope.row.function.parameter.value].datatype}}
        </span>
      </template>
    </el-table-column>  
    <el-table-column
      :label="$t('comment')"
      width="110">
      <template scope="scope">
        <span v-if="modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{modelDesc.columnsDetail[scope.row.function.parameter.value].comment}}
        </span>
      </template>
    </el-table-column>
    <el-table-column
      property="function.returntype"    
      :label="$t('returnType')"
      header-align="center"
      align="center"
      width="120">
    </el-table-column>  
    <el-table-column
      :label="$t('action')"
      header-align="center"
      align="center"
      width="100">
      <template scope="scope">
        <el-button type="edit"  size="mini" icon="edit" :disabled="isReadyCube"  @click="editMeasure(scope.row)"></el-button>
        <el-button type="edit"  size="mini" icon="delete" :disabled="isReadyCube" @click="removeMeasure(scope.row, scope.$index)"></el-button>
      </template>
    </el-table-column>                     
  </el-table>
   <el-button type="primary" icon="plus" @click="addMeasure" class="ksd-mb-20">{{$t('addMeasure')}}</el-button>  
      <el-row v-if="!isPlusVersion">
        <el-col :span="24">{{$t('advancedColumnFamily')}}</el-col>
      </el-row> 
      <el-table class="table_margin" v-if="!isPlusVersion"
        :data="cubeDesc.hbase_mapping.column_family"
        style="width: 100%">
        <el-table-column
            property="name"
            :label="$t('columnFamily')"
            width="150">
        </el-table-column>       
        <el-table-column
            :label="$t('measures')">
            <template scope="scope">  
              <el-col :span="24">
                <area_label :labels="currentMeasure" :selectedlabels="scope.row.columns[0].measure_refs"> 
                </area_label>
              </el-col>    
            </template>
        </el-table-column>
        <el-table-column
        width="110">
            <template scope="scope">
              <el-button type="delete" icon="minus" size="mini" @click="removeColumnFamily(scope.$index)">
              </el-button>               
            </template>
        </el-table-column>                                              
      </el-table>   
     <el-button type="primary" icon="plus" v-if="!isPlusVersion" @click="addColumnFamily">
      {{$t('addColumnFamily')}}</el-button>      
  
  <el-dialog :title="$t('editMeasure')" v-model="measureFormVisible" top="5%" size="small">
    <add_measures  ref="measureForm" :cubeDesc="cubeDesc" :modelDesc="modelDesc" :measureDesc="selected_measure" v-on:validSuccess="measureValidSuccess"></add_measures>
    <span slot="footer" class="dialog-footer">
      <el-button @click="measureFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkMeasureForm">{{$t('yes')}}</el-button>
    </span>     
  </el-dialog>    
</div>  
</template>
<script>
import { mapActions } from 'vuex'
import addMeasures from '../dialog/add_measures'
import parameterTree from '../../common/parameter_tree'
import areaLabel from '../../common/area_label'
import { needLengthMeasureType } from '../../../config/index'
import { handleSuccess, handleError } from 'util/business'
export default {
  name: 'measures',
  props: ['cubeDesc', 'modelDesc'],
  data () {
    return {
      measureFormVisible: false,
      selected_measure: {},
      editDictionaryFormVisible: false,
      selected_dictionary: null,
      currentMeasure: []
    }
  },
  components: {
    'parameter_tree': parameterTree,
    'add_measures': addMeasures,
    'area_label': areaLabel
  },
  methods: {
    ...mapActions({
      loadHiddenFeature: 'LOAD_HIDDEN_FEATURE',
      getCubeSuggestions: 'GET_CUBE_DIMENSIONS'
    }),
    resetMeasures: function () {
      // this.cubeDesc.dimensions.splice(0, this.cubeDesc.dimensions.length)
      this.cubeDesc.measures.splice(0, this.cubeDesc.measures.length)
      this.cubeDesc.hbase_mapping.column_family.splice(0, this.cubeDesc.hbase_mapping.column_family.length)
      // this.cubeDesc.rowkey.rowkey_columns.splice(0, this.cubeDesc.rowkey.rowkey_columns.length)
      // this.initConvertedRowkeys()
    },
    cubeSuggestions: function () {
      this.getCubeSuggestions({model: this.cubeDesc.model_name, cube: this.cubeDesc.name}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cubeDesc, 'measures', data.measures)
          this.$set(this.cubeDesc.hbase_mapping, 'column_family', data.hbase_mapping.column_family)
        })
      }, (res) => {
        handleError(res)
      })
    },
    addMeasure: function () {
      this.selected_measure = {
        name: ' ',
        function: {
          expression: 'SUM',
          parameter: {
            type: 'column',
            value: ''
          },
          returntype: ''
        }
      }
      this.measureFormVisible = true
    },
    editMeasure: function (measure) {
      this.selected_measure = measure
      this.measureFormVisible = true
    },
    checkMeasureForm: function () {
      this.$refs['measureForm'].$emit('measureFormValid')
    },
    measureValidSuccess: function (data) {
      let _this = this
      let index = this.cubeDesc.measures.indexOf(this.selected_measure)
      if (data.measure.function.expression === 'TOP_N' || (data.measure.function.expression === 'COUNT_DISTINCT' && data.measure.function.returntype !== 'bitmap')) {
        if (data.convertedColumns && data.convertedColumns.length > 0) {
          _this.recursion(data.measure.function.parameter, data.convertedColumns, 0)
        }
        if (data.measure.function.expression === 'TOP_N' && data.convertedColumns.length > 0) {
          _this.$set(data.measure.function, 'configuration', {})
          data.convertedColumns.forEach(function (column) {
            if (needLengthMeasureType.indexOf(_this.getEncoding(column.encoding)) >= 0) {
              _this.$set(data.measure.function.configuration, 'topn.encoding.' + column.column, _this.getEncoding(column.encoding) + ':' + column.valueLength)
            } else {
              _this.$set(data.measure.function.configuration, 'topn.encoding.' + column.column, _this.getEncoding(column.encoding))
            }
            _this.$set(data.measure.function.configuration, 'topn.encoding_version.' + column.column, _this.getVersion(column.encoding))
          })
        }
      }
      if (data.measure.function.expression === 'COUNT_DISTINCT' && data.measure.function.returntype === 'bitmap') {
        let dictionaryIndex = -1
        for (let i = 0; i < _this.cubeDesc.dictionaries.length; i++) {
          if (_this.cubeDesc.dictionaries[i].column === data.measure.function.parameter.value) {
            dictionaryIndex = i
            return
          }
        }
        if (dictionaryIndex < 0) {
          if (data.reuseColumn !== '') {
            _this.cubeDesc.dictionaries.push({column: data.measure.function.parameter.value, reuse: data.reuseColumn})
          } else {
            _this.cubeDesc.dictionaries.push({column: data.measure.function.parameter.value, builder: 'org.apache.kylin.dict.GlobalDictionaryBuilder'})
          }
        } else {
          if (data.reuseColumn !== '') {
            _this.$set(_this.cubeDesc.dictionaries, dictionaryIndex, {column: data.measure.function.parameter.value, reuse: data.reuseColumn})
          } else {
            _this.$set(_this.cubeDesc.dictionaries, dictionaryIndex, {column: data.measure.function.parameter.value, builder: 'org.apache.kylin.dict.GlobalDictionaryBuilder'})
          }
        }
      }
      if (_this.selected_measure.function.expression === 'COUNT_DISTINCT' && _this.selected_measure.function.returntype === 'bitmap' && (data.measure.function.expression !== 'COUNT_DISTINCT' || _this.selected_measure.function.parameter.value !== data.measure.function.parameter.value)) {
        let dictionaryIndex = -1
        for (let i = 0; i < _this.cubeDesc.dictionaries; i++) {
          if (_this.cubeDesc.dictionaries[i].column === _this.selected_measure.function.parameter.value) {
            dictionaryIndex = i
            return
          }
        }
        _this.$delete(_this.cubeDesc.dictionaries, dictionaryIndex)
      }
      if (data.measure.function.expression === 'EXTENDED_COLUMN') {
        data.measure.function.returntype = 'extendedcolumn(' + data.measure.function.returntype + ')'
      }
      if (data.measure.function.expression === 'SUM') {
        if (data.sumMeasure.type === 'bigint') {
          data.measure.function.returntype = 'bigint'
        }
        if (data.sumMeasure.type === 'decimal') {
          data.measure.function.returntype = 'decimal(' + data.sumMeasure.value.precision + ',' + data.sumMeasure.value.decimalPlace + ')'
        }
      }
      if (index >= 0) {
        this.$set(this.cubeDesc.measures, index, data.measure)
      } else {
        this.cubeDesc.measures.push(data.measure)
      }
      _this.measureFormVisible = false
    },
    recursion: function (parameter, list, num) {
      if (num < list.length) {
        this.$set(parameter, 'next_parameter', {})
        this.$set(parameter.next_parameter, 'type', 'column')
        this.$set(parameter.next_parameter, 'value', list[num].column)
        num++
        this.recursion(parameter.next_parameter, list, num)
      } else {
        return false
      }
    },
    getEncoding: function (encode) {
      let code = encode.split(':')
      return code[0]
    },
    getVersion: function (encode) {
      let code = encode.split(':')
      return code[1]
    },
    removeMeasure: function (measure, index) {
      if (measure.function.expression === 'COUNT_DISTINCT') {
        for (let i = 0; i < this.cubeDesc.dictionaries.length; i++) {
          if (this.cubeDesc.dictionaries[i].column === measure.function.parameter.value) {
            this.cubeDesc.dictionaries.splice(i, 1)
            break
          }
        }
      }
      this.cubeDesc.measures.splice(index, 1)
    },
    autoAddMeasures: function (arr) {
      let _this = this
      _this.cubeDesc.measures.push({
        name: '_COUNT_',
        function: {
          expression: 'COUNT',
          returntype: 'bigint',
          parameter: {
            type: 'constant',
            value: '1',
            next_parameter: null
          }
        }
      })
      let bigintType = ['int', 'integer', 'smallint', 'bigint', 'tinyint', 'long']
      let numbertype = ['int', 'integer', 'smallint', 'bigint', 'tinyint', 'float', 'double', 'long']
      _this.modelDesc.metrics.forEach(function (metric) {
        let dataType = _this.modelDesc.columnsDetail[metric].datatype
        if ((numbertype.indexOf(dataType) !== -1) || (dataType.substring(0, 7) === 'decimal')) {
          let columnType
          if (bigintType.indexOf(dataType) !== -1) {
            columnType = 'bigint'
          } else {
            if (dataType.indexOf('decimal') !== -1 || dataType === 'double' || dataType === 'float') {
              columnType = 'decimal(19,4)'
            } else {
              columnType = 'decimal(14,0)'
            }
          }
          _this.cubeDesc.measures.push({
            name: metric,
            function: {
              expression: 'SUM',
              parameter: {
                type: 'column',
                value: metric,
                next_parameter: null
              },
              returntype: columnType
            }
          })
        }
      })
      this.initColumnFamily()
    },
    initColumnFamily: function () {
      let _this = this
      let normalMeasures = []
      let distinctCountMeasures = []
      _this.cubeDesc.measures.forEach(function (measure, index) {
        if (measure.function.expression === 'COUNT_DISTINCT') {
          distinctCountMeasures.push(measure.name)
        } else {
          normalMeasures.push(measure.name)
        }
      })
      _this.currentMeasure = normalMeasures.concat(distinctCountMeasures)
      let columnFamilyLength = _this.cubeDesc.hbase_mapping.column_family.length
      if (columnFamilyLength === 0) {
        _this.cubeDesc.hbase_mapping.column_family.push({
          name: 'F1',
          columns: [{
            qualifier: 'M',
            measure_refs: normalMeasures
          }]
        })
        if (distinctCountMeasures.length > 0) {
          _this.cubeDesc.hbase_mapping.column_family.push({
            name: 'F2',
            columns: [{
              qualifier: 'M',
              measure_refs: distinctCountMeasures
            }]
          })
        }
      } else {
        let assignedMeasures = []
        _this.cubeDesc.hbase_mapping.column_family.forEach(function (colFamily, index) {
          colFamily.columns[0].measure_refs.forEach(function (measure, index) {
            assignedMeasures.push(measure)
          })
        })
        _this.cubeDesc.measures.forEach(function (measure, index) {
          if (assignedMeasures.indexOf(measure.name) === -1) {
            if (measure.function.expression === 'COUNT_DISTINCT') {
              _this.cubeDesc.hbase_mapping.column_family[columnFamilyLength - 1].columns[0].measure_refs.push(measure.name)
            } else {
              _this.cubeDesc.hbase_mapping.column_family[0].columns[0].measure_refs.push(measure.name)
            }
          }
        })
      }
      for (let j = 0; j < _this.cubeDesc.hbase_mapping.column_family.length; j++) {
        for (let i = 0; i < _this.cubeDesc.hbase_mapping.column_family[j].columns[0].measure_refs.length; i++) {
          if (_this.currentMeasure.indexOf(_this.cubeDesc.hbase_mapping.column_family[j].columns[0].measure_refs[i]) === -1) {
            _this.cubeDesc.hbase_mapping.column_family[j].columns[0].measure_refs.splice(i, 1)
            i--
          }
        }
        if (_this.cubeDesc.hbase_mapping.column_family[j].columns[0].measure_refs.length === 0) {
          _this.cubeDesc.hbase_mapping.column_family.splice(j, 1)
          j--
        }
      }
    },
    addColumnFamily: function () {
      let _this = this
      let length = _this.cubeDesc.hbase_mapping.column_family.length
      let newFamilyIndex = 1
      if (length > 0) {
        newFamilyIndex = parseInt(_this.cubeDesc.hbase_mapping.column_family[length - 1].name.substring(1)) + 1
      }
      _this.cubeDesc.hbase_mapping.column_family.push({
        name: 'F' + newFamilyIndex,
        columns: [{
          qualifier: 'M',
          measure_refs: []
        }]
      })
    },
    removeColumnFamily: function (index) {
      this.cubeDesc.hbase_mapping.column_family.splice(index, 1)
    }
  },
  watch: {
    'cubeDesc.measures' (val, oldVal) {
      this.initColumnFamily()
    }
  },
  computed: {
    isPlusVersion () {
      var kapVersionInfo = this.$store.state.system.serverAboutKap
      return kapVersionInfo && kapVersionInfo['kap.version'] && kapVersionInfo['kap.version'].indexOf('Plus') !== -1
    },
    isReadyCube () {
      return this.cubeDesc.status === 'READY'
    }
  },
  created () {
    if (this.cubeDesc.measures.length === 0) {
      // this.autoAddMeasures()  //改为自动建议
    }
    this.loadHiddenFeature({feature_name: 'raw_measure'})
    this.loadHiddenFeature({feature_name: 'extendedcolumn-measure'})
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', parameters: 'Parameters', datatype: 'Datatype', comment: 'Comment', returnType: 'Return Type', action: 'Action', addMeasure: 'Add Measure', editMeasure: 'Edit Measure', cancel: 'Cancel', yes: 'Yes', advancedDictionaries: 'Advanced Dictionaries', addDictionary: 'Add Dictionary', editDictionary: 'Edit Dictionary', builderClass: 'Builder Class', reuse: 'Reuse', advancedColumnFamily: 'Advanced Column Family', addColumnFamily: 'Add Column Family', columnFamily: 'Column Family', measures: 'Measures', measuresSuggestion: 'Optimize', resetMeasures: 'Reset', measuresSuggestTip: 'Clicking on the optimize will output the suggested type. Reset will drop all existing measures.'},
    'zh-cn': {name: '名称', expression: '表达式', parameters: '参数', datatype: '数据类型', comment: '注释', returnType: '返回类型', action: '操作', addMeasure: '添加度量', editMeasure: '编辑度量', cancel: '取消', yes: '确定', advancedDictionaries: '高级字典', addDictionary: '添加字典', editDictionary: '编辑字典', builderClass: '构造类', reuse: '复用', advancedColumnFamily: '高级列族', addColumnFamily: '添加列簇', columnFamily: '列簇', measures: '度量', measuresSuggestion: '度量优化', resetMeasures: '重置', measuresSuggestTip: '点击优化度量将输出优化器推荐的度量类型 。重置将会清空所有度量。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .table_margin {
    margin-top: 20px;
    margin-bottom: 20px;
  }
  #measures{
    .el-button--mini{
      // background: transparent;
    }
    .el-button--primary{
      // background: transparent;
    }
    .el-button--primary:hover{
      border-color: @base-color;
    }
  }
</style>
