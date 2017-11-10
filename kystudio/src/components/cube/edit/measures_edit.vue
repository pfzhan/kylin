<template>
<div id="measures">
  <div style="margin-top: 20px;">
    <el-button type="blue" class="measures-button" @click.native="cubeSuggestions" :disabled="isReadyCube || !getSqlResult">{{$t('sqlOutput')}}
    <common-tip :content="$t('outputTipOne')" ><icon name="question-circle" style="color:gray"></icon></common-tip></el-button>
    <el-button type="default" class="measures-button" icon="setting"  @click.native="resetMeasures" :disabled="isReadyCube">{{$t('resetMeasures')}}</el-button>
     <common-tip :content="$t('measuresSuggestTip')" >
       <icon name="question-circle" class="ksd-question-circle"></icon>
    </common-tip>
  </div>
  <el-table class="table_margin"
    :data="cubeDesc.measures"
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
      header-align="center"
      align="center"
      :label="$t('expression')"
      width="150">
      <template scope="scope"> 
        {{filterExpression(scope.row)}}
      </template>
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      :label="$t('parameters')"
      header-align="center"
      align="center" width="150">
      <template scope="scope">
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
      <template scope="scope">
        <span v-if="modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{modelDesc.columnsDetail[scope.row.function.parameter.value].datatype}}
        </span>
      </template>
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      :label="$t('comment')"
      width="110">
      <template scope="scope">
        <span v-if="modelDesc.columnsDetail[scope.row.function.parameter.value]">
          {{modelDesc.columnsDetail[scope.row.function.parameter.value].comment}}
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
    <el-table-column
      :label="$t('action')"
      header-align="center"
      align="center"
      width="100">
      <template scope="scope">
        <el-button type="edit"  size="mini" icon="edit" :disabled="isReadyCube"  @click="editMeasure(scope.row)"></el-button><!--
         处理inline-block的间隙 勿删此处注释--><el-button type="edit"  size="mini" icon="delete" :disabled="isReadyCube" @click="removeMeasure(scope.row, scope.$index)"></el-button>
      </template>
    </el-table-column>
  </el-table>
   <el-button type="blue" icon="plus" :disabled="isReadyCube" @click="addMeasure" class="ksd-mb-20">{{$t('addMeasure')}}</el-button>
      <el-row v-if="useColumnFamily">
        <el-col :span="24">{{$t('advancedColumnFamily')}}</el-col>
      </el-row>
      <el-table class="table_margin" v-if="useColumnFamily"
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
                <area_label :labels="currentMeasure" :selectedlabels="scope.row.columns[0].measure_refs" :refreshInfo="{index: scope.$index, key: 'measure_refs'}" @refreshData="refreshColumnFamily">
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
     <el-button type="blue" icon="plus" v-if="useColumnFamily" @click="addColumnFamily">
      {{$t('addColumnFamily')}}</el-button>
    <el-dialog :title="$t('editMeasure')" v-model="measureFormVisible" top="5%" size="small" :before-close="measureClose" :close-on-press-escape="false" :close-on-click-modal="false">
      <add_measures  ref="measureForm" :cubeDesc="cubeDesc" :modelDesc="modelDesc" :measureDesc="selected_measure" :measureFormVisible="measureFormVisible" v-on:validSuccess="measureValidSuccess"></add_measures>
      <span slot="footer" class="dialog-footer">
        <el-button @click="measureClose()">{{$t('cancel')}}</el-button>
        <el-button type="primary" @click="checkMeasureForm" :loading="loadCheck">{{$t('yes')}}</el-button>
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
import { handleSuccess, handleError, kapConfirm } from 'util/business'
export default {
  name: 'measures',
  props: ['cubeDesc', 'modelDesc', 'cubeInstance', 'sampleSql', 'oldData'],
  data () {
    return {
      selectType: ['bigint', 'int', 'integer', 'smallint', 'tinyint', 'double', 'float'],
      measureFormVisible: false,
      selected_measure: {},
      editDictionaryFormVisible: false,
      selected_dictionary: null,
      currentMeasure: [],
      loadCheck: false
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
    filterExpression (row) {
      if (row.function.expression === 'PERCENTILE') {
        return 'PERCENTILE_APPROX'
      }
      return row.function.expression
    },
    resetMeasures: function () {
      kapConfirm(this.$t('deleteMeasuresTip'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        this.cubeDesc.measures = this.oldData.oldMeasures || []
        this.cubeDesc.hbase_mapping.column_family = this.oldData.oldColumnFamily || []
        this.initColumnFamily()
      })
    },
    cubeSuggestions: function () {
      this.getCubeSuggestions({model: this.cubeDesc.model_name, cube: this.cubeDesc.name}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cubeDesc, 'measures', data.measures)
          this.$set(this.cubeDesc.hbase_mapping, 'column_family', data.hbase_mapping.column_family)
          this.initColumnFamily()
        })
      }, (res) => {
        handleError(res)
      })
    },
    autoAddMeasures: function (arr) {
      this.cubeDesc.measures.push({
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
      this.initColumnFamily()
    },
    addMeasure: function () {
      this.selected_measure = {
        name: '',
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
    measureClose: function () {
      kapConfirm(this.$t('kylinLang.common.willClose'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.measureFormVisible = false
      })
    },
    editMeasure: function (measure) {
      this.selected_measure = measure
      this.measureFormVisible = true
    },
    checkMeasureForm: function () {
      this.$refs['measureForm'].$emit('measureFormValid')
    },
    measureValidSuccess: function (data) {
      this.loadCheck = true
      let index = this.cubeDesc.measures.indexOf(this.selected_measure)
      if (data.measure.function.expression === 'TOP_N' || (data.measure.function.expression === 'COUNT_DISTINCT' && data.measure.function.returntype !== 'bitmap')) {
        if (data.convertedColumns) {
          this.recursion(data.measure.function.parameter, data.convertedColumns, 0)
        }
        if (data.measure.function.expression === 'TOP_N' && data.convertedColumns) {
          this.$set(data.measure.function, 'configuration', {})
          data.convertedColumns.forEach((column) => {
            if (needLengthMeasureType.indexOf(this.getEncoding(column.encoding)) >= 0) {
              this.$set(data.measure.function.configuration, 'topn.encoding.' + column.column, this.getEncoding(column.encoding) + ':' + column.valueLength)
            } else {
              this.$set(data.measure.function.configuration, 'topn.encoding.' + column.column, this.getEncoding(column.encoding))
            }
            this.$set(data.measure.function.configuration, 'topn.encoding_version.' + column.column, this.getVersion(column.encoding))
          })
        }
      }
      if (data.measure.function.expression === 'COUNT_DISTINCT' && data.measure.function.returntype === 'bitmap') {
        let dictionaryIndex = -1
        let len = this.cubeDesc.dictionaries && this.cubeDesc.dictionaries.length || 0
        for (let i = 0; i < len; i++) {
          if (this.cubeDesc.dictionaries[i].column === data.measure.function.parameter.value) {
            dictionaryIndex = i
            break
          }
        }
        if (dictionaryIndex < 0) {
          if (data.reuseColumn !== '') {
            if (this.cubeDesc.dictionaries) {
              this.cubeDesc.dictionaries.push({column: data.measure.function.parameter.value, reuse: data.reuseColumn})
            }
          } else {
            if (this.cubeDesc.dictionaries) {
              this.cubeDesc.dictionaries.push({column: data.measure.function.parameter.value, builder: 'org.apache.kylin.dict.GlobalDictionaryBuilder'})
            }
          }
        } else {
          if (data.reuseColumn !== '') {
            if (this.cubeDesc.dictionaries) {
              this.$set(this.cubeDesc.dictionaries, dictionaryIndex, {column: data.measure.function.parameter.value, reuse: data.reuseColumn})
            }
          } else {
            if (this.cubeDesc.dictionaries) {
              this.$set(this.cubeDesc.dictionaries, dictionaryIndex, {column: data.measure.function.parameter.value, builder: 'org.apache.kylin.dict.GlobalDictionaryBuilder'})
            }
          }
        }
      }
      if (this.selected_measure.function.expression === 'COUNT_DISTINCT' && this.selected_measure.function.returntype === 'bitmap' && (data.measure.function.expression !== 'COUNT_DISTINCT' || this.selected_measure.function.parameter.value !== data.measure.function.parameter.value)) {
        let dictionaryIndex = -1
        let len = this.cubeDesc.dictionaries && this.cubeDesc.dictionaries.length || 0
        for (let i = 0; i < len; i++) {
          if (this.cubeDesc.dictionaries[i].column === this.selected_measure.function.parameter.value) {
            dictionaryIndex = i
            break
          }
        }
        if (this.cubeDesc.dictionaries) {
          this.$delete(this.cubeDesc.dictionaries, dictionaryIndex)
        }
      }
      if (data.nextParam && data.measure.function.expression === 'EXTENDED_COLUMN') {
        data.measure.function.returntype = 'extendedcolumn(' + data.measure.function.returntype + ')'
        this.$set(data.measure.function.parameter, 'next_parameter', data.nextParam)
      }
      if (data.measure.function.expression === 'SUM' && data.measure.function.parameter.type === 'column') {
        if (data.selectableMeasure.type === 'decimal') {
          data.measure.function.returntype = data.selectableMeasure.type + '(' + data.selectableMeasure.value.firstNumber + ',' + data.selectableMeasure.value.secondNumber + ')'
        }
        if (this.selectType.indexOf(data.selectableMeasure.type) >= 0) {
          data.measure.function.returntype = data.selectableMeasure.type
        }
      }
      if (index >= 0) {
        this.$set(this.cubeDesc.measures, index, data.measure)
      } else {
        this.cubeDesc.measures.push(data.measure)
      }
      this.loadCheck = false
      this.measureFormVisible = false
      this.initColumnFamily()
    },
    recursion: function (parameter, list, num) {
      if (num < list.length) {
        this.$set(parameter, 'next_parameter', {})
        this.$set(parameter.next_parameter, 'type', 'column')
        this.$set(parameter.next_parameter, 'value', list[num].column)
        num++
        this.recursion(parameter.next_parameter, list, num)
      } else {
        this.$delete(parameter, 'next_parameter')
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
        var len = this.cubeDesc.dictionaries && this.cubeDesc.dictionaries.length || 0
        for (let i = 0; i < len; i++) {
          if (this.cubeDesc.dictionaries[i].column === measure.function.parameter.value) {
            this.cubeDesc.dictionaries.splice(i, 1)
            break
          }
        }
      }
      this.cubeDesc.measures.splice(index, 1)
      this.initColumnFamily()
    },
    refreshColumnFamily (data, refreshInfo) {
      var index = refreshInfo.index
      var key = refreshInfo.key
      this.$set(this.cubeDesc.hbase_mapping.column_family[index].columns[0], key, data)
    },
    initColumnFamily: function () {
      let _this = this
      let normalMeasures = []
      let distinctCountMeasures = []
      if (this.cubeDesc.measures) {
        this.cubeDesc.measures.forEach((measure, index) => {
          if (measure.function.expression === 'COUNT_DISTINCT') {
            distinctCountMeasures.push(measure.name)
          } else {
            normalMeasures.push(measure.name)
          }
        })
      }
      this.currentMeasure = normalMeasures.concat(distinctCountMeasures)
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
        if (_this.cubeDesc.hbase_mapping.column_family) {
          _this.cubeDesc.hbase_mapping.column_family.forEach((colFamily, index) => {
            colFamily.columns[0].measure_refs.forEach((measure, index) => {
              assignedMeasures.push(measure)
            })
          })
        }
        if (_this.cubeDesc.measures) {
          _this.cubeDesc.measures.forEach((measure, index) => {
            if (assignedMeasures.indexOf(measure.name) === -1) {
              if (measure.function.expression === 'COUNT_DISTINCT') {
                _this.cubeDesc.hbase_mapping.column_family[columnFamilyLength - 1].columns[0].measure_refs.push(measure.name)
              } else {
                _this.cubeDesc.hbase_mapping.column_family[0].columns[0].measure_refs.push(measure.name)
              }
            }
          })
        }
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
  computed: {
    isReadyCube () {
      return this.cubeInstance && this.cubeInstance.segments && this.cubeInstance.segments.length > 0
      // return this.cubeDesc.status === 'READY'
    },
    useColumnFamily () {
      if (this.cubeDesc && this.cubeDesc.engine_type && (+this.cubeDesc.engine_type === 100 || +this.cubeDesc.engine_type === 99)) {
        return false
      } else {
        return true
      }
    },
    getStrategy: function () {
      if (this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'default') {
        return 'dataOriented'
      } else if (this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy'] === 'mixed') {
        return 'mix'
      } else {
        return 'businessOriented'
      }
    },
    getSqlResult: function () {
      let sqlResult = false
      this.sampleSql.result.forEach((row) => {
        if (row.status !== 'FAILED') {
          sqlResult = true
        }
      })
      return sqlResult
    }
  },
  created () {
    if (this.cubeDesc.measures.length === 0) {
      this.autoAddMeasures()
    }
    this.initColumnFamily()
    this.loadHiddenFeature({feature_name: 'raw-measure'})
    this.loadHiddenFeature({feature_name: 'extendedcolumn-measure'})
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', parameters: 'Parameters', datatype: 'Datatype', comment: 'Comment', returnType: 'Return Type', action: 'Action', addMeasure: 'Add Measure', editMeasure: 'Edit Measure', cancel: 'Cancel', yes: 'Yes', advancedDictionaries: 'Advanced Dictionaries', addDictionary: 'Add Dictionary', editDictionary: 'Edit Dictionary', builderClass: 'Builder Class', reuse: 'Reuse', advancedColumnFamily: 'Advanced Column Family', addColumnFamily: 'Add Column Family', columnFamily: 'Column Family', measures: 'Measures', sqlOutput: 'Measures Suggestion', resetMeasures: 'Reset', measuresSuggestTip: 'Clicking on the \'Measures Suggestion\' will output the suggested type. \'Reset\' will call last saving back and overwrite existing measures.', outputTipOne: 'Measures suggested based on inputed SQL patterns.', deleteMeasuresTip: 'Reset will call last saving back and overwrite existing measures. Please confirm to continue?', dataOriented: 'Data Oriented', mix: 'Mix', businessOriented: 'Business Oriented'},
    'zh-cn': {name: '名称', expression: '表达式', parameters: '参数', datatype: '数据类型', comment: '注释', returnType: '返回类型', action: '操作', addMeasure: '添加度量', editMeasure: '编辑度量', cancel: '取消', yes: '确定', advancedDictionaries: '高级字典', addDictionary: '添加字典', editDictionary: '编辑字典', builderClass: '构造类', reuse: '复用', advancedColumnFamily: '高级列簇', addColumnFamily: '添加列簇', columnFamily: '列簇', measures: '度量', sqlOutput: '推荐度量', resetMeasures: '重置', measuresSuggestTip: '点击“推荐度量”将输出优化器推荐的度量类型。“重置”操作会返回上一次保存过的度量列表，并覆盖现有的度量。', outputTipOne: '系统将根据您输入的SQL语句推荐对应度量。', outputTipTwo: '”偏好与输入的SQL查询，系统推荐的度量。', deleteMeasuresTip: '重置操作会返回上一次保存过的度量列表，并覆盖现有的度量，请确认是否继续此操作？', dataOriented: '模型优先', mix: '综合', businessOriented: '业务优先'}
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
    .measures-button {
      height: 40px;
    }
    .measures-button:hover {
      border-color: #218fea !important;
      background: transparent!important;
    }
  }
</style>
