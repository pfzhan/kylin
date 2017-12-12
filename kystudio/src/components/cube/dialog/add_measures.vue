<template>
  <el-form :model="measure" id="add-measure" label-position="right" :rules="rules" label-width="20%" ref="measureForm">
    <el-form-item :label="$t('name')" prop="name">
      <div class="input_width">
        <el-input v-model="measure.name"></el-input>
      </div>
    </el-form-item>
    <el-form-item :label="$t('expression')">
      <span slot="label">{{$t('expression')}}
        <common-tip :content="$t('kylinLang.cube.expressionTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
      </span>
      <el-select v-model="measure.function.expression" class="input_width" @change="changeExpression">
        <el-option
          v-for="item in expressionsConf"
          :key="item.value"
          :label="item.label"
          :value="item.value">
        </el-option>
      </el-select>
    </el-form-item>

    <el-form-item :label="$t('paramType')" >
      <el-select v-model="measure.function.parameter.type" v-if="measure.function.expression ==='SUM'||measure.function.expression ==='TOP_N'" class="input_width" @change="changeParamType">
        <el-option
          v-for="(item, index) in type"
          :key="index"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
      <el-tag v-else>{{getParameterType}}</el-tag>
    </el-form-item>
    <el-form-item>
      <el-checkbox v-model="showDim" v-if="measure.function.parameter.type !== 'constant'">{{$t('includeDimensions')}}</el-checkbox>
    </el-form-item>
    <el-form-item :label="getValueLab" >
    <span slot="label">{{getValueLab}}
        <common-tip :content="paramValTip" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
      </span>
      <el-select v-model="measure.function.parameter.value" :placeholder="$t('kylinLang.common.pleaseSelect')" v-if="measure.function.parameter.type !== 'constant'" class="input_width" @change="changeParamValue" filterable>
        <el-option
          v-for="(item, index) in getParameterValue"
          :key="index"
          :label="item"
          :value="item">
          <span style="float: left">{{ item}}</span>
          <span style="float: right; color: #8492a6; font-size: 13px">{{modelDesc.columnsDetail && modelDesc.columnsDetail[item] && modelDesc.columnsDetail[item].datatype}}</span>
        </el-option>
      </el-select>
      <el-tag v-else>{{getParameterValue}}</el-tag>
    </el-form-item>

    <el-form-item :label="$t('extendedColumn')"  v-if="measure.function.expression === 'EXTENDED_COLUMN'">
      <el-select v-model="nextParam.value" :placeholder="$t('kylinLang.common.pleaseSelect')" class="input_width" filterable>
        <el-option
          v-for="(item, index) in getAllModelDimColumns()"
          :key="index"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>
    <el-form-item :label="getReturnTypeLab" >
      <el-tag v-if="measure.function.expression === 'MIN' ||measure.function.expression === 'MAX' || measure.function.expression === 'RAW' || (measure.function.parameter.type === 'constant' && measure.function.expression !== 'TOP_N')">
        {{getReturnType}}
      </el-tag>
      <el-select v-model="measure.function.returntype" v-if="measure.function.expression === 'TOP_N'|| measure.function.expression === 'PERCENTILE_APPROX' || measure.function.expression === 'COUNT_DISTINCT'">
        <el-option
          v-for="(item, index) in getSelectDataType"
          :key="index"
          :label="item.name"
          :value="item.value">
        </el-option>
      </el-select>
      <el-input v-if="measure.function.expression === 'EXTENDED_COLUMN'" v-model="measure.function.returntype">
      </el-input>
      <el-row class="select-returntype" v-if="selectableMeasure.type != '' && measure.function.expression === 'SUM' && measure.function.parameter.type === 'column'">
        <el-col v-if="otherType.indexOf(selectableMeasure.type) >= 0">
          <el-tag>{{getSumReturnType}}</el-tag>
        </el-col>
        <el-col v-else>
          <el-select v-model="selectableMeasure.type">
            <el-option
              v-for="(item, index) in selectableType"
              :key="index"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
          <div class="decimal" v-if="selectableMeasure.type === 'decimal'">
            <span class="decimal-left">(</span>
            <el-input v-model="selectableMeasure.value.firstNumber"></el-input>
            <span class="douhao">,</span>
            <el-input v-model="selectableMeasure.value.secondNumber"></el-input>
            <span class="decimal-right">)</span>
          </div>
        </el-col>
      </el-row>
    </el-form-item>

    <el-form-item v-if="measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype === 'bitmap'" >
      <el-checkbox v-model="isReuse" @change="changeReuse">{{$t('reuse')}}</el-checkbox>
    </el-form-item>

    <el-form-item v-if="isReuse && measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype === 'bitmap'" :label="$t('reuse')" >
      <el-select v-model="reuseColumn">
        <el-option
          v-for="(item, key) in getCountDistinctBitMapColumn()"
          :key="key"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>

    <el-table id="table-measures" v-if="measure.function.expression === 'TOP_N' || (measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype !== 'bitmap')"
      style="width: 100%"
      border
      :data="convertedColumns">
      <el-table-column
        show-overflow-tooltip
        :label="$t('ID')"
        width="60">
        <template scope="scope">
          <el-tag>{{scope.$index+1}}</el-tag>
        </template>
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        :label="$t('column')">
        <template scope="scope">
          <el-select v-model="scope.row.column" :class="{distinctWidth : isCountDistinct, topnWidth : !isCountDistinct}" filterable>
           <el-option
            v-for="(item, index) in getMultipleColumns"
            :label="item"
            :key="index"
            :value="item">
            </el-option>
          </el-select>
        </template>
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        v-if="measure.function.expression === 'TOP_N'"
        :label="$t('encoding')"
        width="100">
        <template scope="scope">
          <el-select v-model="scope.row.encoding" @change="changeEncoding(scope.row);">
            <el-option
              v-for="(item, index) in initEncodingType(scope.row)"
              :key="index"
              :label="item.name"
              :value="item.name + ':' + item.version">
              <el-tooltip effect="dark" :content="$t('kylinLang.cube.'+$store.state.config.encodingTip[item.name])" placement="top">
                <span style="float: left;width: 90%">{{ item.name }}</span>
                <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-if="item.version>1">{{ item.version }}</span>
              </el-tooltip>
          </el-option>
        </el-select>
      </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip
                       v-if="measure.function.expression === 'TOP_N'"
        :label="$t('length')"
        width="100">
        <template scope="scope">
          <el-input v-model="scope.row.valueLength"  :disabled="scope.row.encoding.indexOf('dict')>=0||scope.row.encoding.indexOf('date')>=0||scope.row.encoding.indexOf('time')>=0||scope.row.encoding.indexOf('boolean')>=0"></el-input>
        </template>
      </el-table-column>
      <el-table-column
        width="50">
        <template scope="scope">
          <el-button type="delete" icon="minus" size="mini" @click="removeProperty(scope.$index)"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-row v-if="measure.function.expression === 'TOP_N' || (measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype !== 'bitmap') ">
     <el-col :span="24" >
    <el-button type="primary" icon="plus" size="mini" @click="addNewProperty" style="margin-top: 10px;height: 30px">
      {{$t('newColumn')}}
    </el-button>
    </el-col>
  </el-row>
  </el-form>
</template>
<script>
import { measuresDataType } from '../../../config'
import { loadBaseEncodings } from '../../../util/business'
import { objectClone, indexOfObjWithSomeKey } from '../../../util/index'
export default {
  name: 'add_measure',
  props: ['measureDesc', 'modelDesc', 'cubeDesc', 'measureFormVisible'],
  data () {
    return {
      measure: objectClone(this.measureDesc),
      expressionsConf: [
        {label: 'SUM', value: 'SUM'},
        {label: 'MIN', value: 'MIN'},
        {label: 'MAX', value: 'MAX'},
        {label: 'COUNT', value: 'COUNT'},
        {label: 'COUNT_DISTINCT', value: 'COUNT_DISTINCT'},
        {label: 'TOP_N', value: 'TOP_N'},
        {label: 'PERCENTILE_APPROX', value: 'PERCENTILE_APPROX'},
        {label: 'RAW', value: 'RAW'},
        {label: 'EXTENDED_COLUMN', value: 'EXTENDED_COLUMN'}
      ],
      type: ['constant', 'column'],
      integerType: ['bigint', 'int', 'integer', 'smallint', 'tinyint'],
      floatType: ['decimal', 'double', 'float'],
      otherType: ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar'],
      showDim: false,
      isReuse: false,
      reuseColumn: '',
      isEdit: 'false',
      firstChange: true,
      convertedColumns: [],
      hisBType: '',
      nextParam: {
        'type': 'column',
        'value': '',
        'next_parameter': null
      },
      selectableMeasure: {
        type: '',
        value: {
          firstNumber: 0,
          secondNumber: 0
        }
      },
      distinctDataTypes: [
        {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
        {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
        {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
        {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
        {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
        {name: 'Precisely', value: 'bitmap'}
      ],
      topNTypes: [
        {name: 'Top 10', value: 'topn(10)'},
        {name: 'Top 100', value: 'topn(100)'},
        {name: 'Top 1000', value: 'topn(1000)'}
      ],
      percentileTypes: [
        {name: 'percentile(100)', value: 'percentile(100)'},
        {name: 'percentile(1000)', value: 'percentile(1000)'},
        {name: 'percentile(10000)', value: 'percentile(10000)'}
      ],
      rules: {
        name: [
            { required: true, message: this.$t('requiredName'), trigger: 'blur' },
            { validator: this.validateName, trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    validateName (rule, value, callback) {
      let nameReuse = false
      let measureIndex = this.cubeDesc.measures.indexOf(this.measureDesc)
      let nameReuseIndex = -1
      if (!value) {
        callback(new Error(this.$t('requiredName')))
      } else {
        for (let i = 0; i < this.cubeDesc.measures.length; i++) {
          if (this.cubeDesc.measures[i].name === this.measure.name) {
            nameReuse = true
            nameReuseIndex = i
          }
        }
        if (nameReuse === true) {
          if (measureIndex >= 0 && measureIndex === nameReuseIndex) {
            callback()
          } else {
            callback(new Error(this.$t('nameReuse')))
          }
        } else {
          callback()
        }
      }
    },
    inModelDimensions: function () {
      let _this = this
      if (_this.measure.function.parameter.value) {
        _this.isEdit = true
        if (_this.modelDesc.metrics && _this.modelDesc.metrics.indexOf(_this.measure.function.parameter.value) !== -1) {
          _this.showDim = false
        } else {
          _this.showDim = true
        }
      } else {
        _this.showDim = false
        _this.selectableMeasure.type = ''
      }
    },
    getExtendedHostColumn: function () {
      let columns = []
      this.cubeDesc.dimensions.forEach((dimension, index) => {
        if (this.modelDesc.factTables.indexOf(dimension.table) === -1) {
          return
        }
        if (this.modelDesc && this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[dimension.table + '.' + dimension.column] && this.modelDesc.columnsDetail[dimension.table + '.' + dimension.column].datatype) {
          let returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
          let returnValue = returnRegex.exec(this.modelDesc.columnsDetail[dimension.table + '.' + dimension.column].datatype)
          if (dimension.column && dimension.derived == null && measuresDataType.indexOf(returnValue[1]) >= 0) {
            columns.push(dimension.table + '.' + dimension.column)
          }
        }
      })
      return columns
    },
    getCommonMetricColumns: function () {
      let columns = []
      if (this.modelDesc.metrics) {
        this.modelDesc.metrics.forEach((metric, index) => {
          if (this.modelDesc && this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[metric] && this.modelDesc.columnsDetail[metric].datatype) {
            let returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
            let returnValue = returnRegex.exec(this.modelDesc.columnsDetail[metric].datatype)
            if (measuresDataType.indexOf(returnValue[1]) >= 0) {
              columns.push(metric)
            }
          }
        })
      }
      return columns
    },
    getAllModelDimMeasureColumns: function () {
      let columns = []
      this.modelDesc.dimensions.forEach((dimension, index) => {
        if (dimension.columns) {
          dimension.columns.forEach((column) => {
            if (this.modelDesc && this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[dimension.table + '.' + column] && this.modelDesc.columnsDetail[dimension.table + '.' + column].datatype) {
              let returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
              let returnValue = returnRegex.exec(this.modelDesc.columnsDetail[dimension.table + '.' + column].datatype)
              if (measuresDataType.indexOf(returnValue[1]) >= 0) {
                columns = columns.concat(dimension.table + '.' + column)
              }
            }
          })
        }
      })
      if (this.modelDesc.metrics) {
        this.modelDesc.metrics.forEach((metric, index) => {
          if (this.modelDesc && this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[metric] && this.modelDesc.columnsDetail[metric].datatype) {
            let returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
            let returnValue = returnRegex.exec(this.modelDesc.columnsDetail[metric].datatype)
            if (measuresDataType.indexOf(returnValue[1]) >= 0) {
              columns.push(metric)
            }
          }
        })
      }
      return columns
    },
    getAllModelDimColumns: function () {
      let columns = []
      this.modelDesc.dimensions.forEach((dimension, index) => {
        if (dimension.columns) {
          dimension.columns.forEach((column) => {
            if (this.modelDesc && this.modelDesc.columnsDetail && this.modelDesc.columnsDetail[dimension.table + '.' + column] && this.modelDesc.columnsDetail[dimension.table + '.' + column].datatype) {
              let returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
              let returnValue = returnRegex.exec(this.modelDesc.columnsDetail[dimension.table + '.' + column].datatype)
              if (measuresDataType.indexOf(returnValue[1]) >= 0) {
                columns.push(dimension.table + '.' + column)
              }
            }
          })
        }
      })
      return columns
    },
    initExtendedColumn: function () {
      if (this.measure.function.expression === 'EXTENDED_COLUMN') {
        this.$nextTick(() => {
          this.nextParam.value = this.measure.function.parameter.next_parameter.value || ''
          let returnValue = /\((\d+)\)/.exec(this.measure.function.returntype)
          this.measure.function.returntype = returnValue[1]
        })
      }
    },
    initSelectableColumn: function () {
      if (this.measure.function.parameter.value !== '' && (this.measure.function.expression === 'SUM' && this.measure.function.parameter.type === 'column')) {
        this.$nextTick(() => {
          this.selectableMeasure.value.firstNumber = ''
          this.selectableMeasure.value.secondNumber = ''
          const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
          let returnValue = returnRegex.exec(this.measure.function.returntype)
          this.selectableMeasure.type = returnValue[1]
          this.hisBType = returnValue[1]
          if (this.selectableMeasure.type === 'decimal') {
            this.selectableMeasure.value.firstNumber = returnValue[2]
            this.selectableMeasure.value.secondNumber = returnValue[3]
          } else if (this.selectableMeasure.type === 'char' || this.selectableMeasure.type === 'varchar') {
            this.selectableMeasure.value.firstNumber = returnValue[2]
          }
        })
      }
    },
    initCountDistinctColumn: function () {
      if (this.measure.function.expression === 'COUNT_DISTINCT') {
        this.$nextTick(() => {
          if (this.cubeDesc.dictionaries) {
            this.cubeDesc.dictionaries.forEach((dictionary) => {
              if (dictionary.reuse && dictionary.column === this.measure.function.parameter.value) {
                this.reuseColumn = dictionary.reuse
                this.isReuse = true
              } else {
                this.isReuse = false
                this.reuseColumn = ''
              }
            })
          }
          if (this.measure.function.parameter.next_parameter) {
            this.recursion(this.measure.function.parameter.next_parameter, this.convertedColumns)
          }
        })
      }
    },
    getCountDistinctBitMapColumn: function () {
      let columns = []
      if (this.cubeDesc.measures) {
        this.cubeDesc.measures.forEach((metric, index) => {
          if (metric.function.expression === 'COUNT_DISTINCT' && metric.function.returntype === 'bitmap') {
            columns.push(metric.function.parameter.value)
          }
        })
      }
      return columns
    },
    initGroupByColumn: function () {
      this.convertedColumns.splice(0, this.convertedColumns.length)
      if (this.measure.function.expression === 'TOP_N') {
        this.$nextTick(() => {
          let returnValue = (/\((\d+)(,\d+)?\)/).exec(this.measure.function.returntype)
          this.measure.function.returntype = 'topn(' + returnValue[1] + ')'
          if (this.measure.function.parameter.next_parameter) {
            this.recursion(this.measure.function.parameter.next_parameter, this.convertedColumns)
            this.convertedColumns.forEach((column) => {
              if (this.measure.function.configuration && this.measure.function.configuration['topn.encoding.' + column.column]) {
                let item = this.measure.function.configuration['topn.encoding.' + column.column]
                let _encoding = this.getEncoding(item)
                let _valueLength = this.getLength(item)
                let version = this.measure.function.configuration['topn.encoding_version.' + column.column] || 1
                this.$set(column, 'encoding', _encoding + ':' + version)
                this.$set(column, 'valueLength', _valueLength)
              } else {
                this.$set(column, 'encoding', 'dict:1')
                this.$set(column, 'valueLength', 0)
              }
            })
          }
        })
      }
    },
    changeEncoding (row) {
      if (this.getEncoding(row.encoding) === 'integer') {
        row.valueLength = 4
      } else {
        row.valueLength = ''
      }
    },
    initEncodingType: function (column) {
      let _this = this
      let baseEncodings = loadBaseEncodings(_this.$store.state.datasource)
      if (column.column) {
        let _this = this
        let datatype = _this.modelDesc.columnsDetail[column.column].datatype
        let filterEncodings = baseEncodings.filterByColumnType(datatype)
        if (this.isEdit) {
          let _encoding = _this.getEncoding(column.encoding)
          let _version = parseInt(_this.getVersion(column.encoding))
          let addEncodings = baseEncodings.addEncoding(_encoding, _version)
          return addEncodings
        } else {
          return filterEncodings
        }
      } else {
        return [{name: 'dict', version: baseEncodings.getEncodingMaxVersion('dict')}]
      }
    },
    getEncoding: function (encode) {
      if (encode) {
        let code = encode.split(':')
        return code[0]
      }
    },
    getLength: function (encode) {
      if (encode) {
        let code = encode.split(':')
        return code[1]
      }
    },
    getVersion: function (encode) {
      if (encode) {
        let code = encode.split(':')
        return code[1]
      }
    },
    removeProperty: function (index) {
      this.convertedColumns.splice(index, 1)
    },
    addNewProperty: function () {
      if (this.measure.function.expression === 'TOP_N') {
        let baseEncodings = loadBaseEncodings(this.$store.state.datasource)
        let GroupBy = {
          column: '',
          encoding: 'dict:' + baseEncodings.getEncodingMaxVersion('dict'),
          valueLength: 0
        }
        this.convertedColumns.push(GroupBy)
      } else {
        let GroupBy = {
          column: ''
        }
        this.convertedColumns.push(GroupBy)
      }
    },
    recursion: function (parameter, list) {
      let _this = this
      list.push({column: parameter.value})
      if (parameter.next_parameter) {
        _this.recursion(parameter.next_parameter, list)
      } else {
        return
      }
    },
    changeReuse: function () {
      if (this.isReuse === false) {
        this.reuseColumn = ''
      }
    },
    changeParamType: function () {
      if (this.measure.function.parameter.value === 1 && this.measure.function.parameter.type === 'column') {
        this.measure.function.parameter.value = ''
      }
    },
    changeExpression: function () {
      if (!this.firstChange && this.measure.function.expression === 'TOP_N') {
        this.measure.function.returntype = 'topn(100)'
      }
      if (!this.firstChange && this.measure.function.expression === 'COUNT_DISTINCT') {
        this.measure.function.returntype = 'hllc(10)'
      }
      if (!this.firstChange && this.measure.function.expression === 'PERCENTILE_APPROX') {
        this.measure.function.returntype = 'percentile(100)'
      }
      if (!this.firstChange && this.measure.function.expression === 'EXTENDED_COLUMN') {
        this.measure.function.returntype = '100'
      }
      if (!this.firstChange && this.measure.function.expression === 'SUM') {
        if (this.measure.function.parameter.value !== '' && this.measure.function.parameter.type === 'column') {
          let colType = this.modelDesc.columnsDetail[this.measure.function.parameter.value].datatype
          this.selectableMeasure.value.firstNumber = ''
          this.selectableMeasure.value.secondNumber = ''
          const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
          let returnValue = returnRegex.exec(colType)
          this.selectableMeasure.type = returnValue[1]
          if (this.integerType.indexOf(this.selectableMeasure.type) >= 0) {
            this.selectableMeasure.type = 'bigint'
          } else if (this.selectableMeasure.type === 'decimal') {
            this.selectableMeasure.type = 'decimal'
            this.selectableMeasure.value.firstNumber = 19
            this.selectableMeasure.value.secondNumber = returnValue[3]
          } else if (this.selectableMeasure.type === 'char' || this.selectableMeasure.type === 'varchar') {
            this.selectableMeasure.value.firstNumber = returnValue[2]
          }
        }
        if (this.measure.function.parameter.value === 1 && this.measure.function.expression !== 'SUM' && this.measure.function.expression !== 'COUNT' && this.measure.function.expression !== 'TOP_N') {
          this.measure.function.parameter.value = ''
        }
      }
      this.firstChange = false
    },
    changeReturnType: function () {
      if (this.hisBType === this.selectableMeasure.type) {
        return
      }
      if (this.measure.function.parameter.value !== '' && (this.measure.function.expression === 'SUM' && this.measure.function.parameter.type === 'column')) {
        this.selectableMeasure.value.firstNumber = ''
        this.selectableMeasure.value.secondNumber = ''
      }
    },
    changeParamValue: function () {
      if (this.measure.function.parameter.value !== '' && (this.measure.function.expression === 'SUM' && this.measure.function.parameter.type === 'column')) {
        this.selectableMeasure.value.firstNumber = ''
        this.selectableMeasure.value.secondNumber = ''
        let colType = this.modelDesc.columnsDetail[this.measure.function.parameter.value].datatype
        const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
        let returnValue = returnRegex.exec(colType)
        this.selectableMeasure.type = returnValue[1]
        if (this.integerType.indexOf(this.selectableMeasure.type) >= 0) {
          this.selectableMeasure.type = 'bigint'
        } else if (this.selectableMeasure.type === 'decimal') {
          this.selectableMeasure.type = 'decimal'
          this.selectableMeasure.value.firstNumber = 19
          this.selectableMeasure.value.secondNumber = returnValue[3]
        } else if (this.selectableMeasure.type === 'char' || this.selectableMeasure.type === 'varchar') {
          this.selectableMeasure.value.firstNumber = returnValue[2]
        }
      }
    },
    initHiddenFeature: function () {
      let rawIndex = indexOfObjWithSomeKey(this.expressionsConf, 'value', 'RAW')
      let extendedIndex = indexOfObjWithSomeKey(this.expressionsConf, 'value', 'EXTENDED_COLUMN')
      if (this.$store.state.system.hiddenExtendedColumn === 'true' && extendedIndex >= 0) {
        this.expressionsConf.splice(extendedIndex, 1)
      }
      if (this.$store.state.system.hiddenRaw === 'true' && rawIndex >= 0) {
        this.expressionsConf.splice(rawIndex, 1)
      }
    },
    checkMeasures: function () {
      if (this.measure.function.parameter.value === '' && this.measure.function.expression !== 'TOP_N') {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('paramValueNull'),
          type: 'error'
        })
        return false
      }
      if (this.measure.function.returntype === '' && this.measure.function.expression !== 'SUM') {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('returntypeNull'),
          type: 'error'
        })
        return false
      }
      if (this.measure.function.expression === 'COUNT_DISTINCT' || this.measure.function.expression === 'TOP_N') {
        if (this.measure.function.expression === 'TOP_N') {
          if (this.measure.function.parameter.value === '') {
            this.$message({
              showClose: true,
              duration: 0,
              message: this.$t('topnParamValueNull'),
              type: 'error'
            })
            return false
          }
          if (this.convertedColumns.length < 1) {
            this.$message({
              showClose: true,
              duration: 0,
              message: this.$t('convertedColumnsTip'),
              type: 'error'
            })
            return false
          }
          if (this.convertedColumns.length > 0) {
            for (let i = 0; i < this.convertedColumns.length; i++) {
              if (this.convertedColumns[i].column === '') {
                this.$message({
                  showClose: true,
                  duration: 0,
                  message: this.$t('emptyColumnsTip'),
                  type: 'error'
                })
                return false
              }
            }
          }
        }
        if (this.measure.function.expression === 'COUNT_DISTINCT') {
          if (this.measure.function.parameter.value === '') {
            this.$message({
              showClose: true,
              duration: 0,
              message: this.$t('countDistParamValueNull'),
              type: 'error'
            })
            return false
          }
          if (this.convertedColumns.length > 0) {
            for (let i = 0; i < this.convertedColumns.length; i++) {
              if (this.convertedColumns[i].column === '') {
                this.$message({
                  showClose: true,
                  duration: 0,
                  message: this.$t('emptyCountDistColumnsTip'),
                  type: 'error'
                })
                return false
              }
            }
          }
        }
        let hasExisted = []
        for (let column in this.convertedColumns) {
          if (hasExisted.indexOf(this.convertedColumns[column].column) === -1) {
            hasExisted.push(this.convertedColumns[column].column)
          } else {
            this.$message({
              showClose: true,
              duration: 0,
              message: this.$t('duplicateColumnPartOne') + this.convertedColumns[column].column + this.$t('duplicateColumnPartTwo'),
              type: 'error'
            })
            return false
          }
        }
      }
      return true
    }
  },
  computed: {
    getValueLab: function () {
      if (this.measure.function.expression === 'EXTENDED_COLUMN') {
        return this.$t('hostColumn')
      } else if (this.measure.function.expression === 'TOP_N') {
        return this.$t('ORDERSUM')
      } else {
        return this.$t('paramValue')
      }
    },
    paramValTip: function () {
      if (this.measure.function.expression === 'EXTENDED_COLUMN') {
        return this.$t('kylinLang.cube.hostColumnTip')
      } else if (this.measure.function.expression === 'TOP_N') {
        return this.$t('kylinLang.cube.orderSumTip')
      } else {
        return this.$t('kylinLang.cube.paramValueTip')
      }
    },
    getReturnTypeLab: function () {
      if (this.measure.function.expression === 'EXTENDED_COLUMN') {
        return this.$t('extendedColumnLength')
      } else {
        return this.$t('returnType')
      }
    },
    getParameterType: function () {
      if (this.measure.function.expression !== 'COUNT') {
        this.measure.function.parameter.type = 'column'
      } else {
        this.measure.function.parameter.type = 'constant'
      }
      return this.measure.function.parameter.type
    },
    getParameterValue: function () {
      if (this.measure.function.parameter.type === 'constant') {
        this.measure.function.parameter.value = 1
        return this.measure.function.parameter.value
      }
      if (this.measure.function.expression === 'EXTENDED_COLUMN') {
        return this.getExtendedHostColumn()
      } else {
        if (this.showDim === true) {
          return this.getAllModelDimMeasureColumns()
        }
        if (this.showDim === false) {
          return this.getCommonMetricColumns()
        }
      }
    },
    getSumReturnType: function () {
      if (this.measure.function.parameter.value !== '' && this.otherType.indexOf(this.selectableMeasure.type) === -1) {
        this.measure.function.returntype = this.selectableMeasure.type
      } else if (this.selectableMeasure.type === 'char' || this.selectableMeasure.type === 'varchar') {
        this.measure.function.returntype = this.selectableMeasure.type + '(' + this.selectableMeasure.value.firstNumber + ')'
      } else if (this.selectableMeasure.type !== 'char' && this.selectableMeasure.type !== 'varchar') {
        this.measure.function.returntype = this.selectableMeasure.type
      }
      return this.measure.function.returntype
    },
    getReturnType: function () {
      if (this.measure.function.parameter.type === 'constant') {
        switch (this.measure.function.expression) {
          case 'SUM':
            this.measure.function.returntype = 'bigint'
            break
          case 'COUNT':
            this.measure.function.returntype = 'bigint'
            break
          default:
            this.measure.function.returntype = ''
            break
        }
      }
      if (this.measure.function.parameter.value !== '' && this.measure.function.parameter.value !== 1 && this.measure.function.parameter.type === 'column') {
        let colType = this.modelDesc.columnsDetail[this.measure.function.parameter.value].datatype
        switch (this.measure.function.expression) {
          case 'MIN':
            this.measure.function.returntype = colType
            break
          case 'MAX':
            this.measure.function.returntype = colType
            break
          case 'RAW':
            this.measure.function.returntype = 'raw'
            break
          default:
            this.measure.function.returntype = ''
            break
        }
      }
      return this.measure.function.returntype
    },
    getMultipleColumns: function () {
      if (this.measure.function.expression === 'TOP_N') {
        return this.getAllModelDimColumns()
      }
      if (this.measure.function.expression === 'COUNT_DISTINCT') {
        return this.getAllModelDimMeasureColumns()
      }
    },
    getSelectDataType: function () {
      if (this.measure.function.expression === 'TOP_N') {
        return this.topNTypes
      }
      if (this.measure.function.expression === 'COUNT_DISTINCT') {
        return this.distinctDataTypes
      }
      if (this.measure.function.expression === 'PERCENTILE_APPROX') {
        return this.percentileTypes
      }
    },
    selectableType: function () {
      if (this.integerType.indexOf(this.selectableMeasure.type) >= 0) {
        return this.integerType
      }
      if (this.floatType.indexOf(this.selectableMeasure.type) >= 0) {
        return this.floatType
      }
    },
    isCountDistinct: function () {
      if (this.measure.function.expression === 'COUNT_DISTINCT') {
        return true
      } else {
        return false
      }
    }
  },
  watch: {
    measureFormVisible (measureFormVisible) {
      this.measure = objectClone(this.measureDesc)
      this.firstChange = true
      this.inModelDimensions()
      this.initHiddenFeature()
      this.initSelectableColumn()
      this.initExtendedColumn()
      this.initGroupByColumn()
      this.initCountDistinctColumn()
    }
  },
  created () {
    this.inModelDimensions()
    this.initHiddenFeature()
    this.initSelectableColumn()
    this.initExtendedColumn()
    this.initGroupByColumn()
    this.initCountDistinctColumn()
    this.$on('measureFormValid', (t) => {
      this.$refs['measureForm'].validate((valid) => {
        if (valid) {
          let measureCheck = this.checkMeasures()
          if (measureCheck) {
            this.$emit('validSuccess', {measure: this.measure, convertedColumns: this.convertedColumns, reuseColumn: this.reuseColumn, selectableMeasure: this.selectableMeasure, nextParam: this.nextParam})
          }
        }
      })
    })
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', paramType: 'Param Type', paramValue: 'Param Value', returnType: 'Return Type', includeDimensions: 'Include Dimensions', ORDERSUM: 'ORDER|SUM by Column', groupByColumn: 'Group by Column', ID: 'ID', column: 'Column', encoding: 'Encoding', length: 'Length', hostColumn: 'Host column On Fact Table', extendedColumn: 'Extended column On Fact Table', extendedColumnLength: 'Maximum length of extended column', reuse: 'Reuse', newColumn: 'New Column', requiredName: 'The measure name is required.', nameReuse: 'The measure name is reused.', convertedColumnsTip: '[ TOP_N] Group by Column is required', emptyColumnsTip: '[ TOP_N] Group by Column should not be empty.', emptyCountDistColumnsTip: '[ COUNT_DISTINCT] Column should not be empty.', paramValueNull: 'Param Value is required', topnParamValueNull: '[ TOP_N] ORDER|SUM by Column  is required.', countDistParamValueNull: '[ COUNT_DISTINCT]  ORDER|SUM by Column  is required.', returntypeNull: 'Return Type is required', duplicateColumnPartOne: 'The column named [ ', duplicateColumnPartTwo: ' ] already exists.'},
    'zh-cn': {name: '名称', expression: '表达式', paramType: '参数类型', paramValue: '参数值', returnType: '返回类型', includeDimensions: '包含维度', ORDERSUM: 'ORDER|SUM by Column', groupByColumn: 'Group by Column', ID: 'ID', column: '列', encoding: '编码', length: '长度', hostColumn: 'Host column On Fact Table', extendedColumn: 'Extended column On Fact Table', extendedColumnLength: 'Maximum length of extended column', reuse: '复用', newColumn: '新加列', requiredName: '请输入Measure名称', nameReuse: 'Measure名称已被使用', convertedColumnsTip: '[ TOP_N] 的Group by Column不能为空', emptyColumnsTip: '[ TOP_N] 的Group by Column不能为空。', emptyCountDistColumnsTip: '[ COUNT_DISTINCT] 的Column不能为空。', paramValueNull: 'Param Value 不能为空。', topnParamValueNull: '[ TOP_N] 的ORDER|SUM by Column不能为空。', countDistParamValueNull: '[ COUNT_DISTINCT] 的ORDER|SUM by Column不能为空。', returntypeNull: '返回类型不能为空', duplicateColumnPartOne: '名为 [ ', duplicateColumnPartTwo: '] 的度量已经存在。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .input_width{
    width: 90%!important;
  }
  #table-measures{
    .el-table__row{
      background: @input-bg;
    }
    .el-table__row:hover td{
      background: @input-bg!important;
    }
    .el-input{
      padding-bottom: 0;
      padding-top: 0;
    }
    .el-input__inner{
      background: @input-bg;
      height: 30px;
    }
    .el-tag{
      top: 0!important;
      background: transparent;
    }
    .el-button--mini{
      background: #ff4949;
    }
    .el-table__header {
      thead {
        tr {
          background: #4f5473;
          th {
            background: #4f5473;
            border-right:1px solid #393e53;
            .cell {
              background: #4f5473;
            }
          }
        }
      }
    }
    .el-table__row {
      background: #2b2d3c;
      td {
        border-right:1px solid #393e53;
      }
    }
  }
  #add-measure{
    .el-input{
      padding-bottom: 0;
    }
    .el-form-item__label{
      // line-height: 36px;
    }
    // .el-form-item{
    //   margin-bottom: 0;
    // }
    .el-icon-caret-top{
      height: 36px;
      line-height: 36px;
    }
    .el-tag{
      position: relative;
    }
    .el-dialog__body .el-input{
      padding: 0!important;
    }
    .select-returntype {
      .el-select{
        float:left;
        width: 30%;
      }
      .decimal{
        float: left;
        width: 60%;
        span{
          float: left;
          margin-left: 5px;
          font-size: 25px;
        }
        .el-input{
          float: left;
          margin-left: 5px;
          width: 15%;
        }
        .decimal-left{
          margin-top: 5px;
        }
        .douhao{
          margin-top: 5px;
        }
        .decimal-right{
          margin-top: 5px;
        }
      }
    }
    .distinctWidth {
      width: 80%
    }
    .topnWidth {
      width: 100%
    }
  }
</style>
