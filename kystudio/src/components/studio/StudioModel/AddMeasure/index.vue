<template>
  <el-dialog append-to-body width="480px" :title="$t(measureTitle)" :visible.sync="measureVisible" top="5%" :close-on-press-escape="false" :close-on-click-modal="false" @close="handleHide(false)">
    <el-form :model="measure" class="add-measure" label-position="top" :rules="rules"  ref="measureForm">
      <el-form-item :label="$t('name')" prop="name">
        <div>
          <el-input v-guide.measureNameInput class="measures-width" size="medium" v-model="measure.name" @blur="upperCaseName"></el-input>
        </div>
      </el-form-item>
      <el-form-item :label="$t('expression')" prop="expression">
        <el-select v-guide.measureExpressionSelect :popper-append-to-body="false" class="measures-width" size="medium" v-model="measure.expression" @change="changeExpression">
          <el-option
            v-for="item in expressionsConf"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="measure.expression === 'TOP_N'|| measure.expression === 'PERCENTILE_APPROX' || measure.expression === 'COUNT_DISTINCT'" :label="$t('return_type')" >
        <el-select :popper-append-to-body="false" size="medium" v-model="measure.return_type" class="measures-width">
          <el-option
            v-for="(item, index) in getSelectDataType"
            :key="index"
            :label="item.name"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <div class="ksd-fs-16 ksd-mb-6 value-label" v-if="measure.expression === 'TOP_N'">{{$t('paramValue')}}</div>
      <el-form-item :label="isOrderBy" class="ksd-mb-10" prop="parameterValue.value" key="parameterItem">
        <el-tag type="info" class="measures-width" v-if="measure.expression === 'SUM(constant)' || measure.expression === 'COUNT(constant)'">1</el-tag>
        <div class="measure-flex-row" v-else>
          <div class="flex-item">
            <el-select v-guide.measureReturnValSelect :class="{
            'measures-addCC': measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N',
            'measures-width': measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N'}"
            size="medium" v-model="measure.parameterValue.value" :placeholder="$t('kylinLang.common.pleaseSelect')"
            filterable @change="changeParamValue" :disabled="isEdit">
              <el-option-group key="column" :label="$t('columns')">
                <el-option
                  v-for="(item, index) in getParameterValue"
                  :key="index"
                  :label="item.name"
                  :value="item.name">
                  <span :title="item.name">{{item.name | omit(26, '...')}}</span>
                  <span class="ky-option-sub-info">{{item.datatype}}</span>
                </el-option>
              </el-option-group>
              <el-option-group key="ccolumn" :label="$t('ccolumns')">
                <el-option
                  v-for="item in ccGroups"
                  :key="item.guid"
                  :label="item.tableAlias + '.' + item.columnName"
                  :value="item.tableAlias + '.' + item.columnName">
                  <span>{{item.tableAlias}}.{{item.columnName}}</span>
                  <span class="ky-option-sub-info">{{item.datatype}}</span>
                </el-option>
              </el-option-group>
            </el-select>
            <common-tip :content="$t('addCCTip')"><el-button size="medium" v-if="measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N'" icon="el-icon-ksd-auto_computed_column" type="primary" plain @click="newCC" class="ksd-ml-6" :disabled="isEdit&&ccVisible"></el-button></common-tip>
          </div>
          <el-button type="primary" size="medium" icon="el-icon-ksd-add_2" plain circle v-if="measure.expression === 'COUNT_DISTINCT'" class="ksd-ml-10" @click="addNewProperty"></el-button>
        </div>
        <CCEditForm v-if="ccVisible" @saveSuccess="saveCC" @delSuccess="delCC" :ccDesc="ccObject" :modelInstance="modelInstance"></CCEditForm>
      </el-form-item>
      <el-form-item :label="isGroupBy" v-if="(measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N')&&measure.convertedColumns.length>0" prop="convertedColumns[0].value" :rules="rules.convertedColValidate" key="topNItem">
        <div class="measure-flex-row" v-for="(column, index) in measure.convertedColumns" :key="index" :class="{'ksd-mt-10': !isGroupBy || (isGroupBy && index > 0)}">
          <div class="flex-item">
            <el-select class="measures-width" size="medium" v-model="column.value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable @change="changeConColParamValue(column.value, index)">
              <el-option
                v-for="(item, index) in getParameterValue2"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span :title="item.name">{{item.name | omit(26, '...')}}</span>
                <span class="ky-option-sub-info">{{item.datatype}}</span>
              </el-option>
            </el-select>
          </div>
          <el-button type="primary" size="medium" v-if="measure.expression === 'TOP_N' && index == 0" icon="el-icon-ksd-add_2" plain circle @click="addNewProperty" class="ksd-ml-10"></el-button>
          <el-button size="medium" icon="el-icon-minus" circle @click="deleteProperty(index)" class="del-pro ksd-ml-10" :class="{'del-margin-more': measure.expression === 'TOP_N' && index > 0}" :disabled="measure.expression === 'TOP_N' && measure.convertedColumns.length == 1"></el-button>
        </div>
      </el-form-item>
      <el-form-item v-if="measure.expression ==='CORR'" class="ksd-mt-10" prop="convertedColumns[0].value" :rules="rules.convertedColValidate" key="corrItem">
        <div>
          <el-select class="measures-addCC" size="medium" v-model="measure.convertedColumns[0].value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable @change="changeCORRParamValue" :disabled="isCorrCCEdit">
            <el-option-group key="column" :label="$t('columns')">
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span :title="item.name">{{item.name | omit(26, '...')}}</span>
                <span class="ky-option-sub-info">{{item.datatype}}</span>
              </el-option>
            </el-option-group>
            <el-option-group key="ccolumn" :label="$t('ccolumns')">
              <el-option
                v-for="item in ccGroups"
                :key="item.guid"
                :label="item.tableAlias + '.' + item.columnName"
                :value="item.tableAlias + '.' + item.columnName">
                <span>{{item.tableAlias}}.{{item.columnName}}</span>
                <span class="ky-option-sub-info">{{item.datatype}}</span>
              </el-option>
            </el-option-group>
          </el-select>
          <common-tip :content="$t('addCCTip')"><el-button size="medium" icon="el-icon-ksd-auto_computed_column" type="primary" plain class="ksd-ml-6" @click="newCorrCC" :disabled="isCorrCCEdit && corrCCVisible"></el-button></common-tip>
          <CCEditForm v-if="corrCCVisible" @saveSuccess="saveCorrCC" @delSuccess="delCorrCC" :ccDesc="corrCCObject" :modelInstance="modelInstance"></CCEditForm>
        </div>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="handleHide(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" type="primary" v-guide.saveMeasureBtn plain @click="checkMeasure">{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { measuresDataType, measureSumAndTopNDataType, measurePercenDataType } from '../../../../config'
import { objectClone } from '../../../../util/index'
import { NamedRegex } from 'config'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
import $ from 'jquery'
@Component({
  props: ['isShow', 'isEditMeasure', 'measureObj', 'modelInstance'],
  components: {
    CCEditForm
  },
  locales: {
    'en': {
      requiredName: 'The measure name is required.',
      name: 'Name',
      expression: 'Function',
      return_type: 'Function Parameter',
      paramValue: 'Column',
      nameReuse: 'The measure name is reused.',
      requiredCCName: 'The column name is required.',
      requiredreturn_type: 'The function parameter is required.',
      requiredExpress: 'The function is required.',
      columns: 'Columns',
      ccolumns: 'Computed Columns',
      requiredParamValue: 'The column is Required.',
      addCCTip: 'Create Computed Column',
      editMeasureTitle: 'Edit Measure',
      addMeasureTitle: 'Add Measure',
      sameColumn: 'Column has been used'
    },
    'zh-cn': {
      requiredName: '请输入度量名称',
      name: '名称',
      expression: '函数',
      return_type: '函数参数',
      paramValue: '列',
      nameReuse: 'Measure 名称已被使用',
      requiredCCName: '请输入列表名称',
      requiredreturn_type: '请选择度量函数参数',
      requiredExpress: '请选择函数。',
      columns: '普通列',
      ccolumns: '可计算列',
      requiredParamValue: '请选择列。',
      addCCTip: '创建可计算列',
      editMeasureTitle: '编辑度量',
      addMeasureTitle: '添加度量',
      sameColumn: '该列已被其他度量使用'
    }
  }
})
export default class AddMeasure extends Vue {
  measureVisible = false
  reuseColumn = ''
  measureTitle = ''
  measure = {
    name: '',
    expression: 'SUM(column)',
    parameterValue: {type: 'column', value: '', table_guid: null},
    convertedColumns: [],
    return_type: ''
  }
  rules = {
    name: [
      { required: true, message: this.$t('requiredName'), trigger: 'blur' },
      { validator: this.validateName, trigger: 'blur' }
    ],
    expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }],
    'parameterValue.value': [
      { required: true, message: this.$t('requiredParamValue'), trigger: 'change' },
      { validator: this.checkColumn }
    ],
    convertedColValidate: [{ required: true, message: this.$t('requiredParamValue'), trigger: 'blur, change' }]
  }
  ccRules = {
    name: [{ required: true, message: this.$t('requiredCCName'), trigger: 'blur' }],
    return_type: [{ required: true, message: this.$t('requiredreturn_type'), trigger: 'change' }],
    expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }]
  }
  ccObject = null
  corrCCObject = null
  isEdit = false
  isCorrCCEdit = false
  ccVisible = false
  corrCCVisible = false
  ccGroups = []
  allTableColumns = []
  expressionsConf = [
    {label: 'SUM (column)', value: 'SUM(column)'},
    {label: 'SUM (constant)', value: 'SUM(constant)'},
    {label: 'MIN', value: 'MIN'},
    {label: 'MAX', value: 'MAX'},
    {label: 'TOP_N', value: 'TOP_N'},
    {label: 'COUNT (column)', value: 'COUNT(column)'},
    {label: 'COUNT (constant)', value: 'COUNT(constant)'},
    {label: 'COUNT_DISTINCT', value: 'COUNT_DISTINCT'},
    // {label: 'CORR (Beta)', value: 'CORR'},
    {label: 'PERCENTILE_APPROX', value: 'PERCENTILE_APPROX'}
  ]
  topNTypes = [
    {name: 'Top 10', value: 'topn(10)'},
    {name: 'Top 100', value: 'topn(100)'},
    {name: 'Top 1000', value: 'topn(1000)'}
  ]
  distinctDataTypes = [
    {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
    {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
    {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
    {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
    {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
    {name: 'Precisely', value: 'bitmap'}
  ]
  percentileTypes = [
    {name: 'percentile(100)', value: 'percentile(100)'},
    {name: 'percentile(1000)', value: 'percentile(1000)'},
    {name: 'percentile(10000)', value: 'percentile(10000)'}
  ]
  integerType = ['bigint', 'int', 'integer', 'smallint', 'tinyint']
  floatType = ['decimal', 'double', 'float']
  otherType = ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar']

  validateName (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('requiredName')))
    } else {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      }
      if (this.modelInstance.all_measures.length) {
        let isResuse = false
        for (let i = 0; i < this.modelInstance.all_measures.length; i++) {
          if (this.modelInstance.all_measures[i].name.toLocaleUpperCase() === this.measure.name.toLocaleUpperCase() && this.measure.guid !== this.modelInstance.all_measures[i].guid) {
            isResuse = true
            break
          }
        }
        if (isResuse) {
          callback(new Error(this.$t('nameReuse')))
        } else {
          callback()
        }
      } else {
        callback()
      }
    }
  }

  checkColumn (rule, value, callback) {
    if (!this.modelInstance.checkSameEditMeasureColumn(this.measure)) {
      callback(new Error(this.$t('sameColumn')))
    } else {
      callback()
    }
  }

  upperCaseName () {
    this.measure.name = this.measure.name.toLocaleUpperCase()
  }

  changeExpression () {
    this.measure.return_type = ''
    this.measure.parameterValue.value = ''
    if (this.measure.expression === 'SUM(constant)' || this.measure.expression === 'COUNT(constant)') {
      this.measure.parameterValue.type = 'constant'
      this.measure.parameterValue.value = 1
    } else {
      this.measure.parameterValue.type = 'column'
      this.measure.parameterValue.value = ''
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      this.measure.return_type = 'hllc(10)'
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      this.measure.return_type = 'percentile(100)'
    }
    if (this.measure.expression === 'TOP_N') {
      this.measure.return_type = 'topn(100)'
    }
    if (this.measure.expression === 'CORR' || this.measure.expression === 'TOP_N') {
      this.measure.convertedColumns = [{type: 'column', value: '', table_guid: null}]
    } else {
      this.measure.convertedColumns = []
    }
  }

  addNewProperty () {
    const GroupBy = {type: 'column', value: '', table_guid: null}
    this.measure.convertedColumns.push(GroupBy)
  }

  deleteProperty (index) {
    this.measure.convertedColumns.splice(index, 1)
  }

  getCCObj (value) {
    const measureNamed = value.split('.')
    const alias = measureNamed[0]
    const column = measureNamed[1]
    const ccObj = this.modelInstance.getCCObj(alias, column)
    return ccObj
  }

  changeParamValue (value) {
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    this.measure.parameterValue.table_guid = nTable && nTable.guid
    const ccObj = this.getCCObj(value)
    if (ccObj) {
      this.ccObject = ccObj
      this.ccVisible = true
      this.isEdit = false
    } else {
      this.ccVisible = false
      this.isEdit = false
    }
  }

  changeConColParamValue (value, index) {
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    this.measure.convertedColumns[index].table_guid = nTable && nTable.guid
  }

  changeCORRParamValue (value) {
    const alias = value.split('.')[0]
    const nTable = this.modelInstance.getTableByAlias(alias)
    this.measure.convertedColumns[0].table_guid = nTable && nTable.guid
    const ccObj = this.getCCObj(value)
    if (ccObj) {
      this.corrCCObject = ccObj
      this.corrCCVisible = true
      this.isCorrCCEdit = false
    } else {
      this.corrCCVisible = false
      this.isCorrCCEdit = false
    }
  }

  resetCCVisble () {
    this.isEdit = false
    this.isCorrCCEdit = false
    this.ccVisible = false
    this.corrCCVisible = false
    this.ccObject = null
    this.corrCCObject = null
  }

  newCC () {
    this.resetCCVisble()
    this.measure.parameterValue.value = ''
    this.isEdit = true
    this.ccVisible = true
  }
  newCorrCC () {
    this.resetCCVisble()
    this.measure.convertedColumns[0].value = ''
    this.isCorrCCEdit = true
    this.corrCCVisible = true
  }
  saveCC (cc) {
    this.measure.parameterValue.value = cc.tableAlias + '.' + cc.columnName
    this.isEdit = false
  }
  saveCorrCC (cc) {
    this.measure.convertedColumns[0].value = cc.tableAlias + '.' + cc.columnName
    this.isCorrCCEdit = false
  }
  delCC (cc) {
    this.measure.parameterValue.value = ''
    this.ccVisible = false
    this.isEdit = false
  }
  delCorrCC (cc) {
    this.measure.convertedColumns[0].value = ''
    this.corrCCVisible = false
    this.isCorrCCEdit = false
  }

  get isOrderBy () {
    if (this.measure.expression === 'TOP_N') {
      return 'Order/ Sum by'
    } else {
      return this.$t('paramValue')
    }
  }

  get isGroupBy () {
    if (this.measure.expression === 'TOP_N') {
      return 'Group by'
    } else {
      return ''
    }
  }
  get getSelectDataType () {
    if (this.measure.expression === 'TOP_N') {
      return this.topNTypes
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      return this.distinctDataTypes
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      return this.percentileTypes
    }
  }

  get getParameterValue () {
    let targetColumns = []
    let filterType = []
    if (this.allTableColumns) {
      if (this.measure.expression === 'SUM(column)' || this.measure.expression === 'TOP_N') {
        filterType = measureSumAndTopNDataType
      } else if (this.measure.expression === 'PERCENTILE_APPROX') {
        filterType = measurePercenDataType
      } else {
        filterType = measuresDataType
      }
      $.each(this.allTableColumns, (index, column) => {
        const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
        const returnValue = returnRegex.exec(column.datatype)
        if (filterType.indexOf(returnValue[1]) >= 0) {
          const columnObj = {name: column.table_alias + '.' + column.name, datatype: column.datatype}
          targetColumns.push(columnObj)
        }
      })
    }
    return targetColumns
  }

  // 支持measure的任意类型
  get getParameterValue2 () {
    let targetColumns = []
    $.each(this.allTableColumns, (index, column) => {
      const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
      const returnValue = returnRegex.exec(column.datatype)
      if (measuresDataType.indexOf(returnValue[1]) >= 0) {
        const columnObj = {name: column.table_alias + '.' + column.name, datatype: column.datatype}
        targetColumns.push(columnObj)
      }
    })
    return targetColumns
  }

  handleHide (isSubmit, measure, isEdit, fromSearch) {
    this.measureVisible = false
    this.$emit('closeAddMeasureDia', {
      isEdit: isEdit,
      fromSearch: fromSearch,
      data: measure,
      isSubmit: isSubmit
    })
    this.$refs['measureForm'].resetFields()
  }
  checkMeasure () {
    this.$refs.measureForm.validate((valid) => {
      if (valid) {
        const measureClone = objectClone(this.measure)
        // 判断该操作是否属于搜索入口进来
        let isFromSearchAciton = measureClone.fromSearch
        if (measureClone.expression.indexOf('SUM') !== -1) {
          measureClone.expression = 'SUM'
        }
        if (measureClone.expression.indexOf('COUNT(constant)') !== -1 || measureClone.expression.indexOf('COUNT(column)') !== -1) {
          measureClone.expression = 'COUNT'
        }
        measureClone.convertedColumns.unshift(measureClone.parameterValue)
        measureClone.parameter_value = measureClone.convertedColumns
        delete measureClone.parameterValue
        delete measureClone.convertedColumns
        delete measureClone.fromSearch
        let action = this.isEditMeasure ? 'editMeasure' : 'addMeasure'
        this.modelInstance[action](measureClone).then(() => {
          this.resetMeasure()
          this.handleHide(true, measureClone, this.isEditMeasure, isFromSearchAciton)
        })
      }
    })
  }

  resetMeasure () {
    this.measure = {
      name: '',
      expression: 'SUM(column)',
      parameterValue: {type: 'column', value: '', table_guid: null},
      convertedColumns: [],
      return_type: ''
    }
    this.ccVisible = false
    this.isEdit = false
  }

  initExpression () {
    let measureObj = objectClone(this.measureObj)
    if (measureObj.parameter_value && measureObj.parameter_value.length) {
      measureObj.parameterValue = measureObj.parameter_value[0]
      measureObj.convertedColumns = measureObj.parameter_value.length > 1 ? measureObj.parameter_value.splice(1, measureObj.parameter_value.length - 1) : []
      delete measureObj.parameter_value
      if (measureObj.parameterValue.type === 'column') {
        this.changeParamValue(measureObj.parameterValue.value)
      }
    }
    if (measureObj.expression === 'SUM' || measureObj.expression === 'COUNT') {
      measureObj.expression = `${measureObj.expression}(${measureObj.parameterValue.type})`
    }
    this.measure = measureObj
  }

  @Watch('isShow')
  onShowChange (val) {
    this.measureVisible = val
    if (this.measureVisible) {
      this.resetMeasure()
      this.measureTitle = this.isEditMeasure ? 'editMeasureTitle' : 'addMeasureTitle'
      this.allTableColumns = this.modelInstance && this.modelInstance.getTableColumns()
      this.ccGroups = this.modelInstance.computed_columns
      this.initExpression()
    }
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .add-measure {
    .measure-flex-row {
      display: flex;
      .flex-item {
        flex-shrink: 1;
        width: 100%;
      }
    }
    .measures-width {
      width: 100%;
    }
    .measures-addCC {
      width: 350px;
    }
    .del-margin-more {
      margin-left: 55px !important;
    }
    .value-label {
      color: @text-title-color;
    }
    .el-button.is-disabled,
    .el-button--primary.is-plain.is-disabled {
      background-color: @grey-4;
      color: @line-border-color;
      .el-icon-minus {
        cursor: not-allowed;
      }
      :hover {
        background-color: @grey-4;
        color: @line-border-color;
      }
    }
  }
</style>
