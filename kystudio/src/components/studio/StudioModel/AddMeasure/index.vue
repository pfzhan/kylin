<template>
  <el-dialog append-to-body width="440px" :title="$t('kylinLang.cube.measure')" :visible.sync="measureVisible" top="5%" :close-on-press-escape="false" :close-on-click-modal="false" @close="handleHide">
    <el-form :model="measure" class="add-measure" label-position="top" :rules="rules"  ref="measureForm">
      <el-form-item :label="$t('name')" prop="name">
        <div>
          <el-input class="measures-width" size="medium" v-model="measure.name" @blur="upperCaseName"></el-input>
        </div>
      </el-form-item>
      <el-form-item :label="$t('expression')">
        <el-select :popper-append-to-body="false" class="measures-width" size="medium" v-model="measure.expression" @change="changeExpression">
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
      <el-form-item :label="isOrderBy" class="ksd-mb-10">
        <el-tag type="info" class="measures-width" v-if="measure.expression === 'SUM(constant)' || measure.expression === 'COUNT(constant)'">1</el-tag>
        <div class="measure-flex-row" v-else>
          <div class="flex-item">
            <el-select :class="{
            'measures-addCC': measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N',
            'measures-width': measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N'}"
            size="medium" v-model="measure.parameter_value[0].value" :placeholder="$t('kylinLang.common.pleaseSelect')"
            filterable @change="changeParamValue" :disabled="isEdit">
              <el-option-group key="column" :label="$t('columns')">
                <el-option
                  v-for="(item, index) in getParameterValue"
                  :key="index"
                  :label="item.name"
                  :value="item.name">
                  <span>{{item.name}}</span>
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
            <el-button size="medium" v-if="measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N'" icon="el-icon-ksd-auto_computed_column" type="primary" plain @click="newCC" class="ksd-ml-6" :disabled="isEdit&&ccVisible"></el-button>
          </div>
          <el-button type="primary" size="medium" icon="el-icon-plus" plain circle v-if="measure.expression === 'COUNT_DISTINCT'" class="ksd-ml-10" @click="addNewProperty"></el-button>
        </div>
        <CCEditForm v-if="ccVisible" @saveSuccess="saveCC" @delSuccess="delCC" :ccDesc="ccObject" :modelInstance="modelInstance"></CCEditForm>
      </el-form-item>
      <el-form-item :label="isGroupBy">
        <div class="measure-flex-row" v-if="measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N'" v-for="(column, index) in measure.converted_columns" :key="index" :class="{'ksd-mt-10': !isGroupBy || (isGroupBy && index > 0)}">
          <div class="flex-item">
            <el-select class="measures-width" size="medium" v-model="column.value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable>
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span>{{item.name}}</span>
                <span class="ky-option-sub-info">{{item.datatype}}</span>
              </el-option>
            </el-select>
          </div>
          <el-button type="primary" size="medium" v-if="measure.expression === 'TOP_N' && index == 0" icon="el-icon-plus" plain circle @click="addNewProperty" class="ksd-ml-10"></el-button>
          <el-button size="medium" icon="el-icon-minus" circle @click="deleteProperty(index)" class="del-pro ksd-ml-10" :class="{'del-margin-more': measure.expression === 'TOP_N' && index > 0}" :disabled="measure.expression === 'TOP_N' && measure.converted_columns.length == 1"></el-button>
        </div>
        <div v-if="measure.expression ==='CORR'" class="ksd-mt-10">
          <el-select class="measures-addCC" size="medium" v-model="measure.converted_columns[0].value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable @change="changeCORRParamValue" :disabled="isCorrCCEdit">
            <el-option-group key="column" :label="$t('columns')">
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span>{{item.name}}</span>
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
          <el-button size="medium" icon="el-icon-ksd-auto_computed_column" type="primary" plain class="ksd-ml-6" @click="newCorrCC" :disabled="isCorrCCEdit && corrCCVisible"></el-button>
          <CCEditForm v-if="corrCCVisible" @saveSuccess="saveCorrCC" @delSuccess="delCorrCC" :ccDesc="corrCCObject" :modelInstance="modelInstance"></CCEditForm>
        </div>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <el-button size="medium" @click="measureVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="checkMeasure">{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { measuresDataType } from '../../../../config'
import { objectClone } from '../../../../util/index'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
import $ from 'jquery'
@Component({
  props: ['isShow', 'isEditMeasure', 'measureObj', 'modelInstance'],
  components: {
    CCEditForm
  },
  locales: {
    'en': {requiredName: 'The measure name is required.', name: 'Name', expression: 'Expression', return_type: 'Return Type', paramValue: 'Param Value', nameReuse: 'The measure name is reused.', requiredCCName: 'The column name is required.', requiredreturn_type: 'The return type is required.', requiredExpress: 'The expression is required.', columns: 'Columns', ccolumns: 'Computed Columns'},
    'zh-cn': {requiredName: '请输入度量名称', name: '名称', expression: '表达式', return_type: '返回类型', paramValue: '参数值', nameReuse: 'Measure名称已被使用', requiredCCName: '请输入列表名称', requiredreturn_type: '请选择度量返回类型', requiredExpress: '请输入表达式。', columns: '普通列', ccolumns: '计算列'}
  }
})
export default class AddMeasure extends Vue {
  measureVisible = false
  reuseColumn = ''
  measure = {
    name: '',
    expression: 'SUM(column)',
    parameter_value: [{type: 'column', value: ''}],
    converted_columns: [],
    return_type: ''
  }
  rules = {
    name: [
      { required: true, message: this.$t('requiredName'), trigger: 'blur' },
      { validator: this.validateName, trigger: 'blur' }
    ]
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
    {label: 'CORR (Beta)', value: 'CORR'},
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
      for (let i = 0; i < this.modelInstance.all_measures.length; i++) {
        if (this.modelInstance.all_measures[i].name.toLocaleUpperCase() === this.measure.name.toLocaleUpperCase() && !this.isEditMeasure) {
          callback(new Error(this.$t('nameReuse')))
        } else {
          callback()
        }
      }
    }
  }

  upperCaseName () {
    this.measure.name = this.measure.name.toLocaleUpperCase()
  }

  changeExpression () {
    this.measure.return_type = ''
    if (this.measure.expression === 'SUM(constant)' || this.measure.expression === 'COUNT(constant)') {
      this.measure.parameter_value[0].type = 'constant'
      this.measure.parameter_value[0].value = 1
    } else {
      this.measure.parameter_value[0].type = 'column'
    }
    if (this.measure.expression === 'TOP_N') {
      this.measure.converted_columns = [{type: 'column', value: ''}]
      this.measure.return_type = 'topn(100)'
    }
    if (this.measure.expression === 'CORR') {
      this.measure.converted_columns = [{type: 'column', value: ''}]
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      this.measure.converted_columns = []
      this.measure.return_type = 'hllc(10)'
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      this.measure.return_type = 'percentile(100)'
    }
  }

  addNewProperty () {
    const GroupBy = {type: 'column', value: ''}
    this.measure.converted_columns.push(GroupBy)
  }

  deleteProperty (index) {
    this.measure.converted_columns.splice(index, 1)
  }

  getCCObj (value) {
    const measureNamed = value.split('.')
    const alias = measureNamed[0]
    const column = measureNamed[1]
    const ccObj = this.modelInstance.getCCObj(alias, column)
    return ccObj
  }

  changeParamValue (value) {
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

  changeCORRParamValue (value) {
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
    this.isEdit = true
    this.ccVisible = true
  }
  newCorrCC () {
    this.resetCCVisble()
    this.isCorrCCEdit = true
    this.corrCCVisible = true
  }
  saveCC (cc) {
    this.measure.parameter_value[0].value = cc.tableAlias + '.' + cc.columnName
    this.isEdit = false
  }
  saveCorrCC (cc) {
    this.measure.converted_columns[0].value = cc.tableAlias + '.' + cc.columnName
    this.isCorrCCEdit = false
  }
  delCC (cc) {
    this.measure.parameter_value[0].value = ''
    this.ccVisible = false
    this.isEdit = false
  }
  delCorrCC (cc) {
    this.measure.converted_columns[0].value = ''
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
    if (this.allTableColumns) {
      $.each(this.allTableColumns, (index, column) => {
        const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
        const returnValue = returnRegex.exec(column.datatype)
        if (measuresDataType.indexOf(returnValue[1]) >= 0) {
          const columnObj = {name: column.table_alias + '.' + column.name, datatype: column.datatype}
          targetColumns.push(columnObj)
        }
      })
    }
    return targetColumns
  }

  handleHide () {
    this.measureVisible = false
    this.$emit('closeAddMeasureDia')
  }
  checkMeasure () {
    this.$refs.measureForm.validate((valid) => {
      if (valid) {
        if (this.isEditMeasure) {
          this.modelInstance.editMeasure(this.measure).then(() => {
            this.resetMeasure()
            this.handleHide()
          })
        } else {
          this.modelInstance.addMeasure(this.measure).then(() => {
            this.resetMeasure()
            this.handleHide()
          })
        }
      }
    })
  }

  resetMeasure () {
    this.measure = {
      name: '',
      expression: 'SUM(column)',
      parameter_value: [{type: 'column', value: ''}],
      converted_columns: [],
      return_type: ''
    }
    this.ccVisible = false
    this.isEdit = false
  }

  initExpression () {
    if (this.measure.expression === 'SUM' || this.measure.expression === 'COUNT') {
      this.measure.expression = `${this.measure.expression}(${this.measure.parameter_value[0].type})`
    }
  }

  @Watch('isShow')
  onShowChange (val) {
    this.measureVisible = val
    if (this.measureVisible) {
      this.resetMeasure()
      this.measure = objectClone(this.measureObj)
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
      width: 88.5%;
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
