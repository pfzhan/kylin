<template>
  <el-dialog v-event-stop width="440px" :title="$t('kylinLang.cube.measure')" :visible.sync="measureVisible" top="5%" :close-on-press-escape="false" :close-on-click-modal="false" @close="handleHide">
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
      <el-form-item v-if="measure.expression === 'TOP_N'|| measure.expression === 'PERCENTILE_APPROX' || measure.expression === 'COUNT_DISTINCT'" :label="$t('returnType')" >
        <el-select :popper-append-to-body="false" size="medium" v-model="measure.returntype" class="measures-width">
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
            <el-select :popper-append-to-body="false" :class="{
            'measures-addCC': measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N',
            'measures-width': measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N'}"
            size="medium" v-model="measure.parameterValue.value" :placeholder="$t('kylinLang.common.pleaseSelect')"
            filterable @change="changeParamValue" :disabled="isEdit">
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span>{{item.name}}</span>
                <span class="option-left ksd-fs-13" style="float: right">{{item.datatype}}</span>
              </el-option>
              <el-option
                v-for="(item, index) in ccGroups"
                :key="index + 'cc'"
                :label="item.name"
                :value="item">
                <span>{{item.name}}</span>
                <span class="option-left ksd-fs-13" style="float: right">{{item.datatype}}</span>
              </el-option>
            </el-select>
            <el-button size="medium" v-if="measure.expression !== 'COUNT_DISTINCT' && measure.expression !== 'TOP_N'" icon="el-icon-ksd-auto_computed_column" type="primary" plain @click="newCC" class="ksd-ml-6" :disabled="isEdit&&ccVisible"></el-button>
          </div>
          <el-button type="primary" size="medium" icon="el-icon-ksd-add" plain circle v-if="measure.expression === 'COUNT_DISTINCT'" class="ksd-ml-10" @click="addNewProperty"></el-button>
        </div>
        <el-form :model="ccObject" class="cc-block" :class="{'editCC': !isEdit}" label-position="top" :rules="ccRules" ref="ccForm" v-show="ccVisible">
          <el-form-item prop="name" class="ksd-mb-10">
            <span slot="label">Column Name <span v-if="!isEdit">: {{ccObject.name}}</span></span>
            <el-input class="measures-width" size="medium" v-model="ccObject.name" v-if="isEdit" @blur="upperCaseCCName"></el-input>

          </el-form-item>
          <el-form-item prop="returnType" class="ksd-mb-10">
            <span slot="label">Return Type <span v-if="!isEdit">: {{ccObject.returnType}}</span></span>
            <el-select :popper-append-to-body="false" size="medium" v-model="ccObject.returnType" class="measures-width" v-if="isEdit">
              <el-option
                v-for="(item, index) in allReturnTypes"
                :key="index"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="Expression" prop="expression" class="ksd-mb-10">
            <kap_editor ref="ccSql" height="100" lang="sql" theme="chrome" v-model="ccObject.expression">
            </kap_editor>
          </el-form-item>
          <div class="btn-group clearfix">
            <el-button size="small" plain @click="delCC" class="ksd-fleft">{{$t('kylinLang.common.delete')}}</el-button>
            <el-button type="primary" size="small" plain @click="addCC" class="ksd-fright" v-if="isEdit">
              {{$t('kylinLang.common.save')}}
            </el-button>
            <el-button size="small" plain @click="resetCC" class="ksd-fright" v-if="isEdit">
              {{$t('kylinLang.query.clear')}}
            </el-button>
            <el-button size="small" plain @click="editCC" class="ksd-fright" v-else>
              {{$t('kylinLang.common.edit')}}
            </el-button>
          </div>
        </el-form>
      </el-form-item>
      <el-form-item :label="isGroupBy">
        <div class="measure-flex-row" v-if="measure.expression === 'COUNT_DISTINCT' || measure.expression === 'TOP_N'" v-for="(column, index) in measure.convertedColumns" :key="index" :class="{'ksd-mt-10': !isGroupBy || (isGroupBy && index > 0)}">
          <div class="flex-item">
            <el-select class="measures-width" size="medium" v-model="column.value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable>
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item.name"
                :value="item.name">
                <span>{{item.name}}</span>
                <span class="option-left ksd-fs-13" style="float: right">{{item.datatype}}</span>
              </el-option>
            </el-select>
          </div>
          <el-button type="primary" size="medium" v-if="measure.expression === 'TOP_N' && index == 0" icon="el-icon-ksd-add" plain circle @click="addNewProperty" class="ksd-ml-10"></el-button>
          <el-button size="medium" icon="el-icon-ksd-minus" circle @click="deleteProperty(index)" class="del-pro ksd-ml-10" :class="{'del-margin-more': measure.expression === 'TOP_N' && index > 0}" :disabled="measure.expression === 'TOP_N' && measure.convertedColumns.length == 1"></el-button>
        </div>
        <div v-if="measure.expression ==='CORR'" class="ksd-mt-10">
          <el-select class="measures-addCC" size="medium" v-model="measure.convertedColumns[0].value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable>
            <el-option
              v-for="(item, index) in getParameterValue"
              :key="index"
              :label="item.name"
              :value="item.name">
              <span>{{item.name}}</span>
              <span class="option-left ksd-fs-13" style="float: right">{{item.datatype}}</span>
            </el-option>
            <el-option
              v-for="(item, index) in ccGroups"
              :key="index + 'cc'"
              :label="item.name"
              :value="item">
              <span>{{item.name}}</span>
              <span class="option-left ksd-fs-13" style="float: right">{{item.datatype}}</span>
            </el-option>
          </el-select>
          <el-button size="medium" icon="el-icon-ksd-auto_computed_column" type="primary" plain class="ksd-ml-6" @click="newCC"></el-button>
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
import $ from 'jquery'
@Component({
  props: ['isShow', 'modelTables', 'allMeasures', 'isEditMeasure', 'measureObj'],
  locales: {
    'en': {requiredName: 'The measure name is required.', name: 'Name', expression: 'Expression', returnType: 'Return Type', paramValue: 'Param Value', nameReuse: 'The measure name is reused.', requiredCCName: 'The column name is required.', requiredReturnType: 'The return type is required.', requiredExpress: 'The expression is required.'},
    'zh-cn': {requiredName: '请输入度量名称', name: '名称', expression: '表达式', returnType: '返回类型', paramValue: '参数值', nameReuse: 'Measure名称已被使用', requiredCCName: '请输入列表名称', requiredReturnType: '请选择度量返回类型', requiredExpress: '请输入表达式。'}
  }
})
export default class AddMeasure extends Vue {
  measureVisible = false
  reuseColumn = ''
  measure = {
    name: '',
    expression: 'SUM(column)',
    parameterValue: {type: 'column', value: ''},
    convertedColumns: [],
    returntype: ''
  }
  rules = {
    name: [
      { required: true, message: this.$t('requiredName'), trigger: 'blur' },
      { validator: this.validateName, trigger: 'blur' }
    ]
  }
  ccRules = {
    name: [{ required: true, message: this.$t('requiredCCName'), trigger: 'blur' }],
    returnType: [{ required: true, message: this.$t('requiredReturnType'), trigger: 'change' }],
    expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }]
  }
  ccObject = {
    name: '',
    returnType: '',
    expression: ''
  }
  activeCCIndex = null
  isEdit = false
  ccVisible = false
  ccGroups = []
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
      for (let i = 0; i < this.allMeasures.length; i++) {
        if (this.allMeasures[i].name.toLocaleUpperCase() === this.measure.name.toLocaleUpperCase() && !this.isEditMeasure) {
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

  upperCaseCCName () {
    this.ccObject.name = this.ccObject.name.toLocaleUpperCase()
  }

  changeExpression () {
    this.measure.returntype = ''
    if (this.measure.expression === 'SUM(constant)' || this.measure.expression === 'COUNT(constant)') {
      this.measure.parameterValue.type = 'constant'
      this.measure.parameterValue.value = 1
    } else {
      this.measure.parameterValue.type = 'column'
    }
    if (this.measure.expression === 'TOP_N') {
      this.measure.convertedColumns = [{type: 'column', value: ''}]
      this.measure.returntype = 'topn(100)'
    }
    if (this.measure.expression === 'CORR') {
      this.measure.convertedColumns = [{type: 'column', value: ''}]
    }
    if (this.measure.expression === 'COUNT_DISTINCT') {
      this.measure.convertedColumns = []
      this.measure.returntype = 'hllc(10)'
    }
    if (this.measure.expression === 'PERCENTILE_APPROX') {
      this.measure.returntype = 'percentile(100)'
    }
  }

  addNewProperty () {
    const GroupBy = {type: 'column', value: ''}
    this.measure.convertedColumns.push(GroupBy)
  }

  deleteProperty (index) {
    this.measure.convertedColumns.splice(index, 1)
  }

  changeParamValue (value) {
    const index = this.ccGroups.indexOf(value)
    if (index > -1) {
      this.activeCCIndex = index
      this.ccObject = this.ccGroups[index]
      this.ccVisible = true
      this.isEdit = false
    }
  }

  newCC () {
    this.isEdit = true
    this.ccVisible = true
    this.$refs['ccForm'].resetFields()
  }
  delCC () {
    if (this.activeCCIndex > -1) {
      this.$nextTick(() => {
        this.ccGroups.splice(this.activeCCIndex, 1)
      })
    }
    this.measure.parameterValue.value = ''
    this.ccVisible = false
    this.isEdit = false
  }
  addCC () {
    this.$refs['ccForm'].validate((valid) => {
      if (valid) {
        this.activeCCIndex = this.ccGroups.push(this.ccObject) - 1
        this.measure.parameterValue.value = this.ccObject.name
        this.isEdit = false
      }
    })
  }
  editCC () {
    this.isEdit = true
  }
  resetCC () {
    this.$refs['ccForm'].resetFields()
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

  get allReturnTypes () {
    return [].concat(this.integerType, this.floatType, this.otherType)
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
    if (this.modelTables) {
      $.each(this.modelTables, (index, table) => {
        $.each(table.columns, (index, column) => {
          const returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?')
          const returnValue = returnRegex.exec(column.datatype)
          if (measuresDataType.indexOf(returnValue[1]) >= 0) {
            const columnObj = {name: table.alias + '.' + column.name, datatype: column.datatype}
            targetColumns.push(columnObj)
          }
        })
      })
    }
    return targetColumns
  }

  handleHide () {
    this.measureVisible = false
    this.$emit('closeAddMeasureDia')
  }

  checkMeasure () {
    this.$refs['measureForm'].validate((valid) => {
      if (valid) {
        this.$emit('saveNewMeasure', this.measure, this.ccObject, this.isEditMeasure)
        this.resetMeasure()
        this.handleHide()
      }
    })
  }

  resetMeasure () {
    this.measure = {
      name: '',
      expression: 'SUM(column)',
      parameterValue: {type: 'column', value: ''},
      convertedColumns: [],
      returntype: ''
    }
    this.ccVisible = false
    this.isEdit = false
    this.ccObject = {
      name: '',
      returnType: '',
      expression: ''
    }
  }

  initExpression () {
    if (this.measure.expression === 'SUM' || this.measure.expression === 'COUNT') {
      this.measure.expression = `${this.measure.expression}(${this.measure.parameterValue.type})`
    }
  }

  @Watch('isShow')
  onShowChange (val) {
    this.measureVisible = val
    if (this.measureVisible) {
      this.resetMeasure()
      this.measure = objectClone(this.measureObj)
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
    .cc-block {
      border: 1px solid @line-border-color;
      padding: 20px;
      background-color: @table-stripe-color;
      margin-top: 10px;
      .el-form-item__label {
        padding: 0;
      }
      .el-form-item.is-required .el-form-item__label:before {
        display: inline-block;
      }
      &.editCC .el-form-item.is-required .el-form-item__label:before {
        display: none;
      }
    }
    .el-button.is-disabled,
    .el-button--primary.is-plain.is-disabled {
      background-color: @grey-4;
      color: @line-border-color;
      .el-icon-ksd-minus {
        cursor: not-allowed;
      }
      :hover {
        background-color: @grey-4;
        color: @line-border-color;
      }
    }
    .option-left {
      color: @text-secondary-color;
    }
    .el-tag--info {
      background-color: @base-background-color;
      color: @text-secondary-color;
      cursor: not-allowed;
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
          width: 20%;
        }
        .douhao{
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
