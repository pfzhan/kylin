<template>
  <el-dialog v-event-stop width="440px" :title="$t('kylinLang.cube.measure')" :visible.sync="measureVisible" top="5%" :close-on-press-escape="false" :close-on-click-modal="false" @close="handleHide">
    <el-form :model="measure" class="add-measure" label-position="top" :rules="rules"  ref="measureForm">
      <el-form-item :label="$t('name')" prop="name">
        <div>
          <el-input class="measures-width" size="medium" v-model="measure.name"></el-input>
        </div>
      </el-form-item>
      <el-form-item :label="$t('expression')">
        <el-select :popper-append-to-body="false" class="measures-width" size="medium" v-model="measure.function.expression" @change="changeExpression">
          <el-option
            v-for="item in expressionsConf"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item v-if="measure.function.expression === 'TOP_N'|| measure.function.expression === 'PERCENTILE_APPROX' || measure.function.expression === 'COUNT_DISTINCT'" :label="$t('returnType')" >
        <el-select :popper-append-to-body="false" size="medium" v-model="measure.function.returntype" class="measures-width">
          <el-option
            v-for="(item, index) in getSelectDataType"
            :key="index"
            :label="item.name"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
      <div class="ksd-fs-16 ksd-mb-6 value-label" v-if="measure.function.expression === 'TOP_N'">{{$t('paramValue')}}</div>
      <el-form-item :label="isOrderBy" class="ksd-mb-10">
        <el-tag type="info" class="measures-width" v-if="measure.function.parameter.type === 'constant'">1</el-tag>
        <el-row :gutter="10" v-else>
          <el-col :span="isHaveConverColumns ? 21 : 24">
            <el-select :popper-append-to-body="false" :class="{
            'measures-addCC': measure.function.expression !== 'COUNT_DISTINCT' && measure.function.expression !== 'TOP_N',
            'measures-width': measure.function.expression === 'COUNT_DISTINCT' || measure.function.expression === 'TOP_N'}"
            size="medium" v-model="measure.function.parameter.value" :placeholder="$t('kylinLang.common.pleaseSelect')"
            filterable @change="changeParamValue" :disabled="isEdit">
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item"
                :value="item">
                <span>{{item}}</span>
                <!-- <span class="option-left ksd-fs-13" style="float: right">{{columnsDesc && columnsDesc[item] && columnsDesc[item].datatype}}</span> -->
              </el-option>
              <el-option
                v-for="(item, index) in ccGroups"
                :key="index + 'cc'"
                :label="item.name"
                :value="item">
                <span>{{item.name}}</span>
                <!-- <span class="option-left ksd-fs-13" style="float: right">{{columnsDesc && columnsDesc[item] && columnsDesc[item].datatype}}</span> -->
              </el-option>
            </el-select>
            <el-button size="medium" v-if="measure.function.expression !== 'COUNT_DISTINCT' && measure.function.expression !== 'TOP_N'" icon="el-icon-ksd-auto_computed_column" type="primary" plain @click="newCC" :disabled="isEdit&&ccVisible"></el-button>
          </el-col>
          <el-col :span="3" class="ksd-right" v-if="measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype !== 'bitmap'">
            <el-button type="primary" size="medium" icon="el-icon-ksd-add" plain circle @click="addNewProperty"></el-button>
          </el-col>
        </el-row>
        <el-form :model="ccObject" class="cc-block" :class="{'editCC': !isEdit}" label-position="top" :rules="ccRules" ref="ccForm" v-show="ccVisible">
          <el-form-item prop="name" class="ksd-mb-10">
            <span slot="label">Column Name <span v-if="!isEdit">: {{ccObject.name}}</span></span>
            <el-input class="measures-width" size="medium" v-model="ccObject.name" v-if="isEdit"></el-input>

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
        <el-row v-if="isHaveConverColumns || measure.function.expression === 'TOP_N'" v-for="(column, index) in convertedColumns" :key="index" :class="{'ksd-mt-10': !isGroupBy || (isGroupBy && index > 0)}" :gutter="10">
          <el-col :span="measure.function.expression === 'TOP_N' ? 18 : 21">
            <el-select class="measures-width" size="medium" v-model="column.column" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable>
              <el-option
                v-for="(item, index) in getParameterValue"
                :key="index"
                :label="item"
                :value="item">
                <span>{{item}}</span>
                <!-- <span class="option-left ksd-fs-13" style="float: right">{{columnsDesc && columnsDesc[item] && columnsDesc[item].datatype}}</span> -->
              </el-option>
            </el-select>
          </el-col>
          <el-col :span="measure.function.expression === 'TOP_N' ? 6 : 3" class="ksd-right">
            <el-button type="primary" size="medium" v-if="measure.function.expression === 'TOP_N' && index == 0" icon="el-icon-ksd-add" plain circle @click="addNewProperty"></el-button>
            <el-button size="medium" icon="el-icon-ksd-minus" circle @click="deleteProperty(index)" class="del-pro" :disabled="measure.function.expression === 'TOP_N' && convertedColumns.length == 1"></el-button>
          </el-col>
        </el-row>
        <div v-if="measure.function.expression ==='CORR'" class="ksd-mt-10">
          <el-select class="measures-addCC" size="medium" v-model="nextParam.value" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable>
            <el-option
              v-for="(item, index) in getParameterValue"
              :key="index"
              :label="item"
              :value="item">
              <span>{{item}}</span>
              <!-- <span class="option-left ksd-fs-13" style="float: right">{{columnsDesc && columnsDesc[item] && columnsDesc[item].datatype}}</span> -->
            </el-option>
          </el-select>
          <el-button size="medium" icon="el-icon-ksd-auto_computed_column" type="primary" plain @click="newCC"></el-button>
        </div>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <el-button size="medium" @click="measureVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="checkMeasure" :loading="loadCheck">{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
@Component({
  props: ['isShow'],
  locales: {
    'en': {requiredName: 'The measure name is required.', name: 'Name', expression: 'Expression', returnType: 'Return Type', paramValue: 'Param Value'},
    'zh-cn': {requiredName: '请输入Measure名称', name: '名称', expression: '表达式', returnType: '返回类型', paramValue: '参数值'}
  }
})
export default class AddMeasure extends Vue {
  measureVisible = false
  loadCheck = false
  reuseColumn = ''
  measure = {
    name: '',
    function: {
      expression: 'COUNT(column)',
      parameter: {
        type: 'column',
        value: ''
      },
      returntype: ''
    }
  }
  rules = {
    name: [
      { required: true, message: this.$t('requiredName'), trigger: 'blur' },
      { validator: this.validateName, trigger: 'blur' }
    ]
  }
  ccRules = {
    name: [{ required: true, message: this.$t('requiredName'), trigger: 'blur' }],
    returnType: [{ required: true, message: this.$t('requiredName'), trigger: 'change' }],
    expression: [{ required: true, message: this.$t('requiredName'), trigger: 'change' }]
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
  convertedColumns = []
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
  nextParam = {
    'type': 'column',
    'value': '',
    'next_parameter': null
  }
  integerType = ['bigint', 'int', 'integer', 'smallint', 'tinyint']
  floatType = ['decimal', 'double', 'float']
  otherType = ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar']
  selectableMeasure = {
    type: '',
    value: {
      firstNumber: 0,
      secondNumber: 0
    }
  }

  validateName (rule, value, callback) {
    let nameReuse = false
    // let measureIndex = this.cubeDesc.measures.indexOf(this.measureDesc)
    // let nameReuseIndex = -1
    if (!value) {
      callback(new Error(this.$t('requiredName')))
    } else {
      // for (let i = 0; i < this.cubeDesc.measures.length; i++) {
      //   if (this.cubeDesc.measures[i].name.toLocaleUpperCase() === this.measure.name.toLocaleUpperCase()) {
      //     nameReuse = true
      //     nameReuseIndex = i
      //   }
      // }
      if (nameReuse === true) {
        // if (measureIndex >= 0 && measureIndex === nameReuseIndex) {
        //   callback()
        // } else {
        //   callback(new Error(this.$t('nameReuse')))
        // }
      } else {
        callback()
      }
    }
  }

  initExpression () {
    if (this.measure.function.parameter.value) {
      if (this.measure.function.parameter.type === 'constant') {
        this.measure.function.expression = `${this.measure.function.expression}(constant)`
      } else {
        if (this.measure.function.expression === 'SUM' || this.measure.function.expression === 'COUNT') {
          this.measure.function.expression = `${this.measure.function.expression}(column)`
        }
      }
    } else {
      this.selectableMeasure.type = ''
      this.measure.function.expression = 'SUM(column)'
    }
  }

  initGroupByColumn () {
    this.convertedColumns.splice(0, this.convertedColumns.length)
    if (this.measure.function.expression === 'TOP_N') {
      this.$nextTick(() => {
        let returnValue = (/\((\d+)(,\d+)?\)/).exec(this.measure.function.returntype)
        this.measure.function.returntype = 'topn(' + returnValue[1] + ')'
        if (this.measure.function.parameter.next_parameter) {
          this.recursion(this.measure.function.parameter.next_parameter, this.convertedColumns)
        }
      })
    }
  }

  initCorrColumn () {
    this.nextParam.value = ''
    if (this.measure.function.expression === 'CORR') {
      this.$nextTick(() => {
        this.nextParam.value = this.measure.function.parameter.next_parameter.value
      })
    }
  }

  changeExpression () {
    if (this.measure.function.expression === 'TOP_N') {
      this.convertedColumns = [{column: ''}]
      this.measure.function.returntype = 'topn(100)'
    }
    if (this.measure.function.expression === 'SUM(constant)' || this.measure.function.expression === 'COUNT(constant)') {
      this.measure.function.parameter.type = 'constant'
      this.measure.function.parameter.value = '1'
    } else {
      this.measure.function.parameter.type = 'column'
    }
    if (this.measure.function.parameter.value === '1' && this.measure.function.expression !== 'SUM(constant)' && this.measure.function.expression !== 'COUNT(constant)') {
      this.measure.function.parameter.value = ''
      this.measure.function.returntype = ''
    }
    if (this.measure.function.expression === 'COUNT_DISTINCT') {
      this.convertedColumns = []
      this.measure.function.returntype = 'hllc(10)'
    }
    if (this.measure.function.expression === 'PERCENTILE_APPROX') {
      this.measure.function.returntype = 'percentile(100)'
    }
  }

  addNewProperty () {
    let GroupBy = {
      column: ''
    }
    this.convertedColumns.push(GroupBy)
  }

  deleteProperty (index) {
    this.convertedColumns.splice(index, 1)
  }

  recursion (parameter, list) {
    let _this = this
    list.push({column: parameter.value})
    if (parameter.next_parameter) {
      _this.recursion(parameter.next_parameter, list)
    } else {
      return
    }
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
    this.measure.function.parameter.value = ''
    this.ccVisible = false
    this.isEdit = false
  }
  addCC () {
    this.$refs['ccForm'].validate((valid) => {
      if (valid) {
        this.activeCCIndex = this.ccGroups.push(this.ccObject) - 1
        this.measure.function.parameter.value = this.ccObject
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
    if (this.measure.function.expression === 'TOP_N') {
      return 'Order/ Sum by'
    } else {
      return this.$t('paramValue')
    }
  }

  get isGroupBy () {
    if (this.measure.function.expression === 'TOP_N') {
      return 'Group by'
    } else {
      return ''
    }
  }

  get allReturnTypes () {
    return [].concat(this.integerType, this.floatType, this.otherType)
  }

  get isHaveConverColumns () {
    if (this.measure.function.expression === 'COUNT_DISTINCT' && this.measure.function.returntype !== 'bitmap') {
      return true
    } else if (!(this.measure.function.expression === 'COUNT_DISTINCT' && this.measure.function.returntype !== 'bitmap') || this.measure.function.expression === 'TOP_N') {
      return false
    }
  }

  get getSelectDataType () {
    if (this.measure.function.expression === 'TOP_N') {
      return this.topNTypes
    }
    if (this.measure.function.expression === 'COUNT_DISTINCT') {
      return this.distinctDataTypes
    }
    if (this.measure.function.expression === 'PERCENTILE_APPROX') {
      return this.percentileTypes
    }
  }

  get getParameterValue () {
    return []
  }

  get selectableType () {
    if (this.integerType.indexOf(this.selectableMeasure.type) >= 0) {
      return this.integerType
    }
    if (this.floatType.indexOf(this.selectableMeasure.type) >= 0) {
      return this.floatType
    }
  }

  handleHide () {
    this.measureVisible = false
    this.$emit('closeAddMeasureDia')
  }

  checkMeasure () {}

  @Watch('isShow')
  onShowChange (val) {
    this.measureVisible = val
    if (this.measureVisible) {
      this.initExpression()
      this.initCorrColumn()
      this.initGroupByColumn()
    }
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .add-measure {
    .measures-width {
      width: 100%;
    }
    .measures-addCC {
      width: 90%;
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
