<template>
<div>
  <span class="ky-close ksd-fright ksd-mt-10 ksd-mr-10" @click="cancelCC"><i class="el-icon-ksd-close"></i></span>
  <el-form :model="ccObject" class="cc-block" :class="{'editCC': !isEdit}" label-position="top" :rules="ccRules" ref="ccForm">
    <el-form-item prop="columnName" class="ksd-mb-10">
      <span slot="label">{{$t('columnName')}} <span v-if="!isEdit">: {{ccObject.columnName}}</span></span>
      <el-input class="measures-width" size="medium" v-model="ccObject.columnName" v-if="isEdit" @blur="upperCaseCCName"></el-input>
    </el-form-item>
    <el-form-item prop="datatype" class="ksd-mb-10">
      <span slot="label">{{$t('returnType')}}<span v-if="!isEdit">: {{ccObject.datatype}}</span></span>
      <el-select size="medium" v-model="ccObject.datatype" class="measures-width" v-if="isEdit">
        <el-option
          v-for="(item, index) in computedRetrunType"
          :key="index"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>
    <el-form-item :label="$t('expression')" prop="expression" class="ksd-mb-10">
      <span slot="label">{{$t('kylinLang.dataSource.expression')}} <common-tip :content="$t('conditionExpress')" ><i class="el-icon-question"></i></common-tip></span>
      <kap_editor ref="ccSql" height="100" lang="sql" theme="chrome" v-model="ccObject.expression">
      </kap_editor>
    </el-form-item>
    <div class="ky-sql-check-msg" v-if="errorMsg">
      <div class="ky-error-title">Error Messgae:</div>
      <div class="ky-error-content">{{errorMsg}}</div>
    </div>
    <div class="btn-group clearfix ksd-mt-6">
      <el-button size="small" plain @click="delCC" class="ksd-fleft" v-if="ccDesc">{{$t('kylinLang.common.delete')}}</el-button>
      <!-- <el-button size="small" plain @click="cancelCC" class="ksd-fleft" v-if="!ccDesc">{{$t('kylinLang.common.cancel')}}</el-button> -->
      <el-button type="primary" size="small" @click="addCC" class="ksd-fright" v-if="isEdit" :loading="checkBtnLoading">
        {{$t('kylinLang.common.save')}}
      </el-button>
      <!-- <el-button size="small" plain @click="resetCC" class="ksd-fright" v-if="isEdit">
        {{$t('kylinLang.query.clear')}}
      </el-button> -->
      <!-- <el-button size="small" plain @click="checkRemoteCC" :loading="checkBtnLoading" class="ksd-fleft" v-if="isEdit">
        {{$t('kylinLang.common.check')}}
      </el-button> -->
      <!-- <el-button size="small" plain @click="editCC" class="ksd-fright" v-else>
        {{$t('kylinLang.common.edit')}}
      </el-button> -->
    </div>
  </el-form>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { computedDataType } from 'config/index'
import { handleError, kapMessage } from 'util/business'
import { objectClone } from 'util/index'
import { modelErrorMsg } from '../ModelEdit/config'
import { NamedRegex } from 'config'
// import $ from 'jquery'
@Component({
  props: ['isShow', 'ccDesc', 'modelInstance'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    // 后台接口请求
    ...mapActions({
      getUsedCC: 'GET_COMPUTED_COLUMNS',
      checkCC: 'CHECK_COMPUTED_EXPRESSION'
    })
  },
  locales: {
    'en': {conditionExpress: 'Note that select one column should contain its table name(or alias table name).', numberNameFormatValidTip: 'Invalid computed column name', sameName: 'Has same computed column name', columnName: 'Column Name', name: 'Name', expression: 'Expression', returnType: 'Return Type', paramValue: 'Param Value', nameReuse: 'The measure name is reused.', requiredCCName: 'The column name is required.', requiredReturnType: 'The return type is required.', requiredExpress: 'The expression is required.'},
    'zh-cn': {conditionExpress: '请注意，表达式中选用某列时，格式为“表名.列名”。', numberNameFormatValidTip: '无效的可计算列列名', sameName: '有同名的可计算列', columnName: '列名', name: '名称', expression: '表达式', returnType: '返回类型', paramValue: '参数值', nameReuse: 'Measure名称已被使用', requiredCCName: '请输入名称', requiredReturnType: '请选择返回类型', requiredExpress: '请输入表达式。'}
  }
})
export default class CCForm extends Vue {
  computedRetrunType = computedDataType
  checkBtnLoading = false
  ccRules = {
    columnName: [
      { required: true, message: this.$t('requiredCCName'), trigger: 'blur' },
      { validator: this.checkCCName, trigger: 'blur' }
    ],
    datatype: [{ required: true, message: this.$t('requiredReturnType'), trigger: 'change' }],
    expression: [{ required: true, message: this.$t('requiredExpress'), trigger: 'change' }]
  }
  ccMeta = JSON.stringify({
    columnName: '',
    datatype: '',
    expression: ''
  })
  ccObject = JSON.parse(this.ccMeta)
  activeCCIndex = null
  isEdit = true
  ccGroups = []
  integerType = ['bigint', 'int', 'integer', 'smallint', 'tinyint']
  floatType = ['decimal', 'double', 'float']
  otherType = ['binary', 'boolean', 'char', 'date', 'string', 'timestamp', 'varchar']
  checkCCName (rule, value, callback) {
    if (/^\d+$|^_+/.test(value) || !NamedRegex.test(value.toUpperCase())) {
      return callback(new Error(this.$t('numberNameFormatValidTip')))
    }
    if (!this.modelInstance.checkSameCCName(value.toUpperCase())) {
      return callback(new Error(this.$t('sameName')))
    }
    callback()
  }
  upperCaseCCName () {
    this.ccObject.columnName = this.ccObject.columnName.toLocaleUpperCase()
  }
  newCC () {
    this.isEdit = true
    this.$refs['ccForm'].resetFields()
  }
  delCC () {
    this.modelInstance.delCC(this.ccObject).then(() => {
      this.ccObject = JSON.parse(this.ccMeta)
      this.$emit('delSuccess', this.ccObject)
    })
  }
  cancelCC () {
    this.$emit('delSuccess', this.ccObject)
  }
  errorMsg = ''
  checkRemoteCC (cb) {
    this.modelInstance.generateMetadata().then((data) => {
      let resData = objectClone(data)
      let ccMeta = this.modelInstance.generateCCMeta(this.ccObject)
      resData.computed_columns.push(ccMeta)
      this.checkBtnLoading = true
      this.checkCC({
        model_desc: resData,
        project: this.currentSelectedProject,
        cc_in_check: this.modelInstance.getFactTable().alias + '.' + this.ccObject.columnName,
        is_seeking_expr_advice: false
      }).then((res) => {
        this.checkBtnLoading = false
        // kapMessage(this.$t('kylinLang.common.checkSuccess'))
        this.errorMsg = ''
        cb && cb()
      }, (res) => {
        this.checkBtnLoading = false
        handleError(res, (data, code, stasut, msg) => {
          this.errorMsg = msg
        })
      })
    }).catch((code) => {
      kapMessage(this.$t(modelErrorMsg[code]), { type: 'warning' })
    })
  }
  addCC () {
    this.$refs['ccForm'].validate((valid) => {
      if (valid) {
        this.checkRemoteCC(() => {
          let factTable = this.modelInstance.getFactTable()
          this.ccObject.table_guid = factTable.guid
          if (!factTable) {
            kapMessage(this.$t(modelErrorMsg['noFact']), { type: 'warning' })
            return
          }
          if (this.ccObject.guid) {
            this.modelInstance.editCC(this.ccObject).then((cc) => {
              this.$emit('saveSuccess', cc)
              this.isEdit = false
            }, () => {
              kapMessage(this.$t('sameName'), { type: 'warning' })
            })
          } else {
            this.modelInstance.addCC(this.ccObject).then((cc) => {
              this.$emit('saveSuccess', cc)
              this.isEdit = false
            }, () => {
              kapMessage(this.$t('sameName'), { type: 'warning' })
            })
          }
        })
      }
    })
  }
  editCC () {
    this.isEdit = true
  }
  setAutoCompleteData (data) {
    let ad = data.map((col) => {
      return {
        meta: col.datatype,
        caption: col.full_colname,
        value: col.full_colname,
        id: col.id,
        scope: 1
      }
    })
    this.$refs.ccSql.$emit('setAutoCompleteData', ad)
  }
  resetCC () {
    this.$refs['ccForm'].resetFields()
    this.ccObject = JSON.parse(this.ccMeta)
  }
  @Watch('isEdit')
  initIsEdit () {
    if (!this.isEdit) {
      this.$refs.ccSql.$emit('setReadOnly')
    }
  }
  @Watch('ccDesc')
  initCCDesc () {
    if (this.ccDesc) {
      this.isEdit = false
    } else {
      this.isEdit = true
    }
    this.ccObject = JSON.parse(this.ccMeta)
    Object.assign(this.ccObject, this.ccDesc)
  }
  mounted () {
    this.initCCDesc()
    let data = this.modelInstance.getTableColumns()
    this.setAutoCompleteData(data)
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
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
    }
</style>
