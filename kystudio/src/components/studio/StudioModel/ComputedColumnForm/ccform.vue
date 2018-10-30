<template>
  <el-form :model="ccObject" class="cc-block" :class="{'editCC': !isEdit}" label-position="top" :rules="ccRules" ref="ccForm">
    <el-form-item prop="columnName" class="ksd-mb-10">
      <span slot="label">{{$t('columnName')}} <span v-if="!isEdit">: {{ccObject.columnName}}</span></span>
      <el-input class="measures-width" size="medium" v-model="ccObject.columnName" v-if="isEdit" @blur="upperCaseCCName"></el-input>
    </el-form-item>
    <el-form-item prop="datatype" class="ksd-mb-10">
      <span slot="label">{{$t('returnType')}}<span v-if="!isEdit">: {{ccObject.datatype}}</span></span>
      <el-select :popper-append-to-body="false" size="medium" v-model="ccObject.datatype" class="measures-width" v-if="isEdit">
        <el-option
          v-for="(item, index) in computedRetrunType"
          :key="index"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>
    <el-form-item :label="$t('expression')" prop="expression" class="ksd-mb-10">
      <kap_editor ref="ccSql" height="100" lang="sql" theme="chrome" v-model="ccObject.expression">
      </kap_editor>
    </el-form-item>
    <div class="btn-group clearfix">
      <el-button size="small" plain @click="delCC" class="ksd-fleft" v-if="ccDesc">{{$t('kylinLang.common.delete')}}</el-button>
      <el-button size="small" plain @click="cancelCC" class="ksd-fleft" v-if="!ccDesc">{{$t('kylinLang.common.cancel')}}</el-button>
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
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { computedDataType } from 'config/index'
// import $ from 'jquery'
@Component({
  props: ['isShow', 'ccDesc', 'modelInstance'],
  methods: {
    // 后台接口请求
    ...mapActions({
      getUsedCC: 'GET_COMPUTED_COLUMNS',
      checkCC: 'CHECK_COMPUTED_EXPRESSION'
    })
  },
  locales: {
    'en': {columnName: 'Column Name', requiredName: 'The measure name is required.', name: 'Name', expression: 'Expression', returnType: 'Return Type', paramValue: 'Param Value', nameReuse: 'The measure name is reused.', requiredCCName: 'The column name is required.', requiredReturnType: 'The return type is required.', requiredExpress: 'The expression is required.'},
    'zh-cn': {columnName: '列名', requiredName: '请输入度量名称', name: '名称', expression: '表达式', returnType: '返回类型', paramValue: '参数值', nameReuse: 'Measure名称已被使用', requiredCCName: '请输入列表名称', requiredReturnType: '请选择度量返回类型', requiredExpress: '请输入表达式。'}
  }
})
export default class CCForm extends Vue {
  computedRetrunType = computedDataType
  ccRules = {
    columnName: [{ required: true, message: this.$t('requiredCCName'), trigger: 'blur' }],
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
  checkRemoteCC () {
    this.modelInstance.generateMetadata().then((data) => {
      this.checkCC(data).then((res) => {
        console.log(res)
      }, (res) => {
        console.log(res)
      })
    })
  }
  addCC () {
    this.$refs['ccForm'].validate((valid) => {
      this.checkRemoteCC()
      if (valid) {
        if (this.ccObject.guid) {
          this.modelInstance.editCC(this.ccObject).then((cc) => {
            this.$emit('saveSuccess', cc)
            this.isEdit = false
          }, () => {
            // 提示已经有同名的CC
          })
        } else {
          this.modelInstance.addCC(this.ccObject).then((cc) => {
            this.$emit('saveSuccess', cc)
            this.isEdit = false
          }, () => {
            // 提示已经有同名的CC
          })
        }
      }
    })
  }
  editCC () {
    this.isEdit = true
  }
  resetCC () {
    this.$refs['ccForm'].resetFields()
    this.ccObject = JSON.parse(this.ccMeta)
  }
  mounted () {
    if (this.ccDesc) {
      this.isEdit = false
    } else {
      this.isEdit = true
    }
    this.ccObject = JSON.parse(this.ccMeta)
    Object.assign(this.ccObject, this.ccDesc)
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
