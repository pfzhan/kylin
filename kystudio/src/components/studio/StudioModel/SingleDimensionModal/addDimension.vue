<template>
  <el-dialog append-to-body :title="$t('adddimension')" @close="isShow && handleClose(false)" v-event-stop  width="440px" :visible.sync="isShow" class="add-dimension-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <div>
      <el-form v-if="isFormShow" :model="dimensionInfo" :rules="rules"  ref="dimensionForm" label-width="100px" label-position="top" class="demo-ruleForm">
        <el-form-item :label="$t('dimensionName')" prop="name">
          <el-input v-model="dimensionInfo.name" ></el-input>
        </el-form-item>
        <el-form-item :label="$t('dimensionCandidate')" prop="column">
          <el-select filterable style="width:350px" place-holder="" v-model="dimensionInfo.column">
            <el-option v-for="(item, index) in allColumns" 
              :key="index"
              :label="item.table_alias + '.' + item.name"
              :value="item.table_alias + '.' + item.name"></el-option>
          </el-select>
          <el-button size="medium" @click="showCC=true" icon="el-icon-ksd-auto_computed_column" class="ksd-ml-10" type="primary" plain></el-button>
          <CCEditForm v-if="showCC" @saveSuccess="saveCC" @delSuccess="delCC" :ccDesc="ccDesc" :modelInstance="modelInstance"></CCEditForm>
        </el-form-item>
        <el-form-item :label="$t('dimensionComment')" prop="comment">
          <el-input type="textarea" v-model="dimensionInfo.comment"></el-input>
        </el-form-item>
      </el-form>
    </div>
    <span slot="footer" class="dialog-footer">
      <el-button @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain size="medium" @click="submit">{{$t('kylinLang.common.ok')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
import store, { types } from './store'
import { NamedRegex } from 'config'
import { objectClone } from 'util/index'
vuex.registerModule(['modals', 'SingleDimensionModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('SingleDimensionModal', {
      callback: state => state.callback,
      isShow: state => state.isShow,
      dimension: state => state.form.dimension,
      modelInstance: state => state.form.modelInstance,
      ccColumns: state => state.form.modelInstance.computed_columns || []
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('SingleDimensionModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
    })
  },
  components: {
    CCEditForm
  },
  locales
})
export default class SingleDimensionModal extends Vue {
  isLoading = false
  isFormShow = false
  showCC = false
  ccDesc = null
  dimensionStr = JSON.stringify({
    name: '',
    column: '',
    comment: '',
    is_dimension: true
  })
  dimensionInfo = JSON.parse(this.dimensionStr)
  selectColumns = []
  currentUseCC = null
  allBaseColumns = []
  rules = {
    name: [
      {required: true, validator: this.checkName, trigger: 'blur'}
    ],
    column: [
      { required: true, message: '请选择', trigger: 'change' }
    ]
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  handleSelectCandidate () {

  }
  saveCC (cc) {
    this.dimensionInfo.column = cc.columnName
    this.dimensionInfo.guid = cc.guid
    this.currentUseCC = cc
  }
  delCC (cc) {
    this.currentUseCC = null
  }
  get allColumns () {
    let cloneCCList = objectClone(this.ccColumns)
    cloneCCList = cloneCCList.map((x) => {
      x.name = x.columnName
      x.table_alias = x.tableAlias
      return x
    })
    return [...this.allBaseColumns, ...cloneCCList]
  }
  // changeColumn () {
  //   let column = this.dimensionInfo.column
  //   for (let i = 0; i < this.savedColumns.length; i++) {
  //     if (this.savedColumns[i].columnName === column) {
  //       this.ccDesc = this.savedColumns[i]
  //       break
  //     }
  //   }
  // }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (this.dimension) {
        Object.assign(this.dimensionInfo, this.dimension)
      } else {
        this.dimensionInfo = JSON.parse(this.dimensionStr)
      }
      this.allBaseColumns = this.modelInstance.getTableColumns()
      this.isFormShow = true
      this.showCC = false
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }
  async submit () {
    this.$refs.dimensionForm.validate((valid) => {
      if (!valid) { return }
      this.handleClose(true)
    })
  }
  @Watch('dimensionInfo.column')
  getColumnInfo (fullName) {
    if (fullName) {
      let dimensionNamed = fullName.split('.')
      let alias = dimensionNamed[0]
      let column = dimensionNamed[1]
      let tableInfo = this.modelInstance.getTableByAlias(alias)
      if (tableInfo) {
        this.dimensionInfo.guid = tableInfo.guid
      }
      let ccObj = this.modelInstance.getCCObj(alias, column)
      if (ccObj) {
        this.dimensionInfo.cc = ccObj
      }
      this.ccDesc = ccObj
      this.dimensionInfo.isCC = !!ccObj
    }
  }
  _handleCloseFunc (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: {
          dimension: objectClone(this.dimensionInfo)
        }
      })
    }, 300)
  }
  handleClose (isSubmit) {
    if (!isSubmit) {
      this._handleCloseFunc(isSubmit)
      return
    }
    if (this.dimension) {
      this.modelInstance.editDimension(this.dimensionInfo, this.dimensionInfo._id).then(() => {
        this._handleCloseFunc(isSubmit)
      }, () => {
        console.log('dimsnion命名重复')
      })
    } else {
      this.modelInstance.addDimension(this.dimensionInfo).then(() => {
        this._handleCloseFunc(isSubmit)
      }, () => {
        console.log('dimsnion命名重复')
      })
    }
  }
  destroyed () {
    if (!module.hot) {
      vuex.unregisterModule(['modals', 'SingleDimensionModal'])
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.add-dimension-dialog{
  .cc-area{
    background-color: @table-stripe-color;
    border:solid 1px @line-split-color;
    padding:24px 20px 20px 20px;
    margin-top: 10px;
  }
}

</style>
