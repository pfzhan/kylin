<template>
  <el-dialog :title="$t('adddimension')" @close="isShow && handleClose(false)" v-event-stop  width="440px" :visible.sync="isShow" class="add-dimension-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <div>
      <el-form v-if="isFormShow" :model="dimensionInfo" :rules="rules"  ref="dimensionForm" label-width="100px" label-position="top" class="demo-ruleForm">
        <el-form-item :label="$t('dimensionName')" prop="name">
          <el-input v-model="dimensionInfo.name" ></el-input>
        </el-form-item>
        <el-form-item :label="$t('dimensionCandidate')" prop="column">
          <el-select :popper-append-to-body="false" filterable style="width:350px" place-holder="" v-model="dimensionInfo.column">
            <el-option v-for="(item, index) in selectColumns"
              :key="index"
              :label="item.columnName"
              :value="item.columnName"></el-option>
          </el-select>
          <el-button size="medium" icon="el-icon-ksd-auto_computed_column" class="ksd-ml-10" type="primary" plain></el-button>
          <CCEditForm @save="saveCC" @del="delCC"></CCEditForm>
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
  props: ['allColumns'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('SingleDimensionModal', {
      callback: state => state.callback,
      isShow: state => state.isShow,
      dimension: state => state.form.dimension,
      modelDesc: state => state.form.modelDesc,
      savedColumns: state => state.form.modelDesc && objectClone(state.form.modelDesc.computed_columns) || []
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
  selectVal = ''
  dimensionStr = JSON.stringify({
    name: '',
    column: '',
    comment: ''
  })
  dimensionInfo = JSON.parse(this.dimensionStr)
  selectColumns = []
  currentUseCC = null
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
  saveCC (cc) {
    this.selectColumns.push(cc)
    this.dimensionInfo.column = cc.columnName
    this.currentUseCC = cc
  }
  delCC (cc) {
    for (let i = 0; i < this.selectColumns.length; i++) {
      if (this.selectColumns[i].name === cc.name) {
        this.selectColumns.splice(i, 1)
        break
      }
    }
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (this.dimension) {
        Object.assign(this.dimensionInfo, this.dimension)
      } else {
        this.dimensionInfo = JSON.parse(this.dimensionStr)
      }
      this.selectColumns = this.savedColumns
      this.isFormShow = true
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
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: {
          dimension: objectClone(this.dimensionInfo),
          cc: this.currentUseCC
        }
      })
    }, 300)
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
