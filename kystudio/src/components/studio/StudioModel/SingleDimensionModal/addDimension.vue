<template>
  <el-dialog :title="$t('adddimension')" @close="isShow && handleClose(false)" v-event-stop  width="440px" :visible.sync="isShow" class="add-dimension-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <div>
      <el-form  status-icon  ref="ruleForm2" label-width="100px" label-position="top" class="demo-ruleForm">
        <el-form-item :label="$t('dimensionName')" prop="pass">
          <el-input></el-input>
        </el-form-item>
        <el-form-item :label="$t('dimensionCandidate')" prop="checkPass">
          <el-select :popper-append-to-body="false" style="width:350px" place-holder="" v-model="dimensionInfo.dimensionColumn"></el-select>

          <el-button size="medium" icon="el-icon-ksd-auto_computed_column" class="ksd-ml-10" type="primary" plain  v-if="addType === 'cc'"></el-button>
          <CCEditForm/>
        </el-form-item>
        <el-form-item :label="$t('dimensionComment')" prop="age">
          <el-input type="textarea"></el-input>
        </el-form-item>
      </el-form>
    </div>
    <span slot="footer" class="dialog-footer">
      <el-button @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain size="medium">{{$t('kylinLang.common.ok')}}</el-button>
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
vuex.registerModule(['modals', 'SingleDimensionModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('SingleDimensionModal', {
      isShow: state => state.isShow,
      addType: state => state.addType,
      dimensionColumn: state => state.form.dimensionColumn
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
  dimensionInfo = {
    dimensionColumn: ''
  }
  @Watch('dimensionColumn')
  initDimensionColumn () {
    this.dimensionInfo.dimensionColumn = this.dimensionColumn
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
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

  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
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
