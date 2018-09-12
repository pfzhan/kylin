<template>
  <el-dialog :title="$t('Add Dimension')" @close="isShow && handleClose(false)" v-event-stop  width="660px" :visible.sync="isShow" class="links_dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <div>
      <el-form  status-icon  ref="ruleForm2" label-width="100px" label-position="top" class="demo-ruleForm">
        <el-form-item label="Dimension Name" prop="pass">
          <el-input></el-input>
        </el-form-item>
        <el-form-item label="Dimension Candidate" prop="checkPass">
          <el-select :popper-append-to-body="false" style="width:100%"></el-select>
        </el-form-item>
        <el-form-item label="Dimension Comment" prop="age">
          <el-input ></el-input>
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
import store, { types } from './store'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('SingleDimensionModal', {
      isShow: state => state.isShow
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
  locales
})
export default class SingleDimensionModal extends Vue {
  isLoading = false
  isFormShow = false
  selectVal = ''
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
  beforeCreate () {
    if (!this.$store.state.modals.SingleDimensionModal) {
      vuex.registerModule(['modals', 'SingleDimensionModal'], store)
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
</style>
