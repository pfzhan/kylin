<template>
   <el-dialog append-to-body :title="$t('kylinLang.model.addCC')" width="440px" :visible="isShow" @close="closeModal()">
      <CCEditForm v-if="isShow" @saveSuccess="saveCC" @saveError="saveCCError" ref="ccForm" :isPureForm="true" :modelInstance="modelInstance"/>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="submit" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
vuex.registerModule(['modals', 'CCAddModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('CCAddModal', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      modelInstance: state => state.form.modelInstance
    })
  },
  methods: {
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO'
    }),
    ...mapMutations('CCAddModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
    CCEditForm
  },
  locales
})
export default class CCAddModal extends Vue {
  btnLoading = false
  saveCC () {
    this.btnLoading = false
    this.closeModal(true)
  }
  saveCCError () {
    this.btnLoading = false
  }
  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    this.btnLoading = true
    this.$refs.ccForm.$emit('addCC')
  }
}
</script>
<style lang="less">
</style>
