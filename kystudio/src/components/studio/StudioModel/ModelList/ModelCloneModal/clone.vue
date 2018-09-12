<template>
  <!-- 模型重命名 -->
  <el-dialog :title="$t('modelClone')" width="440px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="modelClone" :rules="rules" ref="cloneForm" label-width="100px">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-model="modelClone.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="submit">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../../store'
  import { NamedRegex } from 'config'
  import { handleError, kapMessage } from 'util/business'
  import locales from './locales'
  import store, { types } from './store'

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelCloneModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        cloneModel: 'CLONE_MODEL'
      }),
      ...mapMutations('ModelCloneModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelCloneModal extends Vue {
    btnLoading = false
    modelClone = {
      newName: ''
    }
    rules = {
      newName: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    @Watch('modelDesc')
    initModelName () {
      this.modelClone.newName = this.modelDesc.name + '_clone'
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.modelClone.newName = ''
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    async submit () {
      this.$refs.cloneForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.cloneModel({modelName: this.modelDesc.name, newModelName: this.modelClone.newName, project: this.currentSelectedProject}).then(() => {
          this.btnLoading = false
          kapMessage(this.$t('cloneSuccessful'))
          this.closeModal(true)
        }, (res) => {
          this.btnLoading = false
          res && handleError(res)
        })
      })
    }
    beforeCreate () {
      if (!this.$store.state.modals.ModelCloneModal) {
        vuex.registerModule(['modals', 'ModelCloneModal'], store)
      }
    }
    destroyed () {
      if (!module.hot) {
        vuex.unregisterModule(['modals', 'ModelCloneModal'])
      }
    }
  }
</script>
<style lang="less">
  
</style>
