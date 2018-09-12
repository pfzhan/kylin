<template>
  <!-- 模型重命名 -->
  <el-dialog :title="$t('modelRename')" width="440px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="modelEdit" :rules="rules" ref="renameForm" label-width="100px">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-model="modelEdit.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="submit">32323{{$t('kylinLang.common.save')}}</el-button>
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
      ...mapState('ModelRenameModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc
      })
    },
    methods: {
      ...mapActions({
        renameModel: 'RENAME_MODEL'
      }),
      ...mapMutations('ModelRenameModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelRenameModal extends Vue {
    btnLoading = false
    modelEdit = {
      newName: ''
    }
    rules = {
      newName: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    @Watch('modelName')
    initModelName () {
      this.modelEdit.newName = this.modelName
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    closeModal () {
      this.hideModal()
      this.modelEdit.newName = ''
      setTimeout(() => {
        this.resetModalForm()
      }, 200)
    }
    submit () {
      this.$refs.renameForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.modelDesc.alias = this.modelEdit.newName
        this.renameModel({modelDescData: JSON.stringify(this.modelDesc), project: this.currentSelectedProject}).then(() => {
          this.btnLoading = false
          kapMessage(this.$t('updateSuccessful'))
          this.closeModal()
        }, (res) => {
          this.btnLoading = false
          res && handleError(res)
        })
      })
    }
    beforeCreate () {
      if (!this.$store.state.modals.ModelRenameModal) {
        vuex.registerModule(['modals', 'ModelRenameModal'], store)
      }
    }
    destroyed () {
      if (!module.hot) {
        vuex.unregisterModule(['modals', 'ModelRenameModal'])
      }
    }
  }
</script>
<style lang="less">
  
</style>
