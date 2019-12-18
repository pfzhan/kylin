<template>
   <el-dialog :title="$t('kylinLang.model.addModel')" limited-area width="480px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="closeModal()">
      <el-form :model="createModelMeta" @submit.native.prevent :rules="rules" ref="addModelForm" label-width="130px" label-position="top">
        <el-form-item prop="newName">
          <span slot="label">{{$t('kylinLang.model.modelName')}}<common-tip :content="$t('kylinLang.model.modelNameTips')"><i class="el-icon-ksd-what ksd-ml-5"></i></common-tip></span>
          <el-input v-focus="isShow" v-guide.inputModelName  v-model="createModelMeta.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc">
         <el-input
            v-guide.inputModelDesc
            type="textarea"
            :rows="2"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            v-model="createModelMeta.modelDesc">
          </el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button v-guide.addModelSave @click="submit" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../../store'
import { NamedRegex } from 'config'
import { handleError, handleSuccess } from 'util/business'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'ModelAddModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('ModelAddModal', {
      isShow: state => state.isShow,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO'
    }),
    ...mapMutations('ModelAddModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class ModelAddModal extends Vue {
  btnLoading = false;
  createModelMeta = {
    newName: '',
    modelDesc: ''
  }
  rules = {
    newName: [{ required: true, validator: this.checkName, trigger: 'blur' }]
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      this.getModelByModelName({model_name: value, project: this.currentSelectedProject}).then((response) => {
        handleSuccess(response, (data) => {
          if (data && data.value && data.value.length) {
            callback(new Error(this.$t('kylinLang.model.sameModelName')))
          } else {
            callback()
          }
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
  closeModal (isSubmit) {
    this.hideModal()
    this.createModelMeta.newName = ''
    this.$refs.addModelForm.resetFields()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    this.btnLoading = true
    this.$refs.addModelForm.validate(valid => {
      if (valid) {
        var modelName = this.createModelMeta.newName
        this.closeModal(true)
        this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'add', modelDesc: this.createModelMeta.modelDesc }})
      }
      this.btnLoading = false
    })
  }
}
</script>
<style lang="less">
</style>
