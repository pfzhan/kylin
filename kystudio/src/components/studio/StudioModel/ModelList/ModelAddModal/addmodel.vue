<template>
   <el-dialog :title="$t('kylinLang.model.addModel')" width="440px" :visible="isShow" @close="closeModal()">
      <el-form :model="createModelMeta"  :rules="rules" ref="addModelForm" label-width="130px" label-position="top">
        <el-form-item prop="newName" :label="$t('kylinLang.model.modelName')">
          <span slot="label">{{$t('kylinLang.model.modelName')}}
            <common-tip :content="$t('kylinLang.model.modelNameTips')" ><i class="el-icon-question"></i></common-tip>
          </span>
          <el-input v-model="createModelMeta.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc" style="margin-top: 20px;">
         <el-input
            type="textarea"
            :rows="2"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            v-model="createModelMeta.modelDesc">
          </el-input>
        </el-form-item>
      </el-form>
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
import vuex from '../../../../../store'
import { NamedRegex } from 'config'
// import { handleError } from 'util/business'
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
      addModel: ''
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
  };
  rules = {
    newName: [{ validator: this.checkName, trigger: 'blur' }]
  };
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  closeModal (isSubmit) {
    this.hideModal()
    this.createModelMeta.newName = ''
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    // 需要补充重名远程校验
    this.$refs.addModelForm.validate(valid => {
      if (!valid) {
        return
      }
      var modelName = this.createModelMeta.newName
      // this.btnLoading = true
      this.closeModal(true)
      this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'add', modelDesc: this.createModelMeta.modelDesc }})
    })
  }
}
</script>
<style lang="less">
</style>
