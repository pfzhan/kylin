<template>
  <!-- 模型构建 -->
    <el-dialog :title="$t('Model Build')" width="660px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <div>
        <el-form :model="modelBuildMeta" :rules="rules" ref="buildForm" label-width="100px">
          <el-form-item :label="$t('buildRange')" prop="dataRangeVal">
            <el-date-picker
              v-model="modelBuildMeta.dataRangeVal"
              type="datetimerange"
              :range-separator="$t('to')"
              :start-placeholder="$t('startDate')"
              :end-placeholder="$t('endDate')">
            </el-date-picker>
          </el-form-item>
        </el-form>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" @click="setbuildModelRange" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../../store'
  import { handleError, kapMessage } from 'util/business'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'ModelBuildModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelBuildModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        buildModel: 'MODEL_BUILD'
      }),
      ...mapMutations('ModelBuildModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelBuildModal extends Vue {
    btnLoading = false
    modelBuildMeta = {
      dataRangeVal: ''
    }
    rules = {
      dataRangeVal: [
        {required: true, trigger: 'blur'}
      ]
    }
    @Watch('isShow')
    initModelBuldRange () {
      if (this.isShow) {
        this.modelBuildMeta.dataRangeVal = []
        if (this.modelDesc.last_build_end) {
          let lastBuildDate = new Date(+this.modelDesc.last_build_end)
          if (lastBuildDate) {
            this.modelBuildMeta.dataRangeVal.push(lastBuildDate, lastBuildDate)
          }
        }
      } else {
        this.modelBuildMeta.dataRangeVal = []
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    get timeRange () {

    }
    async setbuildModelRange () {
      this.$refs.buildForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        // 处理下时区问题
        let start = this.modelBuildMeta.dataRangeVal[0].getTime()
        let end = this.modelBuildMeta.dataRangeVal[1].getTime()
        this.buildModel({
          model: this.modelDesc.name,
          start: start,
          end: end,
          project: this.currentSelectedProject
        }).then(() => {
          this.btnLoading = false
          kapMessage(this.$t('构建成功'))
          this.closeModal(true)
        }, (res) => {
          this.btnLoading = false
          res && handleError(res)
        })
      })
    }
  }
</script>
<style lang="less">
  
</style>
