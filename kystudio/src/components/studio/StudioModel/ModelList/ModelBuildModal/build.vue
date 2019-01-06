<template>
  <!-- 模型构建 -->
    <el-dialog :title="$t('modelBuild')" width="660px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <div>
        <el-form :model="modelBuildMeta" ref="buildForm" label-position="top">
          <div class="ky-list-title ksd-mt-14">{{$t('buildRange')}}</div>
          <el-form-item prop="isLoadExisted" class="ksd-mt-10 ksd-mb-2">
            <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="true">
              {{$t('loadExistingData')}}
            </el-radio>
            <!-- <div class="item-desc">{{$t('loadExistingDataDesc')}}</div> -->
          </el-form-item>
          <el-form-item prop="dataRangeVal" :rule="modelBuildMeta.isLoadExisted ? [] : [{required: true, trigger: 'blur', message: this.$t('dataRangeValValid')}]">
            <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="false">
              {{$t('customLoadRange')}}
            </el-radio>
            <br/>
            <el-date-picker
              class="ksd-ml-24"
             :disabled="modelBuildMeta.isLoadExisted"
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
  import { handleError, kapMessage, transToUTCMs, getGmtDateFromUtcLike } from 'util/business'
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
      dataRangeVal: '',
      isLoadExisted: true
    }
    @Watch('isShow')
    initModelBuldRange () {
      if (this.isShow) {
        this.modelBuildMeta.dataRangeVal = []
        if (this.modelDesc.last_build_end) {
          let lastBuildDate = getGmtDateFromUtcLike(+this.modelDesc.last_build_end)
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
      this.$refs.buildForm.resetFields()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    get timeRange () {

    }
    _buildModel ({start, end, modelName}) {
      this.buildModel({
        model: modelName,
        start: start,
        end: end,
        project: this.currentSelectedProject
      }).then(() => {
        this.btnLoading = false
        kapMessage(this.$t('kylinLang.common.submitSuccess'))
        this.closeModal(true)
        this.$emit('refreshModelList')
      }, (res) => {
        this.btnLoading = false
        res && handleError(res)
      })
    }
    async setbuildModelRange () {
      this.$refs.buildForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        let start = null
        let end = null
        if (!this.modelBuildMeta.isLoadExisted) {
          start = transToUTCMs(this.modelBuildMeta.dataRangeVal[0]) || null
          end = transToUTCMs(this.modelBuildMeta.dataRangeVal[1]) || null
        }
        this._buildModel({start: start, end: end, modelName: this.modelDesc.name})
      })
    }
    created () {
      this.$on('buildModel', this._buildModel)
    }
  }
</script>
<style lang="less">
  
</style>
