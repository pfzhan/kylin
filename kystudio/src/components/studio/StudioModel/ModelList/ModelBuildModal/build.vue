<template>
  <!-- 模型构建 -->
    <el-dialog :title="$t('modelBuild')" width="660px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <div>
        <el-form :model="modelBuildMeta" ref="buildForm" :rules="rules" label-position="top">
          <!-- <div class="ky-list-title ksd-mt-14">{{$t('buildRange')}}</div> -->
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
            type="datetime"
            v-model="modelBuildMeta.dataRangeVal[0]"
            :is-auto-complete="true"
            :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
            :picker-options="{ disabledDate: time => time.getTime() > modelBuildMeta.dataRangeVal[1] && modelBuildMeta.dataRangeVal[1] !== null }"
            :placeholder="$t('kylinLang.common.startTime')">
          </el-date-picker>
          <span>-</span>
          <el-date-picker
            type="datetime"
            v-model="modelBuildMeta.dataRangeVal[1]"
            :is-auto-complete="true"
            :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
            :picker-options="{ disabledDate: time => time.getTime() < modelBuildMeta.dataRangeVal[0] && modelBuildMeta.dataRangeVal[0] !== null }"
            :placeholder="$t('kylinLang.common.endTime')">
          </el-date-picker>
          <el-tooltip effect="dark" :content="$t('detectAvailableRange')" placement="top">
            <el-button
              v-if="isShow"
              size="small"
              class="ksd-ml-10"
              :disabled="modelBuildMeta.isLoadExisted"
              :loading="isLoadingNewRange"
              icon="el-icon-ksd-data_range_search"
              @click="handleLoadNewestRange">
            </el-button>
          </el-tooltip>
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
  import { handleSuccessAsync } from 'util/index'
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
        buildModel: 'MODEL_BUILD',
        fetchNewestModelRange: 'GET_MODEL_NEWEST_RANGE'
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
    isLoadingNewRange = false
    modelBuildMeta = {
      dataRangeVal: [],
      isLoadExisted: true
    }
    rules = {
      dataRangeVal: [{
        validator: this.validateRange, trigger: 'blur'
      }]
    }
    validateRange (rule, value, callback) {
      const [ startValue, endValue ] = value
      const isLoadExisted = this.modelBuildMeta.isLoadExisted
      if ((!startValue || !endValue || transToUTCMs(startValue) >= transToUTCMs(endValue)) && !isLoadExisted) {
        callback(new Error(this.$t('invaildDate')))
      } else {
        callback()
      }
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
    async handleLoadNewestRange () {
      this.isLoadingNewRange = true
      try {
        const submitData = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid
        }
        const response = await this.fetchNewestModelRange(submitData)
        const result = await handleSuccessAsync(response)
        const startTime = +result.start_time
        const endTime = +result.end_time
        this.modelBuildMeta.dataRangeVal = [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ]
      } catch (e) {
        handleError(e)
      }
      this.isLoadingNewRange = false
    }
    closeModal (isSubmit) {
      this.$refs.buildForm.resetFields()
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    get timeRange () {

    }
    _buildModel ({start, end, modelId}) {
      this.buildModel({
        modelId: modelId,
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
        this._buildModel({start: start, end: end, modelId: this.modelDesc.uuid})
      })
    }
    created () {
      this.$on('buildModel', this._buildModel)
    }
  }
</script>
<style lang="less">
  
</style>
