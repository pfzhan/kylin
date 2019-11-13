<template>
  <!-- 模型构建 -->
    <el-dialog class="model-build" :title="$t('modelBuild')" limited-area width="560px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <div>
        <el-form :model="modelBuildMeta" ref="buildForm" :rules="rules" label-position="top">
          <!-- <div class="ky-list-title ksd-mt-14">{{$t('buildRange')}}</div> -->
          <!-- <el-form-item prop="isLoadExisted" class="ksd-mt-10 ksd-mb-2">
            <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="true">
              {{$t('loadExistingData')}}
            </el-radio>
            <div class="item-desc">{{$t('loadExistingDataDesc')}}</div>
          </el-form-item> -->
          <el-form-item prop="dataRangeVal" :rule="modelBuildMeta.isLoadExisted ? [] : [{required: true, trigger: 'blur', message: this.$t('dataRangeValValid')}]">
            <!-- <el-radio class="font-medium" v-model="modelBuildMeta.isLoadExisted" :label="false">
              {{$t('customLoadRange')}}
            </el-radio>
            <br/> -->
            <el-alert
              class="ksd-pt-0"
              :title="$t('kylinLang.dataSource.rangeInfoTip')"
              type="info"
              :show-background="false"
              :closable="false"
              show-icon>
            </el-alert>
            <div class="ky-no-br-space" style="height:32px;">
              <el-date-picker
                type="datetime"
                class="ksd-mr-5"
                v-model="modelBuildMeta.dataRangeVal[0]"
                :is-auto-complete="true"
                :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                @change="resetError"
                value-format="timestamp"
                :format="format"
                :placeholder="$t('kylinLang.common.startTime')">
              </el-date-picker>
              <el-date-picker
                type="datetime"
                v-model="modelBuildMeta.dataRangeVal[1]"
                :is-auto-complete="true"
                :disabled="modelBuildMeta.isLoadExisted || isLoadingNewRange"
                value-format="timestamp"
                @change="resetError"
                :format="format"
                :placeholder="$t('kylinLang.common.endTime')">
              </el-date-picker>
              <el-tooltip effect="dark" :content="$t('detectAvailableRange')" placement="top">
                <el-button
                  size="medium"
                  class="ksd-ml-10"
                  v-if="$store.state.project.projectPushdownConfig"
                  :disabled="modelBuildMeta.isLoadExisted"
                  :loading="isLoadingNewRange"
                  icon="el-icon-ksd-data_range_search"
                  @click="handleLoadNewestRange">
                </el-button>
              </el-tooltip>
            </div>
          </el-form-item>
        </el-form>
        <div class="error-msg" v-if="isShowRangeDateError">{{loadRangeDateError}}</div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
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
      isLoadExisted: false
    }
    rules = {
      dataRangeVal: [{
        validator: this.validateRange, trigger: 'blur'
      }]
    }
    loadRangeDateError = ''
    isShowRangeDateError = false
    get format () {
      return this.modelDesc.partition_desc && this.modelDesc.partition_desc.partition_date_format || 'yyyy-MM-dd'
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
      this.resetError()
      try {
        const submitData = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid
        }
        const response = await this.fetchNewestModelRange(submitData)
        if (submitData.model !== this.modelDesc.uuid) { // 避免ajax耗时太长导致会覆盖新的model的load range数据
          return
        }
        if (response.body.code === '000') {
          const result = await handleSuccessAsync(response)
          const startTime = +result.start_time
          const endTime = +result.end_time
          this.modelBuildMeta.dataRangeVal = [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ]
        } else if (response.body.code === '999') {
          this.loadRangeDateError = response.body.msg
          this.isShowRangeDateError = true
        }
      } catch (e) {
        handleError(e)
      }
      this.isLoadingNewRange = false
    }
    resetError () {
      this.loadRangeDateError = ''
      this.isShowRangeDateError = false
    }
    closeModal (isSubmit) {
      this.isLoadingNewRange = false
      this.btnLoading = false
      this.$refs.buildForm && this.$refs.buildForm.resetFields()
      this.resetError()
      this.hideModal()
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
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
          start = transToUTCMs(this.modelBuildMeta.dataRangeVal[0])
          end = transToUTCMs(this.modelBuildMeta.dataRangeVal[1])
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
@import '../../../../../assets/styles/variables.less';
  .model-build {
    .item-desc {
      font-size: 12px;
      line-height: 1;
    }
    .error-msg {
      color: @error-color-1;
      font-size: 12px;
      margin-top: 5px;
    }
  }
</style>
