<template>
  <el-dialog class="reload-modal" width="480px"
    :title="$t('dialogTitle')"
    :visible="isShow"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    <el-alert
      :type="tipType"
      :show-background="false"
      :closable="false"
      show-icon>
      <p>{{tipMsg1}}{{tipMsg2}}
         <a v-if="hasDetailInfluence" href="javascript:;" style="display: inline-block;" @click="toggleDetail">{{$t('kylinLang.common.seeDetail')}}  
          <i class="el-icon-arrow-down" v-show="!showDetail"></i>
          <i class="el-icon-arrow-up" v-show="showDetail"></i>
        </a>
      </p>
      <div v-if="showDetail" class="detail-box">
        <p>{{detailMsg1}}</p>
        <p>{{detailMsg2}}</p>
      </div>
    </el-alert> 
    <div class="samping-box">
      <span class="lable-text">{{$t('tableSample')}}</span><el-switch
        class="ksd-ml-10"
        size="small"
        v-model="openSample"
        :active-text="$t('kylinLang.common.OFF')"
        :inactive-text="$t('kylinLang.common.ON')">
      </el-switch>
      <p v-if="!hasColumnInfluence" class="sample-sub-top">{{$t('noEffectSampingTip', {tableName: this.tableName})}}</p>
      <p v-else class="sample-sub-top">{{$t('hasEffectSampingTip')}}</p>
      <el-form ref="sample-form" :rules="rules" :model="sampleOption">
        <el-form-item prop="sampleCount">
          <p>{{$t('sampleCount')}}<el-input v-model="sampleOption.sampleCount" :disabled="!openSample" size="mini" style="width:100px" class="ksd-mrl-5"></el-input>{{$t('rows')}}</p>
        </el-form-item>
      </el-form>
    </div> 
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" :loading="reloadLoading" plain type="primary" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleError } from '../../../../util'
vuex.registerModule(['modals', 'ReloadTableModal'], store)

@Component({
  computed: {
    // Store数据注入
    ...mapState('ReloadTableModal', {
      isShow: state => state.isShow,
      tableName: state => state.tableName,
      checkData: state => state.checkData,
      callback: state => state.callback
    }),
    ...mapGetters([
      'currentSelectedProject',
      'isAutoProject'
    ])
  },
  methods: {
    // Store方法注入
    ...mapMutations('ReloadTableModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      resetModalForm: types.RESET_MODAL
    }),
    // 后台接口请求
    ...mapActions({
      reloadModel: 'RELOAD_DATASOURCE'
    })
  },
  locales
})
export default class ReloadTableModal extends Vue {
  isFormShow = false
  openSample = true
  reloadLoading = false
  maxSampleCount = 20000000
  minSampleCount = 10000
  sampleOption = {sampleCount: this.maxSampleCount}
  rules = {
    'sampleCount': [{ validator: this.validateSampleCount, trigger: 'blur' }]
  }
  showDetail = false
  validateSampleCount (rule, value, callback) {
    if (!this.openSample) {
      return callback()
    }
    if (!/^\d+$/.test(value)) {
      callback(new Error(this.$t('invalidType')))
    } else if (+value > this.maxSampleCount) {
      callback(new Error(this.$t('invalidLarger')))
    } else if (+value < this.minSampleCount) {
      callback(new Error(this.$t('invalidSmaller')))
    } else {
      value = +value
      callback()
    }
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  clearFormValidate () {
    this.$refs['sample-form'].clearValidate()
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.showDetail = false
      this.openSample = true
      this.sampleOption.sampleCount = this.maxSampleCount
    } else {
      this.clearFormValidate()
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }
  @Watch('openSample')
  onOpenSample (val) {
    if (!val) {
      this.clearFormValidate()
    }
  }
  // 有列发生了变化
  get hasColumnInfluence () {
    if (this.isShow) {
      return this.checkData.add_column_count + this.checkData.remove_column_count + this.checkData.data_type_change_column_count > 0
    }
  }
  // 列发生的变化导致其他model和dimension或者measure变化
  get hasDetailInfluence () {
    if (this.isShow) {
      let delMeasureCount = this.checkData.remove_measures_count
      let delDimensionCount = this.checkData.remove_dimensions_count
      let delIndexCount = this.checkData.remove_indexes_count
      let brokenModelCount = this.isAutoProject ? 0 : this.checkData.broken_model_count
      return delMeasureCount + delDimensionCount + delIndexCount + brokenModelCount > 0
    }
  }
  get tipType () {
    return this.hasColumnInfluence ? 'warning' : 'tip'
  }
  get tipMsg1 () {
    if (this.isShow) {
      if (this.hasColumnInfluence) {
        let tipList = []
        if (this.checkData.add_column_count) {
          tipList.push(this.$t('addColumnsTip', { addedColumnsCount: this.checkData.add_column_count }))
        }
        if (this.checkData.remove_column_count) {
          tipList.push(this.$t('reducedColumnsTip', { reducedColumnsCount: this.checkData.remove_column_count }))
        }
        if (this.checkData.data_type_change_column_count) {
          tipList.push(this.$t('changedColumnsTip', { changedColumnsCount: this.checkData.data_type_change_column_count }))
        }
        return this.$t('reloadEffectTip', {tableName: this.tableName, changeChar: tipList.join(this.$t('kylinLang.common.comma')) + this.$t('kylinLang.common.dot')})
      }
      return this.$t('reloadNoEffectTip', { tableName: this.tableName })
    }
  }
  get tipMsg2 () {
    let modelMode = this.isAutoProject ? 'kylinLang.model.indexGroup' : 'kylinLang.common.model'
    return this.hasDetailInfluence ? this.$t('reloadEffectTip1', {modelMode: this.$t(modelMode)}) : ''
  }
  get detailMsg1 () {
    if (!this.isAutoProject) {
      let delModelCount = this.checkData.broken_model_count
      return delModelCount ? this.$t('modelchangeTip', {modelCount: delModelCount}) : ''
    }
    return ''
  }
  get detailMsg2 () {
    if (this.isShow) {
      if (this.hasColumnInfluence) {
        let tipList = []
        let delMeasureCount = this.checkData.remove_measures_count
        let delDimensionCount = this.checkData.remove_dimensions_count
        let delIndexCount = this.checkData.remove_indexes_count
        if (delMeasureCount + delDimensionCount + delIndexCount === 0) {
          return ''
        }
        if (delMeasureCount) {
          tipList.push(this.$t('dimChangeTip', { dimensionCount: delMeasureCount }))
        }
        if (delDimensionCount) {
          tipList.push(this.$t('measureChangeTip', { measureCount: delDimensionCount }))
        }
        if (delIndexCount) {
          tipList.push(this.$t('indexChangeTip', { indexCount: delIndexCount }))
        }
        return this.$t('dimAndMeasureAndIndexChangeTip', {changeChar: tipList.join(this.$t('kylinLang.common.comma'))})
      }
    }
  }
  closeHandler (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit () {
    try {
      await this.$refs['sample-form'].validate()
      this.reloadLoading = true
      await this.reloadModel({
        project: this.currentSelectedProject,
        table: this.tableName,
        need_sample: this.openSample,
        max_rows: this.openSample ? +this.sampleOption.sampleCount : 0
      })
      this.reloadLoading = false
      this.$message({
        message: this.$t('reloadSuccess', {tableName: this.tableName}) + (this.openSample ? this.$t('sampleSuccess') : ''),
        type: 'success'
      })
      this.closeHandler(true)
    } catch (e) {
      this.reloadLoading = false
      // 异常处理
      e && handleError(e)
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.reload-modal {
  .detail-box {
    background: @background-disabled-color;
    padding:10px;
    max-height:94px;
    border: solid 1px @border-color-base;
    margin-top:10px;
    font-size:12px;
  }
  .el-alert {
    padding: 0;
    .el-alert__content {
      width:100%;
    }
  }
  .samping-box {
    margin-top:18px;
    .lable-text {
      font-weight: bold;
      vertical-align: middle;
    }
    .sample-sub-top{
      color:@text-normal-color;
      margin-top:5px;
    }
  }
}

</style>
