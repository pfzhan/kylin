<template>
  <el-dialog
    :title="dialogTitle"
    append-to-body
    limited-area
    top="5vh" 
    class="recommendations-dialog" 
    width="960px" 
    :visible="isShow" 
    :close-on-press-escape="false" 
    :close-on-click-modal="false" 
    @close="isShow && closeModal()">
      <div class="recommendations" v-loading="!dialogInfoLoaded">
        <el-button class="clearBtn" size="mini" @click="clearRecommendations" :loading="clearBtnLoading" :disabled="clearBtnLoading || btnLoading || !canClearRecom">{{$t('clearAllRecom')}}</el-button>
        <el-tabs v-model="activeRecom" type="card">
          <el-tab-pane :label="dimensionTabTitle" name="dimension">
            <Dimension
              ref="dimensionRecom"
              v-if="dialogInfoLoaded"
              v-on:dimensionSelectedChange="refreshSelected"
              :list="dialogInfo.dimension_recommendations"></Dimension>
          </el-tab-pane>
          <el-tab-pane :label="measureTabTitle" name="measure">
            <Measure
              ref="measureRecom"
              v-if="dialogInfoLoaded"
              v-on:measureSelectedChange="refreshSelected"
              :list="dialogInfo.measure_recommendations"></Measure>
          </el-tab-pane>
          <el-tab-pane :label="computedColumnTabTitle" name="computedColumn">
            <ComputedColumn
              ref="ccRecom"
              v-if="dialogInfoLoaded"
              v-on:computedColumnSelectedChange="refreshSelected"
              :list="dialogInfo.cc_recommendations"></ComputedColumn>
          </el-tab-pane>
          <!-- <el-tab-pane :label="aggIndexTabTitle" name="aggIndex">
            <AggIndex
              ref="aggIndexRecom"
              v-if="dialogInfoLoaded"
              :modelDesc="modelDesc"
              v-on:aggIndexSelectedChange="refreshSelected"
              :list="dialogInfo.agg_index_recommendations"></AggIndex>
          </el-tab-pane>
          <el-tab-pane :label="tableIndexTabTitle" name="tableIndex">
            <TableIndex
              ref="tableIndexRecom"
              v-if="dialogInfoLoaded"
              :modelDesc="modelDesc"
              v-on:tableIndexSelectedChange="refreshSelected"
              :list="dialogInfo.table_index_recommendations"></TableIndex>
          </el-tab-pane> -->
          <el-tab-pane :label="indexTabTitle" name="index">
            <IndexGroup
              ref="indexGroup"
              v-if="dialogInfoLoaded"
              :modelDesc="modelDesc"
              v-on:indexSelectedChange="refreshSelected"
              :list="dialogInfo.index_recommendations">
            </IndexGroup>
          </el-tab-pane>
        </el-tabs>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="closeModal()" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :loading="btnLoading" :disabled="clearBtnLoading || btnLoading || !canSubmitRecom" size="medium" @click="submit">{{$t('acceptBtn')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../../store'
  import { handleError, kapMessage } from 'util/business'
  import { handleSuccessAsync, objectClone } from 'util'
  // import { objectClone } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'
  import Dimension from './tabView/dimension'
  import Measure from './tabView/measure'
  import ComputedColumn from './tabView/computedColumn'
  // import AggIndex from './tabView/aggIndex'
  // import TableIndex from './tabView/tableIndex'
  import IndexGroup from './tabView/indexGroup'

  vuex.registerModule(['modals', 'ModelRecommendModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('ModelRecommendModal', {
        isShow: state => state.isShow,
        modelDesc: state => state.form.modelDesc,
        callback: state => state.callback
      })
    },
    components: {
      Dimension,
      Measure,
      ComputedColumn,
      // AggIndex,
      // TableIndex,
      IndexGroup
    },
    methods: {
      ...mapActions({
        loadModelRecommendations: 'GET_MODEL_RECOMMENDATIONS',
        saveModelRecommendations: 'ADOPT_MODEL_RECOMMENDATIONS',
        clearModelRecommendations: 'CLEAR_MODEL_RECOMMENDATIONS'
      }),
      ...mapMutations('ModelRecommendModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class ModelRecommendModal extends Vue {
    activeRecom = 'dimension'
    btnLoading = false // 提交按钮上的防重复提交标志
    clearBtnLoading = false // 清空按钮的防重复提交
    dialogInfoLoaded = false // 标志数据是否加载完成
    // 几个tab的选中个数
    selectedObj = {
      dimension_selectedLen: 0,
      measure_selectedLen: 0,
      cc_selectedLen: 0,
      index_selectedLen: 0
    }
    dialogInfo = {
      dimension_recommendations: [],
      measure_recommendations: [],
      cc_recommendations: [],
      index_recommendations: []
    }

    @Watch('isShow')
    onModalVisibleChange (val) {
      // 关闭弹窗时，重置所有信息
      // 每次重新打开，默认tab 置回维度
      this.activeRecom = 'dimension'
      // 回复按钮的可点状态
      this.btnLoading = false
      this.clearBtnLoading = false
      // 数据字段都置回初值
      // 所有的数据格式要在传入prop之前进行处理
      this.dialogInfo = {
        dimension_recommendations: [],
        measure_recommendations: [],
        cc_recommendations: [],
        index_recommendations: []
      }
      // 初始化数字
      this.selectedObj['dimension_selectedLen'] = 0
      this.selectedObj['measure_selectedLen'] = 0
      this.selectedObj['cc_selectedLen'] = 0
      this.selectedObj['index_selectedLen'] = 0

      // 弹窗显示的时候，才进行数据请求
      if (val) {
        this.loadModalInfo()
      }
    }
    // 弹窗标题
    get dialogTitle () {
      let selectedTotal = this.selectedObj.dimension_selectedLen + this.selectedObj.measure_selectedLen + this.selectedObj.cc_selectedLen + this.selectedObj.index_selectedLen
      let total = this.dialogInfo.dimension_recommendations.length + this.dialogInfo.measure_recommendations.length + this.dialogInfo.cc_recommendations.length + this.dialogInfo.index_recommendations.length
      return this.$t('recommendModalTitle', {selected: selectedTotal, total: total})
    }
    // 几个 tab 标题
    get dimensionTabTitle () {
      return this.$t('tabDimension', {selected: this.selectedObj.dimension_selectedLen, total: this.dialogInfo.dimension_recommendations.length})
    }

    get measureTabTitle () {
      return this.$t('tabMeasure', {selected: this.selectedObj.measure_selectedLen, total: this.dialogInfo.measure_recommendations.length})
    }

    get computedColumnTabTitle () {
      return this.$t('tabCC', {selected: this.selectedObj.cc_selectedLen, total: this.dialogInfo.cc_recommendations.length})
    }

    // get aggIndexTabTitle () {
    //   return {selected: this.selectedObj.agg_index_selectedLen, total: this.dialogInfo.agg_index_recommendations.length}
    // }

    // get tableIndexTabTitle () {
    //   return {selected: this.selectedObj.table_index_selectedLen, total: this.dialogInfo.table_index_recommendations.length}
    // }

    get indexTabTitle () {
      return this.$t('index', {selected: this.selectedObj.index_selectedLen, total: this.dialogInfo.index_recommendations.length})
    }

    // 控制提交按钮是否可点 - 有一条建议选中就可以点了
    get canSubmitRecom () {
      // 提交按钮必须是数据已经加载进来，并且有选择的内容
      return this.dialogInfoLoaded && (this.selectedObj.dimension_selectedLen || this.selectedObj.measure_selectedLen || this.selectedObj.cc_selectedLen || this.selectedObj.index_selectedLen)
    }

    // 控制清空按钮是否可点 - 有一条建议就可以点了
    get canClearRecom () {
      return this.dialogInfo.dimension_recommendations.length || this.dialogInfo.measure_recommendations.length || this.dialogInfo.cc_recommendations.length || this.dialogInfo.index_recommendations.length
    }

    // 各tab 中table 选中变更，tab 上的数字要跟着变
    refreshSelected (data) {
      if (data.selectType === 'all') { // 全选
        this.dialogInfo[data.type + '_recommendations'].forEach((item) => {
          item.isSelected = true
        })
      } else if (data.selectType === 'null') { // 全不选
        this.dialogInfo[data.type + '_recommendations'].forEach((item) => {
          item.isSelected = false
        })
      } else { // 某个选或不选
        this.dialogInfo[data.type + '_recommendations'].forEach((item) => {
          if (item.item_id === data.data.item_id) {
            item.isSelected = data.data.isSelected
          }
        })
      }
      this.selectedObj[data.type + '_selectedLen'] = this.getSelectLen(data.type)
    }

    // 获取选中的优化建议，按tab字段分类
    getSelectLen (type) {
      let temp = this.dialogInfo[type + '_recommendations'].filter((item) => {
        return item.isSelected
      })
      return temp.length
    }

    // 初始化弹窗上每个 tab 需要的信息，以及 tab 上的选中数据的个数
    initDialogInfo (data) {
      // 每次打开弹窗将所有的选项都设为选中
      if (!data) return
      let tempData = objectClone(data)
      for (var prop in tempData) {
        if (prop === 'dimension_recommendations' || prop === 'measure_recommendations' || prop === 'cc_recommendations' || prop === 'index_recommendations') {
          tempData[prop].forEach((item, i) => {
            // 选中状态的初始化
            item.isSelected = true
            // 名字校验用到的字段初始化
            item.validateNameRule = false
            item.validateSameName = false
            if (prop === 'index_recommendations') {
              item.contentShow = []
            }
          })
        }
      }
      // 所有的数据格式要在传入prop之前进行处理
      this.dialogInfo = tempData || {
        dimension_recommendations: [],
        measure_recommendations: [],
        cc_recommendations: [],
        agg_index_recommendations: [],
        table_index_recommendations: []
      }
      // 初始化数字
      this.selectedObj['dimension_selectedLen'] = this.getSelectLen('dimension')
      this.selectedObj['measure_selectedLen'] = this.getSelectLen('measure')
      this.selectedObj['cc_selectedLen'] = this.getSelectLen('cc')
      // this.selectedObj['agg_index_selectedLen'] = this.getSelectLen('agg_index')
      this.selectedObj['index_selectedLen'] = this.getSelectLen('index')
    }

    // 获取当前 model 的优化建议数据
    async loadModalInfo () {
      try {
        this.dialogInfoLoaded = false
        let params = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid
        }
        const response = await this.loadModelRecommendations(params)
        const result = await handleSuccessAsync(response)
        this.dialogInfoLoaded = true
        this.initDialogInfo(result)
      } catch (e) {
        this.dialogInfoLoaded = true
        this.initDialogInfo()
        handleError(e)
      }
    }

    closeModal (isSubmit) {
      this.hideModal()
      this.btnLoading = false
      setTimeout(() => {
        this.callback && this.callback({
          isSubmit: isSubmit
        })
        // 重置弹窗上相关信息
        this.resetModalForm()
      }, 200)
    }

    // 清除建议（全量）
    clearRecommendations () {
      this.clearBtnLoading = true
      let params = {
        project: this.currentSelectedProject,
        model: this.modelDesc.uuid
      }
      for (var prop in this.dialogInfo) {
        if (prop === 'dimension_recommendations' || prop === 'measure_recommendations' || prop === 'cc_recommendations' || prop === 'index_recommendations') {
          let temp = this.dialogInfo[prop].map((item) => {
            return item.item_id
          })
          params[prop] = temp.length > 0 ? temp.join(',') : ''
        }
      }
      // 需要二次确认
      this.$confirm(this.$t('confirmClearRecomm'), this.$t('kylinLang.common.notice'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(async () => {
        this.clearModelRecommendations(params).then((res) => {
          this.clearBtnLoading = false
          kapMessage(this.$t('kylinLang.common.submitSuccess'))
          this.closeModal(true)
        }, (res) => {
          this.clearBtnLoading = false
          res && handleError(res)
        })
      }).catch(() => {
        this.clearBtnLoading = false
      })
    }

    dealParams (prop) {
      let params = this.dialogInfo[prop].filter(item => item.isSelected)
      let temp = objectClone(params) // 直接用params 去delete 属性，会导致 dialogInfo 的属性也跟着变
      temp.forEach((item) => {
        delete item.validateNameRule
        delete item.validateSameName
        delete item.isSelected
      })
      if (prop === 'index_recommendations') {
        return params.map(item => item.item_id)
      }
      return temp
    }

    // 提交建议
    submit () {
      // 要验证几个表单是否通过校验
      if (this.$refs['dimensionRecom'] && !this.$refs['dimensionRecom'].dimensionValidPass) {
        return false
      }
      if (this.$refs['measureRecom'] && !this.$refs['measureRecom'].measureValidPass) {
        return false
      }
      if (this.$refs['ccRecom'] && !this.$refs['ccRecom'].ccValidPass) {
        return false
      }
      this.btnLoading = true
      let params = {
        project: this.dialogInfo.project,
        model: this.dialogInfo.modelId,
        dimension_recommendations: this.dealParams('dimension_recommendations'),
        measure_recommendations: this.dealParams('measure_recommendations'),
        cc_recommendations: this.dealParams('cc_recommendations'),
        index_recommendation_item_ids: this.dealParams('index_recommendations')
      }
      this.saveModelRecommendations(params).then((res) => {
        this.btnLoading = false
        kapMessage(this.$t('kylinLang.common.submitSuccess'))
        this.closeModal(true)
      }, (res) => {
        this.btnLoading = false
        res && handleError(res)
      })
    }
  }
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .recommendations-dialog {
    .recommendations{
      min-height: 172px;
      position: relative;
      .clearBtn{
        position: absolute;
        right:0px;
        z-index: 3;
        top: 4px;
      }
    }
  } 
</style>
