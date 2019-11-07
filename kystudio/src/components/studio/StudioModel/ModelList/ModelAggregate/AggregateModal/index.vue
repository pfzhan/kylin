<template>
  <el-dialog class="aggregate-modal" limited-area width="960px"
    :visible="isShow"
    top="5vh"
    :append-to-body="true"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="isShow && handleClose(false)">
    <span class="el-dialog__title" slot="title">{{$t(modalTitle)}}
      <!-- 超出上限的情况 -->
      <span class="cuboid-error" v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationTotalNum', {num: maxCombinationNum, numTotal: maxCombinationNum * 10})"><i class="el-icon-ksd-what ksd-ml-5"></i></el-tooltip> )</span></span>
      <!-- 数字的情况 -->
      <span v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'number'"><span class="cuboid-result">({{$t('numTitle', {num: cuboidsInfo.total_count.result})}})</span></span>
      <!-- 正在检测的情况 -->
      <i v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'loading'" class="el-icon-loading"></i>
    </span>
    <div class="loading" v-if="isLoading" v-loading="isLoading"></div>
    <template v-if="model">
      <!-- 维度列表展示 -->
      <!-- <div class="dimension-list clearfix" v-if="isDimensionShow">
        <div class="dimension disable" v-for="dimension in dimensions" :key="dimension.value">
          {{dimension.label}}
        </div>
      </div> -->
      <!-- 维度按钮 -->
      <!-- <div class="dimension-buttons">
        <el-button @click="toggleDimensionShow">
          {{$t(isDimensionShow ? 'hideDimensions' : 'showDimensions')}}
          <i class="el-icon-ksd-more_01" :class="{ 'less': isDimensionShow }"></i>
        </el-button>
      </div> -->
      <!-- 聚合组按钮 -->
      <el-alert
        class="ksd-pt-0 ksd-pb-15"
        :title="$t('aggRuleTip')"
        type="info"
        :show-background="false"
        :closable="false"
        show-icon>
      </el-alert>
      <div class="aggregate-buttons ksd-mb-15">
        <el-button type="primary" icon="el-icon-ksd-add_2" @click="handleAddAggregate">{{$t('addAggregateGroup')}}</el-button>
      </div>
      <!-- 聚合组表单 -->
      <div class="aggregate-group" v-for="(aggregate, aggregateIdx) in form.aggregateArray" :key="aggregateIdx" :class="{'js_exceedLimit': !isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregateIdx]) === 'overLimit'}">
        <h1 class="title font-medium">{{$t('aggregateGroupTitle', { id: form.aggregateArray.length - aggregateIdx })}} 
          <!-- 超出上限的情况 -->
          <span class="cuboid-error" v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregateIdx]) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationNum', {num: maxCombinationNum})"><i class="el-icon-ksd-what ksd-ml-5"></i></el-tooltip> )</span></span>
          <!-- 数字的情况 -->
          <span v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregateIdx]) === 'number'"><span class="cuboid-result">({{$t('numTitle', {num: cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregateIdx] && cuboidsInfo.agg_index_counts[aggregateIdx].result})}})</span></span>
          <!-- 正在检测的情况 -->
          <i v-if="!isWaitingCheckCuboids && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregateIdx]) === 'loading'" class="el-icon-loading"></i>
        </h1>
        <div class="actions">
          <el-button size="mini" @click="() => handleCopyAggregate(aggregateIdx)">{{$t('kylinLang.common.copy')}}</el-button>
          <el-button size="mini" @click="() => handleDeleteAggregate(aggregateIdx, form.aggregateArray.length - aggregateIdx)">{{$t('kylinLang.common.delete')}}</el-button>
        </div>
        <div class="body">
          <!-- Include聚合组 -->
          <div class="row ksd-mb-15">
            <el-button plain size="mini" type="primary" v-guide.selectAllIncludesBtn @click="handleAddAllIncludes(aggregateIdx)">{{$t('selectAll')}}</el-button>
            <el-button size="mini" @click="handleRemoveAllIncludes(aggregateIdx, form.aggregateArray.length - aggregateIdx)">{{$t('cancelAll')}}</el-button>
          </div>
          <div class="row">
            <h2 class="title font-medium">{{$t('include')}}</h2>
            <el-select
              v-guide.aggIncludes
              multiple
              filterable
              class="mul-filter-select"
              :class="{'reset-padding': aggregate.includes.length}"
              :ref="`aggregate.include.${aggregateIdx}`"
              :value="aggregate.includes"
              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
              @input="value => handleInput(`aggregateArray.${aggregateIdx}.includes`, value)"
              @remove-tag="value => handleRemoveIncludeRules(value, aggregateIdx)">
              <i slot="prefix" v-show="!aggregate.includes.length" class="el-input__icon el-icon-search"></i>
              <el-option
                v-for="dimension in dimensions"
                :key="dimension.value"
                :label="dimension.label"
                :value="dimension.value">
              </el-option>
            </el-select>
          </div>
          <!-- Mandatory聚合组 -->
          <div class="row mandatory">
            <h2 class="title font-medium">{{$t('mandatory')}}</h2>
            <el-select
              v-guide.aggMandatory
              multiple
              filterable
              class="mul-filter-select"
              :class="{'reset-padding': aggregate.mandatory.length}"
              :value="aggregate.mandatory"
              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
              @change="value => handleInput(`aggregateArray.${aggregateIdx}.mandatory`, value)">
              <i slot="prefix" v-show="!aggregate.mandatory.length" class="el-input__icon el-icon-search"></i>
              <el-option
                v-for="dimension in getUnusedDimensions(aggregateIdx)"
                :key="dimension.value"
                :label="dimension.label"
                :value="dimension.value">
              </el-option>
            </el-select>
          </div>
          <!-- Hierarchy聚合组 -->
          <div class="row hierarchy">
            <h2 class="title font-medium">{{$t('hierarchy')}}</h2>
            <div class="list"
              v-for="(hierarchy, hierarchyRowIdx) in aggregate.hierarchyArray"
              :key="`hierarchy-${hierarchyRowIdx}`">
              <el-select
                v-guide.aggHierarchy
                multiple
                filterable
                class="mul-filter-select"
                :class="{'reset-padding': hierarchy.items.length}"
                :value="hierarchy.items"
                :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`, value)">
                <i slot="prefix" v-show="!hierarchy.items.length" class="el-input__icon el-icon-search"></i>
                <el-option
                  v-for="dimension in getUnusedDimensions(aggregateIdx)"
                  :key="dimension.value"
                  :label="dimension.label"
                  :value="dimension.value">
                </el-option>
              </el-select>
              <div class="list-actions clearfix">
                <el-button circle plain type="primary" size="mini" icon="el-icon-ksd-add_2"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`)">
                </el-button>
                <el-button circle class="delete" size="mini" icon="el-icon-minus"
                  :disabled="aggregate.hierarchyArray.length === 1"
                  @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregateIdx, hierarchyRowIdx)">
                </el-button>
              </div>
            </div>
          </div>
          <!-- Joint聚合组 -->
          <div class="row joint">
            <h2 class="title font-medium">{{$t('joint')}}</h2>
            <div class="list"
              v-for="(joint, jointRowIdx) in aggregate.jointArray"
              :key="`joint-${jointRowIdx}`">
              <el-select
                v-guide.joint
                multiple
                filterable
                class="mul-filter-select"
                :class="{'reset-padding': joint.items.length}"
                :value="joint.items"
                :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`, value)">
                <i slot="prefix" v-show="!joint.items.length" class="el-input__icon el-icon-search"></i>
                <el-option
                  v-for="dimension in getUnusedDimensions(aggregateIdx)"
                  :key="dimension.value"
                  :label="dimension.label"
                  :value="dimension.value">
                </el-option>
              </el-select>
              <div class="list-actions clearfix ksd-no-br-space">
                <el-button circle plain type="primary" size="mini" icon="el-icon-ksd-add_2"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`)">
                </el-button>
                <el-button circle class="delete" size="mini" icon="el-icon-minus"
                  :disabled="aggregate.jointArray.length === 1"
                  @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregateIdx, jointRowIdx)">
                </el-button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer clearfix">
      <div class="left">
        <el-checkbox v-guide.aggCatchUp :value="form.isCatchUp" @input="value => handleInput('isCatchUp', value)">{{$t('kylinLang.common.catchUp')}}</el-checkbox>
      </div>
      <div class="right">
        <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" :disabled="isDisabledSaveBtn" v-if="isShow" plain type="primary" @click="checkCuboids()" :loading="calcLoading">{{$t('checkIndexAmount')}}<el-tooltip placement="top" :content="$t('checkIndexAmountBtnTips')"><i class="plainWhat el-icon-ksd-what ksd-ml-5"></i></el-tooltip></el-button>
        <el-button size="medium" :disabled="isDisabledSaveBtn" v-if="isShow" v-guide.saveAggBtn plain type="primary" :loading="isSubmit" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../../../store'
import locales from './locales'
import { BuildIndexStatus } from 'config/model'
import store, { types, initialAggregateData } from './store'
import { titleMaps, editTypes, getPlaintDimensions, findIncludeDimension } from './handler'
import { handleError, get, set, push, kapConfirm, handleSuccessAsync } from '../../../../../../util'
import { handleSuccess } from 'util/business'

const { EDIT } = editTypes

vuex.registerModule(['modals', 'AggregateModal'], store)

@Component({
  computed: {
    ...mapState('AggregateModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      isLoading: state => state.isLoading,
      editType: state => state.editType,
      callback: state => state.callback,
      model: state => state.model,
      projectName: state => state.projectName
    }),
    ...mapGetters('AggregateModal', [
      'dimensions',
      'dimensionIdMapping'
    ])
  },
  methods: {
    ...mapMutations('AggregateModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapActions({
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS',
      getCalcCuboids: 'GET_AGG_CUBOIDS'
    })
  },
  locales
})
export default class AggregateModal extends Vue {
  isFormShow = false
  isDimensionShow = false
  isSubmit = false
  isWaitingCheckCuboids = true
  maxCombinationNum = 0
  get modalTitle () {
    return titleMaps[this.editType]
  }
  get isFormVaild () {
    const { aggregateArray } = this.form
    for (const { includes } of aggregateArray) {
      if (!includes.length) {
        return false
      }
    }

    return true
  }
  get usedDimensions () {
    const dimensionMaps = {}
    this.form.aggregateArray.forEach(aggregate => {
      aggregate.includes.forEach(include => {
        dimensionMaps[include] = 1
      })
    })
    return Object.keys(dimensionMaps)
  }
  getUnusedDimensions (aggregateIdx) {
    const aggregate = this.form.aggregateArray[aggregateIdx]
    const includes = aggregate.includes
    const mandatory = aggregate.mandatory
    const hierarchy = getPlaintDimensions(aggregate.hierarchyArray)
    const joint = getPlaintDimensions(aggregate.jointArray)

    return this.dimensions.filter(dimension => {
      return includes.includes(dimension.value) && !(
        hierarchy.includes(dimension.value) ||
        mandatory.includes(dimension.value) ||
        joint.includes(dimension.value)
      )
    })
  }
  cuboidsInfo = {
    total_count: {},
    agg_index_counts: []
  }
  resetCuboidInfo () {
    this.cuboidsInfo = {
      total_count: {},
      agg_index_counts: []
    }
  }
  ST = null
  calcLoading = false
  calcCuboids () {
    // 防重复提交
    if (this.calcLoading) {
      return false
    }
    this.calcLoading = true
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      let data = this.getSubmitData()
      if (data.dimensions.length <= 0) {
        this.calcLoading = false
        this.resetCuboidInfo()
        return
      }
      delete data.dimensions // 后台处理规整顺序
      this.getCalcCuboids(data).then((res) => {
        this.isWaitingCheckCuboids = false
        handleSuccess(res, (data) => {
          if (data) {
            this.cuboidsInfo = data
            this.cuboidsInfo.agg_index_counts = data.agg_index_counts.reverse()
            this.maxCombinationNum = data.max_combination_num
            let singleIsLimit = data.agg_index_counts.filter((item) => {
              return !/^\d+$/.test(item.result)
            })
            // 单个索引组的个数超了限制，显示报错，并不往下执行了
            if (singleIsLimit.length > 0) {
              this.$message.error(this.$t('maxCombinationTip'))
              // 操作滚动
              this.dealScrollToFirstError()
            }
          }
          this.calcLoading = false
        })
      }, (res) => {
        this.maxCombinationNum = 0
        // 获取个数失败，文案应该处于待检状态
        this.isWaitingCheckCuboids = true
        this.resetCuboidInfo()
        this.calcLoading = false
        handleError(res)
      })
    }, 1000)
  }
  get isDisabledSaveBtn () {
    // 正在计算的时候按钮disable，选的维度有空的时候，disable，聚合组数为0 时
    return this.calcLoading || this.isSubmit || !this.isFormVaild || !this.form.aggregateArray || this.form.aggregateArray.length === 0
  }
  renderCoboidTextCheck (cuboidsInfo) {
    let cuboidText = ''
    if (this.isWaitingCheckCuboids) {
      cuboidText = ''
    }
    if (!(cuboidsInfo && cuboidsInfo.result !== undefined)) {
      cuboidText = ''
    } else {
      if (cuboidsInfo.status !== 'SUCCESS') {
        cuboidText = 'cuboid-error'
      }
      if (this.calcLoading) {
        cuboidText = 'loading'
      } else {
        if (!/^\d+$/.test(cuboidsInfo.result)) {
          cuboidText = 'overLimit'
        } else {
          cuboidText = 'number'
        }
      }
    }
    return cuboidText
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.resetCuboidInfo()
      // 弹窗打开时，默认计算一次 cuboids 数量
      this.calcCuboids()
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }
  toggleDimensionShow () {
    this.isDimensionShow = !this.isDimensionShow
  }
  toggleIncludeDimension (aggregateIdx, dimensionValue, isUsed) {
    const includeSelectRefId = `aggregate.include.${aggregateIdx}`
    const includeSelectEl = this.$refs[includeSelectRefId][0].$el
    const includeOptEls = includeSelectEl.querySelectorAll('.el-select__tags span .el-tag')

    let targetDimensionEl = findIncludeDimension(includeOptEls, dimensionValue)
    targetDimensionEl && targetDimensionEl.setAttribute('data-tag', isUsed ? 'used' : '')
  }
  handleAddAggregate () {
    const aggregateArray = get(this.form, 'aggregateArray')
    const aggregateData = {
      ...JSON.parse(initialAggregateData),
      id: aggregateArray.length
    }
    this.setModalForm({ aggregateArray: [ aggregateData, ...aggregateArray ] })
    this.isWaitingCheckCuboids = true
    // this.calcCuboids()
  }
  handleCopyAggregate (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    const copyedAggregate = {
      ...aggregateArray[aggregateIdx],
      id: aggregateArray.length
    }
    this.setModalForm({ aggregateArray: [copyedAggregate, ...aggregateArray] })
    this.isWaitingCheckCuboids = true
    // this.calcCuboids()
  }
  handleDeleteAggregate (aggregateIdx, titleId) {
    kapConfirm(this.$t('delAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('delAggregateTitle')).then(() => {
      const aggregateArray = get(this.form, 'aggregateArray')
      aggregateArray.splice(aggregateIdx, 1)
      this.setModalForm({ aggregateArray })
      this.isWaitingCheckCuboids = true
      // this.calcCuboids()
    })
  }
  handleAddDimensionRow (path) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    const newId = dimensionRows.length
    const newDimensionRow = { id: newId, items: [] }
    this.setModalForm({[rootKey]: push(this.form, path, newDimensionRow)[rootKey]})
    this.isWaitingCheckCuboids = true
    // this.calcCuboids()
  }
  handleRemoveDimensionRow (path, aggregateIdx, dimensionRowIndex) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    if (dimensionRows.length > 1) {
      dimensionRows.splice(dimensionRowIndex, 1)[0]
      this.setModalForm({[rootKey]: set(this.form, path, dimensionRows)[rootKey]})
    }
    this.isWaitingCheckCuboids = true
    // this.calcCuboids()
  }
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  handleInput (key, value) {
    if (key !== 'isCatchUp') {
      this.isWaitingCheckCuboids = true
      // this.calcCuboids()
    }
    const rootKey = key.split('.')[0]
    this.setModalForm({[rootKey]: set(this.form, key, value)[rootKey]})
  }
  handleRemoveIncludeRules (removedValue, aggregateIdx) {
    const { aggregateArray = [] } = this.form
    const { mandatory, hierarchyArray, jointArray } = aggregateArray[aggregateIdx]

    if (mandatory.includes(removedValue)) {
      const mandatoryKey = `aggregateArray.${aggregateIdx}.mandatory`
      this.handleInput(mandatoryKey, mandatory.filter(item => item !== removedValue))
    }
    hierarchyArray.forEach((hierarchyGroup, hierarchyRowIdx) => {
      const hierarchy = hierarchyGroup.items
      if (hierarchy.includes(removedValue)) {
        const hierarchyKey = `aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`
        this.handleInput(hierarchyKey, hierarchy.filter(item => item !== removedValue))
      }
    })
    jointArray.forEach((jointGroup, jointRowIdx) => {
      const joint = jointGroup.items
      if (joint.includes(removedValue)) {
        const jointKey = `aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`
        this.handleInput(jointKey, joint.filter(item => item !== removedValue))
      }
    })
  }
  handleAddAllIncludes (aggregateIdx) {
    const allDimensions = this.dimensions.map(dimension => dimension.label)
    this.handleInput(`aggregateArray.${aggregateIdx}.includes`, allDimensions)
  }
  handleRemoveAllIncludes (aggregateIdx, titleId) {
    kapConfirm(this.$t('clearAllAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('clearAggregateTitle')).then(() => {
      const { aggregateArray = [] } = this.form
      const currentAggregate = aggregateArray[aggregateIdx] || {}
      const currentIncludes = currentAggregate.includes || []

      for (const include of currentIncludes) {
        this.handleRemoveIncludeRules(include, aggregateIdx)
      }
      this.handleInput(`aggregateArray.${aggregateIdx}.includes`, [])
    })
  }
  handleBuildIndexTip (data) {
    let tipMsg = this.$t('kylinLang.model.saveIndexSuccess', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
    if (this.form.isCatchUp) {
      if (data.type === BuildIndexStatus.NORM_BUILD) {
        tipMsg += ' ' + this.$t('kylinLang.model.buildIndexSuccess1')
        this.$message({message: tipMsg, type: 'success'})
        return
      }
      if (data.type === BuildIndexStatus.NO_LAYOUT) {
        tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
        this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
      } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
        tipMsg += '<br/>' + this.$t('kylinLang.model.buildIndexFail1', {modelName: this.model.name})
        this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'success', dangerouslyUseHTMLString: true})
      }
    } else {
      this.$message({message: tipMsg, type: 'success'})
    }
  }
  checkCuboids () {
    if (this.checkFormVaild()) {
      this.calcCuboids()
    }
  }
  dealScrollToFirstError () {
    this.$nextTick(() => {
      // 第一个数量超过的元素
      if (document.querySelector('.js_exceedLimit')) {
        let firstErrorDomTop = document.querySelector('.js_exceedLimit').offsetTop - 100
        document.querySelector('.aggregate-modal').querySelector('.el-dialog__body').scrollTop = firstErrorDomTop
      }
    })
  }
  async handleSubmit () {
    this.isSubmit = true
    try {
      if (this.checkFormVaild()) {
        const data = this.getSubmitData()
        delete data.dimensions // 后台处理规整顺序
        // 发一个获取数据的接口
        this.isWaitingCheckCuboids = true
        let cuboidsRes = await this.getCalcCuboids(data)
        let cuboidsResult = await handleSuccessAsync(cuboidsRes)
        this.isWaitingCheckCuboids = false
        if (cuboidsResult) {
          this.maxCombinationNum = cuboidsResult.max_combination_num
          this.cuboidsInfo = cuboidsResult
          this.cuboidsInfo.agg_index_counts = cuboidsResult.agg_index_counts.reverse()
          let singleIsLimit = cuboidsResult.agg_index_counts.filter((item) => {
            return !/^\d+$/.test(item.result)
          })
          // 单个索引组的个数超了限制，显示报错，并不往下执行了
          if (singleIsLimit.length > 0) {
            this.$message.error(this.$t('maxCombinationTip'))
            this.calcLoading = false
            this.isSubmit = false
            // 操作滚动
            this.dealScrollToFirstError()
            return false
          } else {
            // 单个没超过，总量超了，显示总量的报错，也不往下执行了
            if (!/^\d+$/.test(cuboidsResult.total_count.result)) {
              this.$message.error(this.$t('maxTotalCombinationTip'))
              this.calcLoading = false
              this.isSubmit = false
              return false
            }
          }
        }
        // 获取数字正常的情况下，才进行 submit
        let res = await this.submit(data)
        let result = await handleSuccessAsync(res)
        this.handleBuildIndexTip(result)
        this.isSubmit = false
        this.handleClose(true)
      } else {
        this.isSubmit = false
      }
    } catch (e) {
      this.calcLoading = false
      this.isWaitingCheckCuboids = false
      e && handleError(e)
      this.isSubmit = false
    }
  }
  checkFormVaild () {
    if (!this.isFormVaild) {
      this.$message(this.$t('includesEmpty'))
      return false
    } else if (this.usedDimensions.length >= 62) {
      this.$message(this.$t('tooManyDimensions'))
      return false
    }
    return this.isFormVaild
  }
  submit (data) {
    switch (this.editType) {
      case EDIT:
        return this.updateAggregateGroups(data)
    }
  }
  getSubmitData () {
    const { editType, form, usedDimensions, dimensionIdMapping, projectName, model } = this

    switch (editType) {
      case EDIT: {
        const { aggregateArray, isCatchUp } = form
        return {
          projectName,
          modelId: model.uuid,
          isCatchUp,
          dimensions: usedDimensions.map(dimensionItem => dimensionIdMapping[dimensionItem]),
          aggregationGroups: aggregateArray.map(aggregate => ({
            includes: aggregate.includes.map(includeItem => dimensionIdMapping[includeItem]),
            select_rule: {
              mandatory_dims: aggregate.mandatory.map(includeItem => dimensionIdMapping[includeItem]),
              hierarchy_dims: aggregate.hierarchyArray.map(hierarchys => {
                return hierarchys.items.map(hierarchyItem => dimensionIdMapping[hierarchyItem])
              }).filter(hierarchys => hierarchys.length),
              joint_dims: aggregate.jointArray.map(joints => {
                return joints.items.map(jointItem => dimensionIdMapping[jointItem])
              }).filter(joints => joints.length)
            }
          })).reverse()
        }
      }
    }
  }
  repaintDimensionSelector () {
    const { form = {} } = this
    const { aggregateArray = [] } = form

    aggregateArray.forEach((aggregateGroup, aggregateIdx) => {
      const { includes, mandatory, jointArray, hierarchyArray } = aggregateGroup
      const jointBundle = jointArray.reduce((result, jointGroup) => [...result, ...jointGroup.items], [])
      const hierarchyBundle = hierarchyArray.reduce((result, hierarchyGroup) => [...result, ...hierarchyGroup.items], [])
      const selectedItems = [ ...mandatory, ...jointBundle, ...hierarchyBundle ]

      includes.forEach((include) => {
        const isSelected = selectedItems.includes(include)
        this.toggleIncludeDimension(aggregateIdx, include, isSelected)
      })
    })
  }
  updated () {
    this.repaintDimensionSelector()
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';

.aggregate-modal {
  .dialog-footer{
    .el-button{
      .plainWhat{
        color:@base-color;
      }
      &:hover{
        .plainWhat{
          color:@fff;
        }
      }
      &.is-disabled{
        .plainWhat{
          color: #6bb8eb;
        }
        &:hover{
          .plainWhat{
            color: #6bb8eb;
          }
        }
      }
    }
  }
  .mul-filter-select {
    .el-select__tags {
      padding-left: 15px;
    }
    &.reset-padding {
      .el-select__tags {
        padding-left: 0px;
      }
    }
  }
  .cuboid-error {
    .cuboid-result {
      color:@error-color-1;
      font-weight: @font-regular;
      .el-icon-ksd-what{
        color:@error-color-1;
        font-weight: @font-regular;
      }
    }
  }
  .el-button + .el-button { margin-left: 3px;}
  .dimension {
    float: left;
    padding: 6px 13px;
    margin: 0 10px 10px 0;
  }
  .dimension.disable {
    background: @grey-4;
    border: 1px solid @text-secondary-color;
    border-radius: 2px;
  }
  .dimension-buttons {
    text-align: center;
    margin: 0 -20px;
    padding: 0px 0 20px 0;
    border-bottom: 1px solid @line-split-color;
  }
  .less {
    transform: rotate(180deg);
  }
  .aggregate-group {
    position: relative;
    &:not(:last-child) {
      margin-bottom: 25px;
    }
    .title {
      color: @text-title-color;
      margin-bottom: 7px;
    }
    .body {
      border: 1px solid @line-border-color;
      border-radius: 2px;
      padding: 15px;
    }
    .row {
      margin-bottom: 17px;
      &.joint {
        margin-bottom: 0;
      }
    }
    .actions {
      position: absolute;
      top: -3px;
      right: 0;
      font-size: 16px;
      .el-button+.el-button {
        margin-left: 5px;
      }
      i {
        color: @text-title-color;
      }
    }
  }
  h1 {
    font-size: 14px;
  }
  h2 {
    font-size: 14px;
  }
  .el-select {
    width: 100%;
  }
  .el-button {
    i[class^=el-icon-] {
      cursor: inherit;
    }
  }
  .list {
    padding-right: 68px;
    position: relative;
    &:not(:last-child) {
      margin-bottom: 10px;
    }
  }
  .list-actions {
    position: absolute;
    right: 6px;
    top: 0;
    transform: translateY(2px);
    .is-text {
      font-size: 24px;
      border: 0;
      padding: 4px 0px;
    }
    .el-button--medium {
      float: left;
    }
  }
  .mandatory,
  .hierarchy,
  .joint {
    .el-tag {
      background: @base-color;
      border: 1px solid @base-color;
      border-radius: 2px;
      color: @fff;
      .el-icon-close {
        color: @fff;
      }
      .el-icon-close:hover {
        background: @fff;
        color: @base-color;
      }
    }
  }
  .el-tag {
    background: @base-color-10;
    color: @base-color;
    border: 1px solid @base-color;
    &[data-tag='used'] {
      background: @base-color;
      border: 1px solid @base-color;
      border-radius: 2px;
      color: @fff;
      .el-icon-close {
        color: @fff;
      }
      .el-icon-close:hover {
        background: @fff;
        color: @base-color;
      }
    }
  }
  .left {
    float: left;
  }
  .right {
    float: right;
  }
  .dialog-footer {
    line-height: 32px;
  }
  .loading {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
  }
}
</style>
