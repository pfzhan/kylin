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
      <span class="cuboid-error" v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationTotalNum', {num: maxCombinationNum, numTotal: maxCombinationNum * 10 + 1})"><i class="el-icon-ksd-what ksd-ml-5"></i></el-tooltip> )</span></span>
      <!-- 数字的情况 -->
      <span v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'number'"><span class="cuboid-result">({{$t('numTitle', {num: cuboidsInfo.total_count.result})}})</span></span>
      <!-- 正在检测的情况 -->
      <i v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'loading'" class="el-icon-loading"></i>
    </span>
    <!-- <div class="loading" v-if="isLoading" v-loading="isLoading"></div> -->
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
      <div v-loading="calcLoading || isSubmit"
            element-loading-text=" "
            element-loading-spinner=" "
            element-loading-background="rgba(0, 0, 0, 0)">
        <el-alert
          class="ksd-pt-0 ksd-pb-15"
          :title="$t('aggGroupTip')"
          type="info"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
        <div class="aggregate-buttons ksd-mb-15">
          <el-button type="primary" icon="el-icon-ksd-add_2" @click="handleAddAggregate">{{$t('addAggregateGroup')}}</el-button>
        </div>
        <div class="content">
          <!-- 聚合组表单 -->
          <div class="aggregate-group" v-for="(aggregate, aggregateIdx) in form.aggregateArray" :key="aggregate.id" :class="{'js_exceedLimit': !isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit'}">
            <div class="header">
              <h1 class="title font-medium">{{$t('aggregateGroupTitle', { id: form.aggregateArray.length - aggregateIdx })}} 
                <!-- 超出上限的情况 -->
                <span class="cuboid-error" v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationNum', {num: maxCombinationNum})"><i class="el-icon-ksd-what ksd-ml-5"></i></el-tooltip> )</span></span>
                <!-- 数字的情况 -->
                <span v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'number'"><span class="cuboid-result">({{$t('numTitle', {num: cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id] && cuboidsInfo.agg_index_counts[aggregate.id].result})}})</span></span>
                <!-- 正在检测的情况 -->
                <i v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'loading'" class="el-icon-loading"></i>
              </h1>
              <div class="actions">
                <!-- <el-button type="mini" @click="() => openAggregateItem(aggregateIdx)">{{ aggregate.open ? $t('retract') : $t('open') }}</el-button> -->
                <el-button size="mini" @click="() => handleCopyAggregate(aggregateIdx)">{{$t('kylinLang.common.copy')}}</el-button>
                <el-button size="mini" @click="() => handleDeleteAggregate(aggregateIdx, form.aggregateArray.length - aggregateIdx)">{{$t('kylinLang.common.delete')}}</el-button>
              </div>
            </div>
            <div class="body" :class="{'overLimit': !isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit', 'open': aggregate.open}" :style="aggregateStyle[aggregateIdx] ? aggregateStyle[aggregateIdx] : (!aggregate.open && {'display': 'none'})">
              <div class="contain">
                <el-tabs v-model="aggregate.activeTab" type="card">
                  <el-tab-pane :label="$t(item.key, {size: aggregate[item.target].length, total: totalSize(item.name)})" :name="item.key" v-for="item in tabList" :key="item.key"></el-tab-pane>
                </el-tabs>
                <template v-if="aggregate.activeTab === 'dimension'">
                  <!-- Include聚合组 -->
                  <div class="row">
                    <h2 class="title font-medium">{{$t('include')}}</h2>
                    <div class="row ksd-mb-10">
                      <el-button plain size="mini" class="add-all-item" type="primary" v-guide.selectAllIncludesBtn @click="handleAddAllIncludes(aggregateIdx, aggregate.id)">{{$t('selectAllDimension')}}<el-tooltip class="item tip-item" effect="dark" :content="$t('dimensionTabTip')" placement="bottom"><i class="el-icon-ksd-what"></i></el-tooltip></el-button>
                      <el-button size="mini" @click="handleRemoveAllIncludes(aggregateIdx, form.aggregateArray.length - aggregateIdx, aggregate.id)">{{$t('clearAllDimension')}}</el-button>
                    </div>
                    <el-select
                      v-guide.aggIncludes
                      multiple
                      filterable
                      class="mul-filter-select"
                      :class="{'reset-padding': aggregate.includes.length}"
                      :ref="`aggregate.include.${aggregateIdx}`"
                      :value="aggregate.includes"
                      :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                      @input="value => handleInput(`aggregateArray.${aggregateIdx}.includes`, value, aggregate.id)"
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
                      @change="value => handleInput(`aggregateArray.${aggregateIdx}.mandatory`, value, aggregate.id)">
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
                        @change="value => handleInput(`aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`, value, aggregate.id)">
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
                          @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregate.id)">
                        </el-button>
                        <el-button circle class="delete" size="mini" icon="el-icon-minus"
                          :disabled="aggregate.hierarchyArray.length === 1"
                          @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregateIdx, hierarchyRowIdx, aggregate.id)">
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
                        @change="value => handleInput(`aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`, value, aggregate.id)">
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
                          @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregate.id)">
                        </el-button>
                        <el-button circle class="delete" size="mini" icon="el-icon-minus"
                          :disabled="aggregate.jointArray.length === 1"
                          @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregateIdx, jointRowIdx, aggregate.id)">
                        </el-button>
                      </div>
                    </div>
                  </div>
                </template>
                <template v-else>
                  <div class="row">
                    <h2 class="title font-medium">{{$t('includeMeasure')}}</h2>
                    <div class="row ksd-mb-10">
                      <el-button plain size="mini" class="add-all-item" type="primary" @click="handleAddAllMeasure(aggregateIdx, aggregate.id)">{{$t('selectAllMeasure')}}<el-tooltip class="item tip-item" popper-class='aggregate-tip' effect="dark" :content="$t('measureTabTip')" placement="bottom"><i class="el-icon-ksd-what"></i></el-tooltip></el-button>
                      <el-button size="mini" @click="handleRemoveAllMeasure(aggregateIdx, form.aggregateArray.length - aggregateIdx, aggregate.id)">{{$t('clearAllMeasures')}}</el-button>
                    </div>
                    <el-select
                      multiple
                      filterable
                      class="mul-filter-select"
                      :class="{'reset-padding': aggregate.measures.length}"
                      :ref="`aggregate.measures.${aggregateIdx}`"
                      :value="aggregate.measures"
                      :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                      @input="value => handleInput(`aggregateArray.${aggregateIdx}.measures`, value, aggregate.id)"
                      @remove-tag="value => handleRemoveMeasureRules(value, aggregateIdx, aggregate.id)">
                      <i slot="prefix" v-show="!aggregate.measures.length" class="el-input__icon el-icon-search"></i>
                      <el-option
                        v-for="measure in measures"
                        :key="measure.value"
                        :label="measure.label"
                        :disabled="measure.value === 'COUNT_ALL'"
                        :value="measure.value">
                      </el-option>
                    </el-select>
                  </div>
                </template>
              </div>
            </div>
            <div class="open-aggregate-group" @click="openAggregateItem(aggregateIdx)"><i :class="['el-icon-d-arrow-left', {'open': aggregate.open}]"></i></div>
          </div>
        </div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer clearfix">
      <div class="left">
        <el-checkbox v-guide.aggCatchUp :value="form.isCatchUp" @input="value => handleInput('isCatchUp', value)">{{$t('kylinLang.common.catchUp')}}</el-checkbox>
      </div>
      <div class="right">
        <el-button plain size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" :disabled="isDisabledSaveBtn" v-if="isShow" @click="checkCuboids()" :loading="calcLoading">{{$t('checkIndexAmount')}}<el-tooltip placement="top" :content="$t('checkIndexAmountBtnTips')"><i class="plainWhat el-icon-ksd-what ksd-ml-5"></i></el-tooltip></el-button>
        <el-button size="medium" :disabled="isDisabledSaveBtn" v-if="isShow" v-guide.saveAggBtn :loading="isSubmit" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
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
import { handleError, get, set, push, kapConfirm, handleSuccessAsync, sampleGuid, objectClone } from '../../../../../../util'
import { handleSuccess } from 'util/business'

const { EDIT } = editTypes

vuex.registerModule(['modals', 'AggregateModal'], store)

@Component({
  computed: {
    ...mapState('AggregateModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      // isLoading: state => state.isLoading,
      editType: state => state.editType,
      callback: state => state.callback,
      model: state => state.model,
      projectName: state => state.projectName,
      formDataLoaded: state => state.formDataLoaded
    }),
    ...mapGetters('AggregateModal', [
      'dimensions',
      'dimensionIdMapping',
      'measures',
      'measureIdMapping'
    ])
  },
  methods: {
    ...mapMutations('AggregateModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM,
      showLoading: types.SHOW_LOADING,
      hideLoading: types.HIDE_LOADING
    }),
    ...mapActions({
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS',
      getCalcCuboids: 'GET_AGG_CUBOIDS',
      getIndexDiff: 'GET_INDEX_DIFF'
    })
  },
  locales
})
export default class AggregateModal extends Vue {
  isFormShow = false
  isDimensionShow = false
  isSubmit = false
  isWaitingCheckCuboids = {}
  isWaitingCheckAllCuboids = true
  maxCombinationNum = 0
  tabList = [
    {name: 'dimensions', key: 'dimension', size: 0, total: 0, target: 'includes'},
    {name: 'measures', key: 'measure', size: 0, total: 0, target: 'measures'}
  ]
  aggregateStyle = []

  get modalTitle () {
    return titleMaps[this.editType]
  }
  get isFormVaild () {
    const { aggregateArray } = this.form
    for (const { includes, measures } of aggregateArray) {
      if (!includes.length || !measures.length) return false
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
  totalSize (name) {
    return this[name].length
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
    agg_index_counts: {}
  }
  resetCuboidInfo () {
    this.cuboidsInfo = {
      total_count: {},
      agg_index_counts: {}
    }
  }
  calcLoading = false
  calcCuboids () {
    // 防重复提交
    if (this.calcLoading) {
      return false
    }
    // 只要点击了计算，全局的那个待检标志就要置回 false
    this.isWaitingCheckAllCuboids = false
    // 所有 group 中的待检都变成 false
    for (let prop in this.isWaitingCheckCuboids) {
      this.isWaitingCheckCuboids[prop] = false
    }
    this.calcLoading = true
    // this.showLoading()
    let paramsData = this.getSubmitData()
    if (paramsData.dimensions.length <= 0) {
      this.calcLoading = false
      this.resetCuboidInfo()
      return
    }
    delete paramsData.dimensions // 后台处理规整顺序
    this.getCalcCuboids(paramsData).then((res) => {
      handleSuccess(res, (data) => {
        if (data) {
          this.cuboidsInfo = objectClone(data)
          // this.cuboidsInfo.agg_index_counts = data.agg_index_counts.reverse()
          let resultData = objectClone(data.agg_index_counts.reverse())
          // 用聚合组的唯一id 做标识
          this.cuboidsInfo.agg_index_counts = {}
          for (let i = 0; i < this.form.aggregateArray.length; i++) {
            let id = this.form.aggregateArray[i].id
            this.cuboidsInfo.agg_index_counts[id] = objectClone(resultData[i])
          }
          this.maxCombinationNum = data.max_combination_num
          let singleIsLimit = data.agg_index_counts.filter((item) => {
            return !/^\d+$/.test(item.result)
          })
          // 单个索引组的个数超了限制，显示报错，并不往下执行了
          if (singleIsLimit.length > 0) {
            this.$message.error(this.$t('maxCombinationTip'))
            // 操作滚动
            this.dealScrollToFirstError()
          } else {
            // 单个没超过，总量超了，显示总量的报错，也不往下执行了
            if (!/^\d+$/.test(data.total_count.result)) {
              this.$message.error(this.$t('maxTotalCombinationTip'))
            }
          }
        }
        this.calcLoading = false
        // this.hideLoading()
      })
    }, (res) => {
      this.maxCombinationNum = 0
      // 获取个数失败，全局所有的文案应该处于待检状态
      for (let i = 0; i < this.form.aggregateArray.length; i++) {
        let id = this.form.aggregateArray[i].id
        this.isWaitingCheckCuboids[id] = true
      }
      this.resetCuboidInfo()
      this.calcLoading = false
      // this.hideLoading()
      handleError(res)
    })
  }
  get isDisabledSaveBtn () {
    // 正在计算的时候按钮disable，选的维度有空的时候，disable，聚合组数为0 时
    return this.calcLoading || this.isSubmit || !this.isFormVaild || !this.form.aggregateArray || this.form.aggregateArray.length === 0
  }
  renderCoboidTextCheck (cuboidsInfo, id) {
    let cuboidText = ''
    if (this.isWaitingCheckCuboids[id]) { // 如果是待检，直接显示空
      cuboidText = ''
    } else {
      if (this.calcLoading) {
        cuboidText = 'loading'
      } else {
        if (cuboidsInfo && cuboidsInfo.result !== undefined) {
          if (cuboidsInfo.status !== 'SUCCESS') {
            cuboidText = 'cuboid-error'
          }
          if (!/^\d+$/.test(cuboidsInfo.result)) {
            cuboidText = 'overLimit'
          } else {
            cuboidText = 'number'
          }
        } else { // 如果 cuboidsInfo 没数据，就判断是否是在检测中
          cuboidText = ''
        }
      }
    }
    return cuboidText
  }
  @Watch('formDataLoaded')
  onFormDataLoaded (newVal, oldVal) {
    // 打开弹窗时，会有一个请求rule的接口，要等接口回来，再发calcCuboids的数目检测接口
    if (newVal) {
      for (let i = 0; i < this.form.aggregateArray.length; i++) {
        let id = this.form.aggregateArray[i].id
        this.isWaitingCheckCuboids[id] = true
      }
      this.calcCuboids()
    }
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.resetCuboidInfo()
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
    if (!this.$refs[includeSelectRefId] || !this.$refs[includeSelectRefId][0]) return
    const includeSelectEl = this.$refs[includeSelectRefId][0].$el
    const includeOptEls = includeSelectEl.querySelectorAll('.el-select__tags span .el-tag')

    let targetDimensionEl = findIncludeDimension(includeOptEls, dimensionValue)
    targetDimensionEl && targetDimensionEl.setAttribute('data-tag', isUsed ? 'used' : '')
  }
  handleAddAggregate () {
    const aggregateArray = get(this.form, 'aggregateArray')
    const aggregateData = {
      ...JSON.parse(initialAggregateData),
      ...{ measures: this.measures.map(item => item.value) },
      // id: aggregateArray.length
      id: sampleGuid()
    }
    this.aggregateStyle = []
    this.setModalForm({ aggregateArray: [ aggregateData, ...aggregateArray ] })
    this.isWaitingCheckCuboids[aggregateData.id] = true
    this.isWaitingCheckAllCuboids = true
    this.aggregateStyle = []
    // this.calcCuboids()
  }
  handleCopyAggregate (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    const copyedAggregate = {
      ...aggregateArray[aggregateIdx],
      // id: aggregateArray.length
      id: sampleGuid()
    }
    this.setModalForm({ aggregateArray: [copyedAggregate, ...aggregateArray] })
    this.isWaitingCheckCuboids[copyedAggregate.id] = true
    this.isWaitingCheckAllCuboids = true
    this.aggregateStyle = []
    // this.calcCuboids()
  }
  handleDeleteAggregate (aggregateIdx, titleId) {
    kapConfirm(this.$t('delAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('delAggregateTitle')).then(() => {
      const aggregateArray = get(this.form, 'aggregateArray')
      aggregateArray.splice(aggregateIdx, 1)
      this.setModalForm({ aggregateArray })
      // this.isWaitingCheckCuboids = true
      this.isWaitingCheckAllCuboids = true
      this.aggregateStyle = []
      // this.calcCuboids()
    })
  }
  handleAddDimensionRow (path, id) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    const newId = dimensionRows.length
    const newDimensionRow = { id: newId, items: [] }
    this.setModalForm({[rootKey]: push(this.form, path, newDimensionRow)[rootKey]})
    this.isWaitingCheckCuboids[id] = true
    this.isWaitingCheckAllCuboids = true
    // this.calcCuboids()
  }
  handleRemoveDimensionRow (path, aggregateIdx, dimensionRowIndex, id) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    if (dimensionRows.length > 1) {
      dimensionRows.splice(dimensionRowIndex, 1)[0]
      this.setModalForm({[rootKey]: set(this.form, path, dimensionRows)[rootKey]})
    }
    this.isWaitingCheckCuboids[id] = true
    this.isWaitingCheckAllCuboids = true
    // this.calcCuboids()
  }
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  handleInput (key, value, id) {
    if (key !== 'isCatchUp') {
      this.isWaitingCheckCuboids[id] = true
      this.isWaitingCheckAllCuboids = true
      // this.calcCuboids()
    }
    const rootKey = key.split('.')[0]
    this.setModalForm({[rootKey]: set(this.form, key, value)[rootKey]})
  }
  handleRemoveIncludeRules (removedValue, aggregateIdx, id) {
    const { aggregateArray = [] } = this.form
    const { mandatory, hierarchyArray, jointArray } = aggregateArray[aggregateIdx]

    if (mandatory.includes(removedValue)) {
      const mandatoryKey = `aggregateArray.${aggregateIdx}.mandatory`
      this.handleInput(mandatoryKey, mandatory.filter(item => item !== removedValue), id)
    }
    hierarchyArray.forEach((hierarchyGroup, hierarchyRowIdx) => {
      const hierarchy = hierarchyGroup.items
      if (hierarchy.includes(removedValue)) {
        const hierarchyKey = `aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`
        this.handleInput(hierarchyKey, hierarchy.filter(item => item !== removedValue), id)
      }
    })
    jointArray.forEach((jointGroup, jointRowIdx) => {
      const joint = jointGroup.items
      if (joint.includes(removedValue)) {
        const jointKey = `aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`
        this.handleInput(jointKey, joint.filter(item => item !== removedValue), id)
      }
    })
  }
  handleAddAllIncludes (aggregateIdx, id) {
    const allDimensions = this.dimensions.map(dimension => dimension.label)
    this.handleInput(`aggregateArray.${aggregateIdx}.includes`, allDimensions, id)
  }
  handleRemoveAllIncludes (aggregateIdx, titleId, id) {
    kapConfirm(this.$t('clearAllAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('clearAggregateTitle')).then(() => {
      const { aggregateArray = [] } = this.form
      const currentAggregate = aggregateArray[aggregateIdx] || {}
      const currentIncludes = currentAggregate.includes || []

      for (const include of currentIncludes) {
        this.handleRemoveIncludeRules(include, aggregateIdx)
      }
      this.handleInput(`aggregateArray.${aggregateIdx}.includes`, [], id)
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
      // 每次检测前，reset cuboid的信息
      this.resetCuboidInfo()
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
    // this.showLoading()
    try {
      if (this.checkFormVaild()) {
        const data = this.getSubmitData()
        delete data.dimensions // 后台处理规整顺序
        // 重置总的计算标志
        this.isWaitingCheckAllCuboids = false
        for (let prop in this.isWaitingCheckCuboids) {
          this.isWaitingCheckCuboids[prop] = true
        }
        // 发一个获取数据的接口
        let cuboidsRes = await this.getCalcCuboids(data)
        let cuboidsResult = await handleSuccessAsync(cuboidsRes)
        for (let prop in this.isWaitingCheckCuboids) {
          this.isWaitingCheckCuboids[prop] = false
        }
        if (cuboidsResult) {
          this.maxCombinationNum = cuboidsResult.max_combination_num
          this.cuboidsInfo = objectClone(cuboidsResult)
          // this.cuboidsInfo.agg_index_counts = cuboidsResult.agg_index_counts.reverse()
          let resultData = objectClone(cuboidsResult.agg_index_counts.reverse())
          // 用聚合组的唯一id 做标识
          this.cuboidsInfo.agg_index_counts = {}
          for (let i = 0; i < this.form.aggregateArray.length; i++) {
            let id = this.form.aggregateArray[i].id
            this.cuboidsInfo.agg_index_counts[id] = objectClone(resultData[i])
          }
          let singleIsLimit = cuboidsResult.agg_index_counts.filter((item) => {
            return !/^\d+$/.test(item.result)
          })
          // 单个索引组的个数超了限制，显示报错，并不往下执行了
          if (singleIsLimit.length > 0) {
            this.$message.error(this.$t('maxCombinationTip'))
            this.calcLoading = false
            this.isSubmit = false
            // this.hideLoading()
            // 操作滚动
            this.dealScrollToFirstError()
            return false
          } else {
            // 单个没超过，总量超了，显示总量的报错，也不往下执行了
            if (!/^\d+$/.test(cuboidsResult.total_count.result)) {
              this.$message.error(this.$t('maxTotalCombinationTip'))
              this.calcLoading = false
              this.isSubmit = false
              // this.hideLoading()
              return false
            }
          }
        }
        let diffRes = await this.getIndexDiff(data)
        let diffResult = await handleSuccessAsync(diffRes)
        await this.confirmSubmit(diffResult)
        // 获取数字正常的情况下，才进行 submit
        let res = await this.submit(data)
        let result = await handleSuccessAsync(res)
        this.handleBuildIndexTip(result)
        this.isSubmit = false
        this.handleClose(true)
      } else {
        this.isSubmit = false
        // this.hideLoading()
      }
    } catch (e) {
      this.calcLoading = false
      for (let prop in this.isWaitingCheckCuboids) {
        this.isWaitingCheckCuboids[prop] = false
      }
      e && handleError(e)
      this.isSubmit = false
      // this.hideLoading()
    }
  }
  async confirmSubmit (diffResult) {
    if (diffResult.decrease_layouts === 0 && diffResult.increase_layouts > 0) {
      return kapConfirm(this.$t('increaseTips', {increaseNum: diffResult.increase_layouts, model_name: this.model.name}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), confirmButtonClass: 'guideTipSetAggIndexConfitmBtn', type: 'warning', closeOnClickModal: false, showClose: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip'))
    } else if (diffResult.decrease_layouts > 0 && diffResult.increase_layouts === 0) {
      return kapConfirm(this.$t('decreaseTips', {decreaseNum: diffResult.decrease_layouts, model_name: this.model.name}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), confirmButtonClass: 'guideTipSetAggIndexConfitmBtn', type: 'warning', closeOnClickModal: false, showClose: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip'))
    } else if (diffResult.decrease_layouts > 0 && diffResult.increase_layouts > 0) {
      return kapConfirm(this.$t('mixTips', {increaseNum: diffResult.increase_layouts, decreaseNum: diffResult.decrease_layouts, model_name: this.model.name}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), confirmButtonClass: 'guideTipSetAggIndexConfitmBtn', type: 'warning', closeOnClickModal: false, showClose: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip'))
    } else {
      return new Promise((resolve) => {
        resolve()
      })
    }
  }
  checkFormVaild () {
    if (!this.isFormVaild) {
      this.$message(this.$t('includesEmpty'))
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
    const { editType, form, usedDimensions, dimensionIdMapping, measureIdMapping, projectName, model } = this

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
            measures: aggregate.measures.map(measure => measureIdMapping[measure]),
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

  // 删除选中的度量tag
  handleRemoveMeasureRules (value, aggregateIdx, id) {
    value === 'COUNT_ALL' && this.handleInput(`aggregateArray.${aggregateIdx}.measures`, ['COUNT_ALL', ...this.form.aggregateArray[aggregateIdx].measures], id)
  }

  // 选择所有的度量
  handleAddAllMeasure (aggregateIdx, id) {
    const measures = ['COUNT_ALL', ...this.measures.map(m => m.label).filter(item => item !== 'COUNT_ALL')]
    this.handleInput(`aggregateArray.${aggregateIdx}.measures`, measures, id)
  }

  // 清除所有的度量
  handleRemoveAllMeasure (aggregateIdx, titleId, id) {
    kapConfirm(this.$t('clearAllMeasuresTip', {aggId: titleId}), {type: 'warning'}, this.$t('clearMeasureTitle')).then(() => {
      this.handleInput(`aggregateArray.${aggregateIdx}.measures`, this.form.aggregateArray[aggregateIdx].measures.filter(m => ['COUNT_ALL'].includes(m)), id)
    })
  }

  // 是否展开聚合组
  openAggregateItem (index) {
    this.form.aggregateArray[index].open = !this.form.aggregateArray[index].open
    this.computeCurrentAggHeigth(index)
    this.setModalForm(this.form)
  }

  aggregateHeight (index) {
    return document.querySelectorAll('.aggregate-group .body').length ? (document.querySelectorAll('.aggregate-group .body')[index].offsetHeight || null) : null
  }

  computeCurrentAggHeigth (index) {
    let height = this.aggregateHeight(index)
    const showAggregate = () => {
      setTimeout(() => {
        this.aggregateStyle = []
      }, 1000)
    }
    if (this.form.aggregateArray[index].open) {
      this.$set(this.aggregateStyle, index, {'display': 'block'})
      this.$nextTick(() => {
        height = this.aggregateHeight(index)
        this.$set(this.aggregateStyle, index, {'height': 0})
        this.$nextTick(() => {
          this.$set(this.aggregateStyle, index, {...this.aggregateStyle[index], ...{'height': height ? `${height}px` : 'auto'}})
          showAggregate()
        })
      })
    } else {
      this.$set(this.aggregateStyle, index, {'height': height ? `${height}px` : 'auto'})
      this.$nextTick(() => {
        this.$set(this.aggregateStyle, index, {'height': 0})
        showAggregate()
      })
    }
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
  .el-select {
    width: 100%;
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
    .header {
      width: 100%;
      height: 36px;
      background: @regular-background-color;
      line-height: 36px;
      padding: 0 10px;
      box-sizing: border-box;
      border-radius: 2px 2px 0 0;
      border: 1px solid @line-border-color;
    }
    .body {
      border: 1px solid @line-border-color;
      border-top: 0;
      overflow: hidden;
      transition: height 1s;
      box-sizing: border-box;
      &.overLimit{
        border: 1px solid @error-color-1;
      }
      .contain {
        padding: 10px 15px;
        box-sizing: border-box;
      }
    }
    .open-aggregate-group {
      width: 100%;
      height: 20px;
      background: @aceditor-bg-color;
      text-align: center;
      border-radius: 0 0 2px 2px;
      border: 1px solid @line-border-color;
      border-top: 0;
      box-sizing: border-box;
      cursor: pointer;
      .el-icon-d-arrow-left {
        transform: rotate(-90deg);
        &.open {
          transform: rotate(90deg)
        }
      }
      &:hover {
        color: @color-primary;
        background-color: @base-color-9;
      }
    }
    .row {
      margin-bottom: 17px;
      &.joint {
        margin-bottom: 0;
      }
      .add-all-item {
        .el-icon-ksd-what {
          margin-left: 5px;
          color: @color-primary;
        }
        &:hover {
          .el-icon-ksd-what {
            color: @fff;
          }
        }
      }
    }
    .actions {
      position: absolute;
      top: -3px;
      right: 10px;
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
