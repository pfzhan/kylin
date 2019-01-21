<template>
  <el-dialog class="aggregate-modal" width="960px"
    :title="$t(modalTitle)"
    :visible="isShow"
    :append-to-body="true"
    :close-on-click-modal="false"
    @close="isShow && handleClose(false)">
    <div class="loading" v-if="isLoading" v-loading="isLoading"></div>
    <template v-if="model">
      <!-- 维度列表展示 -->
      <div class="dimension-list clearfix" v-if="isDimensionShow">
        <div class="dimension disable" v-for="dimension in dimensions" :key="dimension.value">
          {{dimension.label}}
        </div>
      </div>
      <!-- 维度按钮 -->
      <div class="dimension-buttons">
        <el-button @click="toggleDimensionShow">
          {{$t(isDimensionShow ? 'hideDimensions' : 'showDimensions')}}
          <i class="el-icon-ksd-more_01" :class="{ 'less': isDimensionShow }"></i>
        </el-button>
      </div>
      <!-- 聚合组按钮 -->
      <div class="aggregate-buttons">
        <el-button type="primary" @click="handleAddAggregate">{{$t('addAggregateGroup')}}</el-button>
      </div>
      <!-- 聚合组表单 -->
      <div class="aggregate-group" v-for="(aggregate, aggregateIdx) in form.aggregateArray" :key="aggregateIdx">
        <h1 class="title font-medium">{{$t('aggregateGroupTitle', { id: form.aggregateArray.length - aggregateIdx })}}</h1>
        <div class="actions">
          <el-button size="mini" @click="() => handleCopyAggregate(aggregateIdx)">{{$t('kylinLang.common.copy')}}</el-button>
          <el-button size="mini" @click="() => handleDeleteAggregate(aggregateIdx)">{{$t('kylinLang.common.delete')}}</el-button>
        </div>
        <div class="body">
          <!-- Include聚合组 -->
          <div class="row">
            <el-button plain size="mini" type="primary" v-guide.selectAllIncludesBtn @click="handleAddAllIncludes(aggregateIdx)">{{$t('selectAll')}}</el-button>
            <el-button size="mini" @click="handleRemoveAllIncludes(aggregateIdx)">{{$t('cancelAll')}}</el-button>
          </div>
          <div class="row">
            <h2 class="title font-medium">{{$t('include')}}</h2>
            <el-select
              v-guide.aggIncludes
              multiple
              filterable
              :ref="`aggregate.include.${aggregateIdx}`"
              :value="aggregate.includes"
              :placeholder="$t('kylinLang.common.pleaseSelect')"
              @input="value => handleInput(`aggregateArray.${aggregateIdx}.includes`, value)"
              @remove-tag="value => handleRemoveIncludeRules(value, aggregateIdx)">
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
              :value="aggregate.mandatory"
              :placeholder="$t('kylinLang.common.pleaseSelect')"
              @change="value => handleInput(`aggregateArray.${aggregateIdx}.mandatory`, value)">
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
                :value="hierarchy.items"
                :placeholder="$t('kylinLang.common.pleaseSelect')"
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`, value)">
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
                :value="joint.items"
                :placeholder="$t('kylinLang.common.pleaseSelect')"
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`, value)">
                <el-option
                  v-for="dimension in getUnusedDimensions(aggregateIdx)"
                  :key="dimension.value"
                  :label="dimension.label"
                  :value="dimension.value">
                </el-option>
              </el-select>
              <div class="list-actions clearfix">
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
        <el-checkbox :value="form.isCatchUp" @input="value => handleInput('isCatchUp', value)">{{$t('kylinLang.common.catchUp')}}</el-checkbox>
      </div>
      <div class="right">
        <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" v-if="isShow" v-guide.saveAggBtn plain type="primary" :loading="isSubmit" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
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
import store, { types, initialAggregateData } from './store'
import { titleMaps, editTypes, getPlaintDimensions, findIncludeDimension } from './handler'
import { handleError, get, set, push } from '../../../../../../util'

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
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS'
    })
  },
  locales
})
export default class AggregateModal extends Vue {
  isFormShow = false
  isDimensionShow = false
  isSubmit = false
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
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
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
  }
  handleCopyAggregate (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    const copyedAggregate = {
      ...aggregateArray[aggregateIdx],
      id: aggregateArray.length
    }

    this.setModalForm({ aggregateArray: [copyedAggregate, ...aggregateArray] })
  }
  handleDeleteAggregate (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    aggregateArray.splice(aggregateIdx, 1)

    this.setModalForm({ aggregateArray })
  }
  handleAddDimensionRow (path) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    const newId = dimensionRows.length
    const newDimensionRow = { id: newId, items: [] }

    this.setModalForm({[rootKey]: push(this.form, path, newDimensionRow)[rootKey]})
  }
  handleRemoveDimensionRow (path, aggregateIdx, dimensionRowIndex) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)

    if (dimensionRows.length > 1) {
      dimensionRows.splice(dimensionRowIndex, 1)[0]
      this.setModalForm({[rootKey]: set(this.form, path, dimensionRows)[rootKey]})
    }
  }
  handleClose (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  handleInput (key, value) {
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
  handleRemoveAllIncludes (aggregateIdx) {
    const { aggregateArray = [] } = this.form
    const currentAggregate = aggregateArray[aggregateIdx] || {}
    const currentIncludes = currentAggregate.includes || []

    for (const include of currentIncludes) {
      this.handleRemoveIncludeRules(include, aggregateIdx)
    }
    this.handleInput(`aggregateArray.${aggregateIdx}.includes`, [])
  }
  async handleSubmit () {
    this.isSubmit = true
    try {
      if (this.checkFormVaild()) {
        const data = this.getSubmitData()
        await this.submit(data)
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        this.isSubmit = false
        this.handleClose(true)
      }
    } catch (e) {
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
          }))
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
  .el-button + .el-button { margin-left: 4px;}
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
    padding: 10px 0 20px 0;
    border-bottom: 1px solid @text-placeholder-color;
  }
  .less {
    transform: rotate(180deg);
  }
  .aggregate-buttons {
    margin: 20px 0;
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
      border: 1px solid @text-placeholder-color;
      border-radius: 2px;
      padding: 20px;
    }
    .row {
      margin-bottom: 17px;
    }
    .actions {
      position: absolute;
      top: -7px;
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
    font-size: 16px;
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
    right: 0;
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
