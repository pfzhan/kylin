<template>
  <el-dialog class="aggregate-modal" width="960px"
    :title="$t(modalTitle)"
    :visible="isShow"
    :append-to-body="true"
    :close-on-click-modal="false"
    @close="isShow && handleClose(false)">
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
            <h2 class="title font-medium">{{$t('include')}}</h2>
            <el-select
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
          <div class="row">
            <h2 class="title font-medium">{{$t('mandatory')}}</h2>
            <el-select
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
          <div class="row">
            <h2 class="title font-medium">{{$t('hierarchy')}}</h2>
            <div class="list"
              v-for="(hierarchy, hierarchyRowIdx) in aggregate.hierarchyArray"
              :key="`hierarchy-${hierarchyRowIdx}`">
              <el-select
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
                <el-button plain circle type="primary" size="medium" icon="el-icon-ksd-add_2"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`)">
                </el-button>
                <el-button plain circle class="delete" size="medium" icon="el-icon-minus"
                  :disabled="aggregate.hierarchyArray.length === 1"
                  @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregateIdx, hierarchyRowIdx)">
                </el-button>
              </div>
            </div>
          </div>
          <!-- Joint聚合组 -->
          <div class="row">
            <h2 class="title font-medium">{{$t('joint')}}</h2>
            <div class="list"
              v-for="(joint, jointRowIdx) in aggregate.jointArray"
              :key="`joint-${jointRowIdx}`">
              <el-select
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
                <el-button plain circle type="primary" size="medium" icon="el-icon-ksd-add_2"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`)">
                </el-button>
                <el-button plain circle class="delete" size="medium" icon="el-icon-minus"
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
        <el-button size="medium" plain type="primary" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
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
  async handleSubmit () {
    try {
      if (this.checkFormVaild()) {
        const data = this.getSubmitData()
        await this.submit(data)
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        this.handleClose(true)
      }
    } catch (e) {
      e && handleError(e)
    }
  }
  checkFormVaild () {
    if (!this.isFormVaild) {
      this.$message(this.$t('includesEmpty'))
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
    const { editType, form, dimensions, dimensionIdMapping, projectName, model } = this

    switch (editType) {
      case EDIT: {
        const { aggregateArray, isCatchUp } = form
        return {
          projectName,
          modelName: model.name,
          isCatchUp,
          dimensions: dimensions.map(dimension => dimension.id),
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
  [data-tag='used'] {
    background: @grey-4;
    border: 1px solid @text-secondary-color;
    border-radius: 2px;
    color: @text-normal-color;
    .el-icon-close {
      color: @text-normal-color;
    }
  }
  .el-button {
    i[class^=el-icon-] {
      cursor: inherit;
    }
  }
  .list {
    padding-right: 85px;
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
    .el-button--medium {
      padding: 8px;
      float: left;
    }
    .delete {
      background: @fff;
      border: 1px solid @text-secondary-color;
      color: @text-disabled-color;
      &:hover {
        background: @fff;
        border: 1px solid @text-secondary-color;
        color: @text-disabled-color;
      }
      &.is-disabled {
        background: @grey-4;
        &:hover {
          background: @grey-4;
        }
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
}
</style>
