<template>
  <el-dialog class="aggregate-modal" width="960px"
    :title="$t(modalTitle)"
    :visible="isShow"
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
          <i class="el-icon-ksd-copy" @click="() => handleCopyAggregate(aggregateIdx)"></i>
          <i class="el-icon-ksd-table_delete" @click="() => handleDeleteAggregate(aggregateIdx)"></i>
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
              @input="value => handleInput(`aggregateArray.${aggregateIdx}.includes`, value)">
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
              @change="value => handleInput(`aggregateArray.${aggregateIdx}.mandatory`, value)"
              @remove-tag="value => toggleIncludeDimension(aggregateIdx, value, false)">
              <el-option
                v-for="dimension in getUnusedDimensions(aggregateIdx)"
                :key="dimension.value"
                :label="dimension.label"
                :value="dimension.value"
                @click.native="() => toggleIncludeDimension(aggregateIdx, dimension.value, true)">
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
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`, value)"
                @remove-tag="value => toggleIncludeDimension(aggregateIdx, value, false)">
                <el-option
                  v-for="dimension in getUnusedDimensions(aggregateIdx)"
                  :key="dimension.value"
                  :label="dimension.label"
                  :value="dimension.value"
                  @click.native="() => toggleIncludeDimension(aggregateIdx, dimension.value, true)">
                </el-option>
              </el-select>
              <div class="list-actions clearfix">
                <el-button
                  plain
                  class="is-circle primary"
                  size="medium"
                  icon="el-icon-ksd-add"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`)">
                </el-button>
                <el-button
                  plain
                  class="is-circle delete"
                  size="medium"
                  icon="el-icon-ksd-minus"
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
                @change="value => handleInput(`aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`, value)"
                @remove-tag="value => toggleIncludeDimension(aggregateIdx, value, false)">
                <el-option
                  v-for="dimension in getUnusedDimensions(aggregateIdx)"
                  :key="dimension.value"
                  :label="dimension.label"
                  :value="dimension.value"
                  @click.native="() => toggleIncludeDimension(aggregateIdx, dimension.value, true)">
                </el-option>
              </el-select>
              <div class="list-actions clearfix">
                <el-button
                  plain
                  class="is-circle primary"
                  size="medium"
                  icon="el-icon-ksd-add"
                  @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`)">
                </el-button>
                <el-button
                  plain
                  class="is-circle delete"
                  size="medium"
                  icon="el-icon-ksd-minus"
                  @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregateIdx, jointRowIdx)">
                </el-button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </template>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../../../store'
import locales from './locales'
import store, { types, initialAggregateData } from './store'
import { titleMaps, getSubmitData, getPlaintDimensions, findIncludeDimension } from './handler'
import { handleError, get, set, push } from '../../../../../../util'

vuex.registerModule(['modals', 'AggregateModal'], store)

@Component({
  computed: {
    ...mapState('AggregateModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      model: state => state.model
    })
  },
  methods: {
    ...mapMutations('AggregateModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapActions({
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
  get dimensions () {
    const dimesions = []
    this.model.dimensions.forEach(dimension => {
      dimension.columns.forEach(column => {
        dimesions.push({
          label: `${dimension.table}.${column}`,
          value: `${dimension.table}.${column}`
        })
      })
    })
    return dimesions
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
      const deletedRow = dimensionRows.splice(dimensionRowIndex, 1)[0]
      deletedRow.items.forEach(item => {
        this.toggleIncludeDimension(aggregateIdx, item, false)
      })
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
  async handleSubmit () {
    try {
      // 获取Form格式化后的递交数据
      /* const data = */getSubmitData(this)
      // 验证表单
      // await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      // this.editType === 'new' && await this.saveUser(data)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.saveSuccess')
      })
      this.handleClose(true)
    } catch (e) {
      e && handleError(e)
    }
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
    background: #F1F7FA;
    border: 1px solid #B0BEC5;
    border-radius: 2px;
  }
  .dimension-buttons {
    text-align: center;
    margin: 0 -20px;
    padding: 10px 0 20px 0;
    border-bottom: 1px solid #CFD8DC;
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
      color: #263238;
      margin-bottom: 7px;
    }
    .body {
      border: 1px solid #CFD8DC;
      border-radius: 2px;
      padding: 20px;
    }
    .row {
      margin-bottom: 17px;
    }
    .actions {
      position: absolute;
      top: 2px;
      right: 0;
      font-size: 16px;
      i {
        color: #263238;
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
    background: @base-color-11;
    color: @fff;
    .el-icon-close {
      color: @fff;
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
      background: #FFFFFF;
      border: 1px solid #B0BEC5;
      color: #8E9FA8;
    }
  }
}
</style>
