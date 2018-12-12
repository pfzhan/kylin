<template>
  <el-form :model="value" class="segment-merge">
    <el-form-item prop="autoMergeConfigs" class="auto-merge">
      <el-row>
        <span class="option-title">{{$t('autoMerge')}}</span>
        <el-tooltip
          effect="dark"
          placement="right"
          popper-class="segment-merge-tooltip"
          :content="$t('autoMergeTip')">
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
      </el-row>
      <el-row class="item-value" v-for="(autoMergeConfig, index) in value.autoMergeConfigs" :key="index" :gutter="10">
        <el-col :span="19">
          <el-select
            size="medium"
            :value="autoMergeConfig"
            @input="val => handleInput(`autoMergeConfigs.${index}`, val)">
            <el-option
              v-for="autoMergeType in autoMergeTypes"
              :key="autoMergeType"
              :label="$t(autoMergeType)"
              :value="autoMergeType"
              :disabled="value.autoMergeConfigs.includes(autoMergeType)">
            </el-option>
          </el-select>
        </el-col>
        <el-col :span="5">
          <el-button
            text
            type="primary"
            :disabled="value.autoMergeConfigs.length >= autoMergeTypes.length"
            @click="handleAddConfig('autoMergeConfigs', index)">
            <i class="el-icon-ksd-table_add"></i>
          </el-button>
          <el-button
            text
            type="primary"
            :disabled="value.autoMergeConfigs.length <= 1"
            @click="handleRemoveConfig('autoMergeConfigs', index)">
            <i class="el-icon-ksd-symbol_type"></i>
          </el-button>
        </el-col>
      </el-row>
    </el-form-item>

    <el-form-item prop="volatileConfig" class="volatile">
      <el-row>
        <span class="option-title">{{$t('volatile')}}</span>
        <el-tooltip
          effect="dark"
          placement="right"
          popper-class="segment-merge-tooltip"
          :content="$t('volatileTip')">
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
      </el-row>
      <el-row gutter="5">
        <el-col :span="8">
          <el-form-item prop="volatileConfig.value">
            <el-input
              size="medium"
              :value="value.volatileConfig.value"
              :placeholder="$t('kylinLang.common.pleaseInput')"
              @input="val => !isNaN(+val) && handleInput('volatileConfig.value', +val)">
            </el-input>
          </el-form-item>
        </el-col>
        <el-col :span="11">
          <el-form-item prop="volatileConfig.type">
            <el-select
              size="medium"
              :value="value.volatileConfig.type"
              :placeholder="$t('kylinLang.common.pleaseChoose')"
              @input="val => handleInput('volatileConfig.type', val)">
              <el-option
                v-for="volatileType in volatileTypes"
                :key="volatileType"
                :label="$t(volatileType)"
                :value="volatileType">
              </el-option>
            </el-select>
          </el-form-item>
        </el-col>
      </el-row>
    </el-form-item>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { set } from '../../../util/object'
import { autoMergeTypes, volatileTypes } from './handler'

@Component({
  props: {
    value: {
      type: Object,
      default: () => ({})
    }
  },
  locales
})
export default class SegmentMerge extends Vue {
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes

  handleInput (key, value) {
    const newValue = set(this.value, key, value)
    this.$emit('input', newValue)
  }
  handleAddConfig (key, index) {
    const newValue = JSON.parse(JSON.stringify(this.value))
    newValue[key].splice(index, 0, '')
    this.$emit('input', newValue)
  }
  handleRemoveConfig (key, index) {
    const newValue = JSON.parse(JSON.stringify(this.value))
    newValue[key].splice(index, 1)
    this.$emit('input', newValue)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.segment-merge {
  width: 410px;
  .el-form-item:last-child {
    margin-bottom: 0;
  }
  .el-form-item:not(:last-child) {
    margin-bottom: 10px;
  }
  .el-select {
    width: 100%;
  }
  .item-value:not(:last-child) {
    margin-bottom: 10px;
  }
  .is-text {
    padding: 0;
  }
  .is-text span {
    font-size: 24px;
    line-height: initial;
  }
  .is-text.el-button+.is-text.el-button {
    margin-left: 0;
  }
  .is-disabled * {
    cursor: not-allowed;
  }
}

.segment-merge-tooltip {
  max-width: 300px;
}
</style>
