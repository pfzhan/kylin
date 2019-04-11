<template>
  <div class="editable-block" :class="{ 'is-edit': isEditing, 'is-loading': isLoading }">
    <div class="block-header" v-if="headerContent">
      <span>{{headerContent}}</span>
      <i class="icon el-icon-ksd-table_edit"
        v-if="isEditable && !isEditing"
        @click="handleEdit">
      </i>
    </div>
    <div class="block-body">
      <slot></slot>
    </div>
    <div class="block-foot" v-if="isEditing">
      <el-button size="small" :disabled="isLoading || (!isEdited && isKeepEditing)" @click="handleCancel">{{cancelText}}</el-button><el-button
      plain size="small" type="primary" :loading="isLoading" :disabled="!isEdited && isKeepEditing" @click="handleSubmit">{{$t('kylinLang.common.save')}}</el-button>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component({
  props: {
    isEditable: {
      type: Boolean,
      default: true
    },
    headerContent: {
      type: String
    },
    isKeepEditing: {
      type: Boolean,
      default: false
    },
    isEdited: {
      type: Boolean,
      default: false
    }
  }
})
export default class EditableBlock extends Vue {
  isUserEditing = false
  isLoading = false
  set isEditing (value) {
    this.isUserEditing = value
  }
  get isEditing () {
    return (this.isUserEditing || this.isKeepEditing) && this.isEditable
  }
  get cancelText () {
    return this.isKeepEditing ? this.$t('kylinLang.common.reset') : this.$t('kylinLang.common.cancel')
  }
  handleEdit () {
    this.isEditing = true
  }
  handleCancel () {
    this.isEditing = false
    this.$emit('cancel')
  }
  handleSubmit () {
    this.isLoading = true
    this.$emit('submit', this.handleSuccess, this.handleError)
  }
  handleError () {
    this.isLoading = false
  }
  handleSuccess () {
    this.isLoading = false
    this.isEditing = false
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.editable-block {
  .block-header {
    border-top: 1px solid @line-border-color;
    border-right: 1px solid @line-border-color;
    border-left: 1px solid @line-border-color;
    background: @regular-background-color;
    color: @text-title-color;
    font-size: 14px;
    line-height: 18px;
    font-weight: 500;
    padding: 8px 15px;
    > * {
      vertical-align: middle;
    }
  }
  .block-header .icon {
    color: @text-normal-color;
    border-radius: 50%;
    overflow: hidden;
    font-size: 14px;
    padding: 2px;
    cursor: pointer;
    &:hover {
      background: @text-placeholder-color;
      color: @base-color;
    }
  }
  .block-body {
    border: 1px solid @line-border-color;
    background: @table-stripe-color;
  }
  .block-foot {
    border-bottom: 1px solid @line-border-color;
    border-right: 1px solid @line-border-color;
    border-left: 1px solid @line-border-color;
    padding: 10px 15px;
    background: @table-stripe-color;
    text-align: right;
    .el-button+.el-button {
      margin-left: 10px;
    }
  }
}
</style>

