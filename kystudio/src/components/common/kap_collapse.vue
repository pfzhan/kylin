<template>
  <el-card class="ksd-mb-20 collapse-card">
    <div slot="header">
      <span class="collapse-title">{{title}}</span>
    </div>
    <div v-show="showContent === true">
      <slot name="content" >
      </slot>
    </div>
    <el-button @click="changeShow()" class="show-but">
      <i class="ksd-fs-20" :class="showContent === false ? 'el-icon-caret-bottom':'el-icon-caret-top'"></i>
    </el-button>
  </el-card>
</template>
<script>
export default {
  name: 'collapse-card',
  props: ['defaultShow', 'title'],
  data () {
    return {
      showContent: this.defaultShow
    }
  },
  methods: {
    changeShow () {
      this.showContent = !this.showContent
    }
  },
  locales: {}
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
  .collapse-card {
    border:1px solid @line-border-color;
    box-shadow: none;
    .el-card__header {
      background-color: @grey-3;
      height: 33px;
      padding: 8px 20px; 
      border-bottom: none;
    }
    .el-card__body {
      padding: 0px;
      >div {
        padding: 20px;
      }
    }
    .collapse-title {
      color: @text-title-color;
      font-size: 16px;
      font-weight: @font-medium;
    }
    .show-but {
      width: 100%;
      border: none;
      border-top: 1px solid @line-border-color;
      background-color: @grey-4;
      i {
        color: @text-secondary-color;
      }
    }
    .show-but:hover {
      background-color: @grey-4;
    }
  }
</style>
