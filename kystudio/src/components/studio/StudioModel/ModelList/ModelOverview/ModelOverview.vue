<template>
  <el-tabs class="model-overview" type="border-card" v-model="activeTab">
    <el-tab-pane :label="$t('erDiagram')" name="erDiagram">
      <ModelERDiagram is-show-full-screen :model="model" />
    </el-tab-pane>
    <el-tab-pane :label="$t('dimension')" name="dimension">
      <ModelDimensionList :model="model" />
    </el-tab-pane>
    <el-tab-pane :label="$t('measure')" name="measure">
      <ModelMeasureList :model="model" />
    </el-tab-pane>
  </el-tabs>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { dataGenerator } from '../../../../../util'
import ModelERDiagram from '../../../../common/ModelERDiagram/ModelERDiagram.vue'
import ModelDimensionList from '../../../../common/ModelDimensionList/ModelDimensionList.vue'
import ModelMeasureList from '../../../../common/ModelMeasureList/ModelMeasureList.vue'

@Component({
  props: ['data'],
  components: {
    ModelERDiagram,
    ModelDimensionList,
    ModelMeasureList
  },
  locales
})
export default class ModelOverview extends Vue {
  activeTab = 'erDiagram'

  get model () {
    return dataGenerator.generateModel(this.data)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-overview {
  margin: 15px;
  box-shadow: none;
  border:1px solid rgba(245,245,245,1);
  > .el-tabs__header,
  > .el-tabs__header .el-tabs__item {
    border: transparent;
    color: @text-normal-color;
    &.is-active {
      font-weight: normal;
    }
  }
  > .el-tabs__content {
    padding: 0;
  }
  > .el-tabs__content > .el-tab-pane {
    padding: 20px;
    height: 470px;
    box-sizing: border-box;
    overflow: auto;
  }
  .model-er-diagram {
    margin: -20px;
    height: calc(~'100% + 40px');
    width: calc(~'100% + 40px');
  }
}
</style>
