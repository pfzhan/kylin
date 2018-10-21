<template>
  <div class="related-models">
    <el-row class="header">
      <h1 class="font-medium">
        <span>{{$t('relatedModel')}}</span>
        <span class="count">{{$t('total', { count: relatedModels.length })}}</span>
      </h1>
      <el-button class="btn-drop-down" size="small" @click="isShowRelatedModel = !isShowRelatedModel">
        <i class="el-icon-ksd-more_02" :class="{ show: isShowRelatedModel }"></i>
      </el-button>
    </el-row>
    <el-collapse-transition v-if="isShowRelatedModel">
      <div>
        <el-row>
          <el-input
            size="medium"
            class="search-box"
            v-model="filterText"
            :placeholder="$t('searchModel')">
          </el-input>
        </el-row>
        <el-row v-for="(modelCardGroup, index) in modelCardGroups" :key="index" :gutter="10">
          <el-col v-for="relatedModel in modelCardGroup" :key="relatedModel.uuid" :span="span">
            <div class="model-card">
              <el-row class="model-header">
                <span class="model-name">{{relatedModel.name}}</span>
                <el-tag v-if="relatedModel.status === 'broken'" size="small" type="info">Broken</el-tag>
                <el-tag v-else-if="relatedModel.isOnline" size="small" type="success">Online</el-tag>
                <el-tag v-else size="small" type="danger">Offline</el-tag>
              </el-row>
              <el-row class="model-body">
                <el-col :span="12">{{getGMTDate(relatedModel.startTime)}}</el-col>
                <el-col :span="12">{{getGMTDate(relatedModel.endTime)}}</el-col>
              </el-row>
              <div class="discard">Discard</div>
            </div>
          </el-col>
        </el-row>
        <Waypoint :scrollable-ancestor="scrollableAncestor" @enter="handleLoadMore"></Waypoint>
      </div>
    </el-collapse-transition>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import Waypoint from '../../../common/Waypoint/Waypoint'
import { transToGmtTime } from '../../../../util'

@Component({
  props: {
    projectName: {
      type: String
    },
    table: {
      type: Object
    },
    relatedModels: {
      type: Array,
      default: () => []
    }
  },
  components: {
    Waypoint
  },
  locales
})
export default class RelatedModels extends Vue {
  filterText = ''
  isShowRelatedModel = false
  windowWidth = 0
  scrollableAncestor = null
  get columnCount () {
    return this.windowWidth > 1280 ? 2 : 2
  }
  get span () {
    return 24 / this.columnCount
  }
  get modelCardGroups () {
    const { columnCount, relatedModels } = this
    const modelCardGroups = []

    relatedModels.forEach((relatedModel, index) => {
      if (index % columnCount === 0) {
        modelCardGroups.push([relatedModel])
      } else {
        const groupIdx = Math.floor(index / columnCount)
        modelCardGroups[groupIdx].push(relatedModel)
      }
    })
    return modelCardGroups
  }
  mounted () {
    this.handleResize()
    window.addEventListener('resize', this.handleResize)
    this.scrollableAncestor = document.querySelector('.layout-right')
  }
  beforeDestory () {
    window.removeEventListener('resize', this.handleResize)
  }
  handleResize () {
    this.windowWidth = document.body.clientWidth
  }
  handleLoadMore () {
    this.$emit('load-more')
  }
  getGMTDate (time) {
    return transToGmtTime(time)
  }
}
</script>

<style lang="less">
.related-models {
  .header {
    border-bottom: 1px solid #CFD8DC;
    margin: 25px 0 10px 0;
    h1 {
      font-size: 16px;
      color: #263238;
      line-height: 48px;
    }
    .count {
      font-weight: normal;
    }
  }
  .search-box {
    width: 155px;
    float: right;
    margin-bottom: 10px;
  }
  .btn-drop-down {
    width: 52px;
    position: absolute;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
  }
  .el-icon-ksd-more_02 {
    transform: rotate(0);
    transition: transform .2s;
  }
  .el-icon-ksd-more_02.show {
    transform: rotate(90deg);
  }
  .model-card {
    border: 1px solid #CFD8DC;
    padding: 20px;
    margin-bottom: 10px;
    position: relative;
    &:hover {
      box-shadow: 0 2px 4px 0 #CFD8DC, 0 0 6px 0 #CFD8DC;
      .discard {
        display: block;
        cursor: pointer;
        color: #0988DE;
      }
    }
  }
  .model-name {
    font-size: 14px;
    color: #263238;
    line-height: 24px;
  }
  .model-body {
    font-size: 12px;
    color: #455A64;
  }
  .model-header {
    margin-bottom: 10px;
  }
  .el-tag {
    font-size: 14px;
    margin-left: 5px;
  }
  .discard {
    position: absolute;
    bottom: 20px;
    right: 20px;
    line-height: 21px;
    display: none;
  }
}
</style>
