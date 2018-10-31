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
        <el-row v-if="relatedModels.length" v-for="(modelCardGroup, index) in modelCardGroups" :key="index" :gutter="10">
          <el-col v-for="relatedModel in modelCardGroup" :key="relatedModel.uuid" :span="span">
            <div class="model-card" :class="{ 'is-discard': relatedModel.management_type === 'MODEL_BASED' }">
              <el-row class="model-header">
                <span class="model-name">{{relatedModel.name}}</span>
              </el-row>
              <el-row class="model-body">
                <el-col class="model-status" :span="24">
                  <template v-if="relatedModel.management_type !== 'MODEL_BASED'">
                    <el-tag v-if="relatedModel.status === 'DESCBROKEN'" size="small" type="info">Broken</el-tag>
                    <el-tag v-if="relatedModel.status === 'ONLINE'" size="small" type="success">Online</el-tag>
                    <el-tag v-if="~['OFFLINE', 'NEW'].indexOf(relatedModel.status)" size="small" type="danger">Offline</el-tag>
                  </template>
                </el-col>
                <el-col class="range-time" :span="24">
                  <div class="lable">Start Time :</div>
                  <span v-if="!isNaN(new Date(relatedModel.startTime).getTime())">
                    {{getGMTDate(relatedModel.startTime)}}
                  </span>
                </el-col>
                <el-col class="range-time" :span="24">
                  <div class="lable">End Time :</div>
                  <span v-if="!isNaN(new Date(relatedModel.endTime).getTime())">
                    {{getGMTDate(relatedModel.endTime)}}
                  </span>
                </el-col>
              </el-row>
              <div class="discard" v-if="relatedModel.management_type !== 'MODEL_BASED'" @click="handleDiscard(relatedModel)">Discard</div>
            </div>
          </el-col>
        </el-row>
        <el-row v-if="!relatedModels.length" class="center">{{$t('kylinLang.common.noData')}}</el-row>
        <Waypoint :scrollable-ancestor="scrollableAncestor" @enter="handleLoadMore"></Waypoint>
      </div>
    </el-collapse-transition>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import Waypoint from '../../../common/Waypoint/Waypoint'
import { transToGmtTime, handleError } from '../../../../util'

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
  methods: {
    ...mapActions({
      discardTableModel: 'DISCARD_TABLE_MODEL'
    })
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
    return this.windowWidth > 1500 ? 3 : 2
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
  @Watch('filterText')
  onTilterTextChange () {
    this.$emit('filter', this.filterText)
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
  async handleDiscard (relatedModel) {
    try {
      const modelName = relatedModel.alias
      const { projectName } = this
      const message = this.$t('kylinLang.common.updateSuccess')
      const confirmMessage = this.$t('dicardConfirm')
      const confirmTitle = this.$t('kylinLang.common.notice')
      await this.$confirm(confirmMessage, confirmTitle, {
        confirmButtonText: this.$t('kylinLang.common.ok'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      })
      await this.discardTableModel({ projectName, modelName })
      this.$message({ message, type: 'success' })
      relatedModel.management_type = 'MODEL_BASED'
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
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
    box-sizing: border-box;
    &:hover {
      box-shadow: 0 2px 4px 0 #CFD8DC, 0 0 6px 0 #CFD8DC;
      .discard {
        display: block;
        cursor: pointer;
        color: #0988DE;
      }
    }
  }
  .is-discard {
    background: #F1F7FA;
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
  .model-header,
  .range-time {
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
  }
  .el-tag {
    font-size: 14px;
  }
  .discard {
    position: absolute;
    bottom: 20px;
    right: 20px;
    line-height: 21px;
    display: none;
  }
  .center {
    text-align: center;
    padding: 20px;
  }
  .range-time {
    margin-bottom: 10px;
    line-height: 18px;
    padding-right: 50px;
    .lable {
      float: left;
      width: 70px;
    }
    &:last-child {
      margin-bottom: 0;
    }
  }
  .model-status {
    margin-bottom: 10px;
    height: 24px;
  }
}
</style>
