<template>
  <div class="model-segment">
    <div class="segment-actions clearfix">
      <div class="left">
        <el-button size="medium" type="primary" icon="el-icon-ksd-table_refresh" @click="handleRefreshSegment">{{$t('kylinLang.common.refresh')}}</el-button>
        <el-button size="medium" type="primary" icon="el-icon-ksd-merge" @click="handleMergeSegment">{{$t('merge')}}</el-button>
        <el-button size="medium" type="default" icon="el-icon-ksd-table_delete" @click="handleDeleteSegment">{{$t('kylinLang.common.delete')}}</el-button>
      </div>
      <div class="right">
        <div class="segment-action">
          <span class="input-label">
            {{$t('segmentPeriod')}}
          </span>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="medium"
            v-model="filter.startDate"
            :picker-options="{ disabledDate: getStartDateLimit }"
            :placeholder="$t('chooseStartDate')">
          </el-date-picker>
          <span class="input-split">-</span>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="medium"
            v-model="filter.endDate"
            :picker-options="{ disabledDate: getEndDateLimit }"
            :placeholder="$t('chooseEndDate')">
          </el-date-picker>
        </div>
        <div class="segment-action" v-if="false">
          <span class="input-label">{{$t('primaryPartition')}}</span>
          <el-select v-model="filter.mpValues" size="medium" :placeholder="$t('pleaseChoose')">
            <el-option
              label="Shanghai"
              value="Shanghai">
            </el-option>
          </el-select>
        </div>
      </div>
    </div>

    <div class="segment-views">
      <div class="segment-charts">
        <SegmentChart
          v-loading="isSegmentLoading"
          :segments="segments"
          :scaleType="scaleTypes[scaleTypeIdx]"
          @input="handleSelectSegment"
          @load-more="handleLoadMore">
          <Waypoint class="load-more" :scrollable-ancestor="scrollableAncestor" @enter="handleLoadMore"></Waypoint>
        </SegmentChart>
        <div class="chart-actions">
          <div class="icon-button" @click="handleAddZoom">
            <img :src="iconAdd" />
          </div>
          <div class="icon-button" @click="handleMinusZoom">
            <img :src="iconReduce" />
          </div>
          <div>{{zoom.toFixed(0)}}</div>
          <div>%</div>
          <div class="linear-chart"></div>
          <div class="empty-chart"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import SegmentChart from './SegmentChart/SegmentChart'
import Waypoint from '../../../../common/Waypoint/Waypoint'
import { pageSizeMapping } from '../../../../../config'
import { handleSuccessAsync, handleError } from '../../../../../util'
import iconAdd from './icon_add.svg'
import iconReduce from './icon_reduce.svg'
import { formatSegments } from './handle'
// import { getMockSegments } from './mock'

@Component({
  props: {
    model: {
      type: Object
    },
    isShowActions: {
      type: Boolean,
      default: true
    },
    isShowSettings: {
      type: Boolean,
      default: true
    }
  },
  components: {
    SegmentChart,
    Waypoint
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      refreshSegments: 'REFRESH_SEGMENTS',
      deleteSegments: 'DELETE_SEGMENTS'
    }),
    ...mapActions('SourceTableModal', {
      callSourceTableModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class ModelSegment extends Vue {
  iconAdd = iconAdd
  iconReduce = iconReduce
  zoom = 100
  segments = []
  filter = {
    mpValues: '',
    startDate: '',
    endDate: ''
  }
  scaleTypes = ['hour', 'day', 'month', 'year']
  scaleTypeIdx = 1
  scrollableAncestor = null
  pagination = {
    pageOffset: 0,
    pageSize: pageSizeMapping.SEGMENT_CHART
  }
  selectedSegmentIds = []
  isSegmentLoading = false
  get selectedSegments () {
    return this.selectedSegmentIds.map(
      segmentId => this.segments.find(segment => segment.id === segmentId)
    )
  }
  @Watch('filter.startDate')
  @Watch('filter.endDate')
  @Watch('scaleTypeIdx')
  onDateRangeChange (val) {
    this.loadSegments()
  }
  async mounted () {
    await this.loadSegments()
    this.scrollableAncestor = this.$el.querySelector('.segment-chart .container')
  }
  getStartDateLimit (time) {
    return this.filter.endDate ? time.getTime() > this.filter.endDate.getTime() : false
  }
  getEndDateLimit (time) {
    return this.filter.startDate ? time.getTime() < this.filter.startDate.getTime() : false
  }
  addPagination () {
    this.pagination.pageOffset++
  }
  clearPagination () {
    this.pagination.pageOffset = 0
  }
  async loadSegments (options) {
    try {
      const { isReset = true } = options || {}
      const { startDate, endDate } = this.filter
      const projectName = this.currentSelectedProject
      const modelName = this.model.name
      const startTime = startDate && startDate.getTime()
      const endTime = endDate && endDate.getTime()

      this.isSegmentLoading = true
      isReset && this.clearPagination()
      const res = await this.fetchSegments({ projectName, modelName, startTime, endTime, ...this.pagination })
      const { size, segments } = await handleSuccessAsync(res)
      const formatedSegments = formatSegments(segments)
      if (size > this.segments.length) {
        this.segments = isReset ? formatedSegments : this.segments.concat(formatedSegments)
        this.addPagination()
      }
      this.isSegmentLoading = false
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
  async handleLoadMore () {
    try {
      await this.loadSegments({ isReset: false })
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
  handleAddZoom () {
    if (this.scaleTypeIdx > 0) {
      this.scaleTypeIdx--
    }
  }
  handleMinusZoom () {
    if (this.scaleTypeIdx < this.scaleTypes.length - 1) {
      this.scaleTypeIdx++
    }
  }
  handleSelectSegment (selectedSegmentIds, isSelectable) {
    if (isSelectable) {
      this.selectedSegmentIds = selectedSegmentIds
    } else {
      this.$message(this.$t('selectContinueSegments'))
    }
  }
  async handleRefreshSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (segmentIds.length) {
        const projectName = this.currentSelectedProject
        const modelName = this.model.name
        const isSubmit = await this.refreshSegments({ projectName, modelName, segmentIds })
        isSubmit && this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
      } else {
        this.$message(this.$t('pleaseSelectSegments'))
      }
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
  async handleMergeSegment () {
    const projectName = this.currentSelectedProject
    const model = this.model
    await this.callSourceTableModal({ editType: 'dataMerge', model, projectName })
  }
  async handleDeleteSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (segmentIds.length) {
        const projectName = this.currentSelectedProject
        const modelName = this.model.name
        const confirmTitle = this.$t('kylinLang.common.notice')
        const confirmMessage = this.$t('confirmDeleteSegments')
        const confirmButtonText = this.$t('kylinLang.common.ok')
        const cancelButtonText = this.$t('kylinLang.common.cancel')
        await this.$confirm(confirmMessage, confirmTitle, { type: 'warning', confirmButtonText, cancelButtonText })
        await this.deleteSegments({ projectName, modelName, segmentIds })
        this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
        await this.loadSegments()
      } else {
        this.$message(this.$t('pleaseSelectSegments'))
      }
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-segment {
  padding: 20px 0;
  margin-bottom: 20px;
  .segment-actions {
    margin-bottom: 15px;
    .left {
      float: left;
      display: none;
    }
    .right {
      float: right;
    }
    .segment-action {
      display: inline-block;
      margin-right: 10px;
    }
    .segment-action:last-child {
      margin: 0;
    }
    .el-input {
      width: 200px;
    }
    .el-select .el-input {
      width: 150px;
    }
  }
  .input-label {
    margin-right: 6px;
  }
  .input-split {
    margin: 0 7px;
  }
  .segment-charts {
    position: relative;
    .segment-chart {
      width: calc(~'100% - 25px');
    }
    .stage {
      position: relative;
    }
    .chart-actions {
      position: absolute;
      right: 0;
      top: 10px;
      text-align: center;
    }
    .icon-button {
      width: 21px;
      height: 21px;
      padding: 6px;
      background: #0988DE;
      margin: 0 auto 5px auto;
      cursor: pointer;
    }
    .icon-button img {
      display: block;
      width: 9px;
      height: 9px;
    }
    .linear-chart {
      width: 10px;
      height: 50px;
      background: linear-gradient(0deg, #FFCD58 0%, #FAC954 36%, #FF0000 100%);
      border: 1px solid #CFD8DC;
      margin: 10px auto 5px auto;
    }
    .empty-chart {
      width: 10px;
      height: 10px;
      margin: 0 auto;
      background: #FFFFFF;
      border: 1px solid #B0BEC5;
    }
  }
  .title {
    font-size: 16px;
    color: #263238;
    margin: 20px 0 10px 0;
  }
  .segment-settings {
    display: none;
  }
  .settings {
    padding: 15px 0;
    border: 1px solid #B0BEC5;
    background: white;
  }
  .setting {
    padding: 0 23px;
    &:not(:last-child) {
      border-bottom: 1px solid #B0BEC5;
      padding-bottom: 10px;
    }
    &:not(:first-child) {
      padding-top: 10px;
    }
    .el-input {
      width: 220px;
    }
    .setting-checkbox {
      float: left;
      position: relative;
      transform: translateY(9px);
    }
    .setting-input {
      margin-left: 117px;
      &:not(:last-child) {
        padding-bottom: 10px;
      }
      &:not(:nth-child(2)) .delete-setting {
        margin-left: 55px;
      }
    }
    .is-circle {
      margin-left: 10px;
    }
    .el-select {
      width: 100px;
      margin-left: 5px;
      .el-input {
        width: 100%;
      }
    }
  }
  .load-more {
    position: absolute;
    right: 0;
    top: 0;
    height: 100%;
    width: 1px;
  }
}
</style>
