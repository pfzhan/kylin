<template>
  <div class="model-segment">
    <div class="segment-actions clearfix">
      <div class="left ky-no-br-space">
        <el-button size="small" type="primary" :disabled="!selectedSegments.length" v-if="!isAutoProject" @click="handleRefreshSegment">{{$t('kylinLang.common.refresh')}}</el-button>
        <!-- <el-button size="medium" type="primary" icon="el-icon-ksd-merge" @click="handleMergeSegment">{{$t('merge')}}</el-button> -->
        <el-button size="small" type="default" :disabled="!selectedSegments.length" v-if="!isAutoProject" @click="handleDeleteSegment">{{$t('kylinLang.common.delete')}}</el-button>
        <el-button size="small" type="default" v-if="!isAutoProject" @click="handlePurgeModel">{{$t('kylinLang.common.purge')}}</el-button>
      </div>
      <div class="right">
        <div class="segment-action ky-no-br-space">
          <span class="ksd-mr-5 ksd-fs-14">{{$t('segmentPeriod')}}</span>
          <el-date-picker
            class="date-picker ksd-mr-5"
            type="datetime"
            size="small"
            v-model="filter.startDate"
            :is-auto-complete="true"
            :picker-options="{ disabledDate: getStartDateLimit }"
            :placeholder="$t('chooseStartDate')">
          </el-date-picker>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="small"
            v-model="filter.endDate"
            :is-auto-complete="true"
            :picker-options="{ disabledDate: getEndDateLimit }"
            :placeholder="$t('chooseEndDate')">
          </el-date-picker>
        </div>
        <div class="segment-action" v-if="false">
          <span class="input-label ksd-mr-5">{{$t('primaryPartition')}}</span>
          <el-select v-model="filter.mpValues" size="small" :placeholder="$t('pleaseChoose')">
            <el-option
              label="Shanghai"
              value="Shanghai">
            </el-option>
          </el-select>
        </div>
      </div>
    </div>

    <div class="segment-views ksd-mb-15">
      <el-table border nested  size="medium" :data="segments" @selection-change="handleSelectSegments" @sort-change="handleSortChange">
        <el-table-column type="selection" width="40">
        </el-table-column>
        <el-table-column prop="id" label="Segment Id">
        </el-table-column>
        <el-table-column prop="status" :label="$t('kylinLang.common.status')" width="114">
        </el-table-column>
        <el-table-column :label="$t('storageSize')" width="145" align="right" prop="storage" sortable>
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" width="208" prop="start_time" sortable>
          <template slot-scope="scope">{{scope.row.startTime | utcTime}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" width="208" prop="end_time" sortable>
          <template slot-scope="scope">{{scope.row.endTime | utcTime}}</template>
        </el-table-column>
        <el-table-column class-name="ky-hover-icon" :label="$t('kylinLang.common.action')" width="83">
          <template slot-scope="scope">
            <common-tip :content="$t('showDetail')" class="ksd-ml-10">
              <i class="el-icon-ksd-type_date" @click="handleShowDetail(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10"
        layout="prev, pager, next"
        :background="false"
        :totalSize="totalSegmentCount"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>

    <el-dialog :title="$t('segmentDetail')" append-to-body :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="isShowDetail" width="720px">
      <table class="ksd-table segment-detail" v-if="detailSegment">
        <tr class="ksd-tr">
          <th>{{$t('segmentID')}}</th>
          <td>{{detailSegment.id}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('segmentName')}}</th>
          <td>{{detailSegment.name}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('segmentPath')}}</th>
          <td>{{detailSegment.segmentPath}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('fileNumber')}}</th>
          <td>{{detailSegment.fileNumber}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('storageSize1')}}</th>
          <td>{{detailSegment.bytes_size | dataSize}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('startTime')}}</th>
          <td>{{detailSegment.startTime | utcTime}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('endTime')}}</th>
          <td>{{detailSegment.endTime | utcTime}}</td>
        </tr>
      </table>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { pageCount } from '../../../../../config'
import { handleSuccessAsync, handleError, transToUTCMs } from '../../../../../util'
import { formatSegments } from './handler'

@Component({
  props: {
    model: {
      type: Object
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isAutoProject'
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
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales
})
export default class ModelSegment extends Vue {
  segments = []
  detailSegment = null
  totalSegmentCount = 0
  filter = {
    mpValues: '',
    startDate: '',
    endDate: '',
    reverse: true,
    sortBy: 'last_modify'
  }
  pagination = {
    pageOffset: 0,
    pageSize: pageCount
  }
  selectedSegmentIds = []
  isShowDetail = false
  isSegmentLoading = false
  get selectedSegments () {
    return this.selectedSegmentIds.map(
      segmentId => this.segments.find(segment => segment.id === segmentId)
    )
  }
  @Watch('filter.startDate')
  @Watch('filter.endDate')
  onDateRangeChange (newVal, oldVal) {
    this.loadSegments()
  }
  async mounted () {
    await this.loadSegments()
  }
  getStartDateLimit (time) {
    return this.filter.endDate ? time.getTime() > this.filter.endDate.getTime() : false
  }
  getEndDateLimit (time) {
    return this.filter.startDate ? time.getTime() < this.filter.startDate.getTime() : false
  }
  handleSortChange ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sortBy = prop
    this.loadSegments()
  }
  handleCurrentChange (pager, count) {
    this.pagination.pageOffset = pager
    this.pagination.pageSize = count
    this.loadSegments()
  }
  async loadSegments () {
    try {
      const { startDate, endDate, sortBy, reverse } = this.filter
      const projectName = this.currentSelectedProject
      const modelName = this.model.uuid
      const startTime = startDate && transToUTCMs(startDate)
      const endTime = endDate && transToUTCMs(endDate)

      this.isSegmentLoading = true
      const res = await this.fetchSegments({ projectName, modelName, startTime, endTime, sortBy, reverse, ...this.pagination })
      const { size, segments } = await handleSuccessAsync(res)
      const formatedSegments = formatSegments(this, segments)
      this.segments = formatedSegments
      this.totalSegmentCount = size
      this.isSegmentLoading = false
    } catch (e) {
      handleError(e)
    }
  }
  async handleRefreshSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (segmentIds.length) {
        const projectName = this.currentSelectedProject
        const modelId = this.model.uuid
        await this.callGlobalDetailDialog({
          msg: this.$t('confirmRefreshSegments', {count: segmentIds.length}),
          title: this.$t('refreshSegmentsTitle'),
          details: segmentIds,
          dialogType: 'tip'
        })
        const isSubmit = await this.refreshSegments({ projectName, modelId, segmentIds })
        if (isSubmit) {
          await this.loadSegments()
          this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
        }
      } else {
        this.$message(this.$t('pleaseSelectSegments'))
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleMergeSegment () {
    try {
      const projectName = this.currentSelectedProject
      const model = this.model
      await this.callSourceTableModal({ editType: 'dataMerge', model, projectName })
    } catch (e) {
      handleError(e)
    }
  }
  async handleDeleteSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (!segmentIds.length) {
        this.$message(this.$t('pleaseSelectSegments'))
      } else {
        const projectName = this.currentSelectedProject
        const modelId = this.model.uuid
        const segmentIdStr = this.selectedSegmentIds.join(',')
        await this.callGlobalDetailDialog({
          msg: this.$t('confirmDeleteSegments', {count: segmentIds.length}),
          title: this.$t('deleteSegmentTip'),
          details: segmentIds,
          dialogType: 'warning'
        })
        await this.deleteSegments({ projectName, modelId, segmentIds: segmentIdStr })
        this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
        await this.loadSegments()
      }
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
  handleShowDetail (segment) {
    this.detailSegment = segment
    this.isShowDetail = true
  }
  handleSelectSegments (selectedSegments) {
    this.selectedSegmentIds = selectedSegments.map(segment => segment.id)
  }
  async handlePurgeModel () {
    if (this.segments.length > 0) {
      this.$emit('purge-model', this.model)
    } else {
      this.$message({ type: 'info', message: this.$t('segmentIsEmpty') })
    }
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.segment-detail.ksd-table {
  &.ksd-table{
    table-layout:fixed;
    th {
      width:130px;
      
    }
  }
}
.model-segment {
  .segment-actions {
    margin-bottom: 10px;
    .left {
      float: left;
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
  .input-split {
    margin: 0 7px;
  }
  .segment-charts {
    position: relative;
  }
  .title {
    font-size: 16px;
    color: #263238;
    margin: 20px 0 10px 0;
  }
  .ksd-table td {
    padding-top: 10px;
    padding-bottom: 10px;
  }
}
</style>
