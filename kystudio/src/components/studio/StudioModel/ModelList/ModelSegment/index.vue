<template>
  <div class="model-segment">
    <div class="segment-actions clearfix">
      <div class="left">
        <el-button size="medium" type="primary" icon="el-icon-ksd-table_refresh" @click="handleRefreshSegment">{{$t('kylinLang.common.refresh')}}</el-button>
        <!-- <el-button size="medium" type="primary" icon="el-icon-ksd-merge" @click="handleMergeSegment">{{$t('merge')}}</el-button> -->
        <el-button size="medium" type="default" icon="el-icon-ksd-table_delete" @click="handleDeleteSegment">{{$t('kylinLang.common.delete')}}</el-button>
        <el-button size="medium" type="default" icon="el-icon-ksd-table_delete" v-if="model.management_type!=='TABLE_ORIENTED'" @click="handlePurgeModel">{{$t('kylinLang.common.purge')}}</el-button>
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
            :is-auto-complete="true"
            :picker-options="{ disabledDate: getStartDateLimit }"
            :placeholder="$t('chooseStartDate')">
          </el-date-picker>
          <span class="input-split">-</span>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="medium"
            v-model="filter.endDate"
            :is-auto-complete="true"
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
      <el-table border :data="segments" @selection-change="handleSelectSegments" @sort-change="handleSortChange">
        <el-table-column type="selection" width="35" align="center">
        </el-table-column>
        <el-table-column prop="id" label="Segment Id">
        </el-table-column>
        <el-table-column prop="status" :label="$t('kylinLang.common.status')" width="155" align="center">
        </el-table-column>
        <el-table-column :label="$t('storageSize')" width="145" header-align="center" align="right" prop="storage" sortable>
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" align="center" prop="start_time" sortable>
          <template slot-scope="scope">{{scope.row.startTime | utcTime}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" align="center" prop="end_time" sortable>
          <template slot-scope="scope">{{scope.row.endTime | utcTime}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" width="100" align="center">
          <template slot-scope="scope">
            <i class="el-icon-ksd-type_date" @click="handleShowDetail(scope.row)"></i>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mt-20"
        :totalSize="totalSegmentCount"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>

    <el-dialog :title="$t('segmentDetail')" :visible.sync="isShowDetail">
      <table class="ksd-table" v-if="detailSegment">
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
  @Watch('filter.sortBy')
  @Watch('filter.reverse')
  onDateRangeChange (val) {
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
      const modelName = this.model.name
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
        const segmentNames = this.selectedSegments.map(segment => segment.name)
        const segmentArray = `[${segmentNames.join('\r\n')}]`
        const projectName = this.currentSelectedProject
        const modelName = this.model.name
        const confirmTitle = this.$t('kylinLang.common.notice')
        const confirmMessage = this.$t('confirmRefreshSegments', { segmentArray })
        const confirmButtonText = this.$t('kylinLang.common.ok')
        const cancelButtonText = this.$t('kylinLang.common.cancel')
        const customClass = 'pre'
        await this.$confirm(confirmMessage, confirmTitle, { type: 'warning', confirmButtonText, cancelButtonText, customClass })
        const isSubmit = await this.refreshSegments({ projectName, modelName, segmentIds })
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
        const modelName = this.model.name
        const confirmTitle = this.$t('kylinLang.common.notice')
        const confirmMessage = this.$t('confirmDeleteSegments')
        const confirmButtonText = this.$t('kylinLang.common.ok')
        const cancelButtonText = this.$t('kylinLang.common.cancel')
        await this.$confirm(confirmMessage, confirmTitle, { type: 'warning', confirmButtonText, cancelButtonText })
        await this.deleteSegments({ projectName, modelName, segmentIds })
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

.model-segment {
  padding: 20px 0;
  margin-bottom: 20px;
  .segment-actions {
    margin-bottom: 15px;
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
  .input-label {
    margin-right: 6px;
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
