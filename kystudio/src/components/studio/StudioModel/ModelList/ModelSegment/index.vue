<template>
  <div class="model-segment" v-loading="isLoading">
    <div class="segment-actions clearfix">
      <div class="left ky-no-br-space" v-if="isShowSegmentActions">
        <el-button-group>
          <el-button size="small" icon="el-icon-ksd-add_2" class="ksd-mr-10" type="default" :disabled="!model.partition_desc && segments.length>0" @click="addSegment">Segment</el-button>
        </el-button-group>
        <el-button-group class="ksd-mr-10">
          <el-button size="small" icon="el-icon-ksd-table_refresh" type="primary" :disabled="!selectedSegments.length || hasEventAuthority('refresh')" @click="handleRefreshSegment">{{$t('kylinLang.common.refresh')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-merge" type="default" :disabled="selectedSegments.length < 2 || hasEventAuthority('merge')" @click="handleMergeSegment">{{$t('merge')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-table_delete" type="default" :disabled="!selectedSegments.length || hasEventAuthority('delete')" @click="handleDeleteSegment">{{$t('kylinLang.common.delete')}}</el-button>
        </el-button-group>
        <el-button-group>
          <el-button size="small" icon="el-icon-ksd-repair" type="default" v-if="model.segment_holes.length" @click="handleFixSegment">{{$t('fix')}}<el-tooltip class="item tip-item" :content="$t('fixTips')" placement="bottom"><i class="el-icon-ksd-what"></i></el-tooltip></el-button>
        </el-button-group>
        <!-- <el-button-group>
          <el-button size="small" icon="el-icon-ksd-clear" type="default" :disabled="!segments.length" @click="handlePurgeModel">{{$t('kylinLang.common.purge')}}</el-button>
        </el-button-group> -->

      </div>
      <div class="right">
        <div class="segment-action ky-no-br-space" v-if="!filterSegment">
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
      <el-table border nested  size="medium" :empty-text="emptyText" :data="segments" @selection-change="handleSelectSegments" @sort-change="handleSortChange">
        <el-table-column type="selection" width="44" v-if="!isAutoProject">
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" show-overflow-tooltip width="130" prop="start_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.startTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" show-overflow-tooltip width="130" prop="end_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row,scope.row.endTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          width="145"
          show-overflow-tooltip
          :render-header="renderIndexAmountHeader">
          <template slot-scope="scope">
              <span>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
        <el-table-column prop="status" :label="$t('kylinLang.common.status')" width="114">
          <template slot-scope="scope">
            <el-tooltip :content="$t(scope.row.status)" effect="dark" placement="top">
              <el-tag size="mini" :type="getTagType(scope.row)">{{scope.row.status}}</el-tag>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column prop="last_modified_time" width="135" show-overflow-tooltip :label="$t('modifyTime')">
          <template slot-scope="scope">
            <span>{{scope.row.last_modified_time | toServerGMTDate}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('sourceRecords')" width="140" align="right" prop="row_count" sortable="custom">
        </el-table-column>
        <el-table-column :label="$t('storageSize')" min-width="140" align="right" prop="storage" sortable="custom">
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
        <el-table-column align="left" class-name="ky-hover-icon" fixed="right" :label="$t('kylinLang.common.action')" width="83">
          <template slot-scope="scope">
            <common-tip :content="$t('showDetail')">
              <i class="el-icon-ksd-details" @click="handleShowDetail(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10"
        :background="false"
        :curPage="pagination.page_offset+1"
        :totalSize="totalSegmentCount"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>

    <el-dialog :title="$t('segmentDetail')" append-to-body limited-area :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="isShowDetail" width="720px">
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
          <td>{{segmentTime(detailSegment, detailSegment.startTime) | toServerGMTDate}}</td>
        </tr>
        <tr class="ksd-tr">
          <th>{{$t('endTime')}}</th>
          <td>{{segmentTime(detailSegment, detailSegment.endTime) | toServerGMTDate}}</td>
        </tr>
      </table>
    </el-dialog>

    <!-- <ModelAddSegment v-if="isSegmentOpen" @isWillAddIndex="willAddIndex" @refreshModelList="refreshModelList"/> -->
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { pageCount } from '../../../../../config'
import { handleSuccessAsync, handleError, transToUTCMs, transToServerGmtTime } from '../../../../../util'
import { formatSegments } from './handler'
import ModelAddSegment from '../ModelBuildModal/build.vue'

@Component({
  props: {
    model: {
      type: Object
    },
    isShowSegmentActions: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isAutoProject'
    ])
  },
  components: {
    ModelAddSegment
  },
  methods: {
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      refreshSegments: 'REFRESH_SEGMENTS',
      deleteSegments: 'DELETE_SEGMENTS',
      mergeSegments: 'MERGE_SEGMENTS',
      checkSegments: 'CHECK_SEGMENTS'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
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
    page_offset: 0,
    pageSize: pageCount
  }
  selectedSegmentIds = []
  isShowDetail = false
  isSegmentLoading = false
  isLoading = false
  isSegmentOpen = false
  get selectedSegments () {
    return this.selectedSegmentIds.map(
      segmentId => this.segments.find(segment => segment.id === segmentId)
    )
  }
  segmentTime (row, data) {
    const isFullLoad = row.segRange.date_range_start === 0 && row.segRange.date_range_end === 9223372036854776000
    return isFullLoad ? this.$t('fullLoad') : data
  }
  get emptyText () {
    return this.filter.startDate || this.filter.endDate ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get filterSegment () {
    return this.segments.filter(item => ['Full Load', '全量加载'].includes(item.startTime) && ['Full Load', '全量加载'].includes(item.endTime)).length
  }
  @Watch('filter.startDate')
  @Watch('filter.endDate')
  onDateRangeChange (newVal, oldVal) {
    this.loadSegments()
  }
  async mounted () {
    await this.loadSegments()
    this.$on('refresh', () => {
      this.loadSegments()
    })
  }
  addSegment () {
    let type = 'incremental'
    if (!(this.model.partition_desc && this.model.partition_desc.partition_date_column)) {
      type = 'fullLoad'
    }
    this.isSegmentOpen = true
    this.$nextTick(async () => {
      await this.callModelBuildDialog({
        modelDesc: this.model,
        title: this.$t('addSegment'),
        type: type,
        isAddSegment: true,
        isHaveSegment: !!this.totalSegmentCount,
        disableFullLoad: type === 'fullLoad' && this.segments.length > 0 && this.segments[0].status_to_display !== 'ONLINE' // 已存在全量加载任务时，屏蔽
      })
      await this.loadSegments()
      this.$emit('loadModels')
      this.isSegmentOpen = false
    })
  }
  willAddIndex () {
    this.$emit('willAddIndex')
  }
  // refreshModelList () {
  //   this.$emit('loadModels')
  // }
  // 更改不同状态对应不同type
  getTagType (row) {
    if (row.status === 'ONLINE') {
      return 'success'
    } else if (row.status === 'WARNING') {
      return 'warning'
    } else if (['LOCKED'].includes(row.status)) {
      return 'info'
    } else {
      return ''
    }
  }
  // 状态控制按钮的使用
  hasEventAuthority (type) {
    let typeList = (type) => {
      return this.selectedSegments.length && typeof this.selectedSegments[0] !== 'undefined' ? this.selectedSegments.filter(it => !type.includes(it.status)).length > 0 : false
    }
    if (type === 'refresh') {
      return typeList(['ONLINE', 'WARNING'])
    } else if (type === 'merge') {
      return typeList(['ONLINE'])
    } else if (type === 'delete') {
      return typeList(['ONLINE', 'LOADING', 'REFRESHING', 'MERGING', 'WARNING'])
    }
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
    this.filter.sortBy = prop === 'storage' ? 'bytes_size' : prop
    this.handleCurrentChange(0, this.pagination.pageSize)
  }
  handleCurrentChange (pager, count) {
    this.pagination.page_offset = pager
    this.pagination.pageSize = count
    this.loadSegments()
  }
  async loadSegments () {
    this.isLoading = true
    try {
      const { startDate, endDate, sortBy, reverse } = this.filter
      const projectName = this.currentSelectedProject
      const modelName = this.model.uuid
      const startTime = startDate && transToUTCMs(startDate)
      const endTime = endDate && transToUTCMs(endDate)
      this.isSegmentLoading = true
      const res = await this.fetchSegments({ projectName, modelName, startTime, endTime, sortBy, reverse, ...this.pagination })
      const { total_size, value } = await handleSuccessAsync(res)
      const formatedSegments = formatSegments(this, value)
      this.segments = formatedSegments
      this.totalSegmentCount = total_size
      this.isSegmentLoading = false
      this.isLoading = false
    } catch (e) {
      handleError(e)
      this.isLoading = false
    }
  }
  async handleRefreshSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (segmentIds.length) {
        const projectName = this.currentSelectedProject
        const modelId = this.model.uuid
        let tableData = []
        this.selectedSegments.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(this.segmentTime(seg, seg.startTime))
          obj['end'] = transToServerGmtTime(this.segmentTime(seg, seg.endTime))
          tableData.push(obj)
        })
        await this.callGlobalDetailDialog({
          msg: this.$t('confirmRefreshSegments', {count: segmentIds.length}),
          title: this.$t('refreshSegmentsTitle'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          dialogType: 'tip',
          showDetailBtn: false,
          submitText: this.$t('kylinLang.common.refresh')
        })
        const isSubmit = await this.refreshSegments({ projectName, modelId, segmentIds })
        if (isSubmit) {
          await this.loadSegments()
          this.$emit('loadModels')
          // this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
          this.$message({
            dangerouslyUseHTMLString: true,
            type: 'success',
            duration: 0,
            showClose: true,
            message: (
              <div>
                <span>{this.$t('kylinLang.common.buildSuccess')}</span>
                <a href="javascript:void(0)" onClick={() => this.$router.push('/monitor/job')}>{this.$t('kylinLang.common.toJoblist')}</a>
              </div>
            )
          })
        }
      } else {
        this.$message(this.$t('pleaseSelectSegments'))
      }
    } catch (e) {
      handleError(e)
      this.loadSegments()
    }
  }
  async handleMergeSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (!segmentIds.length) {
        this.$message(this.$t('pleaseSelectSegments'))
      } else {
        const projectName = this.currentSelectedProject
        const modelId = this.model.uuid
        let tableData = []
        this.selectedSegments.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(this.segmentTime(seg, seg.startTime))
          obj['end'] = transToServerGmtTime(this.segmentTime(seg, seg.endTime))
          tableData.push(obj)
        })
        await this.callGlobalDetailDialog({
          msg: this.$t('confirmMergeSegments', {count: segmentIds.length}),
          title: this.$t('mergeSegmentTip'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          dialogType: 'tip',
          showDetailBtn: false,
          submitText: this.$t('merge')
        })
        // 合并segment
        const isSubmit = await this.mergeSegments({ projectName, modelId, segmentIds })
        if (isSubmit) {
          await this.loadSegments()
          this.$emit('loadModels')
          // this.$message({ type: 'success', message: this.$t('kylinLang.common.mergeSuccess') })
          this.$message({
            dangerouslyUseHTMLString: true,
            type: 'success',
            duration: 0,
            showClose: true,
            message: (
              <div>
                <span>{this.$t('kylinLang.common.buildSuccess')}</span>
                <a href="javascript:void(0)" onClick={() => this.$router.push('/monitor/job')}>{this.$t('kylinLang.common.toJoblist')}</a>
              </div>
            )
          })
        }
      }
    } catch (e) {
      handleError(e)
      this.loadSegments()
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
        let tableData = []
        let msg = this.$t('confirmDeleteSegments', {modelName: this.model.name})
        this.selectedSegments.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(this.segmentTime(seg, seg.startTime))
          obj['end'] = transToServerGmtTime(this.segmentTime(seg, seg.endTime))
          tableData.push(obj)
        })
        const res = await this.checkSegments({ projectName, modelId, ids: this.selectedSegmentIds })
        const data = await handleSuccessAsync(res)
        if (data.segment_holes.length) {
          msg = this.$t('segmentWarning', {modelName: this.model.name})
        }
        await this.callGlobalDetailDialog({
          msg: msg,
          title: this.$t('deleteSegmentTip'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          dialogType: 'warning',
          showDetailBtn: false,
          submitText: this.$t('kylinLang.common.delete')
        })
        await this.deleteSegments({ projectName, modelId, segmentIds: segmentIdStr })
        this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
        await this.loadSegments()
        this.$emit('loadModels')
      }
    } catch (e) {
      e !== 'cancel' && handleError(e)
      this.loadSegments()
    }
  }
  handleShowDetail (segment) {
    this.detailSegment = segment
    this.isShowDetail = true
  }
  handleSelectSegments (selectedSegments) {
    this.selectedSegmentIds = selectedSegments.map(segment => segment.id)
  }
  renderIndexAmountHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('kylinLang.common.indexAmount')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('kylinLang.common.indexAmountTip')}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  // async handlePurgeModel () {
  //   if (this.segments.length > 0) {
  //     this.$emit('purge-model', this.model)
  //   } else {
  //     this.$message({ type: 'info', message: this.$t('segmentIsEmpty') })
  //   }
  // }
  handleFixSegment () {
    this.$emit('auto-fix')
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
  background-color: @fff;
  padding: 10px;
  border: 1px solid @line-border-color4;
  margin: 15px;
  .segment-actions {
    margin-bottom: 10px;
    .el-button .el-icon-ksd-what {
      color: @base-color;
      margin-left: 5px;
    }
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
