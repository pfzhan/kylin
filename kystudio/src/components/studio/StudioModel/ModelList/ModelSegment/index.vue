<template>
<div>
  <div class="model-segment" v-loading="isLoading" v-if="!isSubPartitionList">
    <div class="segment-actions clearfix">
      <el-popover
        ref="segmentPopover"
        placement="right"
        width="500"
        trigger="hover">
        <div style="padding:10px">
          <div class="ksd-mb-10">{{$t('segmentSubTitle')}}</div>
          <div class="ksd-center">
            <img src="../../../../../assets/img/image-seg.png" width="400px" alt="">
          </div>
        </div>
      </el-popover>
      <div class="ksd-title-label-small ksd-mb-10">
        <span>{{$t('segmentList')}}<i v-popover:segmentPopover class="el-icon-question ksd-ml-2"></i></span>
        <!-- <span class="right ky-a-like" v-if="$store.state.project.multi_partition_enabled && model.multi_partition_desc" @click="subParValMana(model)">{{$t('viewSubParValuesBtn')}}</span> -->
      </div>
      <div class="left ky-no-br-space" v-if="isShowSegmentActions">
        <el-button-group v-if="$store.state.project.emptySegmentEnable">
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
      </div>
    </div>

    <div class="segment-views ksd-mb-15">
      <el-table border nested  size="medium" :empty-text="emptyText" :data="segments" @selection-change="handleSelectSegments" @sort-change="handleSortChange">
        <el-table-column type="selection" width="44" v-if="!isAutoProject">
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" show-overflow-tooltip prop="start_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.startTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" show-overflow-tooltip prop="end_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row,scope.row.endTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          width="185"
          show-overflow-tooltip
          v-if="$store.state.project.multi_partition_enabled && model.multi_partition_desc"
          :render-header="renderSubPartitionAmountHeader">
          <template slot-scope="scope">
            <el-tooltip :content="$t('disabledSubPartitionEnter', {status: scope.row.status_to_display})" :disabled="scope.row.status_to_display !== 'LOCKED'" effect="dark" placement="top">
              <span :class="['ky-a-like', {'is-disabled': scope.row.status_to_display === 'LOCKED' || !model.multi_partition_desc}]" @click="showSubParSegments(scope.row)">{{scope.row.multi_partition_count}} / {{scope.row.multi_partition_count_total}}</span>
            </el-tooltip>
          </template>
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
              <span v-if="['LOADING', 'REFRESHING', 'MERGING'].indexOf(scope.row.status_to_display) !== -1">-/{{scope.row.index_count_total}}</span>
              <span v-else>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
        <el-table-column prop="status_to_display" :label="$t('kylinLang.common.status')" width="114">
          <template slot-scope="scope">
            <el-tooltip :content="$t(scope.row.status_to_display)" effect="dark" placement="top">
              <el-tag size="mini" :type="getTagType(scope.row, 'segment')">{{scope.row.status_to_display}}</el-tag>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column prop="last_modified_time" show-overflow-tooltip :label="$t('modifyTime')">
          <template slot-scope="scope">
            <span>{{scope.row.last_modified_time | toServerGMTDate}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('sourceRecords')" width="140" align="right" prop="source_count" sortable="custom">
        </el-table-column>
        <el-table-column :label="$t('storageSize')" width="140" align="right" prop="storage" sortable="custom">
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
        <el-table-column align="left" class-name="ky-hover-icon" fixed="right" :label="$t('kylinLang.common.action')" width="83">
          <template slot-scope="scope">
            <common-tip :content="$t('showDetail')">
              <i class="el-icon-ksd-desc" @click="handleShowDetail(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10"
        :background="false"
        :refTag="pageRefTags.segmentPager"
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
          <td class="segment-path">{{detailSegment.segmentPath}}</td>
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

    <el-dialog
      :title="$t('mergeSegmentsTitle')"
      append-to-body
      limited-area
      class="merge-comfirm"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="isShowMergeConfirm"
      width="480px">
      <p class="merge-notices"><i class="el-icon-ksd-alert"></i>{{$t('mergeSegmentDesc')}}<span class="review-details" @click="showDetails = !showDetails">{{$t('showDetail')}}<i :class="[showDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02', 'arrow']"></i></span></p>
      <div class="detail-content" v-if="showDetails">
        <p v-for="item in getDetails" :key="item.value">{{item.text}}</p>
      </div>
      <div class="ksd-mt-20 ksd-title-label-small">{{$t('afterMergeSegment')}}</div>
      <el-table class="ksd-mt-10"
        border
        nested
        size="small"
        max-height="420"
        :data="mergedSegments">
        <el-table-column
          prop="start"
          :label="$t('kylinLang.common.startTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{scope.row.start | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          prop="end"
          :label="$t('kylinLang.common.endTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{scope.row.end | toServerGMTDate}}</template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="isShowMergeConfirm = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="mergeLoading" @click="handleSubmitMerge()">{{$t('merge')}}</el-button>
    </div>
    </el-dialog>

    <el-dialog
      :title="$t('refreshSegmentsTitle')"
      append-to-body
      limited-area
      class="refresh-comfirm"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="isShowRefreshConfirm"
      width="480px">
      <el-alert type="tip" show-icon class="ksd-ptb-0" :show-background="false" :closable="false">
        <span v-if="detailTableData.length">{{$t('confirmRefreshSegments2')}}</span>
        <span v-else>{{$t('confirmRefreshSegments')}}</span>
      </el-alert>
      <div class="ksd-mt-10" v-if="detailTableData.length">
        <el-radio v-model="refreshType" label="refreshOrigin">{{$t('buildCurrentIndexes')}}</el-radio>
        <el-radio v-model="refreshType" label="refreshAll">{{$t('buildAllIndexes')}}</el-radio>
      </div>
      <el-alert v-if="detailTableData.length && refreshType === 'refreshAll'" class="ksd-mt-10 ksd-ptb-0 build-all-tips" type="info" show-icon :show-background="false" :closable="false">
        <span>{{$t('buildAllIndexesTips')}}</span>
      </el-alert>
      <el-table class="ksd-mt-10"
        border
        nested
        size="small"
        max-height="420"
        v-if="detailTableData.length"
        :data="detailTableData">
        <el-table-column
          prop="start"
          :label="$t('kylinLang.common.startTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.startTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          prop="end"
          :label="$t('kylinLang.common.endTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.endTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          prop="currentIndexes"
          align="right"
          width="130"
          :label="$t('currentIndexes')">
          <template slot-scope="scope">
            <span>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="refreshLoading" @click="handleSubmit()">{{$t('kylinLang.common.refresh')}}</el-button>
    </div>
    </el-dialog>

    <!-- <ModelAddSegment v-if="isSegmentOpen" @isWillAddIndex="willAddIndex" @refreshModelList="refreshModelList"/> -->
  </div>
  <div class="subPartition-segment" v-else>
    <div class="clearfix">
      <div class="ksd-fleft">
        <el-tooltip :content="$t('kylinLang.common.back')" effect="dark" placement="top">
          <i class="back-btn el-icon-ksd-iconback_1414" @click="backToSegmentList"></i>
        </el-tooltip>
        <span class="ksd-title-label">{{$t('subParValuesTitle')}}</span>
        <span class="segment-range">(Segment {{currentSegment.startTime | toServerGMTDate}} {{$t('kylinLang.query.to')}} {{currentSegment.endTime | toServerGMTDate}})</span>
      </div>
      <span class="ksd-fright ky-a-like" v-if="$store.state.project.multi_partition_enabled && availableMenus.includes('modelsubpartitionvalues')" @click="subParValMana(model)">{{$t('viewSubParValuesBtn')}}</span>
    </div>
    <div class="clearfix">
      <div class="ksd-fleft ksd-mt-10 ky-no-br-space" v-if="isShowSegmentActions">
        <el-button-group>
          <el-tooltip :content="$t('noIndexTipByBuild')" :disabled="!!model.total_indexes" effect="dark" placement="top">
            <el-button size="small" icon="el-icon-ksd-add_2" :class="['ksd-mr-10', {'disabled-build': controlBuildSubSegment}]" type="default" @click="!controlBuildSubSegment && buildSubSegment()">{{$t('buildSubSegment')}}</el-button>
          </el-tooltip>
        </el-button-group>
        <el-button-group class="ksd-mr-10">
          <el-button size="small" icon="el-icon-ksd-table_refresh" type="primary" :disabled="!selectedSubPartitionSegments.length || hasSubPartitionEventAuthority('refresh')" :loading="refreshSubPartitionLoading" @click="handleRefreshSubSegment">{{$t('kylinLang.common.refresh')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-table_delete" type="default" :disabled="!selectedSubPartitionSegments.length || hasSubPartitionEventAuthority('delete')" @click="handleDeleteSubSegment">{{$t('kylinLang.common.delete')}}</el-button>
        </el-button-group>
      </div>
      <div class="ksd-fright">
        <el-input class="ksd-mt-10" :placeholder="$t('searchPlaceholder')" prefix-icon="el-icon-search" v-global-key-event.enter.debounce="onFilterChange" @clear="onFilterChange()" v-model="subParValuesFilter"></el-input>
      </div>
    </div>
    <el-table
      border
      ref="subPartitionValuesTable"
      :data="pagerTableData"
      style="width: 100%"
      class="ksd-mt-10"
      @sort-change="subPartitionSortChange"
      @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="44"> </el-table-column>
      <el-table-column :label="$t('subParValuesTitle')" prop="values" sortable="custom">
        <template slot-scope="scope">
          {{scope.row.values[0]}}
        </template>
      </el-table-column>
      <el-table-column prop="status" :label="$t('kylinLang.common.status')" width="114">
        <template slot-scope="scope">
          <el-tooltip :content="$t(`partition${scope.row.status}`)" effect="dark" placement="top">
            <el-tag size="mini" :type="getTagType(scope.row, 'subPartition')">{{scope.row.status}}</el-tag>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column prop="last_modified_time" show-overflow-tooltip :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span>{{scope.row.last_modified_time | toServerGMTDate}}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('sourceRecords')" width="160" align="right" prop="source_count" sortable="custom">
      </el-table-column>
      <el-table-column :label="$t('storageSize')" width="140" align="right" prop="bytes_size" sortable="custom">
        <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
      </el-table-column>
    </el-table>
    <kap-pager :totalSize="subPartitionSegmentTotal" :curPage="subSegfilter.page_offset+1"  v-on:handleCurrentChange='pageSizeChange' ref="subPartitionValuesPager" :refTag="pageRefTags.subPartitionSegmentPager" :perPageSize="subSegfilter.page_size" class="ksd-mtb-10 ksd-center" ></kap-pager>

    <el-dialog
      :visible.sync="buildSubParValueVisible"
      width="560px"
      class="build-sub-par-dialog"
      :close-on-click-modal="false"
      :before-close="handleClosebuildSub">
      <span slot="title" class="ksd-title-label">{{$t('buildSubSegment')}}</span>
      <el-alert :title="$t('buildSubParDesc')" class="ksd-mb-10" type="tip" show-icon :closable="false"></el-alert>
      <div class="ksd-mb-5 ksd-title-label-small">{{$t('segmentPeriod2')}}</div>
      <div class="ky-no-br-space ksd-mb-20">
        <el-input style="width:210px;" :disabled="true" :value="currentSegment.startTime | toServerGMTDate"></el-input>
        <el-input style="width:210px;" class="ksd-ml-5" :disabled="true" :value="currentSegment.endTime | toServerGMTDate"></el-input>
      </div>
      <div class="ksd-mb-5 ksd-title-label-small">{{$t('selectSubPartitionValues')}}</div>
      <div class="arealabel-block">
        <arealabel
          :class="['select-sub-partition', {'error-border': duplicateValueError}]"
          ref="selectSubPartition"
          :duplicateremove="false"
          splitChar=","
          :remoteSearch="true"
          :isNeedNotUpperCase="true"
          :allowcreate="true"
          :isSignSameValue="true"
          :remote-method="filterPartitions"
          :selectedlabels="addedPartitionValues"
          :placeholder="$t('multiPartitionPlaceholder')"
          @duplicateTags="checkDuplicateValue"
          @refreshData="refreshPartitionValues"
          @removeTag="removeSelectedMultiPartition"
          :labels="partitionOptions">
        </arealabel>
        <p class="duplicate-tips" v-if="duplicateValueError"><span class="error-msg">{{$t('duplicatePartitionValueTip')}}</span><span class="clear-value-btn" @click="removeDuplicateValue"><i class="el-icon-ksd-clear ksd-mr-5"></i>{{$t('removeDuplicateValue')}}</span></p>
      </div>
      <span slot="footer" class="dialog-footer">
        <div class="ksd-fleft">
          <el-checkbox v-model="isMultipleBuild">
            <span>{{$t('multipleBuild')}}</span>
            <common-tip placement="top" :content="$t('multipleBuildTip')">
              <span class='el-icon-ksd-what'></span>
            </common-tip>
          </el-checkbox>
        </div>
        <el-button plain @click="handleClosebuildSub">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :loading="buildSubParValueLoading" :disabled="duplicateValueError || !partition_values.length" @click="buildSubParValue">{{$t('build')}}</el-button>
      </span>
    </el-dialog>
  </div>
</div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { pageCount, pageRefTags } from '../../../../../config'
import { handleSuccessAsync, handleError, transToUTCMs, transToServerGmtTime, split_array, getQueryString } from '../../../../../util'
import { kapConfirm, postCloudUrlMessage } from 'util/business'
import { formatSegments } from './handler'
import ModelAddSegment from '../ModelBuildModal/build.vue'
import arealabel from '../../../../common/area_label.vue'

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
      'isAutoProject',
      'availableMenus'
    ])
  },
  components: {
    ModelAddSegment,
    arealabel
  },
  methods: {
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      refreshSegments: 'REFRESH_SEGMENTS',
      deleteSegments: 'DELETE_SEGMENTS',
      mergeSegments: 'MERGE_SEGMENTS',
      mergeSegmentCheck: 'MERGE_SEGMENT_CHECK',
      checkSegments: 'CHECK_SEGMENTS',
      buildSubPartitions: 'BUILD_SUB_PARTITIONS',
      fetchSubPartitions: 'FETCH_SUB_PARTITIONS',
      deleteSubPartition: 'DELETE_SUB_PARTITION',
      refreshSubPartition: 'REFRESH_SUB_PARTITION',
      fetchSubPartitionValues: 'FETCH_SUB_PARTITION_VALUES'
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
  pageRefTags = pageRefTags
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
    pageSize: +localStorage.getItem(this.pageRefTags.segmentPager) || pageCount
  }
  selectedSegmentIds = []
  isShowDetail = false
  isSegmentLoading = false
  isLoading = false
  isSegmentOpen = false
  isShowMergeConfirm = false
  showDetails = false
  mergeLoading = false
  mergedSegments = []
  isShowRefreshConfirm = false
  refreshLoading = false
  refreshType = 'refreshOrigin'
  detailTableData = []
  isSubPartitionList = false
  currentSegment = null
  subParValuesFilter = ''
  subSegmentfilter = {
    page_offset: 0,
    page_size: 2000, // 拿全量，前端进行分页，因为构建子分区时要过滤已构建过的子分区值
    status: [],
    sort_by: 'last_modify',
    reverse: true
  }
  subSegfilter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.subPartitionSegmentPager) || pageCount
  }
  pagerTableData = []
  subPartitionSegmentList = [] // 某segment下全量子分区segmentlist
  subBuildedPartitionValues = [] // 某segment下所有已构建过的子分区值
  modelSubPartitionValues = [] // 模型下所有子分区值
  subPartitionSegmentTotal = 0
  buildSubParValueVisible = false
  addedPartitionValues = []
  partitionValuesLabels = []
  partitionOptions = []
  partition_values = []
  removeSelectedMultiPartition = []
  isMultipleBuild = false
  selectedSubPartitionSegments = []
  refreshSubPartitionLoading = false
  buildSubParValueLoading = false
  duplicateValueError = false

  get getDetails () {
    let notices = [
      { text: this.$t('mergeNotice1'), value: 1, isError: false },
      { text: this.$t('mergeNotice2'), value: 2, isError: false },
      { text: this.$t('mergeNotice3'), value: 3, isError: false }
    ]
    return notices
  }

  get controlBuildSubSegment () {
    if (this.model.total_indexes && (this.currentSegment.status !== 'NEW' && (['ONLINE', 'LOADING', 'WARNING'].includes(this.currentSegment.status_to_display) || (this.currentSegment.status_to_display === 'REFRESHING' && this.pagerTableData.filter(item => item.status === 'ONLINE'))))) {
      return false
    } else {
      return true
    }
  }

  hasSubPartitionEventAuthority (type) {
    if (type === 'refresh') {
      return this.selectedSubPartitionSegments.length !== this.selectedSubPartitionSegments.filter(it => it.status === 'ONLINE').length
    } else if (type === 'delete') {
      return this.currentSegment.status_to_display === 'MERGING'
    }
  }

  refreshPartitionValues (val) {
    this.partition_values = val
  }

  async buildSubParValue () {
    this.buildSubParValueLoading = true
    try {
      const partitionValuesArr = split_array(this.partition_values, 1)
      await this.buildSubPartitions({ project: this.currentSelectedProject, model_id: this.model.uuid, segment_id: this.currentSegment.id, partition_values: partitionValuesArr, parallel_build_by_segment: this.isMultipleBuild })
      this.buildSubParValueLoading = false
      this.buildSubParValueVisible = false
      this.$message({
        dangerouslyUseHTMLString: true,
        type: 'success',
        customClass: 'build-full-load-success',
        message: (
          <div>
            <span>{this.$t('kylinLang.common.buildSuccess')}</span>
            <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
          </div>
        )
      })
      this.isMultipleBuild = false
      this.addedPartitionValues = []
      this.partition_values = []
      this.loadSubPartitions()
    } catch (e) {
      handleError(e)
      // this.addedPartitionValues = []
      this.partition_values = []
      this.buildSubParValueLoading = false
      this.isMultipleBuild = false
    }
  }

  async showSubParSegments (row) {
    if (row.status_to_display === 'LOCKED' || !this.model.multi_partition_desc) return
    this.currentSegment = row
    this.subSegmentfilter.model_id = this.model.uuid
    this.subSegmentfilter.segment_id = this.currentSegment.id
    await this.loadSubPartitions()
    this.isSubPartitionList = true
  }
  async loadSubPartitions () {
    try {
      const res = await this.fetchSubPartitions(Object.assign({}, {project: this.currentSelectedProject}, this.subSegmentfilter))
      const { value } = await handleSuccessAsync(res)
      this.subPartitionSegmentList = value
      this.subBuildedPartitionValues = this.subPartitionSegmentList.map((p) => {
        return p.values[0]
      })
      this.pageSizeChange(0)
    } catch (e) {
      handleError(e)
    }
  }
  pageSizeChange (currentPage, pageSize) {
    const {sort_by, reverse} = this.subSegmentfilter
    const size = pageSize || this.subSegfilter.page_size
    this.subSegfilter.page_offset = currentPage
    const filteredData = this.subPartitionSegmentList.filter((s) => {
      return s.values[0].toLowerCase().indexOf(this.subParValuesFilter) !== -1
    }).sort((prev, next) => {
      if (sort_by === 'values') {
        return reverse ? next.values[0].charCodeAt() - prev.values[0].charCodeAt() : prev.values[0].charCodeAt() - next.values[0].charCodeAt()
      } else {
        return reverse ? next[sort_by] - prev[sort_by] : prev[sort_by] - next[sort_by]
      }
    })
    this.subPartitionSegmentTotal = filteredData.length
    this.pagerTableData = filteredData.slice(currentPage * size, (currentPage + 1) * size)
  }
  backToSegmentList () {
    this.isSubPartitionList = false
    this.currentSegment = null
    this.loadSegments()
  }
  onFilterChange () {
    this.pageSizeChange(0)
  }
  subPartitionSortChange ({column, prop, order}) {
    this.subSegmentfilter = {
      ...this.subSegmentfilter,
      sort_by: prop,
      reverse: order === 'descending'
    }
    this.pageSizeChange(0)
  }
  async buildSubSegment () {
    try {
      const res = await this.fetchSubPartitionValues({ project: this.currentSelectedProject, model_id: this.model.uuid })
      const data = await handleSuccessAsync(res)
      this.modelSubPartitionValues = data.map((p) => {
        return p.partition_value[0]
      })
      this.partitionValuesLabels = this.modelSubPartitionValues.filter((v1) => {
        return !this.subBuildedPartitionValues.includes(v1)
      })
      this.partitionOptions = this.partitionValuesLabels.slice(0, 50)
      this.buildSubParValueVisible = true
    } catch (e) {
      handleError(e)
      this.buildSubParValueVisible = false
    }
  }
  handleClosebuildSub () {
    this.buildSubParValueVisible = false
    this.addedPartitionValues = []
    this.partition_values = []
    this.isMultipleBuild = false
  }
  handleSelectionChange (rows) {
    this.selectedSubPartitionSegments = rows
  }
  async handleRefreshSubSegment () {
    const {index_count, index_count_total} = this.currentSegment
    await kapConfirm(this.$t('refreshSubSegmentTip', {subSegsLength: this.selectedSubPartitionSegments.length, indexes: `(${index_count}/${index_count_total})`}), {confirmButtonText: this.$t('kylinLang.common.refresh')}, this.$t('refreshSubSegmentTitle'))
    try {
      this.refreshSubPartitionLoading = true
      const ids = this.selectedSubPartitionSegments.map((sub) => {
        return sub.id
      })
      const isSubmit = await this.refreshSubPartition({project: this.currentSelectedProject, model_id: this.model.uuid, segment_id: this.currentSegment.id, partition_ids: ids})
      if (isSubmit) {
        await this.loadSubPartitions()
        this.$message({
          dangerouslyUseHTMLString: true,
          type: 'success',
          customClass: 'build-full-load-success',
          message: (
            <div>
              <span>{this.$t('kylinLang.common.buildSuccess')}</span>
              <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
            </div>
          )
        })
        this.refreshSubPartitionLoading = false
      }
    } catch (e) {
      handleError(e)
      this.refreshSubPartitionLoading = false
      this.loadSubPartitions()
    }
  }
  async handleDeleteSubSegment () {
    await kapConfirm(this.$t('deleteSubSegmentTip', {subSegsLength: this.selectedSubPartitionSegments.length}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('deleteSubSegmentTitle'))
    try {
      const ids = this.selectedSubPartitionSegments.map((sub) => {
        return sub.id
      })
      await this.deleteSubPartition({project: this.currentSelectedProject, model: this.model.uuid, segment: this.currentSegment.id, ids: ids.join(',')})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.loadSubPartitions()
    } catch (e) {
      handleError(e)
      this.loadSubPartitions()
    }
  }
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
  // 子分区值管理
  subParValMana (model) {
    this.$router.push({name: 'ModelSubPartitionValues', params: { modelName: model.name, modelId: model.uuid, expandTab: 'first' }})
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
        source: 'addSegment',
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
  getTagType (row, type) {
    let status = type === 'segment' ? row.status_to_display : row.status
    if (status === 'ONLINE') {
      return 'success'
    } else if (status === 'WARNING') {
      return 'warning'
    } else if (['LOCKED'].includes(status)) {
      return 'info'
    } else {
      return ''
    }
  }
  // 状态控制按钮的使用
  hasEventAuthority (type) {
    let typeList = (type) => {
      return this.selectedSegments.length && typeof this.selectedSegments[0] !== 'undefined' ? this.selectedSegments.filter(it => !type.includes(it.status_to_display)).length > 0 : false
    }
    if (type === 'refresh') {
      return typeList(['ONLINE', 'WARNING'])
    } else if (type === 'merge') {
      return typeList(['ONLINE', 'WARNING'])
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
  handleClose () {
    this.isShowRefreshConfirm = false
    this.refreshType = 'refreshOrigin'
  }
  async handleSubmit () {
    try {
      const projectName = this.currentSelectedProject
      const modelId = this.model.uuid
      const segmentIds = this.selectedSegmentIds
      const refresh_all_indexes = this.refreshType === 'refreshAll'
      this.refreshLoading = true
      const isSubmit = await this.refreshSegments({ projectName, modelId, segmentIds, refresh_all_indexes })
      if (isSubmit) {
        await this.loadSegments()
        this.$emit('loadModels')
        this.$message({
          dangerouslyUseHTMLString: true,
          type: 'success',
          customClass: 'build-full-load-success',
          message: (
            <div>
              <span>{this.$t('kylinLang.common.buildSuccess')}</span>
              <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
            </div>
          )
        })
        this.refreshLoading = false
      }
      this.isShowRefreshConfirm = false
      this.refreshLoading = false
      this.refreshType = 'refreshOrigin'
    } catch (e) {
      handleError(e)
      this.isShowRefreshConfirm = false
      this.refreshLoading = false
      this.refreshType = 'refreshOrigin'
      this.loadSegments()
    }
  }
  handleRefreshSegment () {
    if (this.selectedSegmentIds.length) {
      this.detailTableData = this.selectedSegments.filter((seg) => {
        return seg.index_count < seg.index_count_total
      })
      this.isShowRefreshConfirm = true
    } else {
      this.$message(this.$t('pleaseSelectSegments'))
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
        // let tableData = []
        // this.selectedSegments.forEach((seg) => {
        //   const obj = {}
        //   obj['start'] = transToServerGmtTime(this.segmentTime(seg, seg.startTime))
        //   obj['end'] = transToServerGmtTime(this.segmentTime(seg, seg.endTime))
        //   tableData.push(obj)
        // })
        // await this.callGlobalDetailDialog({
        //   msg: this.$t('confirmMergeSegments', {count: segmentIds.length}),
        //   title: this.$t('mergeSegmentTip'),
        //   detailTableData: tableData,
        //   detailColumns: [
        //     {column: 'start', label: this.$t('kylinLang.common.startTime')},
        //     {column: 'end', label: this.$t('kylinLang.common.endTime')}
        //   ],
        //   dialogType: 'tip',
        //   showDetailBtn: false,
        //   submitText: this.$t('merge')
        // })
        // check merge segment
        const res = await this.mergeSegmentCheck({ project: projectName, modelId, ids: segmentIds, type: 'MERGE' })
        const data = await handleSuccessAsync(res)
        this.mergedSegments = [data]
        this.isShowMergeConfirm = true
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmitMerge () {
    this.mergeLoading = true
    const projectName = this.currentSelectedProject
    const modelId = this.model.uuid
    const segmentIds = this.selectedSegmentIds
    try {
      // 合并segment
      const isSubmit = await this.mergeSegments({ projectName, modelId, segmentIds })
      if (isSubmit) {
        await this.loadSegments()
        this.mergeLoading = false
        this.$emit('loadModels')
        this.$message({
          dangerouslyUseHTMLString: true,
          type: 'success',
          customClass: 'build-full-load-success',
          message: (
            <div>
              <span>{this.$t('kylinLang.common.buildSuccess')}</span>
              <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
            </div>
          )
        })
      }
    } catch (e) {
      handleError(e)
      this.mergeLoading = false
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
  renderSubPartitionAmountHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('subPratitionAmount')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('subPratitionAmountTip')}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  renderIndexAmountHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('kylinLang.common.indexAmount')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('kylinLang.common.indexAmountTip')}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  checkDuplicateValue (type) {
    this.duplicateValueError = type
  }
  removeDuplicateValue () {
    this.$refs.selectSubPartition && this.$refs.selectSubPartition.clearDuplicateValue()
  }
  handleFixSegment () {
    this.$emit('auto-fix')
  }

  filterPartitions (query) {
    this.partitionOptions = this.partitionValuesLabels.filter(item => item.indexOf(query) >= 0).slice(0, 50)
  }

  // 跳转至job页面
  jumpToJobs () {
    if (getQueryString('from') === 'cloud' || getQueryString('from') === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapJob' })
    } else {
      this.$router.push('/monitor/job')
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
.segment-detail {
  .segment-path {
    word-break: break-all;
  }
}
.merge-comfirm {
  .merge-notices {
    margin-bottom: 5px;
    i {
      color: @text-disabled-color;
      margin-right: 5px;
    }
    color: @text-title-color;
    font-size: 12px;
    .review-details {
      color: @base-color;
      cursor: pointer;
      position: relative;
      display: inline-block;
    }
    .arrow {
      transform: rotate(90deg);
      margin-left: 3px;
      font-size: 7px;
      color: @base-color;
      position: absolute;
      top: 4px;
    }
  }
  .detail-content {
    background-color: @base-background-color-1;
    padding: 10px 15px;
    box-sizing: border-box;
    font-size: 12px;
    color: @text-normal-color;
    .point {
      // color: @text-normal-color;
      margin-right: 5px;
    }
  }
}
.segment-detail {
  .segment-path {
    word-break: break-all;
  }
}
.model-segment {
  background-color: @fff;
  padding: 10px;
  border: 1px solid @line-border-color4;
  margin: 15px;
  .segment-actions {
    margin-bottom: 10px;
    .el-icon-question {
      color: @base-color;
    }
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
.error-msg {
  color: @error-color-1;
  font-size: 12px;
  margin-top: 5px;
}
.subPartition-segment {
  background-color: @fff;
  padding: 10px;
  border: 1px solid @regular-background-color;
  margin: 15px;
  .back-btn {
    cursor: pointer;
    &:hover {
      color: @base-color-2;
    }
  }
  .disabled-build {
    color: @text-disabled-color;
    cursor: not-allowed;
    background-image: none;
    border-color: @line-border-color3;
    background-color: @table-stripe-color;
  }
}
.build-full-load-success {
  padding: 10px 30px 10px 10px;
  align-items: center;
}
.refresh-comfirm {
  .build-all-tips {
    .el-icon-info {
      font-size: 14px;
    }
    .el-alert__content {
      font-size: 12px;
      color: @text-disabled-color;
    }
  }
}
.build-sub-par-dialog {
  .duplicate-tips {
    font-size: 12px;
    margin-top: 5px;
    .clear-value-btn {
      cursor: pointer;
      color: @text-normal-color;
    }
  }
  .select-sub-partition.error-border {
    .el-input__inner {
      border-color: @error-color-1;
    }
  }
}
.segment-views {
  .ky-a-like.is-disabled {
    color: @text-disabled-color;
    cursor: default;
    &:hover {
      color: @text-disabled-color !important;
    }
  }
}
</style>
