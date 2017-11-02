<template>
  <div id="segments">
    <el-row style="margin-bottom: 10px;">
      <div v-show="cube.multilevel_partition_cols.length > 0" class="ksd-fleft mutil-level">
        <el-tooltip class="item" effect="dark" placement="top">
          <div slot="content">
           {{$t('kylinLang.model.primaryPartitionColumn')}}
          </div>
          <el-tag >{{cube.multilevel_partition_cols[0]}}</el-tag>
        </el-tooltip>
        <el-select v-model="filterDetail.mpValues" filterable :placeholder="$t('kylinLang.common.pleaseFilter')" @change="filterChange">
          <el-option
            v-for="item in mpValuesList"
            :key="item.name"
            :label="item.name"
            :value="item.name">
          </el-option>
        </el-select>
      </div>

      <div class="ksd-fright" v-if="segments.length > 0">
        <el-button @click="refresh" v-show="cube.status!=='DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) ">{{$t('REFRESH')}}</el-button>
        <el-button @click="merge" v-show="cube.status!== 'DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) ">{{$t('MERGE')}}</el-button>
        <el-button @click="drop" v-show="cube.status!=='DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) ">{{$t('DROP')}}</el-button>
      </div>
    </el-row>

    <div v-if="segments.length > 0">
      <el-table
        :data="segments"
        border
        ref="segmentsTable"
        :row-class-name="tableRowClassName"
        @cell-click="cellClick"
        @selection-change="selectionChange"
        style="width: 100%">
        <el-table-column
          type="selection"
          :selectable="isSelectAble"
          width="50">
        </el-table-column>
        <el-table-column
          label="Segment ID"
          show-overflow-tooltip
          width="120">
          <template scope="scope">
            <el-tooltip class="item" effect="dark" placement="top">
              <div slot="content">
               {{scope.row.uuid}}
              </div>
              <span>{{scope.row.uuid.substr(0,8)}}</span>
            </el-tooltip>
            <el-tooltip class="item" effect="dark" placement="top" v-if="scope.row.additionalInfo.tmp_op_flag === 'false'">
              <div slot="content">
               {{$t('buildSegment')}}
              </div>
              <i class="el-icon-loading"></i>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('SegmentName')"
          prop="name">
        </el-table-column>
        <el-table-column
          :label="$t('StorageSize')">
          <template scope="scope">
            {{+scope.row.additionalInfo.storageSizeBytes + (+scope.row.additionalInfo.tableIndexStorageSizeBytes || 0)|dataSize}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('startTime')">
          <template scope="scope">
            {{scope.row.date_range_start | utcTime}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('endTime')">
          <template scope="scope">
            {{scope.row.date_range_end | utcTime}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('action')"
          width="60">
          <template scope="scope">
            <el-dropdown trigger="click" @click.native.stop>
              <el-button class="el-dropdown-link">
                <i class="el-icon-more"></i>
              </el-button>
              <el-dropdown-menu slot="dropdown">
                <el-dropdown-item @click.native="viewSeg(scope.row)">{{$t('viewSeg')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </template>
        </el-table-column>
      </el-table>
      <pager ref="pager" class="ksd-right ksd-mb-20" :perPageSize="5" :totalSize="totalSegments"  v-on:handleCurrentChange='currentChange' :totalSum="totalSize*1024 | dataSize">
      </pager>
    </div>

    <el-card class="noSegments" v-else>
      {{$t('NoStorageInfo')}}
    </el-card>

    <el-dialog :title="$t(noteMessage.action)" :visible.sync="noteMessage.actionVisible" size="tiny">
      <el-card>
        <span>{{$t(noteMessage.actionMeg)}}</span>
        <el-row v-for="segment in noteMessage.actionSegs" :key="segment.uuid">
          <el-col>
            <span>[{{segment.name}}]</span>
          </el-col>
        </el-row>
      </el-card>
      <el-alert :closable="false" :title="$t(noteMessage.warningMessage)" type="warning" v-show="noteMessage.warningMessage !== '' && noteMessage.errorMessage === ''">
      </el-alert>
      <el-alert :closable="false" :title="$t(noteMessage.errorMessage)" type="error" v-show="noteMessage.errorMessage !== ''">
      </el-alert>

      <div slot="footer" class="dialog-footer">
        <el-button @click="noteMessage.actionVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="startAction" :loading="btnLoading" :disabled="noteMessage.errorMessage !== ''">{{$t('kylinLang.common.continue')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
import { permissions } from '../../config'
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasPermission, hasRole } from '../../util/business'
import { objectClone, indexOfObjWithSomeKey, objectArraySort } from '../../util/index'
export default {
  name: 'showSegments',
  props: ['cube'],
  data () {
    return {
      segments: [],
      versions: [],
      lockST: null,
      totalSegments: 0,
      totalSize: 0,
      currentPage: 1,
      selectedSeg: {},
      btnLoading: false,
      selected_project: this.$store.state.project.selected_project,
      filterDetail: {
        mpValues: '',
        pageOffset: 0,
        pageSize: 5
      },
      mpValuesList: [],
      noteMessage: {
        actionSegs: [],
        action: '',
        actionMeg: '',
        warningMessage: '',
        errorMessage: '',
        actionVisible: false
      }
    }
  },
  methods: {
    ...mapActions({
      getCubeSegments: 'GET_CUBE_SEGMENTS',
      updateCubeSegments: 'UPDATE_CUBE_SEGMENTS',
      getMPValues: 'GET_MP_VALUES'
    }),
    getProjectIdByName (pname) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === pname) {
          projectId = projectList[s].uuid
        }
      }
      return projectId
    },
    hasSomePermissionOfProject (project) {
      var projectId = this.getProjectIdByName(project)
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    hasOperationPermissionOfProject (project) {
      var projectId = this.getProjectIdByName(project)
      return hasPermission(this, projectId, permissions.OPERATION.mask)
    },
    loadSegments: function () {
      if (this.cube.multilevel_partition_cols.length > 0) {
        this.getMPValues(this.cube.name).then((res) => {
          handleSuccess(res, (data) => {
            this.mpValuesList = objectArraySort(data, true, 'name')
            if (this.mpValuesList.length > 0) {
              this.$nextTick(() => {
                this.filterDetail.mpValues = this.mpValuesList.indexOf(this.filterDetail.mpValues) >= 0 ? this.filterDetail.mpValues : this.mpValuesList[0].name
                this.loadSegmentsDetail()
              })
            } else {
              this.filterDetail.mpValues = ''
              this.segments = []
            }
          })
        }, (res) => {
          handleError(res)
        })
      } else {
        this.loadSegmentsDetail()
      }
    },
    currentChange: function (value) {
      this.filterDetail.pageOffset = value - 1
      this.currentPage = value
      this.loadSegmentsDetail()
    },
    loadSegmentsDetail: function () {
      this.getCubeSegments({name: this.cube.name, filter: this.filterDetail}).then((res) => {
        handleSuccess(res, (data) => {
          this.segments = data.segments
          this.totalSize = data.total_storage_size_kb
          this.totalSegments = data.size
          if (this.selectedSeg[this.currentPage]) {
            this.$nextTick(() => {
              for (let index in this.selectedSeg[this.currentPage]) {
                this.$refs.segmentsTable.toggleRowSelection(this.$refs.segmentsTable.tableData[index], true)
              }
            })
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    tableRowClassName: function (row, index) {
      if (row.additionalInfo.tmp_op_flag === 'false') {
        return 'info-row'
      }
      return ''
    },
    cellClick: function (row, column, cell, event) {
      if (row.additionalInfo.tmp_op_flag === 'false' && column.label === 'Segment ID') {
        this.$router.push({name: 'Monitor'})
      }
    },
    isSelectAble: function (row, index) {
      if (row.additionalInfo.tmp_op_flag === 'false') {
        return false
      }
      return true
    },
    selectionChange: function (val) {
      this.$set(this.selectedSeg, this.currentPage, {})
      val.forEach((segment) => {
        let index = indexOfObjWithSomeKey(this.$refs.segmentsTable.tableData, 'uuid', segment.uuid)
        this.$set(this.selectedSeg[this.currentPage], index, segment)
      })
    },
    viewSeg: function (segment) {
      let segmentDesc = objectClone(segment)
      let type = 'hbase'
      if (+segment.additionalInfo.storageType === 100 || +segment.additionalInfo.storageType === 99) {
        type = 'columnar'
      }
      this.$set(segmentDesc, 'type', type)
      this.$emit('addSegTabs', segmentDesc)
    },
    cloneCubeDesc (data) {
      this.cube.status = data.status
      this.cube.total_storage_size_kb = data.total_storage_size_kb
      this.cube.input_records_count = data.input_records_count
      this.cube.input_records_size = data.input_records_size
      this.cube.segments = data.segments
    },
    refresh: function () {
      this.noteMessage = {
        actionSegs: [],
        action: 'REFRESH',
        actionMeg: 'REFRESHActionMeg',
        warningMessage: '',
        errorMessage: '',
        actionVisible: false
      }
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
        }
      }
      if (this.noteMessage.actionSegs.length < 1) {
        this.$message({
          type: 'error',
          message: this.$t('refreshNumberNote'),
          showClose: true
        })
        return
      }
      if (this.noteMessage.actionSegs.length >= 2) {
        this.noteMessage.warningMessage = 'refreshMultipleNote'
      }
      this.noteMessage.actionVisible = true
    },
    merge: function () {
      this.noteMessage = {
        actionSegs: [],
        action: 'MERGE',
        actionMeg: 'MERGEActionMeg',
        warningMessage: '',
        errorMessage: '',
        actionVisible: false
      }
      let timeRange = []
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
          timeRange.push({start: this.selectedSeg[page][index].date_range_start, end: this.selectedSeg[page][index].date_range_end})
        }
      }
      if (this.noteMessage.actionSegs.length < 2) {
        this.$message({
          type: 'error',
          message: this.$t('mergeNumberNote'),
          showClose: true
        })
        return
      }
      timeRange = objectArraySort(timeRange, true, 'start')
      for (let j = 0; j < timeRange.length - 1; j++) {
        if (timeRange[j].end > timeRange[j + 1].start) {
          this.noteMessage.errorMessage = 'segsOverlap'
          break
        }
      }
      for (let i = 0; i < timeRange.length - 1; i++) {
        if (timeRange[i].end < timeRange[i + 1].start) {
          this.noteMessage.warningMessage = 'mergeDiscontinueNote'
          break
        }
      }
      this.noteMessage.actionVisible = true
    },
    drop: function () {
      this.noteMessage = {
        actionSegs: [],
        action: 'DROP',
        actionMeg: 'DROPActionMeg',
        warningMessage: '',
        errorMessage: '',
        actionVisible: false
      }
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
        }
      }
      if (this.noteMessage.actionSegs.length < 1) {
        this.$message({
          type: 'error',
          message: this.$t('dropNumberNote'),
          showClose: true
        })
        return
      }
      this.noteMessage.actionVisible = true
    },
    startAction: function () {
      this.btnLoading = true
      let segsName = []
      this.noteMessage.actionSegs.forEach((seg) => {
        segsName.push(seg.name)
      })
      let para = {buildType: this.noteMessage.action, segments: segsName, mpValues: this.filterDetail.mpValues, force: true}
      this.updateCubeSegments({name: this.cube.name, segments: para}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t(this.noteMessage.action + 'Successful'),
            showClose: true
          })
          this.selectedSeg = {}
          if (this.noteMessage.action === 'DROP') {
            this.cloneCubeDesc(data)
            this.filterDetail.pageOffset = 0
            this.loadSegments()
          } else {
            this.loadSegmentsDetail()
          }
        })
      }, (res) => {
        handleError(res)
      })
      this.noteMessage.actionVisible = false
      this.btnLoading = false
    },
    filterChange () {
      if (this.filterDetail.mpValues !== '') {
        clearTimeout(this.lockST)
        this.lockST = setTimeout(() => {
          this.selectedSeg = {}
          this.loadSegmentsDetail()
        }, 1000)
      }
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  locales: {
    'en': {REFRESH: 'Refresh', MERGE: 'Merge', DROP: 'Drop', refreshSeg: 'Refresh Segments', mergeSeg: 'Merge Segments', dropSeg: 'Drop Segments', SegmentName: 'Segment Name', cubeVersion: 'Cube Version', StorageSize: 'Storage Size', startTime: 'Start Time', endTime: 'End Time', NoStorageInfo: 'No Segment Info.', cubeHBRegionCount: 'Region Count', cubeHBSize: 'Segment Size', cubeHBStartTime: 'Start Time', cubeHBEndTime: 'End Time', totalSize: 'Total Size :', viewCube: 'View Cube', viewSeg: 'View Details', action: 'Action', mergeNumberNote: 'Merge action will require checking two segments at lease.', mergeDiscontinueNote: 'Selected segments are discontinue, do you want to merge them anyway?', refreshMultipleNote: 'Refresh multiple segments may trigger related build jobs.', refreshNumberNote: 'Refresh action will require one segment at least.', dropNumberNote: 'Drop action will require one segment at least.', REFRESHSuccessful: 'Refresh the segments successful!', MERGESuccessful: 'Merge the segments successful!', DROPSuccessful: 'Drop the segments successful!', REFRESHActionMeg: 'Confirm to refresh the segments?', MERGEActionMeg: 'Confirm to merge the segments?', DROPActionMeg: 'Confirm to drop the segments?', applyLatestCube: 'Apply the latest cube definition to refresh.', applyOriginalCube: 'Apply the original cube definition to refresh.', segsOverlap: 'Selected Segments have overlap in timeline, please try a different segment.', differentDefinition: 'Selected segments have difference on cube definition, please try a differenr segment.', purgeCube: 'Are you sure to purge the cube? ', purgeSuccessful: 'Purge the cube successful!', buildSegment: 'A building segment. Click its \'Segment ID\' can refer to the build job.'},
    'zh-cn': {REFRESH: '刷新', MERGE: '合并', DROP: '删除', refreshSeg: '刷新 Segments', mergeSeg: '合并 Segments', dropSeg: '删除 Segments', SegmentName: 'Segment 名称', cubeVersion: 'Cube版本', StorageSize: '存储空间', startTime: '起始时间', endTime: '结束时间', totalSize: '总大小 ：', RawTableStorageSize: 'Table Index 存储空间', NoStorageInfo: '没有Segment相关信息.', cubeHBSize: '大小', cubeHBStartTime: '开始时间', cubeHBEndTime: '结束时间', viewCube: '查看Cube', viewSeg: '查看详细信息', action: '操作', mergeNumberNote: '合并需要至少选择两个segments。', mergeDiscontinueNote: '您选择的segment不连续，是否继续合并？', refreshMultipleNote: '刷新多个segment可能会触发关联的构建任务。', refreshNumberNote: '刷新需要至少选择一个segment。', dropNumberNote: '删除需要至少选择一个segment。', REFRESHSuccessful: '刷新Segments成功!', MERGESuccessful: '合并Segments成功!', DROPSuccessful: '清理Segments成功!', REFRESHActionMeg: '确定刷新以下Segments?', MERGEActionMeg: '确定合并以下Segments?', DROPActionMeg: '确定删除以下Segments?', applyLatestCube: '应用最新Cube定义刷新。', applyOriginalCube: '应用原Cube定义刷新。', segsOverlap: '您选择的segment时间有重合，请重新选择。', differentDefinition: '您选择的segment属于不同版本的cube，请重新选择。', buildSegment: '当前Segment正在生成，点击\'Segment ID\'可见相关任务。'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .el-scrollbar {
    .el-select-dropdown__item {
      .el-tooltip {
        p:hover {
          color: #fff!important;
        }
      }
    }
  }
  #segments{
    .noSegments {
      font-size: 12px;
      border: 1px solid #393E53;
      background: #292b38;
    }
    .el-row {
      .el-tag {
        background-color: #393e53;
        padding: 0px 10px 0px 10px;
      }
    }
    .tableheader{
      .el-col{
        text-align: left;
        padding-left: 20px;
        white-space: nowrap;
      }
      .left-part{
        border-right:solid 1px #393e53;
        text-align: right;
        padding-right: 20px;
      }
    }
    .mutil-level {
      display: inline-flex;
      .el-tag {
        color: rgb(255, 255, 255);
        height: 36px;
        line-height: 36px;
        margin-right: 10px;
      }
    }
    .el-table {
      tr th:first-child {
        border-right: 1px solid #2c2f3c;
      }
      tr td:first-child {
        border-right: 1px solid #393e53;
      }
      .el-table__header thead th .cell {
        background: #393e53;
        color: #ffffff;
      }
      .el-checkbox .is-disabled .el-checkbox__inner {
        background: transparent;
      }
      .info-row {
        td {
          background: #535C86!important;
        }
      }
      .el-tooltip {
        span {
          font-size: 12px;
          cursor:pointer;
        }
        i {
          font-size: 16px;
          margin-top:4px;
          margin-left: 20px;
          cursor:pointer;
        }
      }
    }
    .el-button--text {
      span {
        color: #20a0ff;
      }
    }
    .el-dialog__body {
      span {
        font-weight: bold;
      }
      .el-alert--warning {
        color: rgb(247,186,42);
        background-color: rgba(247,186,42,0.1);
        border-color: rgba(247,186,42,0.2);
      }
      .el-alert--error {
        color: rgb(255,73,73);
        background-color: rgba(255,73,73,0.1);
        border-color: rgba(255,73,73,0.2);
      }
      .el-card {
        .el-card__body {
          padding-top: 0px;
        }
        border: none;
        box-shadow: none;
      }
      .el-radio__label {
        span {
          font-weight: bold;
          font-size: 14px;
          color: #d4d7e3;
        }
      }
    }
  }
</style>
