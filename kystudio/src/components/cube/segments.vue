<template>
  <div id="segments">
    <el-row style="margin-bottom: 10px;">
      <el-button @click="refresh">{{$t('refresh')}}</el-button>
      <el-button @click="merge">{{$t('merge')}}</el-button>
      <el-button @click="drop">{{$t('drop')}}</el-button>

      <el-tooltip class="item" effect="dark" placement="left">
        <div slot="content">
          <span style="font-weight:bolder;">{{versionsDetail.label}}</span> 
          {{versionsDetail.createTime}} 
        </div>
        <el-select v-model="versionsDetail.version" style="float: right" class="ksd-ml-20" placeholder="Choose Version" @change="changeLable();loadSegmentsDetail()">
          <el-option
            v-for="item in versions"
            :key="item.value"
            :label="item.label"
            :value="item.value">
            <el-tooltip class="item" effect="dark" placement="left">
              <div slot="content">
                <span style="font-weight:bolder;">{{item.label}}</span> 
                {{item.createTime}} 
              </div>
              <p class="cube_versions">{{ item.label }}</p>
            </el-tooltip>
          </el-option>
        </el-select>      
      </el-tooltip>
    </el-row>
    <div v-if="segments.length > 0">
      <el-table v-if="type==='hbase' && segments.length > 0"
        :data="segments"
        border
        ref="segmentsTable"
        :row-class-name="tableRowClassName"
        @row-click="rowClick"
        @selection-change="selectionChange"
        style="width: 100%">
        <el-table-column
          type="selection"
          width="50">
        </el-table-column>
        <el-table-column
          :label="$t('cubeVersion')"
          width="300">
          <template scope="scope">
            <p style="font-size: 12px;">{{scope.row.version.description}}</p>
            <p style="font-size: 10px;color: #6E7188;">{{scope.row.version.createTime}}</p>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('cubeHBSize')">
          <template scope="scope">
            {{scope.row.tableSize | dataSize}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('cubeHBStartTime')">
          <template scope="scope">
            {{scope.row.dateRangeStart | utcTime}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('cubeHBEndTime')">
          <template scope="scope">
            {{scope.row.dateRangeEnd | utcTime}}
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
                <el-dropdown-item @click.native="viewCube(scope.row)">{{$t('viewCube')}}</el-dropdown-item>
                <el-dropdown-item @click.native="viewSeg(scope.row)">{{$t('viewSeg')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </template>
        </el-table-column>
      </el-table>

      <el-table v-if="type==='columnar'&& segments.length > 0"
        :data="segments"
        border
        ref="segmentsTable"
        :row-class-name="tableRowClassName"
        @row-click="rowClick"
        @selection-change="selectionChange"
        style="width: 100%">
        <el-table-column
          type="selection"
          width="50">
        </el-table-column>
        <el-table-column
          :label="$t('cubeVersion')"
          width="300">
          <template scope="scope">
            <p style="font-size: 12px">{{scope.row.version.description}}</p>
            <p style="font-size: 10px;color: #6E7188;">{{scope.row.version.createTime}}</p>
          </template>
        </el-table-column>
        <el-table-column
          label="Storage Size">
          <template scope="scope">
            {{(scope.row.storageSize + scope.row.rawTableStorageSize)|dataSize}}
          </template>
        </el-table-column>
        <el-table-column
          label="Range Start Time">
          <template scope="scope">
            {{scope.row.dateRangeStart | utcTime}}
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('RangeEndTime')">
          <template scope="scope">
            {{scope.row.dateRangeEnd | utcTime}}
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
                <el-dropdown-item @click.native="viewCube(scope.row)">{{$t('viewCube')}}</el-dropdown-item>
                <el-dropdown-item @click.native="viewSeg(scope.row)">{{$t('viewSeg')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </template>
        </el-table-column>
      </el-table>
      <div class="pager">
        <span class="total_size">{{$t('totalSize')}} {{totalSize | dataSize}}</span>
        <pager ref="pager" class=" ksd-right" :perPageSize="5" :totalSize="totalSegments"  v-on:handleCurrentChange='currentChange' >
        </pager>
      </div>
    </div>

    <el-card v-else>
      {{$t('NoStorageInfo')}}
    </el-card>

    <el-dialog :title="noteMessage.action" :visible.sync="noteMessage.actionVisible" size="tiny">
      <el-card>
        <span>{{$t(noteMessage.action + 'ActionMeg')}}</span>
        <el-row v-for="segment in noteMessage.actionSegs" :key="segment.segmentUUID">
          <el-col>
            <span>[{{segment.segmentName}}] - {{segment.version.description}}</span>
          </el-col>
        </el-row>
      </el-card>
      <el-alert :closable="false" :title="$t(noteMessage.warningMessage)" type="warning" v-show="noteMessage.warningMessage !== '' && noteMessage.errorMessage === ''">
      </el-alert>
      <el-alert :closable="false" :title="$t(noteMessage.errorMessage)" type="error" v-show="noteMessage.errorMessage !== ''">
      </el-alert>
      <el-card v-show="noteMessage.hasOldVersion">
        <el-row :gutter="20">
          <el-col :span="24">
            <el-radio v-model="upgrade" :label="true">
              <span>{{$t('applyLatestCube')}}</span>
            </el-radio>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="24">
            <el-radio v-model="upgrade" :label="false">
              <span>{{$t('applyOriginalCube')}}</span>
            </el-radio>
          </el-col>
        </el-row>
      </el-card>

      <div slot="footer" class="dialog-footer">
        <el-button @click="noteMessage.actionVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="startAction" :loading="btnLoading" :disabled="noteMessage.errorMessage !== ''">{{$t('kylinLang.common.continue')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToGmtTime } from '../../util/business'
import { objectClone, indexOfObjWithSomeKey, objectArraySort } from '../../util/index'
export default {
  name: 'showSegments',
  props: ['cube'],
  data () {
    return {
      segments: [],
      type: 'hbase',
      versions: [],
      totalSegments: 0,
      totalSize: 0,
      versionsDetail: {
        label: 'ALL',
        createTime: '',
        version: null,
        pageOffset: 0,
        pageSize: 5
      },
      currentPage: 1,
      selectedSeg: {},
      btnLoading: false,
      upgrade: false,
      noteMessage: {
        actionSegs: [],
        action: '',
        warningMessage: '',
        errorMessage: '',
        hasOldVersion: false,
        actionVisible: false
      }
    }
  },
  methods: {
    ...mapActions({
      getCubeSegments: 'GET_CUBE_SEGMENTS',
      getCubeVersions: 'GET_CUBE_VERSIONS',
      rebuildCube: 'REBUILD_CUBE'
    }),
    loadSegments: function () {
      if (this.cube.desc.storage_type === 100 || this.cube.desc.storage_type === 99) {
        this.type = 'columnar'
      }
      this.loadSegmentsDetail()
      this.getCubeVersions(this.cube.name).then((res) => {
        handleSuccess(res, (data) => {
          this.versions = data
          this.versions.forEach((version) => {
            this.$set(version, 'value', version.version)
            this.$set(version, 'label', 'Version ' + version.version.substr(8))
            this.$set(version, 'createTime', transToGmtTime(version.createTime))
            this.$set(version, 'description', version.label + ' ' + version.createTime)
          })
          this.versions.push({version: 'ALL', createTime: '', value: null, label: 'ALL', description: 'ALL'})
        })
      }, (res) => {
        handleError(res)
      })
    },
    currentChange: function (value) {
      this.versionsDetail.pageOffset = value - 1
      this.currentPage = value
      this.loadSegmentsDetail()
    },
    changeLable: function () {
      let versionIndex = indexOfObjWithSomeKey(this.versions, 'value', this.versionsDetail.version)
      this.versionsDetail.label = this.versions[versionIndex].label || 'ALL'
      this.versionsDetail.createTime = this.versions[versionIndex].createTime || ''
    },
    loadSegmentsDetail: function () {
      this.getCubeSegments({name: this.cube.name, version: this.versionsDetail}).then((res) => {
        handleSuccess(res, (data) => {
          this.segments = data.segments
          this.totalSize = data.total_size
          this.segments.forEach((segment, index) => {
            this.$set(segment, 'version', data.versions[index])
            this.$set(segment.version, 'description', 'Version ' + data.versions[index].version.substr(8))
            this.$set(segment.version, 'createTime', transToGmtTime(data.versions[index].createTime))
          })
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
      if (row.segmentStatus !== 'READY') {
        return 'info-row'
      }
      return ''
    },
    rowClick: function (row, event, column) {
      if (row.segmentStatus !== 'READY') {
        this.$router.push({name: 'Monitor'})
      }
    },
    selectionChange: function (val) {
      this.$set(this.selectedSeg, this.currentPage, {})
      val.forEach((segment) => {
        let index = indexOfObjWithSomeKey(this.$refs.segmentsTable.tableData, 'segmentUUID', segment.segmentUUID)
        this.$set(this.selectedSeg[this.currentPage], index, segment)
      })
    },
    viewCube: function (segment) {
      let cubeDesc = objectClone(this.cube)
      this.$set(cubeDesc, 'cubeVersion', segment.version.version)
      this.$set(cubeDesc, 'versionDescription', segment.version.description)
      this.$emit('addVersionTabs', cubeDesc)
    },
    viewSeg: function (segment) {
      let segmentDesc = objectClone(segment)
      this.$set(segmentDesc, 'type', this.type)
      this.$emit('addSegTabs', segmentDesc)
    },
    refresh: function () {
      this.noteMessage = {
        actionSegs: [],
        action: 'REFRESH',
        warningMessage: '',
        errorMessage: '',
        hasOldVersion: false,
        actionVisible: false
      }
      this.upgrade = true
      let currentVersion = this.versions[this.versions.length - 2].version
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
          if (this.selectedSeg[page][index].version !== currentVersion) {
            this.noteMessage.hasOldVersion = true
          }
        }
      }
      if (this.noteMessage.actionSegs.length < 1) {
        this.$message({
          type: 'error',
          message: this.$t('refreshNumberNote'),
          duration: 3000
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
        warningMessage: '',
        errorMessage: '',
        hasOldVersion: false,
        actionVisible: false
      }
      this.upgrdate = false
      let segNode = []
      let version = null
      let timeRange = []
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
          segNode.push(parseInt(page) * this.versionsDetail.pageSize + parseInt(index))
          timeRange.push({start: this.selectedSeg[page][index].dateRangeStart, end: this.selectedSeg[page][index].dateRangeEnd})
          version = version || this.selectedSeg[page][index].version.version
          if (version !== this.selectedSeg[page][index].version.version) {
            this.noteMessage.errorMessage = 'differentDefinition'
          }
        }
      }
      if (this.noteMessage.actionSegs.length < 2) {
        this.$message({
          type: 'error',
          message: this.$t('mergeNumberNote'),
          duration: 3000
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
      for (let i = 1; i < segNode.length; i++) {
        if (segNode[i] - segNode[i - 1] !== 1) {
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
        warningMessage: '',
        errorMessage: '',
        hasOldVersion: false,
        actionVisible: false
      }
      this.upgrdate = false
      for (let page in this.selectedSeg) {
        for (let index in this.selectedSeg[page]) {
          this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
        }
      }
      if (this.noteMessage.actionSegs.length < 1) {
        this.$message({
          type: 'error',
          message: this.$t('dropNumberNote'),
          duration: 3000
        })
        return
      }
      this.noteMessage.actionVisible = true
    },
    startAction: function () {
      this.btnLoading = true
      let segsName = []
      this.noteMessage.actionSegs.forEach((seg) => {
        segsName.push(seg.segmentName)
      })
      let para = {buildType: this.noteMessage.action, segments: segsName, upgrade: this.upgrade}
      this.rebuildCube({cubeName: this.cube.name, para: para}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t(this.noteMessage.action + 'Successful'),
            duration: 3000
          })
        })
        this.selectedSeg = {}
        this.loadSegmentsDetail()
      }, (res) => {
        handleError(res)
      })
      this.noteMessage.actionVisible = false
      this.btnLoading = false
    }
  },
  locales: {
    'en': {refresh: 'Refresh', merge: 'Merge', drop: 'Drop', cubeVersion: 'Cube Version', StorageSize: 'Storage Size', RangeStartTime: 'Range Start Time:', RangeEndTime: 'Range End Time', NoStorageInfo: 'No Segment Info.', cubeHBRegionCount: 'Region Count', cubeHBSize: 'Segment Size', cubeHBStartTime: 'Range Start Time', cubeHBEndTime: 'Range End Time', totalSize: 'Total Size :', viewCube: 'View Cube', viewSeg: 'View Segment Detail', action: 'Action', mergeNumberNote: 'Merge action will require checking two segments at lease.', mergeDiscontinueNote: 'Selected segments are discontinue, do you want to merge them anyway?', refreshMultipleNote: 'Refresh multiple segments may trigger related build jobs.', refreshNumberNote: 'Refresh action will require one segment at least.', dropNumberNote: 'Drop action will require one segment at least.', REFRESHSuccessful: 'Refresh the segments successful!', MERGESuccessful: 'Merge the segments successful!', DROPSuccessful: 'Drop the segments successful!', REFRESHActionMeg: 'Confirm to refresh the segments?', MWEGEActionMeg: 'Confirm to merge the segments?', DROPActionMeg: 'Confirm to drop the segments?', applyLatestCube: 'Apply the latest cube definition to refresh.', applyOriginalCube: 'Apply the original cube definition to refresh.', segsOverlap: 'Selected Segments have overlap in timeline, please try a different segment.', differentDefinition: 'Selected segments have difference on cube definition, please try a differenr segment.'},
    'zh-cn': {refresh: '刷新', merge: '合并', drop: '删除', cubeVersion: 'Cube版本', StorageSize: '存储空间', RangeStartTime: '区间起始时间', RangeEndTime: '区间终止时间', totalSize: '总大小 ：', RawTableStorageSize: 'Table Index 存储空间', NoStorageInfo: '没有Segment相关信息.', cubeHBSize: '大小', cubeHBStartTime: '开始时间', cubeHBEndTime: '结束时间', viewCube: '查看Cube', viewSeg: '查看Segment详细信息', action: '操作', mergeNumberNote: '合并需要至少选择两个segments。', mergeDiscontinueNote: '您选择的segment不连续，是否继续合并？', refreshMultipleNote: '刷新多个segment可能会触发关联的构建任务。', refreshNumberNote: '刷新需要至少选择一个segment。', dropNumberNote: '删除需要至少选择一个segment。', REFRESHSuccessful: '刷新Segments成功!', MERGESuccessful: '合并Segments成功!', DROPSuccessful: '清理Segments成功!', REFRESHActionMeg: '确定刷新以下Segments?', MERGEActionMeg: '确定合并以下Segments?', DROPActionMeg: '确定删除以下Segments?', applyLatestCube: '应用最新Cube定义刷新。', applyOriginalCube: '应用原Cube定义刷新。', segsOverlap: '您选择的segment时间有重合，请重新选择。', differentDefinition: '您选择的segment属于不同版本的cube，请重新选择。'}
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
    .el-select {
      width: 15%;
    }
    .el-table {
      .el-table__header thead th .cell {
        background: #393e53;
        color: #ffffff;
      }
      .info-row {
        background-color: #535C86;
      }
      .info-row:hover {
        background-color: #535C86!important;
      }
    }
    .pager {
      margin-bottom: 0px!important;
      .total_size {
        position: absolute;
        line-height: 32px;
        right: 23%;
        color: #fff;
        font-size: 13px;
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
