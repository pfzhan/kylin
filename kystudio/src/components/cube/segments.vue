<template>
  <div class="segments">
    <el-row class="ksd-mb-16 ksd-mt-4" v-if="segments.length > 0 || (timeFilter.end !== '' && timeFilter.end !== null && timeFilter.start !== '')">
      <div class="ksd-fleft">
        <el-button size="medium" plain type="primary" @click="refresh" v-show="cube.status!=='DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " :disabled="selectionSegs.length < 1">{{$t('REFRESH')}}</el-button>
        <el-button size="medium" plain type="primary" @click="merge" v-show="cube.status!== 'DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " :disabled="selectionSegs.length < 2">{{$t('MERGE')}}</el-button>
        <el-button size="medium" plain type="primary" @click="drop" v-show="cube.status!=='DESCBROKEN' && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " :disabled="selectionSegs.length < 1">{{$t('DROP')}}</el-button>
      </div>

      <div v-show="cube.multilevel_partition_cols.length > 0" class="ksd-fright mutil-level">
        <el-tooltip class="item ksd-mr-6 ksd-ml-20" effect="dark" :content="cube.multilevel_partition_cols[0]" placement="top">
          <span class="ksd-lineheight-32">{{cube.multilevel_partition_cols[0] && cube.multilevel_partition_cols[0].length > 10 ? cube.multilevel_partition_cols[0].slice(0, 10) + '...' : cube.multilevel_partition_cols[0]}}</span>
        </el-tooltip>
        <el-select size="medium" v-model="filterDetail.mpValues" filterable :placeholder="$t('kylinLang.common.pleaseFilter')" @change="filterChange">
          <el-option
            v-for="item in mpValuesList"
            :key="item.name"
            :label="item.name"
            :value="item.name">
          </el-option>
        </el-select>
      </div>

      <div v-show="cube.partitionDateColumn" class="ksd-fright time_filter">
        <div v-if="cube.isStandardPartitioned">
          <el-date-picker size="medium" :clearable="false" ref="startDateInput" 
            v-model="timeFilter.start" type="datetime" @change="timeChange">
          </el-date-picker>
          <span> - </span>
          <el-date-picker size="medium" :clearable="false" ref="endDateInput"
            v-model="timeFilter.end" type="datetime" @change="timeChange">
          </el-date-picker>
        </div>
        <div v-else>
          <el-input size="medium" v-model="timeFilter.start" @input="timeChange"></el-input>
          <span> - </span>
          <el-input size="medium" v-model="timeFilter.end" @input="timeChange" :placeholder="$t('kylinLang.common.pleaseInput')"></el-input>
        </div>
      </div>
    </el-row>

    <div v-if="segments.length > 0">

    </div>
    <el-card class="noSegments" v-else>
      {{$t('NoStorageInfo')}}
    </el-card>


    <section>
      <div id="timelineChart"></div>
    </section>


    <el-dialog width="440px" :title="$t(noteMessage.action)" :visible.sync="noteMessage.actionVisible">
      <span>{{$t(noteMessage.actionMeg)}}</span>
      <el-row v-for="segment in noteMessage.actionSegs" :key="segment.uuid">
        <el-col>
          <span>[{{segment.name}}]</span>
        </el-col>
      </el-row>
      <el-alert class="ksd-mt-10" :closable="false" :title="$t(noteMessage.warningMessage)" type="warning" v-show="noteMessage.warningMessage !== '' && noteMessage.errorMessage === ''">
      </el-alert>
      <el-alert class="ksd-mt-10" :closable="false" :title="$t(noteMessage.errorMessage)" type="error" v-show="noteMessage.errorMessage !== ''">
      </el-alert>

      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="noteMessage.actionVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" type="primary" plain @click="startAction" :loading="btnLoading" :disabled="noteMessage.errorMessage !== ''">{{$t('kylinLang.common.continue')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog width="660px" :title="$t('segmentsDetail')" :visible.sync="detailVisible">
      <segment_detail :extraoption="segmentDesc" class="ksd-mb-40"></segment_detail>
    </el-dialog>
  </div>
</template>
<script>
import * as d3 from 'd3'
import { permissions } from '../../config'
import { mapActions } from 'vuex'
import segmentDetail from './segment_detail'
import { handleSuccess, handleError, hasPermission, hasRole, transToUtcTimeFormat, transToUTCMs } from '../../util/business'
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
        pageSize: 10,
        startTime: 0,
        endTime: null
      },
      timeFilter: {
        start: this.cube.isStandardPartitioned ? transToUtcTimeFormat(0) : 0,
        end: null
      },
      mpValuesList: [],
      noteMessage: {
        actionSegs: [],
        action: '',
        actionMeg: '',
        warningMessage: '',
        errorMessage: '',
        actionVisible: false
      },
      detailVisible: false,
      segmentDesc: {},
      selectionSegs: []
    }
  },
  components: {
    'segment_detail': segmentDetail
  },
  methods: {
    ...mapActions({
      getCubeSegments: 'GET_CUBE_SEGMENTS',
      updateCubeSegments: 'UPDATE_CUBE_SEGMENTS',
      getMPValues: 'GET_MP_VALUES'
    }),
    hasSomePermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    hasOperationPermissionOfProject () {
      return hasPermission(this, permissions.OPERATION.mask)
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
        const mockData = {
          size: 3,
          total_storage_size_kb: 137,
          segments: [{
            size_kb: 36,
            snapshots: null,
            source_offset_end: 0,
            source_offset_start: 0,
            status: 'READY',
            storage_location_identifier: 'KYLIN_HE2YMKK60C',
            total_shards: 0,
            uuid: '6be0d737-1dc7-41d8-ab8d-a1bd1689307c',
            name: '20120111164354_20130109174429',
            last_build_time: 1532167086872,
            date_range_start: 1326300234000,
            date_range_end: 1357753469000
          }, {
            size_kb: 36,
            snapshots: null,
            source_offset_end: 0,
            source_offset_start: 0,
            status: 'READY',
            storage_location_identifier: 'KYLIN_HE2YMKK60C',
            total_shards: 0,
            uuid: '6be0d737-1dc7-41d8-ab8d-a1bd1689307c',
            name: '20120111164354_20130109174429',
            last_build_time: 1532167086872,
            date_range_start: 1325593246000,
            date_range_end: 1326300234000
          }, {
            size_kb: 36,
            snapshots: null,
            source_offset_end: 0,
            source_offset_start: 0,
            status: 'READY',
            storage_location_identifier: 'KYLIN_HE2YMKK60C',
            total_shards: 0,
            uuid: '6be0d737-1dc7-41d8-ab8d-a1bd1689307c',
            name: '20120111164354_20130109174429',
            last_build_time: 1532167086872,
            date_range_start: 0,
            date_range_end: 1325593246000
          }]
        }
        handleSuccess(res, (data) => {
          this.segments = mockData.segments.map((p) => {
            p.from = new Date(p.date_range_start)
            p.to = p.date_range_end > 8640000000000000 ? new Date(8640000000000000) : new Date(p.date_range_end)
            return p
          })
          this.totalSize = data.total_storage_size_kb
          this.totalSegments = data.size
          // if (this.selectedSeg[this.currentPage]) {
          //   this.$nextTick(() => {
          //     for (let index in this.selectedSeg[this.currentPage]) {
          //       this.$refs.segmentsTable.toggleRowSelection(this.$refs.segmentsTable.tableData[index], true)
          //     }
          //   })
          // }
          // this.$nextTick(() => {
          //   this.selectionSegs.forEach((segment) => {
          //     let index = indexOfObjWithSomeKey(this.segments, 'uuid', segment.uuid)
          //     if (index >= 0) {
          //       this.$refs.segmentsTable.toggleRowSelection(this.$refs.segmentsTable.tableData[index], true)
          //     }
          //   })
          // })

          let element = this.$el.querySelectorAll('#timelineChart')[0]
          let timeGroup = [{'label': 'Normal', 'data': this.segments}]
          this.initTimeline(element, timeGroup)
        })
      }, (res) => {
        handleError(res)
      })
    },
    tableRowClassName: function (row, index) {
      if (row.additionalInfo && row.additionalInfo.tmp_op_flag === 'false') {
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
    //   this.$nextTick(() => {
    //     this.$set(this.selectedSeg, this.currentPage, {})
    //     val.forEach((segment) => {
    //       let index = indexOfObjWithSomeKey(this.$refs.segmentsTable.tableData, 'uuid', segment.uuid)
    //       this.$set(this.selectedSeg[this.currentPage], index, segment)
    //     })
    //   })
    },
    selectRow: function (selection, row) {
      if (selection.indexOf(row) >= 0) {
        this.selectionSegs.push(row)
      } else {
        let index = indexOfObjWithSomeKey(this.selectionSegs, 'uuid', row.uuid)
        this.selectionSegs.splice(index, 1)
      }
    },
    selectAll: function (selection) {
      if (selection.length > 0) {
        selection.forEach((segment) => {
          let index = indexOfObjWithSomeKey(this.selectionSegs, 'uuid', segment.uuid)
          if (index < 0) {
            this.selectionSegs.push(segment)
          }
        })
      } else {
        this.segments.forEach((segment) => {
          let index = indexOfObjWithSomeKey(this.selectionSegs, 'uuid', segment.uuid)
          if (index >= 0) {
            this.selectionSegs.splice(index, 1)
          }
        })
      }
    },
    viewSeg: function (segment) {
      this.segmentDesc = objectClone(segment)
      let type = 'hbase'
      if (+segment.additionalInfo.storageType === 100 || +segment.additionalInfo.storageType === 99) {
        type = 'columnar'
      }
      this.$set(this.segmentDesc, 'type', type)
      this.$set(this.segmentDesc, 'isInteger', this.isInteger)
      this.detailVisible = true
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
      // for (let page in this.selectedSeg) {
      //   for (let index in this.selectedSeg[page]) {
      //     this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
      //   }
      // }
      this.noteMessage.actionSegs = objectClone(this.selectionSegs)
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
      // for (let page in this.selectedSeg) {
      //   for (let index in this.selectedSeg[page]) {
      //     this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
      //     let segRange = {
      //       start: this.cube.is_streaming ? this.selectedSeg[page][index].source_offset_start : this.selectedSeg[page][index].date_range_start,
      //       end: this.cube.is_streaming ? this.selectedSeg[page][index].source_offset_end : this.selectedSeg[page][index].date_range_end
      //     }
      //     timeRange.push(segRange)
      //   }
      // }
      this.noteMessage.actionSegs = objectClone(this.selectionSegs)
      this.noteMessage.actionSegs.forEach((segment) => {
        let segRange = {
          start: this.cube.is_streaming ? segment.source_offset_start : segment.date_range_start,
          end: this.cube.is_streaming ? segment.source_offset_end : segment.date_range_end
        }
        timeRange.push(segRange)
      })
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
      // for (let page in this.selectedSeg) {
      //   for (let index in this.selectedSeg[page]) {
      //     this.noteMessage.actionSegs.push(this.selectedSeg[page][index])
      //   }
      // }
      this.noteMessage.actionSegs = objectClone(this.selectionSegs)
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
          // this.selectedSeg = {}
          this.selectionSegs = []
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
          // this.selectedSeg = {}
          this.selectionSegs = []
          this.loadSegmentsDetail()
        }, 1000)
      }
    },
    timeChange () {
      clearTimeout(this.lockST)
      this.lockST = setTimeout(() => {
        if (this.timeFilter.end === '' || this.timeFilter.end === null || this.timeFilter.start === '') {
          this.$message({
            type: 'error',
            duration: 0,
            message: this.$t('inputDate'),
            showClose: true
          })
          return
        }
        this.filterDetail.startTime = this.cube.isStandardPartitioned ? transToUTCMs(this.timeFilter.start) : parseInt(this.timeFilter.start)
        this.filterDetail.endTime = this.cube.isStandardPartitioned ? transToUTCMs(this.timeFilter.end) : parseInt(this.timeFilter.end)
        if (this.filterDetail.endTime < this.filterDetail.startTime) {
          this.$message({
            type: 'error',
            duration: 0,
            message: this.$t('timeCompare'),
            showClose: true
          })
          return
        }
        // this.selectedSeg = {}
        this.selectionSegs = []
        this.loadSegmentsDetail()
      }, 1000)
    },
    initTimeline (element, data, callback) {
      let allElements = data.reduce((agg, e) => agg.concat(e.data), [])

      let minDt = d3.min(allElements, getPointMinDt)
      let maxDt = d3.max(allElements, getPointMaxDt)

      let elementWidth = element.clientWidth
      let elementHeight = element.clientHeight

      let margin = {
        top: 0,
        right: 0,
        bottom: 20,
        left: 0
      }
      // let width = 800
      let width = elementWidth - margin.left - margin.right
      // let height = 130
      let height = elementHeight - margin.top - margin.bottom

      let groupWidth = 0
      let x = d3.time.scale().domain([minDt, maxDt]).range([groupWidth, width])

      let xAxis = d3.axisBottom(x).tickSize(-height)

      let zoom = d3.zoom().on('zoom', zoomed)

      let svg = d3.select(element)
      .append('svg').attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
      .call(zoom)

      svg.append('defs')
      .append('clipPath')
      .attr('id', 'chart-content')
      .append('rect')
      .attr('x', groupWidth)
      .attr('y', 0)
      .attr('height', height)
      .attr('width', width - groupWidth)

      svg.append('rect').attr('class', 'chart-bounds').attr('x', groupWidth).attr('y', 0).attr('height', height).attr('width', width - groupWidth)

      svg.append('g').attr('class', 'x axis')
      .attr('transform', 'translate(0,' + height + ')').call(xAxis)

      let groupHeight = height / data.length

      svg.selectAll('.group-section')
      .data(data)
      .enter()
      .append('line')
      .attr('class', 'group-section')
      .attr('x1', 0)
      .attr('x2', width)
      .attr('y1', function (d, i) {
        return groupHeight * data.length
      }).attr('y2', function (d, i) {
        return groupHeight * data.length
      })

      let groupIntervalItems = svg.selectAll('.group-interval-item')
                                  .data(data).enter().append('g')
                                  .attr('class', 'item')
                                  .attr('transform', function (d, i) {
                                    return 'translate(0, ' + groupHeight * i + ')'
                                  })
                                  .selectAll('.dot')
                                  .data(function (d) {
                                    return d.data
                                  })
                                  .enter()

      let intervalBarHeight = 0.8 * groupHeight
      let intervalBarMargin = (groupHeight - intervalBarHeight) / 2
      groupIntervalItems.append('rect')
      .attr('class', withCustom('interval'))
      .attr('width', function (d) {
        return Math.max(8, x(d.to) - x(d.from))
      })
      .attr('height', intervalBarHeight)
      .attr('y', intervalBarMargin)
      .attr('x', function (d) {
        return x(d.from)
      })

      zoomed()

      function getPointMaxDt (p) {
        return p.to
      }
      function getPointMinDt (p) {
        return p.from
      }

      function withCustom (defaultClass) {
        return function (d) {
          return d.customClass ? [d.customClass, defaultClass].join(' ') : defaultClass
        }
      }

      function zoomed () {
        if (d3.event) {
          let t = d3.event.transform.rescaleX(x)
          // if (isNaN(t(minDt))) {
          //   return
          // }
          svg.select('.x.axis').call(xAxis.scale(t))
          svg.selectAll('rect.interval').attr('x', function (d) {
            return t(d.from)
          }).attr('width', function (d) {
            return Math.max(8, t(d.to) - t(d.from))
          })
        }
        svg.selectAll('.interval-text').attr('x', function (d) {
          var positionData = getTextPositionData.call(this, d)
          if (positionData.upToPosition - groupWidth - 10 < positionData.textWidth) {
            return positionData.upToPosition
          } else if (positionData.xPosition < groupWidth && positionData.upToPosition > groupWidth) {
            return groupWidth
          }
          return positionData.xPosition
        }).attr('text-anchor', function (d) {
          var positionData = getTextPositionData.call(this, d)
          if (positionData.upToPosition - groupWidth - 10 < positionData.textWidth) {
            return 'end'
          }
          return 'start'
        }).attr('dx', function (d) {
          var positionData = getTextPositionData.call(this, d)
          if (positionData.upToPosition - groupWidth - 10 < positionData.textWidth) {
            return '-0.5em'
          }
          return '0.5em'
        }).text(function (d) {
          var positionData = getTextPositionData.call(this, d)
          var percent = (positionData.width - 30) / positionData.textWidth
          if (percent < 1) {
            if (positionData.width > 30) {
              return d.label.substr(0, Math.floor(d.label.length * percent)) + '...'
            } else {
              return ''
            }
          }

          return d.label
        })

        function getTextPositionData (d) {
          this.textSizeInPx = this.textSizeInPx
          var from = x(d.from)
          var to = x(d.to)
          return {
            xPosition: from,
            upToPosition: to,
            width: to - from,
            textWidth: this.textSizeInPx
          }
        }
      }
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    isInteger () {
      if (!this.cube.partitionDateColumn) {
        return false
      } else if (this.cube.isStandardPartitioned) {
        return false
      } else {
        return true
      }
    }
  },
  locales: {
    'en': {REFRESH: 'Refresh', MERGE: 'Merge', DROP: 'Drop', refreshSeg: 'Refresh Segments', mergeSeg: 'Merge Segments', dropSeg: 'Drop Segments', SegmentName: 'Segment Name', cubeVersion: 'Cube Version', StorageSize: 'Storage Size', startTime: 'Start Time', endTime: 'End Time', NoStorageInfo: 'No Segment Info.', cubeHBRegionCount: 'Region Count', cubeHBSize: 'Segment Size', cubeHBStartTime: 'Start Time', cubeHBEndTime: 'End Time', totalSize: 'Total Size :', viewCube: 'View Cube', viewSeg: 'View Details', action: 'Action', mergeNumberNote: 'Merge action will require checking two segments at lease.', mergeDiscontinueNote: 'Selected segments are discontinue, do you want to merge them anyway?', refreshMultipleNote: 'Refresh multiple segments may trigger related build jobs.', refreshNumberNote: 'Refresh action will require one segment at least.', dropNumberNote: 'Drop action will require one segment at least.', REFRESHSuccessful: 'Refresh the segments successful!', MERGESuccessful: 'Merge the segments successful!', DROPSuccessful: 'Drop the segments successful!', REFRESHActionMeg: 'Confirm to refresh the segments?', MERGEActionMeg: 'Confirm to merge the segments?', DROPActionMeg: 'Confirm to drop the segments?', applyLatestCube: 'Apply the latest cube definition to refresh.', applyOriginalCube: 'Apply the original cube definition to refresh.', segsOverlap: 'Selected Segments have overlap in timeline, please try a different segment.', differentDefinition: 'Selected segments have difference on cube definition, please try a differenr segment.', purgeCube: 'Are you sure to purge the cube? ', purgeSuccessful: 'Purge the cube successful!', buildSegment: 'A building segment. Click its \'Segment ID\' can refer to the build job.', segmentsDetail: 'Segment Detail', lockSeg: 'This segment is locked temporarily and will be replaced by the new segment.', inputDate: 'Please input the time.', timeCompare: 'End time should be later than the start time.'},
    'zh-cn': {REFRESH: '刷新', MERGE: '合并', DROP: '删除', refreshSeg: '刷新 Segments', mergeSeg: '合并 Segments', dropSeg: '删除 Segments', SegmentName: 'Segment 名称', cubeVersion: 'Cube版本', StorageSize: '存储空间', startTime: '起始时间', endTime: '结束时间', totalSize: '总大小 ：', RawTableStorageSize: 'Table Index 存储空间', NoStorageInfo: '没有Segment相关信息.', cubeHBSize: '大小', cubeHBStartTime: '开始时间', cubeHBEndTime: '结束时间', viewCube: '查看Cube', viewSeg: '查看详细信息', action: '操作', mergeNumberNote: '合并需要至少选择两个segments。', mergeDiscontinueNote: '您选择的segment不连续，是否继续合并？', refreshMultipleNote: '刷新多个segment可能会触发关联的构建任务。', refreshNumberNote: '刷新需要至少选择一个segment。', dropNumberNote: '删除需要至少选择一个segment。', REFRESHSuccessful: '刷新Segments成功!', MERGESuccessful: '合并Segments成功!', DROPSuccessful: '清理Segments成功!', REFRESHActionMeg: '确定刷新以下Segments?', MERGEActionMeg: '确定合并以下Segments?', DROPActionMeg: '确定删除以下Segments?', applyLatestCube: '应用最新Cube定义刷新。', applyOriginalCube: '应用原Cube定义刷新。', segsOverlap: '您选择的segment时间有重合，请重新选择。', differentDefinition: '您选择的segment属于不同版本的cube，请重新选择。', buildSegment: '当前Segment正在生成，点击\'Segment ID\'可见相关任务。', segmentsDetail: 'Segment 详情', lockSeg: '该segment被暂时锁定，当新的segment刷新完毕后将会被替换。', inputDate: '请输入时间。', timeCompare: '结束时间应晚于起始时间'}
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .segments{
    section {
      height: 100%;
      width: 100%;
      #timelineChart {
        width: 100%;
        height: 150px;
        line {
          stroke: @text-secondary-color;
        }
        line.vertical-marker.now {
          stroke: @text-secondary-color;
        }
        rect, .timeline-chart rect.chart-bounds {
          fill: transparent; 
        }
        rect.interval {
          ry: 5;
          rx: 5;
          fill: @text-secondary-color;
          stroke: @text-secondary-color; 
        }
      }
    }


    .noSegments {
      border: none;
      margin-top: 10px;
      background-color: @breadcrumbs-bg-color;
      box-shadow: none;
      text-align: center;
    }
    .mutil-level {
      display: inline-flex;
      .el-select {
        width: 180px;
      }
    }
    .time_filter {
      .el-input {
        width: 180px;
        .el-input__inner {
          padding-right: 10px;
        }
      }
      span {
        color: @text-secondary-color;
      }
    }
    .segments-list {
      width: 100%;
      .el-tooltip {
        i{
          margin-top:4px;
          margin-left: 20px;
        }
        .lock_seg {
          color: @text-normal-color;
        }
      }
    }
  }
</style>
