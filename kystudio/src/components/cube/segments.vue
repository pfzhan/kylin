<template>
  <div>
   <el-card v-for="(segment, index) in segments" :key="index" v-if="type==='hbase'">
    <h5><b>{{$t('SegmentID')}}</b> {{segment.segmentUUID}}</h5>
    <ul>
      <li>Htable: <span class="text-red">{{segment.tableName}}</span></li>
      <li>Segment Name: <span class="text-red">{{segment.segmentName}}</span></li>
      <li>Segment Status: <span class="text-red">{{segment.segmentStatus}}</span></li>
      <li>Source Count: <span class="text-red">{{segment.sourceCount}}</span></li>
      <li v-if="segment.sourceOffsetStart>0">SourceOffsetStart: <span class="text-red">{{segment.sourceOffsetStart}}</span></li>
      <li v-if="segment.sourceOffsetEnd>0">SourceOffsetEnd: <span class="text-red">{{segment.sourceOffsetEnd}}</span></li>
      <li>{{$t('cubeHBRegionCount')}}<span class="text-red">{{segment.regionCount}}</span></li>
       <li>{{$t('cubeHBSize')}}<span class="text-red">{{segment.tableSize}}</span></li>
      <li>{{$t('cubeHBStartTime')}}<span class="text-red">{{segment.dateRangeStart | utcTime }}</span></li>
      <li>{{$t('cubeHBEndTime')}}<span class="text-red">{{segment.dateRangeEnd | utcTime}}</span></li>

    </ul>
  </el-card>
  <el-card v-if="type==='columnar'" v-for="(segment, index) in segments" :key="index">
    <h5><b>{{$t('SegmentID')}}</b> {{segment.segmentUUID}}</h5>
    <ul>
      <li>{{$t('SegmentName')}}<span class="text-red">{{segment.segmentName}}</span></li>
      <li>{{$t('SegmentPath')}}<span class="text-red">{{segment.segmentPath}}</span></li>
      <li>{{$t('FileNumber')}}<span class="text-red">{{segment.fileCount}}</span></li>
      <li>{{$t('StorageSize')}}<span class="text-red">{{segment.storageSize }}</span></li>
      <li>{{$t('RangeStartTime')}}<span class="text-red">{{segment.dateRangeStart | utcTime}}</span></li>
      <li>{{$t('RangeEndTime')}}<span class="text-red">{{segment.dateRangeEnd | utcTime}}</span></li>
      <li v-if="segment.rawTableSegmentPath">{{$t('RawTableSegmentPath')}}<span class="text-red">{{segment.rawTableSegmentPath}}</span></li>
      <li v-if="segment.rawTableFileCount>=0 && segment.rawTableSegmentPath">{{$t('RawTableFileNumber')}}<span class="text-red">{{segment.rawTableFileCount}}</span></li>
      <li v-if="segment.rawTableStorageSize>=0 && segment.rawTableSegmentPath">{{$t('RawTableStorageSize')}}<span class="text-red">{{segment.rawTableStorageSize}}</span></li>
    </ul>
  </el-card>
  <el-card v-if="segments.length > 0">
     <h5><b>{{$t('TotalSize')}}</b> <span class="text-red">{{totalSize}}</span></h5>
     <h5><b>{{$t('TotalNumber')}}</b> <span class="text-red">{{segments.length}}</span></h5>
     <h5><b>{{$t('CubeID')}}</b> <span class="text-red">{{cube.desc.uuid}}</span></h5>
  </el-card>
  <el-card v-else>
    {{$t('NoStorageInfo')}}
  </el-card>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'showSegments',
  props: ['cube'],
  data () {
    return {
      segments: [],
      type: 'hbase',
      totalSize: 0
    }
  },
  methods: {
    ...mapActions({
      getHbaseInfo: 'GET_HBASE_INFO',
      getColumnarInfo: 'GET_COLUMNAR_INFO'
    }),
    loadSegments: function () {
      let _this = this
      // _this.cube.desc.engine_type === 99   roger 要求去掉engine_type的判断
      if (_this.cube.desc.storage_type === 100 || _this.cube.desc.storage_type === 99) {
        this.type = 'columnar'
        this.getColumnarInfo(this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            data.forEach(function (segment) {
              _this.segments.push(segment)
              _this.totalSize += segment.storageSize
              if (segment.rawTableStorageSize) {
                _this.totalSize += segment.rawTableStorageSize
              }
            })
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 3000
            })
          })
        })
      } else {
        this.getHbaseInfo(this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            data.forEach(function (segment) {
              _this.segments.push(segment)
              _this.totalSize += segment.tableSize
              if (segment.rawTableStorageSize) {
                _this.totalSize += segment.rawTableStorageSize
              }
            })
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 3000
            })
          })
        })
      }
    }
  },
  locales: {
    'en': {SegmentID: 'Segment ID ', SegmentName: 'Segment Name', SegmentPath: 'Segment Path', FileNumber: 'File Number', StorageSize: 'Storage Size', RangeStartTime: 'Range Start Time', RangeEndTime: 'Range End Time', RawTableSegmentPath: 'Raw Table Segment Path', RawTableFileNumber: 'Raw Table File Number', RawTableStorageSize: 'Raw Table StorageSize', TotalSize: 'Total Size', TotalNumber: 'Total Number', CubeID: 'Cube ID', NoStorageInfo: 'No Storage Info.', cubeHBRegionCount: 'Region Count: ', cubeHBSize: 'Size: ', cubeHBStartTime: 'Start Time: ', cubeHBEndTime: 'End Time: '},
    'zh-cn': {SegmentID: 'Segment ID ', SegmentName: 'Segment 名称', SegmentPath: 'Segment 路径', FileNumber: '索引文件数', StorageSize: '存储空间', RangeStartTime: '区间起始时间', RangeEndTime: '区间终止时间', RawTableSegmentPath: 'Raw Table Segment 路径', RawTableFileNumber: 'Raw Table 索引文件数', RawTableStorageSize: 'Raw Table 存储空间', TotalSize: '总大小', TotalNumber: '总个数', CubeID: 'Cube ID', NoStorageInfo: '没有Storage相关信息.', cubeHBRegionCount: 'Region数量: ', cubeHBSize: '大小: ', cubeHBStartTime: '开始时间: ', cubeHBEndTime: '结束时间: '}
  }
}
</script>
<style scoped>
.text-red {
  color: #dd4b39;
}

</style>
