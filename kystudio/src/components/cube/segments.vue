<template>
  <div id="segments">
   <!-- <el-card v-for="(segment, index) in segments" :key="index" v-if="type==='hbase'"> -->
    <!-- <h5><b>{{$t('SegmentID')}}</b> {{segment.segmentUUID}}</h5> -->
     <div class="ksd-common-table rowkeys ksd-mt-10"  v-for="(segment, index) in segments" :key="index" v-if="type==='hbase'">
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">{{$t('SegmentID')}}</el-col>
          <el-col :span="20">{{segment.segmentUUID}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">HTable:</el-col>
          <el-col :span="20">{{segment.tableName}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">Segment Name:</el-col>
          <el-col :span="20">{{segment.segmentName}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">Segment Status:</el-col>
          <el-col :span="20">{{segment.segmentStatus}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">Source Count:</el-col>
          <el-col :span="20">{{segment.sourceCount}}</el-col>
       </el-row>
       <el-row class="tableheader" v-if="segment.sourceOffsetStart>0">
          <el-col :span="4" class="left-part">Source Offset Start:</el-col>
          <el-col :span="20">{{segment.sourceOffsetStart}}</el-col>
       </el-row>
        <el-row class="tableheader" v-if="segment.sourceOffsetEnd>0">
          <el-col :span="4" class="left-part">Source Offset End:</el-col>
          <el-col :span="20">{{segment.sourceOffsetEnd}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBRegionCount')}}</el-col>
          <el-col :span="20">{{segment.regionCount}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBSize')}}</el-col>
          <el-col :span="20">{{segment.tableSize|dataSize}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBStartTime')}}</el-col>
          <el-col :span="20">{{segment.dateRangeStart| utcTime}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBEndTime')}}</el-col>
          <el-col :span="20">{{segment.dateRangeEnd| utcTime}}</el-col>
       </el-row>
     </div>
    <!-- <ul>  -->
      <!-- <li>Htable: <span class="text-red">{{segment.tableName}}</span></li> -->
      <!-- <li>Segment Name: <span class="text-red">{{segment.segmentName}}</span></li> -->
      <!-- <li>Segment Status: <span class="text-red">{{segment.segmentStatus}}</span></li> -->
      <!-- <li>Source Count: <span class="text-red">{{segment.sourceCount}}</span></li> -->
     <!--  <li v-if="segment.sourceOffsetStart>0">SourceOffsetStart: <span class="text-red">{{segment.sourceOffsetStart}}</span></li> -->
      <!-- <li v-if="segment.sourceOffsetEnd>0">SourceOffsetEnd: <span class="text-red">{{segment.sourceOffsetEnd}}</span></li> -->
      <!-- <li>{{$t('cubeHBRegionCount')}}<span class="text-red">{{segment.regionCount}}</span></li> -->
       <!-- <li>{{$t('cubeHBSize')}}<span class="text-red">{{segment.tableSize}}</span></li> -->
      <!-- <li>{{$t('cubeHBStartTime')}}<span class="text-red">{{segment.dateRangeStart | utcTime }}</span></li> -->
      <!-- <li>{{$t('cubeHBEndTime')}}<span class="text-red">{{segment.dateRangeEnd | utcTime}}</span></li> -->

    <!-- </ul> -->
  <!-- </el-card> -->
  <!-- <el-card v-if="type==='columnar'" v-for="(segment, index) in segments" :key="index"> -->
   <div class="ksd-common-table ksd-mt-10"  v-if="type==='columnar'" v-for="(segment, index) in segments" :key="index">
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part"><b>{{$t('SegmentID')}}</b></el-col>
          <el-col :span="20">{{segment.segmentUUID}}</el-col>
       </el-row>
        <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('SegmentName')}}</b></el-col>
        <el-col :span="20">{{segment.segmentName}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('SegmentPath')}}</b></el-col>
        <el-col :span="20">{{segment.segmentPath}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('FileNumber')}}</b></el-col>
        <el-col :span="20">{{segment.fileCount}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('StorageSize')}}</b></el-col>
        <el-col :span="20">{{segment.storageSize|dataSize}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('RangeStartTime')}}</b></el-col>
        <el-col :span="20">{{segment.dateRangeStart | utcTime}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('RangeEndTime')}}</b></el-col>
        <el-col :span="20">{{segment.dateRangeEnd | utcTime}}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="segment.rawTableSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableSegmentPath')}}</b></el-col>
        <el-col :span="20">{{segment.rawTableSegmentPath }}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="segment.rawTableFileCount>=0 && segment.rawTableSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableFileNumber')}}</b></el-col>
        <el-col :span="20">{{segment.rawTableFileCount }}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="segment.rawTableStorageSize>=0 && segment.rawTableSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableStorageSize')}}</b></el-col>
        <el-col :span="20">{{segment.rawTableStorageSize |dataSize}}</el-col>
      </el-row>
    </div>
    <!-- <h5><b>{{$t('SegmentID')}}</b> {{segment.segmentUUID}}</h5> -->
    <!-- <ul> -->
      <!-- <li>{{$t('SegmentName')}}<span class="text-red">{{segment.segmentName}}</span></li> -->
      <!-- <li>{{$t('SegmentPath')}}<span class="text-red">{{segment.segmentPath}}</span></li> -->
      <!-- <li>{{$t('FileNumber')}}<span class="text-red">{{segment.fileCount}}</span></li> -->
      <!-- <li>{{$t('StorageSize')}}<span class="text-red">{{segment.storageSize|dataSize }}</span></li> -->
      <!-- <li>{{$t('RangeStartTime')}}<span class="text-red">{{segment.dateRangeStart | utcTime}}</span></li> -->
      <!-- <li>{{$t('RangeEndTime')}}<span class="text-red">{{segment.dateRangeEnd | utcTime}}</span></li> -->
      <!-- <li v-if="segment.rawTableSegmentPath">{{$t('RawTableSegmentPath')}}<span class="text-red">{{segment.rawTableSegmentPath}}</span></li> -->
      <!-- <li v-if="segment.rawTableFileCount>=0 && segment.rawTableSegmentPath">{{$t('RawTableFileNumber')}}<span class="text-red">{{segment.rawTableFileCount}}</span></li> -->
      <!-- <li v-if="segment.rawTableStorageSize>=0 && segment.rawTableSegmentPath">{{$t('RawTableStorageSize')}}<span class="text-red">{{segment.rawTableStorageSize|dataSize}}</span></li> -->
    <!-- </ul> -->
  <!-- </el-card> -->
   <div class="ksd-common-table ksd-mt-10"  v-if="segments.length > 0">
     <el-row class="tableheader" >
        <el-col :span="4" class="left-part"><b>{{$t('TotalSize')}}</b></el-col>
        <el-col :span="20">{{totalSize |dataSize}}</el-col>
     </el-row>
     <el-row class="tableheader" >
        <el-col :span="4" class="left-part"><b>{{$t('TotalNumber')}}</b></el-col>
        <el-col :span="20">{{segments.length}}</el-col>
     </el-row>
     <el-row class="tableheader" >
        <el-col :span="4" class="left-part"><b>{{$t('CubeID')}}</b></el-col>
        <el-col :span="20">{{cube.desc.uuid}}</el-col>
     </el-row>
   </div>
 <!--  <el-card v-if="segments.length > 0">
    <table class="ksd-common-table">
      <tr>
        <th>{{$t('TotalSize')}}</th>
        <td>{{totalSize |dataSize}}</td>
      </tr>
      <tr>
        <th>{{$t('TotalSize')}}</th>
        <td>{{segments.length}}</td>
      </tr>
      <tr>
        <th>{{$t('CubeID')}}</th>
        <td>{{cube.desc.uuid}}</td>
      </tr>
    </table>
     <h5><b>{{$t('TotalSize')}}</b> <span class="text-red">{{totalSize|dataSize}}</span></h5>
     <h5><b>{{$t('TotalNumber')}}</b> <span class="text-red">{{segments.length}}</span></h5>
     <h5><b>{{$t('CubeID')}}</b> <span class="text-red">{{cube.desc.uuid}}</span></h5>
  </el-card> -->
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
      _this.segments = []
      // _this.cube.desc.engine_type === 99   roger 要求去掉engine_type的判断
      if (_this.cube.desc.storage_type === 100 || _this.cube.desc.storage_type === 99) {
        this.type = 'columnar'
        this.getColumnarInfo(this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            data[0].forEach(function (segment) {
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
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
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
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
            })
          })
        })
      }
    }
  },
  locales: {
    'en': {SegmentID: 'Segment ID:', SegmentName: 'Segment Name:', SegmentPath: 'Segment Path:', FileNumber: 'File Number:', StorageSize: 'Storage Size:', RangeStartTime: 'Start Time:', RangeEndTime: 'End Time:', RawTableSegmentPath: 'Table Index Segment Path:', RawTableFileNumber: 'Table Index File Number:', RawTableStorageSize: 'Table Index Storage Size:', TotalSize: 'Total Size:', TotalNumber: 'Total Segment Number:', CubeID: 'Cube ID:', NoStorageInfo: 'No Storage Info.', cubeHBRegionCount: 'Region Count: ', cubeHBSize: 'Storage Size: ', cubeHBStartTime: 'Start Time: ', cubeHBEndTime: 'End Time: '},
    'zh-cn': {SegmentID: 'Segment ID：', SegmentName: 'Segment 名称：', SegmentPath: 'Segment 路径：', FileNumber: '索引文件数：', StorageSize: '存储空间：', RangeStartTime: '起始时间：', RangeEndTime: '结束时间：', RawTableSegmentPath: 'Table Index Segment 路径：', RawTableFileNumber: 'Table Index 索引文件数：', RawTableStorageSize: 'Table Index 存储空间：', TotalSize: '总大小：', TotalNumber: '总个数：', CubeID: 'Cube ID：', NoStorageInfo: '没有存储的相关信息。', cubeHBRegionCount: 'Region数量: ', cubeHBSize: '存储空间: ', cubeHBStartTime: '起始时间: ', cubeHBEndTime: '结束时间: '}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .text-red {
    color: #dd4b39;
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
        font-weight:bold;
      }
    }
    // .el-card{
      // table{
      //   width: 100%;
      //   // border:solid 1px #393e53
      // }
      // th{
      //   background-color: @tableBC;
      //   // border:solid 1px #393e53
      // }
      // td{
      //   background-color: @tableBC;
      //   // border:solid 1px #393e53

      // }
      // // border-color: @grey-color;
      // padding: 10px;
      // background: @tableBC;
    // }
  }
</style>
