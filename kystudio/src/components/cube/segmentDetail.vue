<template>
  <div id="segments">
     <div class="ksd-common-table ksd-mt-20 ksd-mr-20 ksd-ml-20" v-if="extraoption.type==='hbase'">
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">{{$t('SegmentID')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.uuid}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">{{$t('SegmentName')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.name}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">HTable</el-col>
          <el-col :span="20" class="right-part">{{extraoption.additionalInfo.hbaseTableName}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">Segment Status</el-col>
          <el-col :span="20" class="right-part">{{extraoption.status}}</el-col>
       </el-row>
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part">Source Count</el-col>
          <el-col :span="20" class="right-part">{{extraoption.input_records}}</el-col>
       </el-row>
       <el-row class="tableheader" v-if="extraoption.source_offset_start>0">
          <el-col :span="4" class="left-part">Source Offset Start</el-col>
          <el-col :span="20" class="right-part">{{extraoption.source_offset_start}}</el-col>
       </el-row>
        <el-row class="tableheader" v-if="extraoption.source_offset_end>0">
          <el-col :span="4" class="left-part">Source Offset End</el-col>
          <el-col :span="20" class="right-part">{{extraoption.source_offset_end}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBRegionCount')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.additionalInfo.hbaseRegionCount}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBSize')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.additionalInfo.storageSizeBytes|dataSize}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBStartTime')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.date_range_start| utcTimeOrInt(isInteger)}}</el-col>
       </el-row>
       <el-row class="tableheader">
          <el-col :span="4" class="left-part">{{$t('cubeHBEndTime')}}</el-col>
          <el-col :span="20" class="right-part">{{extraoption.date_range_end| utcTimeOrInt(isInteger)}}</el-col>
       </el-row>
     </div>

   <div class="ksd-common-table ksd-mt-20 ksd-mt-20 ksd-mr-20 ksd-ml-20"  v-if="extraoption.type==='columnar'">
       <el-row class="tableheader" >
          <el-col :span="4" class="left-part"><b>{{$t('SegmentID')}}</b></el-col>
          <el-col :span="20" class="right-part">{{extraoption.uuid}}</el-col>
       </el-row>
        <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('SegmentName')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.name}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('SegmentPath')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.segmentPath}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('FileNumber')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.storageFileCount}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('StorageSize')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.storageSizeBytes|dataSize}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('RangeStartTime')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.date_range_start | utcTimeOrInt(isInteger)}}</el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part"><b>{{$t('RangeEndTime')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.date_range_end | utcTimeOrInt(isInteger)}}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.additionalInfo.tableIndexSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableSegmentPath')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.tableIndexSegmentPath }}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.additionalInfo.tableIndexFileCount>=0 && extraoption.additionalInfo.tableIndexSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableFileNumber')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.tableIndexFileCount }}</el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.additionalInfo.tableIndexStorageSizeBytes>=0 && extraoption.additionalInfo.tableIndexSegmentPath">
        <el-col :span="4" class="left-part"><b>{{$t('RawTableStorageSize')}}</b></el-col>
        <el-col :span="20" class="right-part">{{extraoption.additionalInfo.tableIndexStorageSizeBytes |dataSize}}</el-col>
      </el-row>
    </div>
</div>
</template>
<script>
export default {
  name: 'showSegments',
  props: ['extraoption'],
  data () {
    return {
      segments: []
    }
  },
  computed: {
    isInteger () {
      return this.extraoption.isInteger
    }
  },
  locales: {
    'en': {SegmentID: 'Segment ID', SegmentName: 'Segment Name', SegmentPath: 'Segment Path', FileNumber: 'File Number', StorageSize: 'Storage Size', RangeStartTime: 'Start Time', RangeEndTime: 'End Time', RawTableSegmentPath: 'Table Index Segment Path', RawTableFileNumber: 'Table Index File Number', RawTableStorageSize: 'Table Index Storage Size', TotalSize: 'Total Size', TotalNumber: 'Total Segment Number', CubeID: 'Cube ID', NoStorageInfo: 'No Storage Info.', cubeHBRegionCount: 'Region Count', cubeHBSize: 'Storage Size', cubeHBStartTime: 'Start Time', cubeHBEndTime: 'End Time'},
    'zh-cn': {SegmentID: 'Segment ID', SegmentName: 'Segment 名称', SegmentPath: 'Segment 路径', FileNumber: '索引文件数', StorageSize: '存储空间', RangeStartTime: '起始时间', RangeEndTime: '结束时间', RawTableSegmentPath: 'Table Index Segment 路径', RawTableFileNumber: 'Table Index 索引文件数', RawTableStorageSize: 'Table Index 存储空间', TotalSize: '总大小', TotalNumber: '总个数', CubeID: 'Cube ID', NoStorageInfo: '没有存储的相关信息。', cubeHBRegionCount: 'Region 数量', cubeHBSize: '存储空间', cubeHBStartTime: '起始时间', cubeHBEndTime: '结束时间'}
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
      height: auto;
      .el-col{
        text-align: left;
        padding-left: 20px;
        white-space: nowrap;
      }
      .left-part{
        text-align: right;
        padding-right: 20px;
        font-weight:bold;
        border-right:none;
      }
      .right-part{
        border-left:solid 1px #393e53;
        white-space: pre-wrap;
        min-height: 38px;
      }
    }
  }
</style>
