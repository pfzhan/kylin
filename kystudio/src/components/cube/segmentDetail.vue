<template>
  <div id="segment_detail">
    <div class="ksd-common-table rowkeys ksd-mt-10" v-if="extraoption.type==='hbase'">
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('SegmentID')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentUUID}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>Htable</span>
          </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.tableName}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('segmentName')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentName}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('segmentStatus')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentStatus}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeVersion')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>
            <el-tag type="primary">{{extraoption.version.description}}</el-tag>
            <p>{{extraoption.version.createTime}}</p>
          </span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('sourceCount')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.sourceCount}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.sourceOffsetStart>0">
        <el-col :span="4" class="left-part">
          <span>{{$t('sourceOffsetStart')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.sourceOffsetStart}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.sourceOffsetEnd>0">
        <el-col :span="4" class="left-part">
          <span>{{$t('sourceOffsetEnd')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.sourceOffsetEnd}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeHBRegionCount')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.regionCount}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeHBSize')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.tableSize}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeHBStartTime')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.dateRangeStart| utcTime}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeHBEndTime')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.dateRangeEnd| utcTime}}</span>
        </el-col>
      </el-row>
    </div>

    <div class="ksd-common-table ksd-mt-10"  v-if="extraoption.type==='columnar'">
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('SegmentID')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentUUID}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('SegmentName')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentName}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('segmentStatus')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentStatus}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" >
        <el-col :span="4" class="left-part">
          <span>{{$t('cubeVersion')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <el-tag type="primary">{{extraoption.version.description}}</el-tag>
          <p>{{extraoption.version.createTime}}</p>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('SegmentPath')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.segmentPath}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('FileNumber')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.fileCount}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('StorageSize')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.storageSize|dataSize}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('RangeStartTime')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.dateRangeStart | utcTime}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader">
        <el-col :span="4" class="left-part">
          <span>{{$t('RangeEndTime')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.dateRangeEnd | utcTime}}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.rawTableSegmentPath">
        <el-col :span="4" class="left-part">
          <span>{{$t('RawTableSegmentPath')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.rawTableSegmentPath }}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.rawTableFileCount>=0 && extraoption.rawTableSegmentPath">
        <el-col :span="4" class="left-part">
          <span>{{$t('RawTableFileNumber')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.rawTableFileCount }}</span>
        </el-col>
      </el-row>
      <el-row class="tableheader" v-if="extraoption.rawTableStorageSize>=0 && extraoption.rawTableSegmentPath">
        <el-col :span="4" class="left-part">
          <span>{{$t('RawTableStorageSize')}}</span>
        </el-col>
        <el-col :span="20" class="right-part">
          <span>{{extraoption.rawTableStorageSize |dataSize}}</span>
        </el-col>
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
  locales: {
    'en': {SegmentID: 'Segment ID', SegmentName: 'Segment Name', SegmentStatus: 'Segment Status', SourceCount: 'Source Count', SegmentPath: 'Segment Path', FileNumber: 'File Number', StorageSize: 'Storage Size', RangeStartTime: 'Range Start Time:', RangeEndTime: 'Range End Time:', RawTableSegmentPath: 'Table Index Segment Path', RawTableFileNumber: 'Table Index File Number', RawTableStorageSize: 'Table Index Storage Size', TotalSize: 'Total Size', CubeID: 'Cube ID:', cubeHBRegionCount: 'Region Count', cubeHBSize: 'Size', cubeHBStartTime: 'Start Time', cubeHBEndTime: 'End Time', cubeVersion: 'Cube Version', sourceOffsetStart: 'Source Offset Start', sourceOffsetEnd: 'Source Offset End'},
    'zh-cn': {SegmentID: 'Segment ID', segmentName: 'Segment 名称', segmentStatus: 'Segment 状态', sourceCount: 'Source 数量', SegmentPath: 'Segment 路径：', FileNumber: '索引文件数：', StorageSize: '存储空间', RangeStartTime: '区间起始时间', RangeEndTime: '区间终止时间', RawTableSegmentPath: 'Table Index Segment 路径', RawTableFileNumber: 'Table Index 索引文件数', RawTableStorageSize: 'Table Index 存储空间：', TotalSize: '总大小', CubeID: 'Cube ID', cubeHBRegionCount: 'Region数量', cubeHBSize: '大小', cubeHBStartTime: '开始时间', cubeHBEndTime: '结束时间', cubeVersion: 'Cube版本', sourceOffsetStart: 'Source Offset Start', sourceOffsetEnd: 'Source Offset End'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  #segment_detail {
    padding: 30px 30px 30px 30px;
    .left-part {
      font-weight: bolder;
      font-size: 12px;
      border-right: 1px solid #393e53;
      span {
        float: right;
        margin-right: 15px;
      }
    }
    .right-part {
      font-size: 12px;
      span {
        float: left;
        margin-left: 15px;
      }
      .el-tag--primary {
        margin: 7px 15px 7px 0px;
        background: #218fea;
      }
      p {
        display: flex;
      }
    }
  }
</style>
