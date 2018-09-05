<template>
  <div class="segment-detail">
    <table class="ksd-table" v-if="extraoption.type==='hbase'">
      <tr class="ksd-tr">
        <th>{{$t('SegmentID')}}</th>
        <td>{{extraoption.uuid}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('SegmentName')}}</th>
        <td>{{extraoption.name}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>HTable</th>
        <td>     
          {{extraoption.additionalInfo.hbaseTableName}}        
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>Segment Status</th>
        <td>      
          {{extraoption.status}}
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>Source Count</th>
        <td>{{extraoption.input_records}}</td>
      </tr>
      <tr class="ksd-tr" v-if="extraoption.source_offset_start>0">
        <th>Source Offset Start</th>
        <td>{{extraoption.source_offset_start}}</td>
      </tr>
      <tr class="ksd-tr" v-if="extraoption.source_offset_end>0">
        <th>Source Offset End</th>
        <td>{{extraoption.source_offset_end}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('cubeHBRegionCount')}}</th>
        <td>     
          {{extraoption.additionalInfo.hbaseRegionCount}}      
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('cubeHBSize')}}</th>
        <td>      
          {{extraoption.additionalInfo.storageSizeBytes|dataSize}}
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('cubeHBStartTime')}}</th>
        <td>{{extraoption.date_range_start| utcTimeOrInt(isInteger)}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('cubeHBEndTime')}}</th>
        <td>{{extraoption.date_range_end| utcTimeOrInt(isInteger)}}</td>
      </tr>
    </table>


    <table class="ksd-table"  v-if="extraoption.type==='columnar'">
      <tr class="ksd-tr">
        <th>{{$t('SegmentID')}}</th>
        <td>{{extraoption.uuid}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('SegmentName')}}</th>
        <td>{{extraoption.name}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('SegmentPath')}}</th>
        <td>
          <div style="word-break:break-all">     
          {{extraoption.additionalInfo.segmentPath}}  
          </div>      
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('FileNumber')}}</th>
        <td>      
          {{extraoption.additionalInfo.storageFileCount}}
        </td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('StorageSize')}}</th>
        <td>{{extraoption.additionalInfo.storageSizeBytes|dataSize}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('RangeStartTime')}}</th>
        <td>{{extraoption.date_range_start | utcTimeOrInt(isInteger)}}</td>
      </tr>
      <tr class="ksd-tr">
        <th>{{$t('RangeEndTime')}}</th>
        <td>{{extraoption.date_range_end | utcTimeOrInt(isInteger)}}</td>
      </tr>
      <tr class="ksd-tr" v-if="extraoption.additionalInfo.tableIndexSegmentPath">
        <th>{{$t('RawTableSegmentPath')}}</th>
        <td>
          <div style="word-break:break-all">     
            {{extraoption.additionalInfo.tableIndexSegmentPath }} 
          </div>      
        </td>
      </tr>
      <tr class="ksd-tr" v-if="extraoption.additionalInfo.tableIndexFileCount>=0 && extraoption.additionalInfo.tableIndexSegmentPath">
        <th>{{$t('RawTableFileNumber')}}</th>
        <td>      
          {{extraoption.additionalInfo.tableIndexFileCount }}
        </td>
      </tr>
      <tr class="ksd-tr" v-if="extraoption.additionalInfo.tableIndexStorageSizeBytes>=0 && extraoption.additionalInfo.tableIndexSegmentPath">
        <th>{{$t('RawTableStorageSize')}}</th>
        <td>{{extraoption.additionalInfo.tableIndexStorageSizeBytes |dataSize}}</td>
      </tr>
    </table>
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
 @import '../../assets/styles/variables.less';
  .segment-detail{
    .ksd-table tr{
      th {
        width: 120px;
      }
    }
  }
</style>
