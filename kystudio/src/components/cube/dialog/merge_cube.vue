<template>
<div v-if="cubeDesc.partitionDateColumn">
  <el-row :gutter="20">
    <el-col :span="8">{{$t('partitionDateColumn')}}</el-col>
    <el-col :span="16">{{cubeDesc.partitionDateColumn}}</el-col>
  </el-row>
  <el-row :gutter="20">
    <el-col :span="8">{{$t('mergeStartSegment')}}</el-col>
    <el-col :span="16">  
      <el-select v-model="startSegment" class="select" >
        <el-option 
          v-for="(item, index) in cubeDesc.segments"
          :key="index"
          :label="item.name"
          :value="item">
        </el-option>
      </el-select>
    </el-col>
  </el-row>
  <el-row :gutter="20">
    <el-col :span="8">{{$t('mergeEndSegment')}}</el-col>
    <el-col :span="16">  
      <el-select v-model="endSegment" class="select" >
        <el-option 
          v-for="(item, index) in cubeDesc.segments"
          :key="index"
          :label="item.name"
          :value="item">
        </el-option>
      </el-select>
    </el-col>
  </el-row>  
  <el-row :gutter="20">
    <el-col :span="8">{{$t('startSegmentDetail')}}</el-col>
    <el-col :span="16">
      <el-card>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('startDate')}}</el-col>
          <el-col :span="16">{{startSegment.date_range_start | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('endDate')}}</el-col>
          <el-col :span="16">{{startSegment.date_range_end | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildTime')}}</el-col>
          <el-col :span="16">{{startSegLastBuild}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildID')}}</el-col>
          <el-col :span="16">{{startSegment.last_build_job_id}}</el-col>
        </el-row>
      </el-card>                  
    </el-col>
  </el-row>    
  <el-row :gutter="20">
    <el-col :span="8">{{$t('endSegmentDetail')}}</el-col>
    <el-col :span="16">
      <el-card>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('startDate')}}</el-col>
          <el-col :span="16">{{endSegment.date_range_start | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('endDate')}}</el-col>
          <el-col :span="16">{{endSegment.date_range_end | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildTime')}}</el-col>
          <el-col :span="16">{{endSegLastBuild}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildID')}}</el-col>
          <el-col :span="16">{{endSegment.last_build_job_id}}</el-col>
        </el-row> 
      </el-card>                 
    </el-col>
  </el-row>     
</div> 
<div v-else>
  {{$t('noPartition')}}
</div>
</template>
<script>
import { transToGmtTime } from '../../../util/business'
export default {
  name: 'merge_cube',
  props: ['cubeDesc'],
  data () {
    return {
      startSegment: this.cubeDesc.segments[0],
      endSegment: this.cubeDesc.segments[this.cubeDesc.segments.length - 1]
    }
  },
  computed: {
    startSegLastBuild () {
      return transToGmtTime(this.startSegment.last_build_time, this)
    },
    endSegLastBuild () {
      return transToGmtTime(this.endSegment.last_build_time, this)
    }
  },
  created () {
    let _this = this
    this.$on('mergeCubeFormValid', (t) => {
      _this.$emit('validSuccess', {date_range_start: this.startSegment.date_range_start, date_range_end: this.endSegment.date_range_end})
    })
  },
  watch: {
    cubeDesc (cubeDesc) {
      this.startSegment = this.cubeDesc.segments[0]
      this.endSegment = this.cubeDesc.segments[this.cubeDesc.segments.length - 1]
    }
  },
  locales: {
    'en': {partitionDateColumn: 'PARTITION DATE COLUMN', mergeStartSegment: 'MERGE START SEGMENT', mergeEndSegment: 'MERGE END SEGMENT', startSegmentDetail: 'START SEGMENT DETAIL', endSegmentDetail: 'END SEGMENT DETAIL', startDate: 'Start Date (Include)', endDate: 'End Date (Exclude)', lastBuildTime: 'Last build Time', lastBuildID: 'Last build ID', noPartition: 'No partition date column defined.'},
    'zh-cn': {partitionDateColumn: '分区日期列', mergeStartSegment: '合并的初始的SEGMENT', mergeEndSegment: '合并的结束的SEGMENT', startSegmentDetail: '初始的SEGMENT的详细信息', endSegmentDetail: '结束的SEGMENT的详细信息', startDate: '起始日期 (包含)', endDate: '结束日期 (不包含)', lastBuildTime: '上次构建时间', lastBuildID: '上次构建ID', noPartition: '没有定义分区日期列. '}
  }
}
</script>
<style scoped="">
.select {
  width: 100%
}
</style>
