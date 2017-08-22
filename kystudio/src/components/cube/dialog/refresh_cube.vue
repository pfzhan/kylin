<template>
<div v-if="cubeDesc.partitionDateColumn">
  <el-row :gutter="20">
    <el-col :span="8">{{$t('partitionDateColumn')}}</el-col>
    <el-col :span="16">{{cubeDesc.partitionDateColumn}}</el-col>
  </el-row>
  <el-row :gutter="20">
    <el-col :span="8">{{$t('refreshSegment')}}</el-col>
    <el-col :span="16">  
      <el-select v-model="selected_segment" class="select" @change="changeSegment">
        <el-option 
          v-for="(item, index) in cubeDesc.segments"
          :key="index"
          :label="item.name"
          :value="item.uuid">
        </el-option>
      </el-select>
    </el-col>
  </el-row>
  <el-row :gutter="20">
    <el-col :span="8">{{$t('segmentDetail')}}</el-col>
    <el-col :span="16">
      <el-card>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('startDate')}}</el-col>
          <el-col :span="16">{{selectSeg.detail.date_range_start | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('endDate')}}</el-col>
          <el-col :span="16">{{selectSeg.detail.date_range_end | utcTime}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildTime')}}</el-col>
          <el-col :span="16">{{lastBuild}}</el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="8">{{$t('lastBuildID')}}</el-col>
          <el-col :span="16">{{selectSeg.detail.last_build_job_id}}</el-col>
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
  name: 'refresh_cube',
  props: ['cubeDesc'],
  data () {
    return {
      selected_segment: this.cubeDesc.segments[0].uuid,
      selectSeg: {
        detail: this.cubeDesc.segments[0]
      }
    }
  },
  methods: {
    transToGmtTime,
    changeSegment: function (item) {
      this.cubeDesc.segments.forEach((segment) => {
        if (segment.uuid === item) {
          this.$set(this.selectSeg, 'detail', segment)
        }
      })
    }
  },
  created () {
    this.$on('refreshCubeFormValid', (t) => {
      this.$emit('validSuccess', this.selectSeg.detail, !!this.cubeDesc.partitionDateColumn)
    })
  },
  computed: {
    lastBuild () {
      return transToGmtTime(this.selectSeg.detail.last_build_time)
    }
  },
  watch: {
    cubeDesc (cubeDesc) {
      this.selected_segment = this.cubeDesc.segments[0].uuid
      this.$set(this.selectSeg, 'detail', this.cubeDesc.segments[0])
    }
  },
  locales: {
    'en': {partitionDateColumn: 'PARTITION DATE COLUMN', refreshSegment: 'REFRESH SEGMENT', segmentDetail: 'SEGMENT DETAIL', startDate: 'Start Date (Include)', endDate: 'End Date (Exclude)', lastBuildTime: 'Last build Time', lastBuildID: 'Last build ID', noPartition: 'No partition column is defined in model. Thereby cube refreshing would trigger a full build job, which may cost a lot of resources.'},
    'zh-cn': {partitionDateColumn: '分区日期列', refreshSegment: '刷新的SEGMENT', segmentDetail: 'SEGMENT详细信息', startDate: '起始日期 (包含)', endDate: '结束日期 (不包含)', lastBuildTime: '上次构建时间', lastBuildID: '上次构建ID', noPartition: '相关模型中未定义分割日期／时间列。因而刷新cube将触发一个全量构建的任务，可能占用较多资源。'}
  }
}
</script>
<style scoped="">
.select {
  width: 100%
}
</style>
