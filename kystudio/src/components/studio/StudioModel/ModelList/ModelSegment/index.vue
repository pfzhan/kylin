<template>
  <div class="model-segment">
    <div class="segment-actions clearfix">
      <div class="left">
        <el-button size="medium" type="primary" icon="el-icon-ksd-table_refresh">{{$t('kylinLang.common.refresh')}}</el-button>
        <el-button size="medium" type="primary" icon="el-icon-ksd-merge" @click="mergeSegment">{{$t('merge')}}</el-button>
        <el-button size="medium" type="primary" icon="el-icon-ksd-drop">{{$t('kylinLang.common.drop')}}</el-button>
      </div>
      <div class="right">
        <div class="segment-action">
          <span class="input-label">
            {{$t('segmentPeriod')}}
          </span>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="medium"
            v-model="filter.startDate"
            :picker-options="{ disabledDate: getStartDateLimit }"
            :placeholder="$t('chooseStartTime')">
          </el-date-picker>
          <span class="input-split">-</span>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="medium"
            v-model="filter.endDate"
            :picker-options="{ disabledDate: getEndDateLimit }"
            :placeholder="$t('chooseEndTime')">
          </el-date-picker>
        </div>
        <div class="segment-action">
          <span class="input-label">{{$t('primaryPartition')}}</span>
          <el-select v-model="filter.mpValues" size="medium" :placeholder="$t('pleaseChoose')">
            <el-option
              label="Shanghai"
              value="Shanghai">
            </el-option>
          </el-select>
        </div>
      </div>
    </div>

    <div class="segment-views">
      <div class="segment-charts">
        <SegmentChart v-if="segments.length" :segments-data="segments" @select="selectSegment" v-model="zoom" />
        <div class="chart-actions">
          <div class="icon-button">
            <img :src="iconAdd" />
          </div>
          <div class="icon-button">
            <img :src="iconReduce" />
          </div>
          <div>{{zoom.toFixed(0)}}</div>
          <div>%</div>
          <div class="linear-chart"></div>
          <div class="empty-chart"></div>
        </div>
      </div>
    </div>

    <div class="segment-settings">
      <h1 class="title font-medium">{{$t('segmentSetting')}}</h1>
      <div class="settings">
        <div class="setting" v-for="(config, configName) in configs" :key="configName">
          <el-checkbox class="setting-checkbox" v-model="config.isEnabled">{{$t(configName)}}</el-checkbox>
          <div class="setting-input"
            v-for="(setting, index) in config.settings"
            :key="index">
            <el-input
              v-model="setting.value"
              :placeholder="$t('textInput')">
            </el-input>
            <el-select class="setting-select" v-model="setting.key">
              <el-option
                v-for="item in labels"
                :key="item.value"
                :label="item.label"
                :value="item.value">
              </el-option>
            </el-select>
            <el-button
              v-if="index === 0 && config.isAddible"
              @click="addSetting(configName)"
              size="small"
              class="is-circle primary new-setting"
              icon="el-icon-ksd-add">
            </el-button>
            <el-button
              v-if="config.isAddible"
              @click="deleteSetting(configName, index)"
              size="small"
              class="is-circle delete-setting"
              icon="el-icon-ksd-minus"
              :disabled="config.settings.length <= 1">
            </el-button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import mock from './mock'
import SegmentChart from './SegmentChart'
import { handleSuccessAsync } from '../../../../../util'
import iconAdd from './icon_add.svg'
import iconReduce from './icon_reduce.svg'

@Component({
  props: {
    model: {
      type: Object
    },
    isShowActions: {
      type: Boolean,
      default: true
    },
    isShowSettings: {
      type: Boolean,
      default: true
    }
  },
  components: {
    SegmentChart
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getCubesList: 'GET_CUBES_LIST',
      getCubeSegments: 'GET_CUBE_SEGMENTS'
    })
  },
  locales
})
export default class ModelSegment extends Vue {
  iconAdd = iconAdd
  iconReduce = iconReduce
  configs = {
    autoMerge: {
      isEnabled: true,
      isAddible: true,
      settings: [{
        key: 'Day',
        value: ''
      }]
    },
    retension: {
      isEnabled: true,
      isAddible: true,
      settings: [{
        key: 'Day',
        value: ''
      }]
    },
    volatile: {
      isEnabled: true,
      isAddible: false,
      settings: [{
        key: 'Day',
        value: ''
      }]
    }
  }
  zoom = 100
  segments = []
  labels = [{
    label: 'Day',
    value: 'Day'
  }]
  filter = {
    pageOffset: 0,
    pageSize: 999,
    mpValues: '',
    startDate: '',
    endDate: ''
  }
  get selectedSegments () {
    return this.segments.filter(segment => segment.isSelected)
  }
  async mounted () {
    // await this.fetchSegments()
    this.segments = [
      ...mock.segments.map(segment => ({
        ...segment,
        from: new Date(segment.date_range_start),
        to: segment.date_range_end ? new Date(segment.date_range_end) : new Date(8640000000000000),
        isSelected: false
      }))
    ]
  }
  async fetchSegments () {
    const cubeRes = await this.getCubesList({
      projectName: this.currentSelectedProject,
      modelName: this.model.name
    })
    const { cubes } = await handleSuccessAsync(cubeRes)

    const segmentRes = await Promise.all(cubes.map(cube => this.getCubeSegments({
      name: cube.name,
      filter: this.filter
    })))

    segmentRes.forEach(async (res, index) => {
      const segmentsData = await handleSuccessAsync(res)
      this.segments = [
        ...this.segments, ...segmentsData.segments.map(segment => ({
          ...segment,
          from: new Date(segment.date_range_start),
          to: segment.date_range_end ? new Date(segment.date_range_end) : new Date(8640000000000000),
          isSelected: false
        }))
      ]
    })
  }
  getStartDateLimit (time) {
    return this.filter.endDate ? time.getTime() > this.filter.endDate.getTime() : false
  }
  getEndDateLimit (time) {
    return this.filter.startDate ? time.getTime() < this.filter.startDate.getTime() : false
  }
  deleteSetting (configName, index) {
    const currentSettings = this.configs[configName].settings
    currentSettings.length > 1 && currentSettings.splice(index, 1)
  }
  addSetting (configName) {
    const currentSettings = this.configs[configName].settings
    currentSettings.push({ key: 'Day', value: '' })
  }
  addZoom () {
    if (this.zoom < 300) {
      this.zoom += 10
    }
    if (this.zoom > 300) {
      this.zoom = 300
    }
  }
  minusZoom () {
    if (this.zoom > 0) {
      this.zoom -= 10
    }
    if (this.zoom <= 5) {
      this.zoom = 5
    }
  }
  selectSegment (data, isSelectable) {
    if (isSelectable) {
      this.segments = this.segments.map(segment => {
        if (segment.uuid === data.uuid) {
          segment.isSelected = !segment.isSelected
        }
        return segment
      })
    } else {
      this.$message('请选择相邻的segment')
    }
  }
  mergeSegment () {
    if (this.selectedSegments.length) {
      let minDate = Infinity
      let maxDate = -Infinity

      this.selectedSegments.forEach(segment => {
        if (segment.date_range_start < minDate) {
          minDate = segment.date_range_start
        }
        if (segment.date_range_end > maxDate) {
          maxDate = segment.date_range_end
        }
      })

      this.segments = this.segments.filter(segment => !this.selectedSegments.some(selected => selected.uuid === segment.uuid))
      const id = Math.random() * 100 + 100
      this.segments.push({
        size_kb: 36,
        snapshots: null,
        source_offset_end: 0,
        source_offset_start: 0,
        status: 'READY',
        storage_location_identifier: 'KYLIN_HE2YMKK60C',
        total_shards: 0,
        uuid: `6be0d737-1dc7-41d8-ab8d-a1bd1689307c${id}`,
        name: '20120111164354_20130109174429',
        last_build_time: 1532167086872,
        date_range_start: minDate,
        date_range_end: maxDate,
        hit_count: 100,
        isMerging: true,
        from: new Date(minDate),
        to: maxDate ? new Date(maxDate) : new Date(8640000000000000),
        isSelected: false
      })
    }
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-segment {
  padding: 20px 0;
  margin-bottom: 20px;
  .segment-actions {
    .left {
      float: left;
      display: none;
    }
    .right {
      float: right;
    }
    .segment-action {
      display: inline-block;
      margin-right: 10px;
    }
    .segment-action:last-child {
      margin: 0;
    }
    .el-input {
      width: 200px;
    }
    .el-select .el-input {
      width: 150px;
    }
  }
  .input-label {
    margin-right: 6px;
  }
  .input-split {
    margin: 0 7px;
  }
  .segment-charts {
    position: relative;
    .segment-chart {
      width: calc(~'100% - 25px');
    }
    .chart-actions {
      position: absolute;
      right: 0;
      top: 10px;
      text-align: center;
    }
    .icon-button {
      width: 21px;
      height: 21px;
      padding: 6px;
      background: #0988DE;
      margin: 0 auto 5px auto;
      cursor: pointer;
    }
    .icon-button img {
      display: block;
      width: 9px;
      height: 9px;
    }
    .linear-chart {
      width: 10px;
      height: 50px;
      background: linear-gradient(0deg, #FFCD58 0%, #FAC954 36%, #FF0000 100%);
      border: 1px solid #CFD8DC;
      margin: 10px auto 5px auto;
    }
    .empty-chart {
      width: 10px;
      height: 10px;
      margin: 0 auto;
      background: #FFFFFF;
      border: 1px solid #B0BEC5;
    }
  }
  .title {
    font-size: 16px;
    color: #263238;
    margin: 20px 0 10px 0;
  }
  .segment-settings {
    display: none;
  }
  .settings {
    padding: 15px 0;
    border: 1px solid #B0BEC5;
    background: white;
  }
  .setting {
    padding: 0 23px;
    &:not(:last-child) {
      border-bottom: 1px solid #B0BEC5;
      padding-bottom: 10px;
    }
    &:not(:first-child) {
      padding-top: 10px;
    }
    .el-input {
      width: 220px;
    }
    .setting-checkbox {
      float: left;
      position: relative;
      transform: translateY(9px);
    }
    .setting-input {
      margin-left: 117px;
      &:not(:last-child) {
        padding-bottom: 10px;
      }
      &:not(:nth-child(2)) .delete-setting {
        margin-left: 46px;
      }
    }
    .is-circle {
      margin-left: 10px;
    }
    .el-select {
      width: 100px;
      margin-left: 5px;
      .el-input {
        width: 100%;
      }
    }
  }
}
</style>
