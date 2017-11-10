<template>
    <div class="advanced-setting-view ksd-common-table ksd-mt-10">
      <div class="config-setting">
        <el-row class="config-title">
          <el-col :span="24"> <b>Cubing Engine</b></el-col>
        </el-row>
        <el-row class="config-context">
          <el-col :span="8" class="left-part"> <b>Engine Type:</b></el-col>
          <el-col :span="16" class="right-part">
            <b>{{engineType}}</b>
          </el-col>
        </el-row>
      </div>
      <div class="config-setting" v-if="convertedProperties.length > 0">
        <el-row class="config-title">
          <el-col :span="24"> <b>{{$t('cubeDefault')}}</b></el-col>
        </el-row>
        <el-row class="config-context" v-for="(property, property_index) in convertedProperties" :key="property_index">
          <el-col :span="8" class="left-part"> <b>{{property.key}}</b></el-col>
          <el-col :span="16" class="right-part">
            <b>{{property.value}}</b>
          </el-col>
        </el-row>
      </div>

      <div class="config-setting" v-if="jobConvertedProperties.length > 0">
        <el-row class="config-title">
          <el-col :span="24"> <b>{{$t('jobRelated')}}</b></el-col>
        </el-row>
        <el-row class="config-context" v-for="(property, property_index) in jobConvertedProperties" :key="property_index">
          <el-col :span="8" class="left-part"> <b>{{property.key}}</b></el-col>
          <el-col :span="16" class="right-part">
            <b>{{property.value}}</b>
          </el-col>
        </el-row>
      </div>

      <div class="config-setting" v-if="hiveConvertedProperties.length > 0">
        <el-row class="config-title">
          <el-col :span="24"> <b>{{$t('hiveRelated')}}</b></el-col>
        </el-row>
        <el-row class="config-context" v-for="(property, property_index) in hiveConvertedProperties" :key="property_index">
          <el-col :span="8" class="left-part"> <b>{{property.key}}</b></el-col>
          <el-col :span="16" class="right-part">
            <b>{{property.value}}</b>
          </el-col>
        </el-row>
      </div>
    </div>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      properties: fromObjToArr(this.cubeDesc.desc.override_kylin_properties),
      jobConvertedProperties: [],
      hiveConvertedProperties: [],
      convertedProperties: []
    }
  },
  methods: {
    initProperty: function () {
      for (let key of Object.keys(this.cubeDesc.desc.override_kylin_properties)) {
        if (key.indexOf('kylin.job.mr.config.override.') === 0) {
          this.jobConvertedProperties.push({key: key, value: this.cubeDesc.desc.override_kylin_properties[key]})
        } else if (key.indexOf('kylin.hive.config.override.') === 0) {
          this.hiveConvertedProperties.push({key: key, value: this.cubeDesc.desc.override_kylin_properties[key]})
        } else if (key.indexOf('kap.smart.conf.aggGroup.strategy') !== 0) {
          this.convertedProperties.push({key: key, value: this.cubeDesc.desc.override_kylin_properties[key]})
        }
      }
    }
  },
  computed: {
    engineType () {
      if (+this.cubeDesc.desc.engine_type === 2 || +this.cubeDesc.desc.engine_type === 100) {
        return 'MapReduce'
      } else if (+this.cubeDesc.desc.engine_type === 4 || +this.cubeDesc.desc.engine_type === 98) {
        return 'Spark (Beta)'
      }
      return ''
    }
  },
  created: function () {
    this.initProperty()
  },
  locales: {
    'en': {tip: 'Tip', propertyTip: 'Cube level properties will overwrite configuration in kylin.prperties', cubeDefault: 'Cube default configuration', jobRelated: 'Job related configuration', hiveRelated: 'Hive related configuration'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.prperties中的属性值', cubeDefault: 'Cube 默认配置', jobRelated: 'Job 相关配置', hiveRelated: 'Hive 相关配置'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .advanced-setting-view{
    font-size: 12px;
    border-color: #393e53;
    border-right: none;
    border-top: 1px solid #393e53;
    padding: 0px;
    background: #292b38;
    .left-part {
      white-space: nowrap;
      overflow: hidden;
      border-right: 1px solid #393e53;
    }
    .el-row {
      border-left: 1px solid #393e53;
      border-bottom: 1px solid #393e53;
      border-right: 1px solid #393e53;
      height: 30px;
    }
    .config-setting {
      margin-top: 15px;
    }
    .config-title {
      background: #2f3142;
      border-top: 1px solid #393e53;
      b {
        color: #d0d3df;
        margin-left: 10px;
        float: left;
      }
    }
    .config-context {
      .left-part {
        b {
          float: left;
          color: #9095ab;
          margin-left: 30px;
          font-weight: normal
        }
      }
      .right-part {
        b {
          float: left;
          color: #fff;
          margin-left: 30px;
          font-weight: normal
        }
      }
    }
  }
</style>
