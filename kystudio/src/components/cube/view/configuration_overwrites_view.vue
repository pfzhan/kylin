<template>
    <div class="advanced-setting-view ksd-mt-20">
    <p class="config-title ksd-lineheight-14">Cubing Engine</p>
    <table class="ksd-table ksd-mt-10">
      <tr class="ksd-tr">
        <th style="width: 110px;">
          Engine Type:
        </th>
        <td>        
         {{engineType}}
        </td>
      </tr>
    </table>

    <p class="config-title ksd-lineheight-14 ksd-mt-20" v-if="convertedProperties.length > 0">{{$t('cubeDefault')}}</p>
    <table class="ksd-table ksd-mt-10" v-if="convertedProperties.length > 0">
      <tr class="ksd-tr" v-for="(property, property_index) in convertedProperties" :key="property_index">
        <th>
          {{property.key}} :
        </th>
        <td>        
         {{property.value}}
        </td>
      </tr>
    </table>

    <p class="config-title ksd-lineheight-14 ksd-mt-20" v-if="jobConvertedProperties.length > 0">{{$t('jobRelated')}}</p>
    <table class="ksd-table ksd-mt-10" v-if="jobConvertedProperties.length > 0">
      <tr class="ksd-tr" v-for="(property, property_index) in jobConvertedProperties" :key="property_index">
        <th>
          {{property.key}} :
        </th>
        <td>        
         {{property.value}}
        </td>
      </tr>
    </table>

    <p class="config-title ksd-lineheight-14 ksd-mt-20" v-if="hiveConvertedProperties.length > 0">{{$t('hiveRelated')}}</p>
    <table class="ksd-table ksd-mt-10" v-if="hiveConvertedProperties.length > 0">
      <tr class="ksd-tr" v-for="(property, property_index) in hiveConvertedProperties" :key="property_index">
        <th>
          {{property.key}} :
        </th>
        <td>        
         {{property.value}}
        </td>
      </tr>
    </table>
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
  @import '../../../assets/styles/variables.less';
  .advanced-setting-view{
    .config-title {
      color: @text-title-color;
      font-weight: bold;
    }
    tr th {
      width: 320px;
    }
  }
</style>
