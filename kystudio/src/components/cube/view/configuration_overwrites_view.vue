<template>
  <el-card class="box-card" id="configuration_overwrites_view">
    <el-row>
      <el-col :span="18">
        <el-row v-for="(property, property_index) in properties" :key="property_index">
          <el-col :span="9">{{property.key}}</el-col>
          <el-col :span="9">{{property.value}}</el-col>
        </el-row>
      </el-col>
      <el-col :span="6">{{$t('tip')}}
        <el-row >
          <el-col :span="24">{{$t('propertyTip')}}</el-col>
        </el-row>
      </el-col>
    </el-row>
  </el-card>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      properties: fromObjToArr(this.cubeDesc.desc.override_kylin_properties)
    }
  },
  locales: {
    'en': {tip: 'Tip', propertyTip: 'Cube level properties will overwrite configuration in kylin.prperties'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.prperties中的属性值'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  #configuration_overwrites_view{
    border-color: @grey-color!important;
    padding: 20px;
  }
</style>
