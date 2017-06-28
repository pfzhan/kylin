<template>
  <el-card class="box-card" id="configuration_overwrites_view">
    <el-row v-if="properties.length > 1">
      <el-col :span="18" style="border-right: 1px solid #393e53;">
        <el-row v-for="(property, property_index) in properties" :key="property_index" style="border-top: 1px solid #393e53;padding: 5px 0 5px 20px;">
          <el-col :span="10" style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{{property.key}}</el-col>
          <el-col :span="13" :offset="1">{{property.value}}</el-col>
        </el-row>
        <el-row v-for="(property, property_index) in properties" :key="property_index" style="border-top: 1px solid #393e53;padding: 5px 0 5px 20px;">
          <el-col :span="10" style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{{property.key}}</el-col>
          <el-col :span="13" :offset="1">{{property.value}}</el-col>
        </el-row>
      </el-col>
      <el-col style="height: 100%;padding-left: 20px;padding-right: 20px;" :span="6">{{$t('tip')}}
        <el-row >
          <el-col :span="24">{{$t('propertyTip')}}</el-col>
        </el-row>
      </el-col>
    </el-row>
    <el-row v-else>
      <el-col :span="18">
        <el-row v-for="(property, property_index) in properties" :key="property_index" style="border-top: 1px solid #393e53;padding: 5px 0 5px 20px;">
          <el-col :span="5" style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{{property.key}}</el-col>
          <el-col :span="18" :offset="1">{{property.value}}</el-col>
        </el-row>
      </el-col>
      <el-col style="height: 100%;padding-left: 20px;padding-right: 20px;border-left: 1px solid #393e53;" :span="6">{{$t('tip')}}
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
    padding: 0px;
    background: #292b38;
  }
</style>
