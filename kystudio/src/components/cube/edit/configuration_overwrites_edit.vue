<template>
<div class="box-card">
  <el-row>
    <el-col :span="18">
      <el-row v-for="property in getDefaultConfig" :gutter="20">
        <el-col :span="1"><el-checkbox v-model="property.selected" v-if="property.isDefault"></el-checkbox></el-col>
        <el-col :span="8">
          <el-input v-model="property.key" :disabled="!property.selected"></el-input>
        </el-col>
        <el-col :span="8">
          <el-input  v-model="property.value" :disabled="!property.selected"></el-input>
        </el-col>
        <el-col :span="1">
          <el-button type="primary" icon="minus" size="small" v-if="!property.isDefault" @click.native="removeProperty(index)"></el-button>
        </el-col>        
      </el-row>
      <el-button type="primary" icon="plus" @click.native="addNewProperty"></el-button>
    </el-col>
    <el-col :span="6">{{$t('tip')}}
      <el-row >
        <el-col :span="24">{{$t('propertyTip')}}</el-col>
      </el-row>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
import { mapActions } from 'vuex'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      convertedProperties: [],
      properties: fromObjToArr(this.cubeDesc.override_kylin_properties)
    }
  },
  methods: {
    ...mapActions({
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    }),
    addNewProperty: function () {
      console.log(this.cubeDesc.override_kylin_properties)
    }
  },
  created: function () {
    this.loadConfig()
  },
  computed: {
    getDefaultConfig: function () {
      let _this = this
      let arr = []
      let defaultConfigs = fromObjToArr(this.$store.state.config.defaultConfig)
      defaultConfigs.forEach(function (config) {
        arr.push({selected: false, key: config.key, value: config.value, isDefault: true})
      })
      _this.properties.forEach(function (property) {
        if (defaultConfigs.indexOf(property) === -1) {
          arr.push({selected: true, key: property.key, value: property.value, isDefault: false})
        } else {
          let index = defaultConfigs.indexOf(property)
          _this.$set(arr[index], 'selected', true)
        }
      })
      return arr
    }
  },
  locales: {
    'en': {tip: 'Tip', propertyTip: 'Cube level properties will overwrite configuration in kylin.prperties'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.prperties中的属性值'}
  }
}
</script>
<style scoped="">

</style>
